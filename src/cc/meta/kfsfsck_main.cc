//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kfsfsck_main.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2008/06/18
//
//
// Copyright 2010 Quantcast Corp.
//
// This file is part of Kosmos File System (KFS).
//
// Licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// \brief An online fsck: download the list of fileid's that are
// missing blocks; then use the checkpoint/logs to print out the
// pathname (i.e., inode # -> pathname)
// 
//----------------------------------------------------------------------------

#include "kfstree.h"
#include "logger.h"
#include "checkpoint.h"
#include "restore.h"
#include "replay.h"
#include "util.h"
#include "common/log.h"
#include "libkfsIO/TcpSocket.h"

#include <sys/stat.h>
#include <iostream>
#include <sstream>
#include <cassert>
#include <boost/scoped_array.hpp>

using std::cout;
using std::endl;
using std::set;
using std::istringstream;
using std::ostringstream;

using namespace KFS;

static int restoreCheckpoint(const string &lockfn);
static int replayLogs();

static void getFsckInfo(string metahost, int metaport, 
                        set<fid_t> &lostFiles, set<fid_t> &endangeredFiles);
static void
getFileIds(set<fid_t> &fileids, Properties &prop, string key, string value);

static int
getResponse(char *buf, int bufSize, int *delims, TcpSocket *sock);

const string KFS_VERSION_STR = "KFS/1.0";
int CMD_BUF_SIZE = 4096;

int main(int argc, char **argv)
{
    // use options: -l for logdir -c for checkpoint dir
    char optchar;
    bool help = false;
    string logdir, cpdir;
    string metahost;
    string lockFn;
    int metaport = -1;
    int status;

    KFS::MsgLogger::Init(NULL);
    KFS::MsgLogger::SetLevel(MsgLogger::kLogLevelINFO);

    while ((optchar = getopt(argc, argv, "hl:c:m:p:L:")) != -1) {
        switch (optchar) {
            case 'L':
                lockFn = optarg;
                break;
            case 'l': 
                logdir = optarg;
                break;
            case 'c':
                cpdir = optarg;
                break;
            case 'm':
                metahost = optarg;
                break;
            case 'p':
                metaport = atoi(optarg);
                break;
            case 'h':
                help = true;
                break;
            default:
                KFS_LOG_VA_ERROR("Unrecognized flag %c", optchar);
                help = true;
                break;
        }
    }

    if (help) {
        cout << "Usage: " << argv[0] << " [-L <lockfile>] [-l <logdir>] [-c <cpdir>] [-m <metahost>] [-p <metaport>] "
             << endl;
        exit(-1);
    }

    set<fid_t> lostFiles, endangeredFiles;

    getFsckInfo(metahost, metaport, lostFiles, endangeredFiles);

    if (lostFiles.empty() && endangeredFiles.empty()) {
        cout << "Filesystem is HEALTHY" << endl;
        exit(0);
    }

    metatree.disableFidToPathname();

    logger_setup_paths(logdir);
    checkpointer_setup_paths(cpdir);
    status = restoreCheckpoint(lockFn);
    if (status != 0)
        panic("restore checkpoint failed!", false);
    status = replayLogs();

    int numLostFiles = 0;
    if (!lostFiles.empty()) {
        cout << "Lost files: " << endl;
        // Files are truly lost when we got valid paths for them;
        numLostFiles = metatree.listPaths(std::cout, lostFiles);
        cout << "Total # of lost files (" << numLostFiles << "): " << endl;
    }
    
    if (!endangeredFiles.empty()) {
        cout << "Endangered files (" << endangeredFiles.size() << "): " << endl;
        metatree.listPaths(std::cout, endangeredFiles);
    }
    
    cout << "Filesystem is " << (numLostFiles == 0 ? "HEALTHY" : "CORRUPT") << endl;

    exit(0);
}

static int restoreCheckpoint(const string &lockFn)
{
    int status = 0;
    
    if (lockFn != "")
        acquire_lockfile(lockFn, 10);

    if (file_exists(LASTCP)) {
        Restorer r;
        status = r.rebuild(LASTCP) ? 0 : -EIO;
    } else {
        status = metatree.new_tree();
    }
    return status;
}

static int replayLogs()
{
    int status, lastlog = -1, lognum;
    ino_t lastino;
    struct stat buf;

    // we want to replay log files that are "complete"---those that
    // won't ever be written to again.  so, starting with the log
    // associated with the CP, replay all the log files upto the
    // "last" log file.

    // get the inode # for the last file
    status = stat(LASTLOG.c_str(), &buf);
    if (status < 0)
        // no "last" log file; so nothing to do
        return status;

    // get the inode # for the log file that corresponds to last and
    // then replay those
    lastino = buf.st_ino;

    for (lognum = replayer.logno(); ;lognum++) {
        string logfn = oplog.logfile(lognum);

        status = stat(logfn.c_str(), &buf);
        if (status < 0)
            break;

        if (buf.st_ino == lastino) {
            lastlog = lognum;
            assert(buf.st_nlink == 2);
            break;
        }
    }

    
    if (lastlog == replayer.logno()) {
        cout << "No new logs since the last log; so, skipping checkpoint" << endl;
        return -2;
    }

    if (lastlog < 0)
        return -1;

    cout << "Replaying logs from log." << replayer.logno() << " ... log." << lastlog << endl;

    for (lognum = replayer.logno(); lognum <= lastlog;lognum++) {
        string logfn = oplog.logfile(lognum);

        replayer.openlog(logfn);

        status = replayer.playlog();
        if (status != 0)
            panic("log replay failed", false);
    }

    oplog.setLog(lognum);

    cout << "Replay of logs finished" << endl;

    return status;
}

void getFsckInfo(string metahost, int metaport, 
                 set<fid_t> &lostFiles, set<fid_t> &endangeredFiles)
{
    TcpSocket sock;
    ServerLocation loc(metahost, metaport);
    int ret, len;
    ostringstream os;
    Properties prop;
    boost::scoped_array<char> buf;

    ret = sock.Connect(loc);
    
    if (ret < 0) {
        cout << "Unable to connect to metaserver...exiting" << endl;
        exit(-1);
    }

    os << "FSCK\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Cseq: " << 1 << "\r\n\r\n";

    ret = sock.DoSynchSend(os.str().c_str(), os.str().length());
    if (ret <= 0) {
        cout << "Unable to send fsck rpc to metaserver...exiting" << endl;
        exit(-1);
    }
    
    // get the response and get the data
    buf.reset(new char[CMD_BUF_SIZE]);
    int nread = getResponse(buf.get(), CMD_BUF_SIZE, &len, &sock);

    if (nread <= 0) {
        cout << "Unable to get fsck rpc reply from metaserver...exiting" << endl;
        exit(-1);
    }
    istringstream ist(buf.get());
    const char kSeparator = ':';
    struct timeval timeout = {300, 0};

    prop.loadProperties(ist, kSeparator, false);

    int contentLength = prop.getValue("Content-length", 0);
    if (contentLength == 0)
        return;
    
    // get the content length
    buf.reset(new char[contentLength + 1]);
    nread = sock.DoSynchRecv(buf.get(), contentLength, timeout);
    if (nread <= 0) {
        cout << "Unable to get fsck rpc reply from metaserver...exiting" << endl;
        exit(-1);
    }

    istringstream contentStream(buf.get());

    prop.loadProperties(contentStream, kSeparator, false);
    getFileIds(endangeredFiles, prop, "Num endangered blocks", "Endangered files");
    getFileIds(lostFiles, prop, "Num lost blocks", "Lost files");
}

void
getFileIds(set<fid_t> &fileids, Properties &prop, string key, string value)
{
    int blkCount = prop.getValue(key.c_str(), 0);
    string ids = prop.getValue(value.c_str(), "");
    istringstream ist(ids);
    for (int i = 0; i < blkCount; i++) {
        fid_t fid;
        ist >> fid;
        fileids.insert(fid);
    }
}

static int
getResponse(char *buf, int bufSize,
            int *delims,
            TcpSocket *sock)
{
    int nread;
    int i;

    *delims = -1;
    while (1) {
        struct timeval timeout;

        timeout.tv_sec = 300;
        timeout.tv_usec = 0;

        nread = sock->DoSynchPeek(buf, bufSize, timeout);
        if (nread <= 0)
            return nread;
        for (i = 4; i <= nread; i++) {
            if (i < 4)
                break;
            if ((buf[i - 3] == '\r') &&
                (buf[i - 2] == '\n') &&
                (buf[i - 1] == '\r') &&
                (buf[i] == '\n')) {
                // valid stuff is from 0..i; so, length of resulting
                // string is i+1.
                memset(buf, '\0', bufSize);
                *delims = (i + 1);
                nread = sock->Recv(buf, *delims);
                return nread;
            }
        }
    }
    return -1;
}
