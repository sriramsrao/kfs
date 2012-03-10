//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: restoremtime_main.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2008/06/18
//
//
// Copyright 2008 Quantcast Corp.
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
// \brief Given a checkpoint, write out the list of files in the tree and their
// metadata in a "ls -l" format.
// 
//----------------------------------------------------------------------------

#include "kfstree.h"
#include "logger.h"
#include "checkpoint.h"
#include "restore.h"
#include "replay.h"
#include "util.h"
#include "common/log.h"

#include <sys/stat.h>
#include <iostream>
#include <cassert>

using std::cout;
using std::endl;
using std::istringstream;
using namespace KFS;

static int restoreCheckpoint();
static int replayLogs();
static void restoreMtime(string pathname);

int main(int argc, char **argv)
{
    // use options: -l for logdir -c for checkpoint dir
    char optchar;
    bool help = false;
    string logdir, cpdir, pathFn;
    int status;

    KFS::MsgLogger::Init(NULL);
    KFS::MsgLogger::SetLevel(MsgLogger::kLogLevelINFO);

    while ((optchar = getopt(argc, argv, "hl:c:f:")) != -1) {
        switch (optchar) {
            case 'l': 
                logdir = optarg;
                break;
            case 'c':
                cpdir = optarg;
                break;
            case 'f':
                pathFn = optarg;
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
        cout << "Usage: " << argv[0] << " [-l <logdir>] [-c <cpdir>] [-f <file list>]"
             << endl;
        exit(-1);
    }

    metatree.disableFidToPathname();

    logger_setup_paths(logdir);
    checkpointer_setup_paths(cpdir);
    status = restoreCheckpoint();
    if (status != 0)
        panic("restore checkpoint failed!", false);
    status = replayLogs();

    restoreMtime(pathFn);
    cp.do_CP();
    exit(0);
}

void restoreMtime(string pathname)
{
    ifstream ifs;

    int lineno = 0;
    const int MAXLINE = 1024;
    char line[MAXLINE];
    struct timeval mtime;
    MetaFattr *fa;
    string path;

    ifs.open(pathname.c_str());
    while (!ifs.eof()) {
        ++lineno;
        ifs.getline(line, MAXLINE);
        
        istringstream is(line);
        is >> path;
        
        fa = metatree.lookupPath(ROOTFID, path);
        if (fa == NULL) {
            cout << lineno << " : Skipping line: " << path << endl;
            continue;
        }

        is >> mtime.tv_sec;
        is >> mtime.tv_usec;
        fa->mtime = mtime;
    }
}

static int restoreCheckpoint()
{
    int status = 0;

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
