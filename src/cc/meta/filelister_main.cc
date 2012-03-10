//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: filelister_main.cc 1552 2011-01-06 22:21:54Z sriramr $
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
using namespace KFS;

static int restoreCheckpoint(const string &lockfn);
static int replayLogs();

int main(int argc, char **argv)
{
    // use options: -l for logdir -c for checkpoint dir
    char optchar;
    bool help = false;
    string logdir, cpdir, pathFn;
    // the name of the lock file that is used to synchronize between
    // the various tools that load the checkpoint file.
    string lockfn;
    int status;

    KFS::MsgLogger::Init(NULL);
    KFS::MsgLogger::SetLevel(MsgLogger::kLogLevelINFO);

    while ((optchar = getopt(argc, argv, "hl:c:f:L:")) != -1) {
        switch (optchar) {
            case 'L':
                lockfn = optarg;
                break;
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
        cout << "Usage: " << argv[0] << " [-L <lockfile>] [-l <logdir>] [-c <cpdir>] [-f <output fn>]"
             << endl;
        exit(-1);
    }

    metatree.disableFidToPathname();

    logger_setup_paths(logdir);
    checkpointer_setup_paths(cpdir);

    status = restoreCheckpoint(lockfn);
    if (status != 0)
        panic("restore checkpoint failed!", false);
    status = replayLogs();

    ofstream ofs;

    ofs.open(pathFn.c_str());
    metatree.listPaths(ofs);
    exit(0);
}

static int restoreCheckpoint(const string &lockfn)
{
    int status = 0;

    if (lockfn != "")
        acquire_lockfile(lockfn, 10);

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
