//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: EmulatorSetup.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2008/08/29
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
//
// \brief Code to setup the emulator---read in the logs/checkpoint/..
//----------------------------------------------------------------------------

#include "LayoutEmulator.h"
#include "EmulatorSetup.h"

#include "meta/kfstree.h"
#include "meta/checkpoint.h"
#include "meta/replay.h"
#include "meta/restore.h"
#include "meta/logger.h"
#include "meta/util.h"
#include "common/log.h"

#include <string>
#include <iostream>
#include <fstream>
#include <sstream>

using std::string;
using std::cout;
using std::endl;
using std::istringstream;

using namespace KFS;

static int16_t desiredMinReplicasPerFile = 1;
static int
restoreCheckpoint()
{
    string logfile;
    int status;
    if (file_exists(LASTCP)) {
        Restorer r;
        status = r.rebuild(LASTCP, desiredMinReplicasPerFile) ? 0 : -EIO;
        // gLayoutEmulator.InitRecoveryStartTime();
    } else {
        status = metatree.new_tree();
    }
    return status;
}

static
int replayLogs()
{
    int status = replayer.playAllLogs();
    return status;
}

void
KFS::EmulatorSetup(string &logdir, string &cpdir, string &networkFn, 
                   string &chunkmapFn, int16_t minReplicasPerFile,
                   bool addChunksToReplicationChecker)
{
    int status;

    cout << "Restoring checkpoint..." << endl;

    desiredMinReplicasPerFile = minReplicasPerFile;

    logger_setup_paths(logdir);
    checkpointer_setup_paths(cpdir);
    status = restoreCheckpoint();
    if (status != 0)
        panic("restore checkpoint failed!", false);

    cout << "Replaying logs..." << endl;

    status = replayLogs();
    if (status != 0)
        panic("replay logs failed!", false);

    cout << "Reading network defn..." << endl;

    gLayoutEmulator.ReadNetworkDefn(networkFn);

    cout << "Loading chunkmap..." << endl;
    status = gLayoutEmulator.LoadChunkmap(chunkmapFn, addChunksToReplicationChecker);
    if (status != 0)
        panic("Unable to load chunkmap", false);
}
