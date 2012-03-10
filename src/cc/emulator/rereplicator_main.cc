//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: rereplicator_main.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2008/08/27
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
// \brief Driver program to run the metaserver in emulator mode and
// work out a plan for increasing replication for all blocks in the FS.
//
//----------------------------------------------------------------------------

#include "LayoutEmulator.h"
#include "EmulatorSetup.h"

#include "meta/util.h"
#include "common/log.h"

#include <unistd.h>

using std::string;
using std::cout;
using std::endl;

using namespace KFS;

int
main(int argc, char **argv)
{
    KFS::MsgLogger::Init(NULL);
    string logdir, cpdir, networkFn, chunkmapFn;
    string rebalancePlanFn;
    char optchar;
    bool help = false;
    int status;
    int16_t minReplicasPerFile = 1;

    while ((optchar = getopt(argc, argv, "c:l:n:b:r:m:h")) != -1) {
        switch (optchar) {
            case 'l': 
                logdir = optarg;
                break;
            case 'c':
                cpdir = optarg;
                break;
            case 'n':
                networkFn = optarg;
                break;
            case 'b':
                chunkmapFn = optarg;
                break;
            case 'r':
                rebalancePlanFn = optarg;
                break;
            case 'm':
                minReplicasPerFile = atoi(optarg);
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
        cout << "Usage: " << argv[0] << " [-l <logdir>] [-c <cpdir>] [-n <network def>] "
             << "[-b <chunkmap file>] [-r <rebalance plan file>] " 
             << "[-m <min replicas per file>] " << endl;
        exit(-1);
    }

    EmulatorSetup(logdir, cpdir, networkFn, chunkmapFn, minReplicasPerFile, true);

    status = gLayoutEmulator.SetRebalancePlanOutFile(rebalancePlanFn);
    if (status < 0)
        exit(-1);
    
    MsgLogger::SetLevel(MsgLogger::kLogLevelINFO);

    gLayoutEmulator.ToggleRebalancing(false);

    gLayoutEmulator.PrintChunkserverBlockCount();
    // now the testing can start...

    int ndone = 0;
    while (1) {
        ndone = gLayoutEmulator.BuildRebalancePlan();
        if (ndone == 0)
            break;
        
    }

    gLayoutEmulator.DumpChunkToServerMap(".");
    gLayoutEmulator.PrintChunkserverBlockCount();
}

