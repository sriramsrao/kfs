//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: replicachecker_main.cc 1552 2011-01-06 22:21:54Z sriramr $
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
// \brief Read in a network map, a block location map and verify that
// the three copies of a replica are on three different racks.
//
//----------------------------------------------------------------------------

#include "LayoutEmulator.h"
#include "EmulatorSetup.h"

#include "common/log.h"
using std::string;
using std::cout;
using std::endl;

using namespace KFS;

int
main(int argc, char **argv)
{
    KFS::MsgLogger::Init(NULL);
    string logdir, cpdir, networkFn, chunkmapFn;
    char optchar;
    bool help = false;
    int status;
    bool checkSize = false;
    bool verbose = false;

    while ((optchar = getopt(argc, argv, "svc:l:n:b:r:h")) != -1) {
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
            case 'h':
                help = true;
                break;
            case 's':
				checkSize = true;
				break;
            case 'v':
            	verbose = true;
            	break;
            default:
                KFS_LOG_VA_ERROR("Unrecognized flag %c", optchar);
                help = true;
                break;
        }
    }

    if (help) {
        cout << "Usage: " << argv[0] << " [-l <logdir>] [-c <cpdir>] [-n <network def>] [-b <chunkmap file>] [-s] [-v]"
             << endl;
        cout << "      -l : meta log directory"<<endl;
        cout << "      -c : meta checkpoint directory"<<endl;
        cout << "      -n : network definition file"<<endl;
        cout << "      -b : chunk map dump file"<<endl;
        cout << "      -s : match replica sizes"<<endl;
        cout << "      -v : verbose "<<endl;

        exit(-1);
    }

    EmulatorSetup(logdir, cpdir, networkFn, chunkmapFn);

    MsgLogger::SetLevel(MsgLogger::kLogLevelINFO);

    //cout << "Checking that the replicas are on three different racks" << endl;

    status = gLayoutEmulator.VerifyRackAwareReplication(checkSize, verbose);
    if (status == 0) {
        //cout << "Pass: For each chunk, the replicas are on  different racks" << endl;
        return 0;
    }

    //cout << "Failure: For " << status << "  chunks, replicas are on same rack" << endl;
    return -1;
}
