//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kfsfileenum_main.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2008/05/05
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
// \brief Debug utility to list out where the chunks of a file are.
//----------------------------------------------------------------------------

extern "C" {
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
};

#include <iostream>
#include <string>
using std::string;
using std::cout;
using std::endl;
#include "common/log.h"
#include "libkfsClient/KfsClient.h"

using namespace KFS;
KfsClientPtr gKfsClient;

int
main(int argc, char **argv)
{
    bool help = false;
    const char *server = NULL;
    const char *filename = NULL;
    int port = -1, retval;
    char optchar;
    bool verboseLogging = false;

    KFS::MsgLogger::Init(NULL);

    while ((optchar = getopt(argc, argv, "hs:p:f:v")) != -1) {
        switch (optchar) {
            case 's':
                server = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 'f':
                filename = optarg;
                break;
            case 'h':
                help = true;
                break;
            case 'v':
                verboseLogging = true;
                break;
            default:
                KFS_LOG_VA_ERROR("Unrecognized flag %c", optchar);
                help = true;
                break;
        }
    }

    if (help || (server == NULL) || (port < 0) || (filename == NULL)) {
        cout << "Usage: " << argv[0] << " -s <server name> -p <port> -f <path> {-v}" 
             << endl;
        exit(-1);
    }

    gKfsClient = getKfsClientFactory()->GetClient(server, port);
    if (!gKfsClient) {
        cout << "kfs client failed to initialize...exiting" << endl;
        exit(-1);
    }

    if (verboseLogging) {
        KFS::MsgLogger::SetLevel(KFS::MsgLogger::kLogLevelDEBUG);
    } else {
        KFS::MsgLogger::SetLevel(KFS::MsgLogger::kLogLevelINFO);
    } 

    retval = gKfsClient->EnumerateBlocks(filename);
    exit(retval);
}


