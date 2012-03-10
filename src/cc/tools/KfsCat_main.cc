//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: KfsCat_main.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2006/10/28
//
// Copyright 2008 Quantcast Corp.
// Copyright 2006-2008 Kosmix Corp.
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
// \brief Program that behaves like cat...: 
// Kfscat -p <kfsConfig file> [filename1...n]
// and output the files in the order of appearance to stdout.
//
//----------------------------------------------------------------------------

#include <iostream>    
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <fstream>
#include "libkfsClient/KfsClient.h"

using std::cout;
using std::endl;
using std::ifstream;
using std::string;
using namespace KFS;

KfsClient *gKfsClient;
static ssize_t DoCat(const char *pahtname);

int
main(int argc, char **argv)
{
    string serverHost = "";
    int port = -1;
    bool help = false;
    bool verboseLogging = false;
    char optchar;

    KFS::MsgLogger::Init(NULL);

    while ((optchar = getopt(argc, argv, "hs:p:v")) != -1) {
        switch (optchar) {
            case 'h':
                help = true;
                break;
            case 'v':
                verboseLogging = true;
                break;
            case 's':
                serverHost = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            default:
                KFS_LOG_ERROR("Unrecognized flag %c", optchar);
                help = true;
                break;
        }
    }

    if (help || (serverHost == "") || (port < 0)) {
        cout << "Usage: " << argv[0] << " -s <meta server name> -p <port>"
             << " [filename1...n]" << endl;
        exit(0);
    }

    if (verboseLogging) {
	KFS::MsgLogger::SetLevel(KFS::MsgLogger::kLogLevelDEBUG);
    } else {
	KFS::MsgLogger::SetLevel(KFS::MsgLogger::kLogLevelINFO);
    } 

    gKfsClient = KfsClient::Instance();
    gKfsClient->Init(serverHost, port);
    if (!gKfsClient->IsInitialized()) {
        cout << "kfs client failed to initialize...exiting" << endl;
        exit(0);
    }

    int i = 1;
    while (i < argc) {
	if ((strncmp(argv[i], "-p", 2) == 0) || (strncmp(argv[i], "-s", 2) == 0)) {
            i += 2;
            continue;
        }
        // cout << "Cat'ing: " << argv[i] << endl;
        DoCat(argv[i]);
        i++;
    }
}

ssize_t
DoCat(const char *pathname)
{
    const int mByte = 1024 * 1024;
    char dataBuf[mByte];
    int res, fd;    
    size_t bytesRead = 0;
    struct stat statBuf;

    fd = gKfsClient->Open(pathname, O_RDONLY);
    if (fd < 0) {
        cout << "Open failed: " << fd << endl;
        return -ENOENT;
    }

    gKfsClient->Stat(pathname, statBuf);

    while (1) {
        res = gKfsClient->Read(fd, dataBuf, mByte);
        if (res <= 0)
            break;
        cout << dataBuf;
        bytesRead += res;
        if (bytesRead >= (size_t) statBuf.st_size)
            break;
    }
    gKfsClient->Close(fd);

    return bytesRead;
}
    
