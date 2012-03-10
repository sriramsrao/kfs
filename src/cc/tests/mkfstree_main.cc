//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: mkfstree_main.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2008/05/09
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
// \brief This program evaluates the memory use on the metaserver by
// creating a directory hierarchy.  For input, provide a file that
// lists the directory hierarchy to be created with the path to a
// complete file, one per line.
//
//----------------------------------------------------------------------------

#include <iostream>    
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <fstream>
#include "libkfsClient/KfsClient.h"

using std::ios_base;
using std::cout;
using std::endl;
using std::ifstream;
using std::string;

using namespace KFS;

KfsClientPtr gKfsClient;
int
main(int argc, char **argv)
{
    char optchar;
    string kfspathname = "";
    char *kfsPropsFile = NULL;
    char *dataFile = NULL;
    bool help = false;
    ifstream ifs;

    while ((optchar = getopt(argc, argv, "f:p:")) != -1) {
        switch (optchar) {
            case 'f':
                dataFile = optarg;
                break;
            case 'p':
                kfsPropsFile = optarg;
                break;
            default:
                cout << "Unrecognized flag: " << optchar << endl;
                help = true;
                break;
        }
    }

    if (help || (kfsPropsFile == NULL) || (dataFile == NULL)) {
        cout << "Usage: " << argv[0] << " -p <Kfs Client properties file> "
             << " -f <data file>" << endl;
        exit(0);
    }

    ifs.open(dataFile, ios_base::in);
    if (!ifs) {
        cout << "Unable to open: " << dataFile << endl;
        exit(-1);
    }

    gKfsClient = getKfsClientFactory()->GetClient(kfsPropsFile);
    if (!gKfsClient) {
        cout << "kfs client failed to initialize...exiting" << endl;
        exit(-1);
    }

    int MAX_LINE_LENGTH = 32768;
    char *line = new char[MAX_LINE_LENGTH];
    int dirFd, fd;
    int count = 0;

    while (1) {
        ifs.getline(line, MAX_LINE_LENGTH);
        if (ifs.eof())
            break;
        kfspathname = line;
        string kfsdirname, kfsfilename;
        string::size_type slash = kfspathname.rfind('/');
    
        if (slash == string::npos) {
            cout << "Bad kfs path: " << kfsdirname << endl;
            exit(-1);
        }

        kfsdirname.assign(kfspathname, 0, slash);
        kfsfilename.assign(kfspathname, slash + 1, kfspathname.size());
        if (kfsfilename.rfind(".crc") != string::npos)
            continue;
        ++count;
        if ((count % 10000) == 0)
            cout << "Done with " << count << " non-crc files" << endl;

        dirFd = gKfsClient->Mkdirs(kfsdirname.c_str());
        if (dirFd < 0) {
            cout << "Mkdir failed: " << dirFd << endl;
            break;
        }
        fd = gKfsClient->Create(kfspathname.c_str());
        if (fd < 0) {
            cout << "Create failed for path: " << kfspathname << " error: " << fd << endl;
            break;
        }
        gKfsClient->Close(fd);
        gKfsClient->Close(dirFd);
    }
}
