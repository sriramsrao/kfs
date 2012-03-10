//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: KfsSample_main.cc 353 2009-05-28 21:54:49Z sjakub $ 
//
// Created 2007/09/05
// Author: Sriram Rao (Kosmix Corp.) 
//
// Copyright 2007 Kosmix Corp.
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
// \brief A sample C++ program that demonstrates KFS APIs.  To run
// this program, you need:
//   - link with libkfsClient.a
//   - a KFS deployment
//
//----------------------------------------------------------------------------

#include <iostream>    
#include <fstream>
#include <cerrno>

extern "C" {
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
#include <fcntl.h>
}

#include "libkfsClient/KfsClient.h"

using std::cout;
using std::endl;
using std::ifstream;
using namespace KFS;

KfsClientPtr gKfsClient;

// Make the directory hierarchy in KFS defined by path.
bool doMkdirs(const char *path);

// generate sample data for testing
void generateData(char *buf, int numBytes);

int
main(int argc, char **argv)
{
    string serverHost = "";
    int port = -1;
    bool help = false;
    char optchar;
    KfsFileStat statInfo;

    while ((optchar = getopt(argc, argv, "hp:s:")) != -1) {
        switch (optchar) {
            case 'p':
                port = atoi(optarg);
                break;
            case 's':
                serverHost = optarg;
                break;
            case 'h':
                help = true;
                break;
            default:
                cout << "Unrecognized flag " << optchar << endl;
                help = true;
                break;
        }
    }

    if (help || (serverHost == "") || (port < 0)) {
        cout << "Usage: " << argv[0] << " -s <meta server name> -p <port> "
             << endl;
        exit(0);
    }

    //
    // Get a handle to the KFS client object.  This is our entry into
    // the KFS namespace.
    //
    gKfsClient = getKfsClientFactory()->GetClient(serverHost, port);
    if (!gKfsClient) {
        cout << "kfs client failed to initialize...exiting" << endl;
        exit(-1);
    }

    // Now that the client is init'ed, do some file-I/O

    // Make /ctest
    string basedir = "ctest";
    if (!doMkdirs(basedir)) {
        exit(0);
    }

    // What we just created better be a directory
    if (!gKfsClient->IsDirectory(basedir)) {
        cout << "KFS doesn't think: " << basedir << " is a dir!" << endl;
        exit(-1);
    }

    // Create a simple file with default replication (at most 3)
    string tempFn(basedir + "/foo.1");
    int fd;

    // fd is our file-handle to the file we are creating; this
    // file handle should be used in subsequent I/O calls on
    // the file.
    if ((fd = gKfsClient->Create(tempFn)) < 0) {
        cout << "Create failed: " << ErrorCodeToStr(fd) << endl;
        exit(-1);
    }
            
    // Get the directory listings
    vector<string> entries;
    int res;

    if ((res = gKfsClient->Readdir(basedir, entries)) < 0) {
        cout << "Readdir failed! " << ErrorCodeToStr(res) << endl;
        exit(-1);
    }

    cout << "Read dir returned: ";
    for (uint32_t i = 0; i < entries.size(); i++) {
        cout << entries[i] << endl;
    }

    // write something to the file
    int numBytes = 2048;
    char *dataBuf = new char[numBytes];
            
    generateData(dataBuf, numBytes);
          
    // make a copy and write out using the copy; we keep the original
    // so we can validate what we get back is what we wrote.
    char *d1 = new char[numBytes];
    memcpy(d1, dataBuf, numBytes);

    res = gKfsClient->Write(fd, dataBuf, numBytes);
    if (res != numBytes) {
        cout << "Was able to write only: " << res << " instead of " << numBytes << endl;
    }

    // flush out the changes
    gKfsClient->Sync(fd);

    // Close the file-handle
    gKfsClient->Close(fd);
            
    // Determine the file-size
    gKfsClient->Stat(tempFn, statInfo);
    kfsOff_t sz = statInfo.size;

    if (sz != numBytes) {
        cout << "KFS thinks the file's size is: " << sz << " instead of " << numBytes << endl;
    }

    // rename the file
    string npath = basedir + "/foo.2";
    gKfsClient->Rename(tempFn, npath);

    if (gKfsClient->Exists(tempFn)) {
        cout << tempFn << " still exists after rename!" << endl;
        exit(-1);
    }

    // Re-create the file and try a rename that should fail...
    int fd1 = gKfsClient->Create(tempFn);
    
    if (!gKfsClient->Exists(tempFn)) {
        cout << " After rec-create..., " << tempFn << " doesn't exist!" << endl;
        exit(-1);
    }

    gKfsClient->Close(fd1);

    // try to rename and don't allow overwrite
    if (gKfsClient->Rename(npath, tempFn, false) == 0) {
        cout << "Rename  with overwrite disabled succeeded...error!" << endl;
        exit(-1);
    }

    // Remove the file
    gKfsClient->Remove(tempFn);

    // Re-open the file
    if ((fd = gKfsClient->Open(npath, O_RDWR)) < 0) {
        cout << "Open on : " << npath << " failed!" << ErrorCodeToStr(fd) << endl;
        exit(-1);
    }
            
    // read some bytes
    res = gKfsClient->Read(fd, d1, 128);

    // Verify what we read matches what we wrote
    for (int i = 0; i < 128; i++) {
        if (dataBuf[i] != d1[i]) {
            cout << "Data mismatch at : " << i << endl;
        }
    }
            
    // seek to offset 40
    gKfsClient->Seek(fd, 40);

    // Seek and verify that we are we think we are
    sz = gKfsClient->Tell(fd);
    if (sz != 40) {
        cout << "After seek, we are at: " << sz << " should be at 40 " << endl;
    }

    gKfsClient->Close(fd);

    // remove the file
    gKfsClient->Remove(npath);

    // remove the dir
    if ((res = gKfsClient->Rmdir(basedir)) < 0) {
        cout << "Unable to remove: " << basedir << " : " << ErrorCodeToStr(res) << endl;
    } else {
        cout << "Testts passed!" << endl;
    }
}

bool
doMkdirs(const char *path)
{
    int res;

    res = gKfsClient->Mkdirs((char *) path);
    if ((res < 0) && (res != -EEXIST)) {
        cout << "Mkdir failed: " << ErrorCodeToStr(res) << endl;
        return false;
    }
    return true;
}


void generateData(char *buf, int numBytes)
{
    int i;
    
    srand(100);
    for (i = 0; i < numBytes; i++) {
        buf[i] = (char) ('a' + (rand() % 26));
    }
}
