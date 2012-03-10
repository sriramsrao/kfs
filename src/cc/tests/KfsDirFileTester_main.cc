//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: KfsDirFileTester_main.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2006/06/12
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
// \brief Program to test file and directory operations such as,
// create/remove and mkdir/rmdir/readdir.
//
//----------------------------------------------------------------------------

#include <iostream>    
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <fstream>
#include "libkfsClient/KfsClient.h"

#define MIN_FILE_SIZE 2048
#define MAX_FILE_SIZE (4096 * 8)
#define MAX_FILE_NAME_LEN 256

using std::cout;
using std::endl;
using std::ifstream;

using namespace KFS;

KfsClientPtr gKfsClient;

int doMkdir(char *dirname);
int doRmdir(char *dirname);
int doReaddir(char *dirname, uint32_t expectEntries);
int doFileCreate(char *parentDir, char *name);
int doFileRemove(char *parentDir, char *name);


int
main(int argc, char **argv)
{
    char dirname[256];
    char name[256];

    if (argc < 2) {
        cout << "Usage: " << argv[0] << " <kfs-client-properties file> " << endl;
        exit(0);
    }

    gKfsClient = getKfsClientFactory()->GetClient(argv[1]);
    if (!gKfsClient) {
        cout << "kfs client failed to initialize...exiting" << endl;
        exit(0);
    }
    
    srand(100);

    strcpy(dirname, "/dir2");
    strcpy(name, "foo1");

    if (doRmdir(dirname) < 0) {
        cout << "rmdir failed...ok..." << endl;
    }
    if (doMkdir(dirname) < 0) {
        cout << "mkdir failed..." << endl;
        // exit(0);
    }

    if (doFileCreate(dirname, name) < 0) {
        cout << "File ops create" << endl;
        exit(0);
    }

    if (doReaddir(dirname, 1) < 0) {
        cout << "readdir failed" << endl;
        exit(0);
    }

    if (doFileRemove(dirname, name) < 0) {
        cout << "File ops create" << endl;
        exit(0);
    }

    if (doRmdir(dirname) < 0) {
        cout << "rmdir failed..." << endl;
        exit(0);
    }

    // the dir better not be there...
    if (doReaddir(dirname, 0) > 0) {
        cout << "readdir on a removed dir: " << dirname << " succeeded (incorrect)"<< endl;
        exit(0);
    }

    cout << "Test passed" << endl;
}

int
doMkdir(char *dirname)
{
    int res;

    cout << "Making dir: " << dirname << endl;

    res = gKfsClient->Mkdir(dirname);
    if (res < 0) {
        cout << "Mkdir failed: " << res << endl;
        return res;
    }
    cout << "Mkdir returned: " << res << endl;
    return res;
}

int
doRmdir(char *dirname)
{
    int res;

    cout << "Removing dir: " << dirname << endl;

    res = gKfsClient->Rmdir(dirname);
    if (res < 0) {
        cout << "Rmdir failed: " << res << endl;
        return res;
    }
    cout << "Rmdir returned: " << res << endl;
    return res;
}

int
doReaddir(char *dirname, uint32_t expectEntries)
{
    vector<string> result;
    vector<string>::size_type i;
    int res;

    res = gKfsClient->Readdir(dirname, result);
    if (res < 0) {
        cout << "Readdir failed: " << res << endl;
        return res;
    }
    cout << "directory contents: " << endl;
    for (i = 0; i < result.size(); ++i) {
        cout << " File-name: " << result[i];
    }
    cout << endl;
    return result.size() == expectEntries;

}

int doFileCreate(char *parentDir, char *name)
{
    int fd;
    char fileName[MAX_FILE_NAME_LEN];
    char buf[4096];
    int bufsize = 4096;

    memset(fileName, 0, MAX_FILE_NAME_LEN);
    snprintf(fileName, MAX_FILE_NAME_LEN, "%s/%s", 
             parentDir, name);

    fd = gKfsClient->Create(fileName);
    if (fd < 0) {
        cout << "Create failed: " << endl;
        return -1;
    }

    // write something to it...so we can test that, when we remove the
    // file, the chunks associated with the file should also get removed.
    //
    memset(buf, 'a', bufsize);
    if (gKfsClient->Write(fd, buf, bufsize) < 0) {
        cout << "write failed: " << endl;
        return -1;
    }
    // flush out the changes to the server
    gKfsClient->Close(fd);

    return 0;
}

int doFileRemove(char *parentDir, char *name)
{
    int res;
    char fileName[MAX_FILE_NAME_LEN];

    memset(fileName, 0, MAX_FILE_NAME_LEN);
    snprintf(fileName, MAX_FILE_NAME_LEN, "%s/%s", 
             parentDir, name);

    res = gKfsClient->Remove(fileName);

    cout << "remove returned: " << res << endl;
    return res;
}


    
