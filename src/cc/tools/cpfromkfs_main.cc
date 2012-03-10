//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: cpfromkfs_main.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2006/06/23
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
// \brief Tool that copies out a file/directory from KFS to the local file
// system.  This tool is analogous to restore, such as, restore a previously
// backed up directory from KFS.
//
//----------------------------------------------------------------------------

#include <iostream>    
#include <fstream>
#include <cerrno>

extern "C" {
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
}

#include "libkfsClient/KfsClient.h"
#include "common/log.h"

#define MAX_FILE_NAME_LEN 256

using std::cout;
using std::cerr;
using std::endl;
using std::ofstream;

using namespace KFS;
KfsClientPtr gKfsClient;

// Given a kfsdirname, restore it to dirname.  Dirname will be created
// if it doesn't exist. 
int RestoreDir(string &dirname, string &kfsdirname);

// Given a kfsdirname/filename, restore it to dirname/filename.  The
// operation here is simple: read the file from KFS and dump it to filename.
//
int RestoreFile(string &kfspath, string &localpath);

// does the guts of the work
int RestoreFile2(string kfsfilename, string localfilename);

bool skipHoles = false;

off_t start = -1, stop = -1;

int
main(int argc, char **argv)
{
    string kfsPath = "";
    string localPath = "";
    string serverHost = "";
    int port = -1;
    bool help = false;
    bool verboseLogging = false;
    char optchar;
    struct stat statInfo;

    KFS::MsgLogger::Init(NULL);

    while ((optchar = getopt(argc, argv, "d:hp:s:k:a:b:Sv")) != -1) {
        switch (optchar) {
            case 'd':
                localPath = optarg;
                break;
            case 's':
                serverHost = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 'k':
                kfsPath = optarg;
                break;
            case 'h':
                help = true;
                break;
            case 'S':
                skipHoles = true;
                break;
            case 'v':
                verboseLogging = true;
                break;
            case 'a':
                start = atoll(optarg);
                break;
            case 'b':
                stop = atoll(optarg);
                break;
            default:
                KFS_LOG_VA_ERROR("Unrecognized flag %c", optchar);
                help = true;
                break;
        }
    }

    if (help || (kfsPath == "") || (localPath == "") || (serverHost == "") || (port < 0)) {
        cerr << "Usage: " << argv[0] << " -s <meta server name> -p <port> "
             << " -k <kfs source path> -d <local path> {-v} {-S}" << endl;
        cerr << "<local path> of - means stdout and is supported only if <kfs path> is a file" << endl;
        cerr << "-S skip holes" << endl;
        exit(1);
    }

    gKfsClient = getKfsClientFactory()->GetClient(serverHost, port);
    if (!gKfsClient) {
        cout << "kfs client failed to initialize...exiting" << endl;
        exit(1);
    }

    if (verboseLogging) {
        KFS::MsgLogger::SetLevel(KFS::MsgLogger::kLogLevelDEBUG);
    } else {
        KFS::MsgLogger::SetLevel(KFS::MsgLogger::kLogLevelWARN);
    } 

    if (gKfsClient->Stat(kfsPath.c_str(), statInfo) < 0) {
	cout << "KFS path: " << kfsPath << " is non-existent!" << endl;
	exit(-1);
    }

    int retval;

    if (!S_ISDIR(statInfo.st_mode)) {
	retval = RestoreFile(kfsPath, localPath);
    } else {
        retval = RestoreDir(kfsPath, localPath);
    }
    exit(retval);
}

int
RestoreFile(string &kfsPath, string &localPath)
{
    string filename;
    string::size_type slash = kfsPath.rfind('/');
    struct stat statInfo;
    string localParentDir;

    // get everything after the last slash
    if (slash != string::npos) {
	filename.assign(kfsPath, slash+1, string::npos);
    } else {
	filename = kfsPath;
    }
    
    // get the path in local FS.  If we what we is an existing file or
    // directory in local FS, localParentDir will point to it; if localPath is
    // non-existent, then we find the parent dir and check for its
    // existence.  That is, we are trying to handle cp kfs://file/a to
    // /path/b and we are checking for existence of "/path"
    localParentDir = localPath;
    if (stat(localPath.c_str(), &statInfo)) {
	slash = localPath.rfind('/');
	if (slash == string::npos)
	    localParentDir = "";
	else {
	    localParentDir.assign(localPath, 0, slash);
	    stat(localParentDir.c_str(), &statInfo);

	    // this is the target filename
	    filename.assign(localPath, slash + 1, string::npos);
	}
    }

    if (localPath == "-")
        statInfo.st_mode = S_IFREG;
    
    if (S_ISDIR(statInfo.st_mode)) {
	return RestoreFile2(kfsPath, localParentDir + "/" + filename);
    }
    
    if (S_ISREG(statInfo.st_mode)) {
	return RestoreFile2(kfsPath, localPath);
    }
    
    // need to make the local dir
    cout << "Local Path: " << localPath << " is non-existent!" << endl;
    return -1;

}

int
RestoreDir(string &kfsdirname, string &dirname)
{
    string kfssubdir, subdir;
    int res, retval = 0;
    vector<KfsFileAttr> fileInfo;
    vector<KfsFileAttr>::size_type i;

    if ((res = gKfsClient->ReaddirPlus((char *) kfsdirname.c_str(), fileInfo)) < 0) {
        cout << "Readdir plus failed: " << res << endl;
        return res;
    }
    
    for (i = 0; i < fileInfo.size(); ++i) {
        if (fileInfo[i].isDirectory) {
            if ((fileInfo[i].filename == ".") ||
                (fileInfo[i].filename == ".."))
                continue;
	    subdir = dirname + "/" + fileInfo[i].filename;
#if defined (__sun__)
            mkdir(subdir.c_str(), S_IRWXU|S_IRWXG|S_IRWXO);
#else
            mkdir(subdir.c_str(), ALLPERMS);
#endif
            kfssubdir = kfsdirname + "/" + fileInfo[i].filename.c_str();
            res = RestoreDir(subdir, kfssubdir);
            if (res < 0)
                retval = res;

        } else {
            res = RestoreFile2(kfsdirname + "/" + fileInfo[i].filename,
                               dirname + "/" + fileInfo[i].filename);
            if (res < 0)
                retval = res;
        }
    }
    return retval;
}

// 
// Guts of the work
//
int
RestoreFile2(string kfsfilename, string localfilename)
{
    const int bufsize = 65536;
    char kfsBuf[bufsize];
    int kfsfd, n = 0, nRead, toRead;
    int localFd;

    kfsfd = gKfsClient->Open((char *) kfsfilename.c_str(), O_RDONLY);
    if (kfsfd < 0) {
        cout << "Open failed: " << endl;
        exit(-1);
    }

    if (localfilename == "-")
        // send to stdout
        localFd = dup(1);
    else
        localFd = open(localfilename.c_str(), O_WRONLY | O_CREAT, S_IRUSR|S_IWUSR);

    if (localFd < 0) {
        cout << "Unable to open: " << localfilename << endl;
        exit(-1);
    }

    if (skipHoles)
        gKfsClient->SkipHolesInFile(kfsfd);

    if (start > 0)
        gKfsClient->Seek(kfsfd, start, SEEK_SET);

    off_t pos = 0;
    while (1) {
        toRead = bufsize;

        nRead = gKfsClient->Read(kfsfd, kfsBuf, toRead);
        if (nRead <= 0) {
	    // EOF
            break;
        }
        n += nRead;
        write(localFd, kfsBuf, nRead);
        pos = gKfsClient->Tell(kfsfd);
        if ((stop > 0) && (pos > stop)) {
            KFS_LOG_VA_INFO("Stopping since: pos=%ld ; stop=%ld", pos, stop);
            break;
        }
        
    }
    gKfsClient->Close(kfsfd);
    close(localFd);
    return 0;

}
