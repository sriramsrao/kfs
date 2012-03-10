//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: cptokfs_main.cc 1552 2011-01-06 22:21:54Z sriramr $
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
// \brief Tool that copies a file/directory from a local file system to
// KFS.  This tool is analogous to dump---backup a directory from a
// file system into KFS.
//
//----------------------------------------------------------------------------

#include <iostream>    
#include <fstream>
#include <cerrno>
#include <boost/scoped_array.hpp>
using boost::scoped_array;
using std::ios_base;

extern "C" {
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
#include <openssl/md5.h>
#include <fcntl.h>
}

#include "libkfsClient/KfsClient.h"
#include "common/log.h"

#define MAX_FILE_NAME_LEN 256

using std::cout;
using std::endl;
using std::ifstream;
using std::ofstream;
using namespace KFS;

KfsClientPtr gKfsClient;
bool doMkdirs(const char *path);

//
// For the purpose of the cp -r, take the leaf from sourcePath and
// make that directory in kfsPath. 
//
void MakeKfsLeafDir(const string &sourcePath, string &kfsPath);

//
// Given a file defined by sourcePath, copy it to KFS as defined by
// kfsPath
//
int BackupFile(const string &sourcePath, const string &kfsPath);

// Given a dirname, backit up it to dirname.  Dirname will be created
// if it doesn't exist.  
void BackupDir(const string &dirname, string &kfsdirname);

// Guts of the work
int BackupFile2(string srcfilename, string kfsfilename);

static int  gTestNumReWrites = -1;
static int  gNumReplicas = 3;
static bool gDryRunFlag = false;
static bool gIgnoreSrcErrorsFlag = false;
static bool gAppendMode = false;
static int  gBufSize = 64 << 20;
static bool gTruncateFlag = false;
static bool gDeleteFlag = false;

int
main(int argc, char **argv)
{
    DIR *dirp;
    string kfsPath = "";
    string serverHost = "";
    int port = -1;
    string sourcePath = "";
    bool help = false;
    bool verboseLogging = false;
    char optchar;
    struct stat statInfo;

    KFS::MsgLogger::Init(NULL);

    while ((optchar = getopt(argc, argv, "d:hk:p:s:R:r:vniatxb:")) != -1) {
        switch (optchar) {
            case 'd':
                sourcePath = optarg;
                break;
            case 'k':
                kfsPath = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 's':
                serverHost = optarg;
                break;
            case 'h':
                help = true;
                break;
            case 'v':
                verboseLogging = true;
                break;
            case 'r':
                gNumReplicas = atoi(optarg);
                break;
            case 'R':
                gTestNumReWrites = atoi(optarg);
                break;
            case 'n':
                gDryRunFlag = true;
                break;
            case 'i':
                gIgnoreSrcErrorsFlag = true;
                break;
            case 'a':
                gAppendMode = true;
                break;
            case 'b':
                gBufSize = (int)atof(optarg);
                break;
            case 't':
                gTruncateFlag = true;
                break;
            case 'x':
                gDeleteFlag = true;
                break;
          default:
                KFS_LOG_VA_ERROR("Unrecognized flag %c", optchar);
                help = true;
                break;
        }
    }

    if (help || (sourcePath == "") ||
            (kfsPath == "") || (serverHost == "") || (port < 0) ||
            gBufSize < 1 || gBufSize > (64 << 20)) {
        cout << "Usage: " << argv[0] << " -s <meta server name> -p <port>"
            " -d <source path> -k <Kfs path> {-v}\n"
            "<source path> of - means stdin and is supported only if <kfs path> is a file\n"
            " [-r] -- replication factor\n"
            " [-R] -- testing -- number test rewrites\n"
            " [-n] -- dry run\n"
            " [-a] -- append\n"
            " [-b] -- buffer size in bytes\n"
            " [-t] -- truncate\n"
            " [-x] -- delete\n"
        ;
        exit(-1);
    }

    gKfsClient = getKfsClientFactory()->GetClient(serverHost, port);
    if (!gKfsClient) {
	cout << "kfs client failed to initialize...exiting" << endl;
        exit(-1);
    }

    if (verboseLogging) {
        KFS::MsgLogger::SetLevel(KFS::MsgLogger::kLogLevelDEBUG);
    } else {
        KFS::MsgLogger::SetLevel(KFS::MsgLogger::kLogLevelWARN);
    } 

    statInfo.st_mode = S_IFREG;
    if ((sourcePath != "-") && (stat(sourcePath.c_str(), &statInfo) < 0)) {
        KFS_LOG_VA_INFO("Source path: %s is non-existent", sourcePath.c_str());
	exit(-1);
    }

    if (!S_ISDIR(statInfo.st_mode)) {
	BackupFile(sourcePath.c_str(), kfsPath);
	exit(0);
    }

    if ((dirp = opendir(sourcePath.c_str())) == NULL) {
	perror(sourcePath.c_str());
        exit(-1);
    }

    // when doing cp -r a/b kfs://c, we need to create c/b in KFS.
    MakeKfsLeafDir(sourcePath.c_str(), kfsPath);

    BackupDir(sourcePath.c_str(), kfsPath);

    closedir(dirp);
}

void
MakeKfsLeafDir(const string &sourcePath, string &kfsPath)
{
    string leaf;
    string::size_type slash = sourcePath.rfind('/');

    // get everything after the last slash
    if (slash != string::npos) {
	leaf.assign(sourcePath, slash+1, string::npos);
    } else {
	leaf = sourcePath;
    }
    if (kfsPath[kfsPath.size()-1] != '/')
        kfsPath += "/";

    kfsPath += leaf;
    doMkdirs(kfsPath.c_str());
}

int
BackupFile(const string &sourcePath, const string &kfsPath)
{
    string filename;
    string::size_type slash = sourcePath.rfind('/');

    // get everything after the last slash
    if (slash != string::npos) {
	filename.assign(sourcePath, slash+1, string::npos);
    } else {
	filename = sourcePath;
    }

    // for the dest side: if kfsPath is a dir, we are copying to
    // kfsPath with srcFilename; otherwise, kfsPath is a file (that
    // potentially exists) and we are ovewriting/creating it
    if (gKfsClient->IsDirectory(kfsPath.c_str())) {
        string dst = kfsPath;

        if (dst[kfsPath.size() - 1] != '/')
            dst += "/";
        
        return BackupFile2(sourcePath, dst + filename);
    }
    
    // kfsPath is the filename that is being specified for the cp
    // target.  try to copy to there...
    return BackupFile2(sourcePath, kfsPath);
}

void
BackupDir(const string &dirname, string &kfsdirname)
{
    string subdir, kfssubdir;
    DIR *dirp;
    struct dirent *fileInfo;

    if ((dirp = opendir(dirname.c_str())) == NULL) {
        perror(dirname.c_str());
        if (gIgnoreSrcErrorsFlag) {
            return;
        }
        exit(-1);
    }

    if (!doMkdirs(kfsdirname.c_str())) {
        cout << "Unable to make kfs dir: " << kfsdirname << endl;
        closedir(dirp);
	return;
    }

    while ((fileInfo = readdir(dirp)) != NULL) {
        if (strcmp(fileInfo->d_name, ".") == 0)
            continue;
        if (strcmp(fileInfo->d_name, "..") == 0)
            continue;
        
        string name = dirname + "/" + fileInfo->d_name;
        struct stat buf;
        stat(name.c_str(), &buf);

        if (S_ISDIR(buf.st_mode)) {
	    subdir = dirname + "/" + fileInfo->d_name;
            kfssubdir = kfsdirname + "/" + fileInfo->d_name;
            BackupDir(subdir, kfssubdir);
        } else if (S_ISREG(buf.st_mode)) {
	    BackupFile2(dirname + "/" + fileInfo->d_name, kfsdirname + "/" + fileInfo->d_name);
        }
    }
    closedir(dirp);
}

//
// Guts of the work to copy the file.
//
int
BackupFile2(string srcfilename, string kfsfilename)
{
    // 64MB buffers
    scoped_array<char> kfsBuf;
    int kfsfd, nRead;
    int srcFd;
    int res;

    if (srcfilename == "-")
        // dup stdin() and read from there
        srcFd = dup(0);
    else
        srcFd = open(srcfilename.c_str(), O_RDONLY);
    if (srcFd  < 0) {
        perror(srcfilename.c_str());
        if (gIgnoreSrcErrorsFlag) {
            return 0;
        }
        exit(-1);
    }
    if (gDryRunFlag) {
        close(srcFd);
        return 0;
    }

    if (gDeleteFlag) {
        gKfsClient->Remove(kfsfilename.c_str());
    }
    kfsfd = gKfsClient->Open(
        kfsfilename.c_str(),
        (O_CREAT | O_WRONLY) |
            (gAppendMode ? O_APPEND : 0) |
            (gTruncateFlag ? O_TRUNC : 0),
        gNumReplicas
    );
    if (kfsfd < 0) {
        cout << "Create " << kfsfilename << " failed: " << kfsfd << endl;
        exit(-1);
    }
    kfsBuf.reset(new char[gBufSize]);

    while ((nRead = read(srcFd, kfsBuf.get(), gBufSize)) > 0) {
        for (char* p = kfsBuf.get(), * const e = p + nRead; p < e; ) {
            for (int i = 0; ;) {
                res = gKfsClient->Write(kfsfd, p, e - p);
                if (res <= 0 || (gAppendMode && p + res != e)) {
                    cout << "Write" << (gAppendMode ? " append" : "") <<
                        " failed with error code: " << res << endl;
                    exit(-1);
                }
                if (++i > gTestNumReWrites || gAppendMode) {
                    p += res;
                    break;
                }
                gKfsClient->Sync(kfsfd);
                const int nw = i <= 1 ? 0 : res / i;
                p += nw;
                gKfsClient->Seek(kfsfd, nw - res, SEEK_CUR);
            }
        }
    }
    if ((res = gKfsClient->Close(kfsfd)) != 0) {
        cout << "close failed with error code: " << res << endl;
        exit(-1);
    }
    close(srcFd);

    return 0;
}

bool
doMkdirs(const char *path)
{
    int res;
    if (gDryRunFlag) {
        return true;
    }
    res = gKfsClient->Mkdirs((char *) path);
    if ((res < 0) && (res != -EEXIST)) {
        cout << "Mkdir failed: " << ErrorCodeToStr(res) << endl;
        return false;
    }
    return true;
}

