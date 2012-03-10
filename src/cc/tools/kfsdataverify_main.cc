//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kfsdataverify_main.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2008/06/11
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
// \brief After we push data to KFS, we need to verify that the data
// pushed matches the source.  This tool does such a verification by
// computing checksums on the data in the way KFS-chunkserver does and
// then pulls the checksums from the (3) chunkservers and compares them
// all.  When all values agree, we know that the data we wrote to KFS
// matches what is in the source.
// 
//----------------------------------------------------------------------------

#include <dirent.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string>
#include <boost/scoped_array.hpp>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string.h>

#include "libkfsIO/Checksum.h"
#include "common/log.h"
#include "libkfsIO/FileHandle.h"
#include "libkfsClient/KfsClient.h"

using std::cout;
using std::cerr;
using std::endl;
using std::string;
using boost::scoped_array;
using std::ofstream;
using std::ifstream;
using std::istringstream;
using std::vector;
using std::ios_base;

using namespace KFS;

KfsClientPtr gKfsClient;

static int verifyFile(const char *srcFn, const char *kfsFn, ofstream &cksumS);
static int verifyChecksums(const char *cksumFn);

int main(int argc, char **argv)
{
    char optchar;
    bool help = false;
    ofstream cksumS;
    int port = -1, retval = -1;
    const char *metaserver = NULL, *srcFn = NULL, *kfsFn = NULL;
    const char *cksumFn = NULL;
    bool verboseLogging = false;
    bool checkReplicas = false;

    KFS::MsgLogger::Init(NULL);

    while ((optchar = getopt(argc, argv, "s:p:f:k:c:hdv")) != -1) {
        switch (optchar) {
            case 's':
                metaserver = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 'f':
                srcFn = optarg;
                break;
            case 'k':
                kfsFn = optarg;
                break;
            case 'd':
                checkReplicas = true;
                break;
            case 'c':
                cksumFn = optarg;
                break;
            case 'v':
                verboseLogging = true;
                break;
            case 'h':
            default:
                help = true;
                break;
        }
    }

    help = help || (!metaserver) || (port < 0);

    if (help) {
        cout << "Usage: " << argv[0] << " -s <metaserver> -p <port> {-v} "
             << " [-f <srcFn> -k <KFS file> {-c <cksum save file>}] | "
             << " [-c <cksum file>] | "
             << " [-k <KFS file> -d]"
             << endl;
        exit(-1);
    }

    gKfsClient = getKfsClientFactory()->GetClient(metaserver, port);
    if (!gKfsClient) {
        cout << "kfs client failed to initialize...exiting" << endl;
        exit(-1);
    }

    if (verboseLogging) {
        gKfsClient->SetLogLevel("DEBUG");
    } else {
        gKfsClient->SetLogLevel("INFO");
    } 

    if (checkReplicas) {
        string md5sum;
        bool match = gKfsClient->CompareChunkReplicas(kfsFn, md5sum);
        retval = match ? 0 : -1;
        if (match) {
            cout << md5sum << endl;
            cerr << "Verdict: for file " << kfsFn << " all replicas are identical" << endl;
        }
        else
            cerr << "Verdict: for file " << kfsFn << " all replicas are not identical!" << endl;
        exit(retval);
    }

    if (srcFn != NULL) {
        if (cksumFn != NULL)
            cksumS.open(cksumFn, std::ios_base::app);
        
        retval = verifyFile(srcFn, kfsFn, cksumS);
    } else if (cksumFn != NULL) {
        retval = verifyChecksums(cksumFn);
    }
    exit(retval);
}

// Given a file of the form: <kfsfn> <checksums>, one tuple per line,
// get the checksums from KFS and verify that things match
static int verifyChecksums(const char *cksumFn)
{
    scoped_array<char> line;
    int linelen = 1 << 20;
    ifstream ifs;

    line.reset(new char[linelen]);
    ifs.open(cksumFn, ios_base::in);
    if (!ifs) {
        cout << "Unable to open: " << cksumFn << endl;
        return -1;
    }
    while (!ifs.eof()) {
        ifs.getline(line.get(), linelen);
        istringstream ist(line.get());
        uint32_t cksum;
        vector<uint32_t> cksums;
        string srcFn, kfsFn;

        ist >> kfsFn;
        if (kfsFn == "")
            continue;
        ist >> srcFn;
        while (ist >> cksum) {
            cksums.push_back(cksum);
        }
        if (!gKfsClient->VerifyDataChecksums(kfsFn.c_str(), cksums)) {
            cout << "Checksum mismatch in file: " << srcFn << " kfsfn: " << kfsFn << endl;
        }
    }
    return 0;
}

static int verifyFile(const char *srcFn, const char *kfsFn, ofstream &cksumS)
{
    int fd, res;
    FileHandlePtr f;
    scoped_array<char> data;
    vector<uint32_t> checksums;

    fd = open(srcFn, O_RDONLY);
    if (fd < 0) {
        cout << "Unable to open: " << srcFn << endl;
        return -1;
    }
    f.reset(new FileHandle_t(fd));

    data.reset(new char[KFS::CHUNKSIZE]);

    cksumS << srcFn << ' ';
    while (1) {
        res = read(f->mFd, data.get(), KFS::CHUNKSIZE);
        if (res <= 0)
            break;

        if ((size_t) res != KFS::CHUNKSIZE) {
            memset(data.get() + res, 0, KFS::CHUNKSIZE - res);
        }
        // go thru block by block and verify checksum
        for (int i = 0; i < res; i += CHECKSUM_BLOCKSIZE) {
            char *startPt = data.get() + i;
            uint32_t cksum = ComputeBlockChecksum(startPt, CHECKSUM_BLOCKSIZE);

            checksums.push_back(cksum);

            cksumS << cksum << ' ';
        }
    }
    cksumS << endl;
    if (!gKfsClient->VerifyDataChecksums(kfsFn, checksums)) {
        cout << "Checksum mismatch in file: " << srcFn << " kfsfn: " << kfsFn << endl;
        return -1;
    }
    return 0;
}

