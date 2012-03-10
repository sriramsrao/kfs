//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: chunkscrubber_main.cc 1552 2011-01-06 22:21:54Z sriramr $
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
// \brief A tool that scrubs the chunks in a directory and validates checksums.
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

#include "libkfsIO/Checksum.h"
#include "libkfsIO/Globals.h"
#include "common/log.h"
#include "libkfsIO/FileHandle.h"
#include "Chunk.h"
#include "ChunkManager.h"

using std::cout;
using std::endl;
using std::string;
using boost::scoped_array;

using namespace KFS;

static void scrubFile(string &fn, bool verbose);

int main(int argc, char **argv)
{
    char optchar;
    bool help = false;
    const char *chunkDir = NULL;
    int res, count;
    struct dirent **entries;
    struct stat statBuf;
    bool verbose = false, samplingMode = false;
    double randval;

    KFS::MsgLogger::Init(NULL);
    KFS::MsgLogger::SetLevel(MsgLogger::kLogLevelINFO);

    while ((optchar = getopt(argc, argv, "hvsd:")) != -1) {
        switch (optchar) {
            case 'd': 
                chunkDir = optarg;
                break;
            case 'v':
                verbose = true;
                break;
            case 's':
                samplingMode = true;
                break;
            case 'h':
            default:
                help = true;
                break;
        }
    }

    if ((help) || (chunkDir == NULL)) {
        cout << "Usage: " << argv[0] << " -d <chunkdir> {-v} {-s}" << endl;
        cout << " where -s means scrub only about 10% of the files" << endl;
        exit(-1);
    }
    
    res = scandir(chunkDir, &entries, 0, alphasort);
    if (res < 0) {
        cout << "Unable to open: " << chunkDir << endl;
        exit(-1);
    }

    time_t seed = time(NULL);
    srand48(seed);
    count = res;
    for (int i = 0; i < count; i++) {
        string fn = chunkDir;
        fn = fn + "/" + entries[i]->d_name;
        res = stat(fn.c_str(), &statBuf);
        free(entries[i]);
        if ((res < 0) || (!S_ISREG(statBuf.st_mode)))
            continue;
        if (samplingMode) {
            randval = drand48();
            if (randval > 0.1)
                continue;
        }
        scrubFile(fn, verbose);
        // scrubs will keep the disk very busy; slow it down so that
        // the system isn't overwhelmed
        if ((i % 10) == 0) {
            sleep(10);
        }
    }
    free(entries);
    exit(0);
}

static int Deserialize(ChunkInfo_t& chunkInfo, int fd, bool validate)
{
    DiskChunkInfo_t dci;
    int res;

    res = pread(fd, &dci, sizeof(DiskChunkInfo_t), 0);
    if (res != sizeof(DiskChunkInfo_t))
        return -EINVAL;
    return chunkInfo.Deserialize(dci, validate);
}

static void scrubFile(string &fn, bool verbose)
{
    ChunkInfo_t chunkInfo;
    int fd, res;
    FileHandlePtr f;
    scoped_array<char> data;

    fd = open(fn.c_str(), O_RDONLY);
    if (fd < 0) {
        cout << "Unable to open: " << fn << endl;
        return;
    }
    f.reset(new FileHandle_t(fd));
    res = Deserialize(chunkInfo, f->mFd, true);
    if (res < 0) {
        cout << "Deserialize of chunkinfo failed for: " << fn << endl;
        return;
    }
    if (verbose) {
        cout << "fid: "<< chunkInfo.fileId << endl;
        cout << "chunkId: "<< chunkInfo.chunkId << endl;
        cout << "size: "<< chunkInfo.chunkSize << endl;
        cout << "version: "<< chunkInfo.chunkVersion << endl;
    }
    data.reset(new char[KFS::CHUNKSIZE]);
    res = pread(f->mFd, data.get(), KFS::CHUNKSIZE, KFS_CHUNK_HEADER_SIZE);
    if (res < 0) {
        cout << "Unable to read data for : " << fn << endl;
        return;
    }
    if ((size_t) res != KFS::CHUNKSIZE) {
        long size = chunkInfo.chunkSize;
        if (res != chunkInfo.chunkSize) {
            cout << "Size mismatch: chunk header says: " << size 
                 << " but file size is: " << res << endl;
        }

        memset(data.get() + size, 0, KFS::CHUNKSIZE - size);
    }
    // go thru block by block and verify checksum
    for (int i = 0; i < res; i += CHECKSUM_BLOCKSIZE) {
        char *startPt = data.get() + i;
        uint32_t cksum = ComputeBlockChecksum(startPt, CHECKSUM_BLOCKSIZE);
        // uint32_t cksum = ComputeBlockChecksum(startPt, res);
        uint32_t blkno = OffsetToChecksumBlockNum(i);

        if (cksum != chunkInfo.chunkBlockChecksum[blkno]) {
            cout << "fn = " << fn << " : Checksum mismatch for block (" << blkno << "):"
                 << ", offset = " << i << " Computed: " << cksum << " from chunk: " << chunkInfo.chunkBlockChecksum[blkno] 
                 << endl;
        }
    }
}
