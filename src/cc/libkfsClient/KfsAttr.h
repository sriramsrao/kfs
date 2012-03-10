//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: KfsAttr.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2006/06/09
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
// 
// \file KfsAttr.h
// \brief Kfs File/Chunk attributes.
//
//----------------------------------------------------------------------------

#ifndef LIBKFSCLIENT_KFSATTR_H
#define LIBKFSCLIENT_KFSATTR_H

extern "C" {
#include <sys/time.h>
}

#include <string>
#include <vector>
using std::string;
using std::vector;

#include "common/kfstypes.h"
#include "common/kfsdecls.h"

namespace KFS {

struct ChunkAttr {
    /// where is this chunk hosted.  There is a minor convention here:
    /// for writes, chunkServerLoc[0] holds the location of the
    /// "master" server that runs the transaction for writes.
    vector<ServerLocation> chunkServerLoc;

    /// what is the id of this chunk
    kfsChunkId_t chunkId;
    // what is the version # of this chunk
    int64_t  chunkVersion;

    /// did we request an allocation for this chunk?
    bool didAllocation;
    /// what is the size of this chunk
    off_t	chunkSize;
    ChunkAttr(): chunkId(-1), chunkVersion(-1), didAllocation(false),
                 chunkSize(0) { }

    // during reads, if we can't get data from a server, we avoid it
    // and look elsewhere.  when we avoid it, we take the server out
    // of the list of candidates.
    void AvoidServer(ServerLocation &loc) {
        std::vector<ServerLocation>::iterator iter;

        iter = std::find(chunkServerLoc.begin(), chunkServerLoc.end(), loc);
        if (iter != chunkServerLoc.end())
            chunkServerLoc.erase(iter);

        if (chunkServerLoc.size() == 0) {
            // all the servers we could talk to are gone; so, we need
            // to re-figure where the data is.
            chunkId = -1;
        }
    }
};

///
/// \brief Attributes downloaded from the metadata server for a file.
///
struct KfsServerAttr {
    /// the id of the file
    kfsFileId_t fileId;
    struct timeval mtime; /// modification time
    struct timeval ctime; /// attribute change time
    struct timeval crtime; /// creation time
    /// how many chunks does it have
    long long	chunkCount;
    /// server keeps a hint and will tell us....
    off_t	fileSize; 
    /// is this a directory?
    bool	isDirectory;
    /// what is the deg. of replication for this file
    int16_t numReplicas;
};


///
/// \brief Server attributes + chunk attributes
///
struct FileAttr {
    /// the id of this file
    kfsFileId_t fileId;
    struct timeval mtime; /// modification time
    struct timeval ctime; /// attribute change time
    struct timeval crtime; /// creation time
    /// is this a directory?
    bool	isDirectory;
    /// the size of this file in bytes---computed value
    off_t	fileSize;
    
    /// how many chunks does it have
    long long	chunkCount;
    
    /// what is the deg. of replication for this file
    int16_t numReplicas;

    FileAttr() {
        fileId = (kfsFileId_t) -1;
        fileSize = 0;
        chunkCount = 0;
        numReplicas = 0;
        isDirectory = false;
    }
    void Reset() {
        fileId = (kfsFileId_t) -1;
        fileSize = 0;
        chunkCount = 0;
        numReplicas = 0;
        isDirectory = false;
    }
    void Init(bool isDir) {
        isDirectory = isDir;
        gettimeofday(&mtime, NULL);
        ctime = mtime;
        crtime = mtime;
    }
    FileAttr& operator= (const KfsServerAttr &other) {
        fileId = other.fileId;
        chunkCount = other.chunkCount;
        numReplicas = other.numReplicas;
        fileSize = 0; // need to compute this
        isDirectory = other.isDirectory;
        mtime = other.mtime;
        ctime = other.ctime;
        crtime = other.crtime;
        return *this;
    }
};

///
/// \brief File attributes as usable by applications.
///
/// 
struct KfsFileAttr {
    /// the name of this file
    string filename;
    /// the id of this file
    kfsFileId_t fileId;
    struct timeval mtime; /// modification time
    struct timeval ctime; /// attribute change time
    struct timeval crtime; /// creation time
    /// is this a directory?
    bool	isDirectory;
    /// the size of this file in bytes
    off_t	fileSize;
    /// what is the deg. of replication for this file
    int16_t numReplicas;

    KfsFileAttr() : fileSize(-1) { }

    void Clear() {
        filename = "";
        fileId = 0;
    }

    KfsFileAttr& operator= (const KfsServerAttr &other) {
        fileId = other.fileId;
        fileSize = -1; // force a calc if needed
        isDirectory = other.isDirectory;
        mtime = other.mtime;
        ctime = other.ctime;
        crtime = other.crtime;
        numReplicas = other.numReplicas;
        return *this;
    }

    KfsFileAttr& operator= (const FileAttr &other) {
        fileId = other.fileId;
        fileSize = other.fileSize;
        isDirectory = other.isDirectory;
        mtime = other.mtime;
        ctime = other.ctime;
        crtime = other.crtime;
        numReplicas = other.numReplicas;
        return *this;
    }

    bool operator < (const KfsFileAttr & other) const {
        return filename < other.filename;
    }

};

struct FileChunkInfo {
    FileChunkInfo() { }
    FileChunkInfo(const string &f, kfsFileId_t t) : 
        filename(f), fileId(t), chunkCount(0) { }
    string filename;
    /// the id of this file
    kfsFileId_t fileId;
    
    /// how many chunks does it have
    long long	chunkCount;

    /// degree of replication for the file
    int16_t numReplicas;

    /// file-offset corresponding to the last chunk
    off_t lastChunkOffset;

    /// info about a single chunk: typically the last chunk of the file
    ChunkAttr cattr;
};

}

#endif // LIBKFSCLIENT_KFSATTR_H
