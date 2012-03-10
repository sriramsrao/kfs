//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: Chunk.h 1878 2011-02-15 05:51:18Z sriramr $
//
// Created 2006/03/22
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
//----------------------------------------------------------------------------

#ifndef _CHUNKSERVER_CHUNK_H
#define _CHUNKSERVER_CHUNK_H

#include <stdint.h>
#include <unistd.h>
#include <string.h>
#include <boost/shared_ptr.hpp>

#include <vector>
#include <iomanip>

#include "common/log.h"
#include "common/kfstypes.h"
#include "libkfsIO/FileHandle.h"
#include "libkfsIO/Checksum.h"
#include "Utils.h"

namespace KFS
{

///
/// \file Chunk.h
/// \brief Declarations related to a Chunk in KFS.
///


/// 
/// \brief ChunkInfo_t
/// For each chunk, the chunkserver maintains a meta-data file.  This
/// file defines the chunk attributes such as, the file it is
/// associated with, the chunk version #, and the checksums for each
/// block of the file.  For each chunk, this structure is read in at
/// startup time.
///

/// The max # of checksum blocks we have for a given chunk
const uint32_t MAX_CHUNK_CHECKSUM_BLOCKS = CHUNKSIZE /  CHECKSUM_BLOCKSIZE;

/// In the chunk header, we store upto 256 char of the file that
/// originally created the chunk.  
const size_t MAX_FILENAME_LEN = 256;

const int CHUNK_META_MAGIC = 0xCAFECAFE;
const int CHUNK_META_VERSION = 0x1;

// This structure is on-disk
struct DiskChunkInfo_t {
    DiskChunkInfo_t() : metaMagic (CHUNK_META_MAGIC), metaVersion(CHUNK_META_VERSION) { }
    DiskChunkInfo_t(kfsFileId_t f, kfsChunkId_t c, off_t s, kfsSeq_t v) :
        metaMagic (CHUNK_META_MAGIC), metaVersion(CHUNK_META_VERSION),
        fileId(f), chunkId(c), chunkVersion(v), chunkSize(s), numReads(0),
        chunkIndexSize(0)
    { 
        memset(filename, 0, MAX_FILENAME_LEN);
    }
    void SetChunkIndexSize(off_t s) {
        chunkIndexSize = s;
    }
    void SetChecksums(const uint32_t *checksums) {
        memcpy(chunkBlockChecksum, checksums, MAX_CHUNK_CHECKSUM_BLOCKS * sizeof(uint32_t));
    }

    int Validate() const {
        if (metaMagic != CHUNK_META_MAGIC) {
            KFS_LOG_STREAM_INFO <<
                "Magic # mismatch (got: " << std::hex << metaMagic <<
                ", expect: " << CHUNK_META_MAGIC << ")" << std::dec <<
            KFS_LOG_EOM;
            return -KFS::EBADCKSUM;
        }
        if (metaVersion != CHUNK_META_VERSION) {
            KFS_LOG_STREAM_INFO <<
                "Version # mismatch (got: << " << std::hex << metaVersion <<
                ", expect: << " << CHUNK_META_VERSION << ")" << std::dec <<
            KFS_LOG_EOM;
            return -KFS::EBADCKSUM;
        }
        return 0;
    }

    int Validate(kfsChunkId_t cid) const {
        int ret = Validate();
        if (ret < 0)
            return ret;

        if (chunkId != cid) {
            KFS_LOG_STREAM_INFO <<
                "Chunkid mismatch (got: " << chunkId << ", expect: " << cid << ")" <<
            KFS_LOG_EOM;
            return -KFS::EBADCKSUM;
        }
        return 0;
    }


    int metaMagic;
    int metaVersion;

    kfsFileId_t fileId;
    kfsChunkId_t chunkId;
    kfsSeq_t chunkVersion;
    off_t  chunkSize; 
    uint32_t chunkBlockChecksum[MAX_CHUNK_CHECKSUM_BLOCKS];
    // some statistics about the chunk: 
    // -- version # has an estimate of the # of writes
    // -- track the # of reads
    // ...
    uint32_t numReads;
    char filename[MAX_FILENAME_LEN];
    // for chunks created using the record-append interface, if there
    // is an index stored in the chunk, its size; the index is stored
    // beyond the end-of-chunk (i.e., after KFS_CHUNK_SIZE bytes in
    // the chunk).
    off_t chunkIndexSize;
};

// This structure is in-core
struct ChunkInfo_t {

    ChunkInfo_t() : fileId(0), chunkId(0), chunkVersion(0), chunkSize(0), 
                    chunkIndexSize(0),
                    expectedIndexSize(0),
                    chunkBlockChecksum(NULL)
    {
        // memset(chunkBlockChecksum, 0, sizeof(chunkBlockChecksum));
    }
    ~ChunkInfo_t() {
        delete [] chunkBlockChecksum;
    }
    ChunkInfo_t(const ChunkInfo_t &other) :
        fileId(other.fileId), chunkId(other.chunkId), chunkVersion(other.chunkVersion),
        chunkSize(other.chunkSize), chunkIndexSize(other.chunkIndexSize), 
        chunkBlockChecksum(NULL), dirname(other.dirname) { }
    ChunkInfo_t& operator= (const ChunkInfo_t &other) 
    {
        fileId = other.fileId;
        chunkId = other.chunkId;
        chunkVersion = other.chunkVersion;
        chunkSize = other.chunkSize;
        chunkIndexSize = other.chunkIndexSize;
        dirname = other.dirname;

        SetChecksums(other.chunkBlockChecksum);

        return *this;
    }

    void Init(kfsFileId_t f, kfsChunkId_t c, off_t v) {
        fileId = f;
        chunkId = c;
        chunkVersion = v;
        chunkBlockChecksum = new uint32_t[MAX_CHUNK_CHECKSUM_BLOCKS];
        memset(chunkBlockChecksum, 0, MAX_CHUNK_CHECKSUM_BLOCKS * sizeof(uint32_t));
    }

    void SetDirname(const std::string &path) {
        dirname = path;
    }
    
    std::string GetDirname() const {
        return dirname;
    }

    bool AreChecksumsLoaded() const {
        return chunkBlockChecksum != NULL;
    }

    void UnloadChecksums() {
        delete [] chunkBlockChecksum;
        chunkBlockChecksum = NULL;
        KFS_LOG_STREAM_DEBUG <<
            "Unloading chunk checksum for chunk " << chunkId <<
        KFS_LOG_EOM;
    }

    void SetChecksums(const uint32_t *checksums) {
        delete [] chunkBlockChecksum;
        if (checksums == NULL) {
            chunkBlockChecksum = NULL;
            return;
        }

        chunkBlockChecksum = new uint32_t[MAX_CHUNK_CHECKSUM_BLOCKS];
        memcpy(chunkBlockChecksum, checksums, MAX_CHUNK_CHECKSUM_BLOCKS * sizeof(uint32_t));
    }

    void VerifyChecksumsLoaded() const {
        assert(chunkBlockChecksum != NULL);
        if (chunkBlockChecksum == NULL)
            die("Checksums are not loaded!");
    }

    // save the chunk meta-data to the buffer; 
    void Serialize(IOBuffer *dataBuf) {
        DiskChunkInfo_t dci(fileId, chunkId, chunkSize, chunkVersion);

        assert(chunkBlockChecksum != NULL);
        dci.SetChecksums(chunkBlockChecksum);
        dci.SetChunkIndexSize(chunkIndexSize);
        dataBuf->CopyIn((char *) &dci, sizeof(DiskChunkInfo_t));
    }

    int Deserialize(const DiskChunkInfo_t &dci, bool validate) {
        if (validate) {
            if (dci.metaMagic != CHUNK_META_MAGIC) {
                KFS_LOG_STREAM_INFO <<
                    "Magic # mismatch (got: " << std::hex << dci.metaMagic <<
                    ", expect: " << CHUNK_META_MAGIC << ")" << std::dec <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
            if (dci.metaVersion != CHUNK_META_VERSION) {
                KFS_LOG_STREAM_INFO <<
                    "Version # mismatch (got: << " << std::hex << dci.metaVersion <<
                    ", expect: << " << CHUNK_META_VERSION << ")" << std::dec <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
        }
        fileId = dci.fileId;
        chunkId = dci.chunkId;
        chunkVersion = dci.chunkVersion;
        chunkSize = dci.chunkSize;
        chunkIndexSize = dci.chunkIndexSize;
        KFS_LOG_STREAM_INFO << 
            "Chunk index size for chunk " << chunkId <<
            " is: " << chunkIndexSize << KFS_LOG_EOM;

        delete [] chunkBlockChecksum;
        chunkBlockChecksum = new uint32_t[MAX_CHUNK_CHECKSUM_BLOCKS];
        memcpy(chunkBlockChecksum, dci.chunkBlockChecksum,
               MAX_CHUNK_CHECKSUM_BLOCKS * sizeof(uint32_t));
        KFS_LOG_STREAM_DEBUG <<
            "Loading chunk checksum for chunk " << chunkId <<
        KFS_LOG_EOM;

        return 0;
    }

    kfsFileId_t fileId;
    kfsChunkId_t chunkId;
    kfsSeq_t chunkVersion;
    off_t  chunkSize; 
    // size of the per-chunk index.  This is stored beyond the end-of-chunk.
    off_t chunkIndexSize;
    // to handle race conditions between reading the meta-data and the
    // chunk being closed, stash the expected index size.  If this
    // doesn't match the actual, reload the metadata.
    off_t  expectedIndexSize;
    // uint32_t chunkBlockChecksum[MAX_CHUNK_CHECKSUM_BLOCKS];
    // this is unpinned; whenever we open the chunk, this has to be
    // paged in...damn..would've been nice if this was at the end
    uint32_t *chunkBlockChecksum;
    // some statistics about the chunk: 
    // -- version # has an estimate of the # of writes
    // -- track the # of reads
    // ...
    uint32_t numReads;
    // Record the directory on which the chunk is stored.  For JBOD
    // settings, we will have one directory per drive; stash the
    // directory so that we can find the chunk file on disk.
    std::string dirname;
};

}

#endif // _CHUNKSERVER_CHUNK_H
