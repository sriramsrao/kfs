//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: chunksorter.h $
//
// Created 2011/02/08
//
// Copyright 2011 Yahoo Corporation.  All rights reserved.
// This file is part of the Sailfish project.
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
// \brief A daemon process that sorts the data in chunks created by
// atomic-append.  The data in the chunks contains k/v pairs
//
//----------------------------------------------------------------------------

#ifndef SORTER_CHUNKSORTER_H
#define SORTER_CHUNKSORTER_H

#include "libkfsIO/KfsCallbackObj.h"
#include "libkfsIO/IOBuffer.h"
#include "libkfsIO/NetConnection.h"
#include "libkfsIO/NetManager.h"

#include "sailfish/mrtools/ifile_base.h"
#include "chunk/Chunk.h"
#include "chunk/ChunkManager.h"

#include <string>
#include <vector>

#ifdef USE_INTEL_IPP
#include <ipp.h>
#else
#include <lzo/lzo1x.h>
#include <lzo/lzoconf.h>
#endif

namespace KFS
{

    // set a limit of 4M
    const int INDEX_BUFFER_SIZE = (2 << 22);
    class SortWorker;

    typedef boost::shared_ptr<SortWorker> SortWorkerPtr;

    class SortWorkerManager : public KFS::KfsCallbackObj {
    public:
        SortWorkerManager();
        ~SortWorkerManager();
        int HandleEvent(int code, void *data);
        int mainLoop(int chunkserverPort);
    private:
        std::vector<SortWorkerPtr> mWorkers;
        TcpSocket mSock;
        NetConnectionPtr mNetConnection;
        NetManager mNetManager;

        // methods to read RPCs from chunkserver
        bool HandleCmd(IOBuffer *iobuf, int msgLen);

        void HandleAllocChunk(AllocChunkOp *op);
        void HandleRecordAppend(RecordAppendOp *op);
        void HandleFlush(SorterFlushOp *op);
        void HandleSortFile(SortFileOp *op);
    };

    // have one of these per chunk
    class SortWorker {
    public:
        SortWorker(kfsFileId_t f, kfsChunkId_t c, int64_t v);
        ~SortWorker();
        void append(RecordAppendOp *op);
        void flush(SorterFlushOp *op);
        kfsChunkId_t getChunkId() const {
            return mChunkId;
        }
        void sortFile(SortFileOp *op);
    private:
        kfsFileId_t mFileId;
        kfsChunkId_t mChunkId;
        int64_t mChunkVersion;

        std::vector<IFileRecord_t> mRecords;
        // how much data is in the buffer---sans the 16K header
        int  mChunkDataLen;
        // how much data is in the index buffer
        int mIndexBufferDataLen;
        // data read from the chunk: includes 16K header and then data
        unsigned char *mChunkBuffer;
        // pointer to the data portion within mChunkBuffer
        unsigned char *mChunkData;
        // buffer for holding the index
        char *mIndexBuffer;
        // struct to hold the per-chunk header
        DiskChunkInfo_t *mDci;

        // compression/decompression related stuff
        unsigned char *mUncompBuffer;
        int mUncompBufferSz;
#ifdef USE_INTEL_IPP
        IppLZOState_8u *mWrkMem;
#else
        lzo_voidp mWrkMem;
#endif
        // when operating in file mode...read from file and write to file
        int readFromChunk(const std::string &inputFn);
        void parseChunk();
        int parseOneRecord(const char *buffer);

        // write out a sorted chunk
        int writeToChunk(const std::string &outputFn, std::vector<IFileRecord_t> &records,
            int &indexLen);
        int compressRecords(const unsigned char *ubufStart, int ulen,
            unsigned char **compressedBuf);
    };
}

#endif // SORTER_CHUNKSORTER_H
