//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: ChunkSortHelper.h $
//
// Created 2011/02/11
//
// Copyright 2011 Yahoo Corporation.  All rights reserved.
//
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
// \brief Code that interfaces with the chunksorter service.
//----------------------------------------------------------------------------

#ifndef CHUNK_CHUNKSORTHELPER_H
#define CHUNK_CHUNKSORTHELPER_H

#include <cassert>
#include <string>
#include <inttypes.h>
#include <list>
#include <boost/shared_ptr.hpp>
#include "libkfsIO/Acceptor.h"
#include "libkfsIO/NetConnection.h"
#include "KfsOps.h"

namespace KFS
{
    // To the sorter, we forward records; records are fire-n'-forget;
    // the only ops we expect to get a reply are "MakeChunkStable"
    class ChunkSortHelper : public KfsCallbackObj {
    public:
        ChunkSortHelper(NetConnectionPtr &c) : 
            mNetConnection(c), mSeqnum(0) { 
            SET_HANDLER(this, &ChunkSortHelper::HandleEvent);
        }
        
        int HandleEvent(int code, void *data);
        
        void Enqueue(KfsOp *op);
        kfsSeq_t NextSeqnum();

    private:
        NetConnectionPtr mNetConnection;

        // pending requests for make-chunk-stable
        std::list<kfsChunkId_t> mPending;
        kfsSeq_t mSeqnum;

        void HandleResponse(IOBuffer *iobuf, int msgLen);
        void Reset();
    };

    typedef boost::shared_ptr<ChunkSortHelper> ChunkSortHelperPtr;

    class ChunkSortHelperManager : public IAcceptorOwner {
    public:
        ChunkSortHelperManager() : mAcceptor(0), 
                                   mIsSorterInStreamingMode(false),
                                   mPort(-1),
                                   mNextWorkerToAssign (0) { }
        void StartAcceptor();
        KfsCallbackObj *CreateKfsCallbackObj(NetConnectionPtr &conn);

        void SetParameters(const Properties &props);
        inline bool IsSorterInStreamingMode() const {
            return mIsSorterInStreamingMode;
        }

        ChunkSortHelperPtr GetChunkSortHelper();
        void Remove(ChunkSortHelper *entry);
    private:
        Acceptor *mAcceptor;
        // by default, sorter reads data from disk; in settings with
        // more memory, chunkserver can stream data to sorter.
        bool mIsSorterInStreamingMode;
        int mPort;
        uint32_t mNextWorkerToAssign;
        std::vector<ChunkSortHelperPtr> mWorkers;
    };

    extern ChunkSortHelperManager gChunkSortHelperManager;

}

#endif // CHUNK_CHUNKSORTHELPER_H
