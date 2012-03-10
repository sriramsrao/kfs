//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: ChunkServerEmulator.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2008/08/27
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
// \brief An emulator for a chunkserver.
//
//----------------------------------------------------------------------------

#ifndef EMULATOR_CHUNKSERVEREMULATOR_H
#define EMULATOR_CHUNKSERVEREMULATOR_H

#include <string>
#include <boost/shared_ptr.hpp>
#include <set>

#include "meta/ChunkServer.h"

namespace KFS
{

    class ChunkServerEmulator : public ChunkServer {
    public:
        ChunkServerEmulator(const ServerLocation &loc,int rack);
        void Enqueue(MetaChunkRequest *r);
        void Dispatch();

        // when this emulated server goes down, fail the pending ops
        // that were destined to this node
        void FailPendingOps();

        void HostingChunk(kfsChunkId_t cid, size_t chunksize) {
            mChunks.insert(cid);
            mNumChunks++;
            mUsedSpace += chunksize;
            mAllocSpace += chunksize;
        }
        void SetRebalancePlanOutFd(int fd) {
            mOutFd = fd;
        }
    private:
        std::set<kfsChunkId_t> mChunks;
        int mOutFd;
        
    };

    typedef boost::shared_ptr<ChunkServerEmulator> ChunkServerEmulatorPtr;

}

#endif // EMULATOR_CHUNKSERVEREMULATOR_H
