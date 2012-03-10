//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: ChunkServerEmulator.cc 1552 2011-01-06 22:21:54Z sriramr $
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
//----------------------------------------------------------------------------

#include "common/log.h"
#include "common/kfsdecls.h"
#include "meta/request.h"
#include "ChunkServerEmulator.h"
#include "LayoutEmulator.h"

#include <list>

using namespace KFS;
using std::set;
using std::list;

ChunkServerEmulator::ChunkServerEmulator(const ServerLocation &loc, int rack) : mOutFd(-1)
{
    uint64_t totalSpace = ((uint64_t) 1) << 30;

    // set it at 3.4TB
    totalSpace *= 3400;
    SetSpace(totalSpace, 0, 0);
    SetServerLocation(loc);
    SetRack(rack);
}

void
ChunkServerEmulator::Enqueue(MetaChunkRequest *r)
{
    mPendingReqs.enqueue(r);
}

void
ChunkServerEmulator::Dispatch()
{
    MetaRequest *r;
    size_t chunksz;
    list<MetaRequest *> reqs;

    while((r = mPendingReqs.dequeue_nowait())) {
        reqs.push_back(r);
    }
    
    while (!reqs.empty()) {
        r = reqs.front();
        reqs.pop_front();

        if (r->op == META_CHUNK_REPLICATE) {
            MetaChunkReplicate *mcr = static_cast<MetaChunkReplicate *>(r);
            mcr->status = 0;
            mcr->chunkVersion = 1;
            mNumChunks++;
            chunksz = gLayoutEmulator.GetChunkSize(mcr->chunkId);
            mUsedSpace += chunksz;
            mAllocSpace += chunksz;

            // the chunk better not be present on this node
            assert(mChunks.count(mcr->chunkId) == 0);
            mChunks.insert(mcr->chunkId);

            gLayoutEmulator.ChunkReplicationDone(mcr);
            KFS_LOG_VA_DEBUG("Moving chunk %lld to %s", mcr->chunkId,
                             mcr->server->GetServerLocation().ToString().c_str());
            if (mOutFd > 0) {
                RebalancePlanInfo_t rpi;

                rpi.chunkId = mcr->chunkId;
                strncpy(rpi.dst, mcr->server->GetServerLocation().ToString().c_str(), 
                        RebalancePlanInfo_t::hostnamelen);
                strncpy(rpi.src, mcr->srcLocation.ToString().c_str(), 
                        RebalancePlanInfo_t::hostnamelen);
                write(mOutFd, &rpi, sizeof(RebalancePlanInfo_t));
            }
        } else if (r->op == META_CHUNK_DELETE) {
            MetaChunkDelete *mcd = static_cast<MetaChunkDelete *>(r);
            mNumChunks--;
            assert(mChunks.count(mcd->chunkId) > 0);
            mChunks.erase(mcd->chunkId);
            mUsedSpace -= gLayoutEmulator.GetChunkSize(mcd->chunkId);
        } else {
            KFS_LOG_VA_INFO("Unexpected op: %d", r->op);
        }
        delete r;
    }
}

void
ChunkServerEmulator::FailPendingOps()
{
    MetaRequest *r;
    list<MetaRequest *> reqs;

    while((r = mPendingReqs.dequeue_nowait())) {
        reqs.push_back(r);
    }

    while (!reqs.empty()) {
        r = reqs.front();
        reqs.pop_front();

        if (r->op == META_CHUNK_REPLICATE) {
            MetaChunkReplicate *mcr = static_cast<MetaChunkReplicate *>(r);

            mcr->status = -EIO;
            gLayoutEmulator.ChunkReplicationDone(mcr);
        }
        delete r;
    }
}
