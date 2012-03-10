//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: ChunkSortHelper.cc $
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
// \brief Code that interfaces with chunksorter service.
//----------------------------------------------------------------------------

#include "ChunkSortHelper.h"

#include "common/log.h"
#include "common/kfstypes.h"

#include "ChunkManager.h"

#include <vector>
#include <openssl/rand.h>

using std::list;
using std::string;
using std::vector;
using namespace KFS;

ChunkSortHelperManager KFS::gChunkSortHelperManager;

void
ChunkSortHelperManager::StartAcceptor()
{
    if (mPort < 0)
        return;

    mAcceptor = new Acceptor(mPort, this);
    if (!mAcceptor->IsAcceptorStarted()) {
        KFS_LOG_FATAL("Unable to start sorter acceptor!");
        exit(-1);
    }
    mNextWorkerToAssign = 0;
}

KfsCallbackObj *
ChunkSortHelperManager::CreateKfsCallbackObj(NetConnectionPtr &conn)
{
    ChunkSortHelperPtr csh;

    csh.reset(new ChunkSortHelper(conn));

    mWorkers.push_back(csh);
    return csh.get();
}

void
ChunkSortHelperManager::SetParameters(const Properties &props)
{
    mPort = props.getValue("chunkServer.sorter.port", -1);
    mIsSorterInStreamingMode = 
        props.getValue("chunkServer.sorter.stream_records", 0);
}

// Round-robin over the workers
ChunkSortHelperPtr
ChunkSortHelperManager::GetChunkSortHelper()
{
    ChunkSortHelperPtr csh;
    if (mWorkers.size() == 0)
        return csh;

    csh = mWorkers[mNextWorkerToAssign];
    mNextWorkerToAssign++;
    mNextWorkerToAssign %= mWorkers.size();
    return csh;
}

void
ChunkSortHelperManager::Remove(ChunkSortHelper *entry)
{
    // Take out the worker so we don't assign new work to it
    for (vector<ChunkSortHelperPtr>::iterator iter = mWorkers.begin();
         iter != mWorkers.end(); iter++) {
        ChunkSortHelperPtr csh = *iter;
        if (csh.get() == entry) {
            mWorkers.erase(iter);
            break;
        }
    }
    if (mNextWorkerToAssign >= mWorkers.size())
        mNextWorkerToAssign = 0;
}

inline static kfsSeq_t
InitialSeqNo()
{
    kfsSeq_t ret = 1;
    RAND_pseudo_bytes(reinterpret_cast<unsigned char*>(&ret), int(sizeof(ret)));
    return ((ret < 0 ? -ret : ret) >> 1);
}

static kfsSeq_t
NextSeq()
{
    static kfsSeq_t sSeqno = InitialSeqNo();
    return sSeqno++;
}

kfsSeq_t
ChunkSortHelper::NextSeqnum()
{
    mSeqnum = NextSeq();
    return mSeqnum;
}

void
ChunkSortHelper::Enqueue(KfsOp *op)
{
    if ((!mNetConnection) || (!mNetConnection->IsGood())) {
        op->status = -1;
        return;
    }

    IOBuffer::OStream os;

    op->Request(os);
    mNetConnection->Write(&os);
    if (op->op == CMD_RECORD_APPEND) {
        // send the data over
        RecordAppendOp *ra = static_cast<RecordAppendOp *>(op);
        mNetConnection->Write(&ra->dataBuf, ra->numBytes);
    }
    if (op->op == CMD_SORTER_FLUSH) {
        SorterFlushOp *shf = static_cast<SorterFlushOp *> (op);
        mPending.push_back(shf->chunkId);
    } else if (op->op == CMD_SORT_FILE) {
        SortFileOp *shf = static_cast<SortFileOp *> (op);
        mPending.push_back(shf->chunkId);
    }
    if (mNetConnection)
        mNetConnection->StartFlush();
}

void
ChunkSortHelper::HandleResponse(IOBuffer *iobuf, int msgLen)
{
    if (mPending.empty()) {
        KFS_LOG_STREAM_INFO << "Got a sort reply when nothing is pending!"
            << KFS_LOG_EOM;
    }

    int res = -1;
    const char separator = ':';
    IOBuffer::IStream is(*iobuf, msgLen);
    Properties prop;
    kfsChunkId_t &req = mPending.front();

    prop.loadProperties(is, separator, false);
    res = prop.getValue("Status", -1);
    kfsChunkId_t cid = prop.getValue("Chunk-handle", (off_t) -1);
    uint32_t indexSz = prop.getValue("Index-size", 0);

    if (cid != req) {
        KFS_LOG_STREAM_INFO << "Out of order reply for sort?, expect: " 
            << req << " but got back: " << cid << KFS_LOG_EOM;
        return;
    }

    if (res < 0) {
        KFS_LOG_STREAM_INFO << "Sorting of chunk: " << req 
            << " failed!  Notifying metaserver to re-replicate..."
            << KFS_LOG_EOM;
        // For now, we'll treat this as a lost chunk.  It maybe good
        // idea to retry this a couple of times before giving up...
        // I assume this is a rare event...if we find we are losing
        // chunks then we should robustify it.
        gChunkManager.ChunkIOFailed(cid, -EIO);
    } else {
        gChunkManager.ChunkSortDone(req, indexSz);
    }
    mPending.pop_front();
}

int
ChunkSortHelper::HandleEvent(int code, void *data)
{
    IOBuffer *iobuf;
    int msgLen;

    switch(code) {
        case EVENT_NET_READ:
            iobuf = (IOBuffer *) data;
            while (IsMsgAvail(iobuf, &msgLen)) {
                HandleResponse(iobuf, msgLen);
                iobuf->Consume(msgLen);
            }
            break;

        case EVENT_NET_WROTE:
            break;
            
        case EVENT_INACTIVITY_TIMEOUT:
            break;
        case EVENT_NET_ERROR:
        default:
            KFS_LOG_STREAM_INFO << "Looks like connection broke; shutting down worker" 
                << KFS_LOG_EOM;
            Reset();
            break;
    }
    return 0;

}

void
ChunkSortHelper::Reset()
{
    mNetConnection->Close();
    mNetConnection.reset();
    mPending.clear();
    gChunkSortHelperManager.Remove(this);
}


