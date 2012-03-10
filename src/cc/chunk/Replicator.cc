//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: Replicator.cc 2007 2011-02-28 19:16:04Z sriramr $
//
// Created 2007/01/17
//
// Copyright 2008 Quantcast Corp.
// Copyright 2007-2008 Kosmix Corp.
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
// \brief Code for dealing with a replicating a chunk.  The metaserver
//asks a destination chunkserver to obtain a copy of a chunk from a source
//chunkserver; in response, the destination chunkserver pulls the data
//down and writes it out to disk.  At the end replication, the
//destination chunkserver notifies the metaserver.
//
//----------------------------------------------------------------------------

#include "Replicator.h"
#include "ChunkServer.h"
#include "Utils.h"
#include "libkfsIO/Globals.h"
#include "libkfsIO/Checksum.h"

#include <string>
#include <sstream>

#include "common/log.h"
#include <boost/scoped_array.hpp>
using boost::scoped_array;

using std::string;
using std::ostringstream;
using std::istringstream;
using std::pair;
using std::make_pair;
using namespace KFS;
using namespace KFS::libkfsio;

typedef std::map<kfsChunkId_t, Replicator*, std::less<kfsChunkId_t>,
    boost::fast_pool_allocator<std::pair<const kfsChunkId_t, Replicator*> >
> InFlightReplications;
static InFlightReplications sInFlightReplications;
static size_t sReplicationCount = 0;

size_t
Replicator::GetNumReplications()
{
    if (sInFlightReplications.empty()) {
        sReplicationCount = 0;
    }
    return sReplicationCount;
}

void
Replicator::CancelAll()
{
    for (InFlightReplications::iterator it = sInFlightReplications.begin();
            it != sInFlightReplications.end();
            ++it) {
        it->second->mCancelFlag = true;
    }
    sReplicationCount = 0;
}

Replicator::Replicator(ReplicateChunkOp *op) :
    mTimer(libkfsio::globalNetManager(), *this),
    mFileId(op->fid),
    mChunkId(op->chunkId), 
    mChunkVersion(op->chunkVersion), 
    mOwner(op),
    mOffset(0),
    mChunkMetadataOp(0), 
    mReadOp(0),
    mWriteOp(op->chunkId, op->chunkVersion),
    mGetChunkIndexOp(0),
    mDone(false),
    mCancelFlag(false)
{
    mReadOp.chunkId = op->chunkId;
    mReadOp.chunkVersion = op->chunkVersion;
    mReadOp.clnt = this;
    mWriteOp.clnt = this;
    mChunkMetadataOp.clnt = this;
    mGetChunkIndexOp.clnt = this;
    mGetChunkIndexOp.chunkId = op->chunkId;
    mWriteOp.Reset();
    mWriteOp.isFromReReplication = true;
    SET_HANDLER(&mReadOp, &ReadOp::HandleReplicatorDone);
}

Replicator::~Replicator()
{
    InFlightReplications::iterator const it =
        sInFlightReplications.find(mChunkId);
    if (it != sInFlightReplications.end() && it->second == this) {
        if (! mCancelFlag && sReplicationCount > 0) {
            sReplicationCount--;
        }
        sInFlightReplications.erase(it);
    }
}


void
Replicator::Start(RemoteSyncSMPtr &peer)
{
#ifdef DEBUG
    verifyExecutingOnEventProcessor();
#endif
    mPeer = peer;
    Timeout();
}

void
Replicator::Timeout()
{
    mTimer.RemoveTimeout();
    mChunkMetadataOp.seq = mPeer->NextSeqnum();
    mChunkMetadataOp.chunkId = mChunkId;
    
    SET_HANDLER(this, &Replicator::HandleStartDone);
    
    mPeer->Enqueue(&mChunkMetadataOp);
}

int
Replicator::HandleStartDone(int code, void *data)
{
#ifdef DEBUG
    verifyExecutingOnEventProcessor();
#endif

    if (mCancelFlag || mChunkMetadataOp.status < 0) {
        Terminate();
        return 0;
    }
    mChunkSize    = mChunkMetadataOp.chunkSize;
    mChunkVersion = mChunkMetadataOp.chunkVersion;
    if ((mChunkSize < 0) || (mChunkSize > CHUNKSIZE)) {
        KFS_LOG_STREAM_INFO <<
            "Invalid chunksize: " << mChunkSize <<
        KFS_LOG_EOM;
        Terminate();
        return 0;
    }
    
    if (mChunkMetadataOp.indexSize < 0) {
        KFS_LOG_STREAM_INFO <<
            "Index on chunk: " << mChunkId << " isn't ready yet...waiting" << 
            KFS_LOG_EOM;
        // Try in a minute
        mTimer.SetTimeout(60);
        return 0;
    }

    pair<InFlightReplications::iterator, bool> const ret =
        sInFlightReplications.insert(make_pair(mChunkId, this));
    if (ret.second) {
        sReplicationCount++;
    } else {
        assert(ret.first->second && ret.first->second != this);
        Replicator& other = *ret.first->second;
        KFS_LOG_STREAM_INFO <<
            "Canceling replicaton: " << ret.first->first <<
            " from " << other.mPeer->GetLocation().ToString() <<
            " offset " << other.mOffset <<
            (other.mCancelFlag ? " already canceled?" : "") <<
            " restarting from " << mPeer->GetLocation().ToString() <<
        KFS_LOG_EOM;
        other.mCancelFlag = true;
        ret.first->second = this;
        if (mCancelFlag) {
            // Non debug version -- an attempt to restart? &other == this
            // Delete chunk and declare error.
            mCancelFlag = false;
            Terminate();
            return 0;
        }
    }

    mReadOp.chunkVersion = mChunkVersion;
    // Delete stale copy if it exists, before replication.
    // Replication request implicitly makes previous copy stale.
    const bool kDeleteOkFlag = true;
    gChunkManager.StaleChunk(mChunkId, kDeleteOkFlag);
    // set the version to a value that will never be used; if
    // replication is successful, we then bump up the counter.
    mWriteOp.chunkVersion = 0;
    if (gChunkManager.AllocChunk(mFileId, mChunkId, 0, true) < 0) {
        Terminate();
        return -1;
    }
    KFS_LOG_STREAM_INFO <<
        "Starting re-replication for chunk " << mChunkId <<
        " with size " << mChunkSize <<
    KFS_LOG_EOM;
    // Get the per-chunk index if there is one
    if (mChunkMetadataOp.indexSize > 0) {
        // Get the index first
        ReadChunkIndex();
    } else {
        Read();
    }
    return 0;
}

void
Replicator::ReadChunkIndex()
{
    mGetChunkIndexOp.seq = mPeer->NextSeqnum();
    mGetChunkIndexOp.dataBuf = new IOBuffer();
    SET_HANDLER(this, &Replicator::HandleReadChunkIndexDone);
    mPeer->Enqueue(&mGetChunkIndexOp);
}

int
Replicator::HandleReadChunkIndexDone(int code, void *data)
{
    int res;

    if ((mGetChunkIndexOp.status < 0) || 
        ((int) mChunkMetadataOp.indexSize != mGetChunkIndexOp.dataBuf->BytesConsumable())) {
        KFS_LOG_STREAM_INFO << "Unable to retrieve chunk index from chunk: " 
            << mChunkId << " for peer: " <<  KFS_LOG_EOM;
        Terminate();
        return -1;
    }

    SET_HANDLER(this, &Replicator::HandleWriteChunkIndexDone);
    res = gChunkManager.WriteChunkIndex(mChunkId, *mGetChunkIndexOp.dataBuf, this);
    if (res < 0) {
        KFS_LOG_STREAM_INFO << "Unable to write chunk index for chunk: " 
            << mChunkId << KFS_LOG_EOM;
        Terminate();
        return -1;
    }
    return 0;
}

int
Replicator::HandleWriteChunkIndexDone(int code, void *data)
{
    if (code != EVENT_DISK_WROTE) {
        KFS_LOG_STREAM_INFO << "Unable to write chunk index for chunk: " 
            << mChunkId << " error: " << code << KFS_LOG_EOM;
        Terminate();
        return -1;
    }
    Read();
    return 0;
}


void
Replicator::Read()
{
#ifdef DEBUG
    verifyExecutingOnEventProcessor();
#endif
    ReplicatorPtr const self = shared_from_this();
    assert(! mCancelFlag);

    if (mOffset == (off_t) mChunkSize) {
        KFS_LOG_STREAM_INFO <<
            "Offset: " << mOffset << " is past end " << mChunkSize <<
            " of chunk " << mChunkId <<
        KFS_LOG_EOM;
        mDone = true;
        Terminate();
        return;
    }
    if (mOffset > (off_t) mChunkSize) {
        KFS_LOG_STREAM_ERROR <<
            "Offset: " << mOffset << " is well past end " << mChunkSize <<
            " of chunk " << mChunkId <<
        KFS_LOG_EOM;
        mDone = false;
        Terminate();
        return;
    }

    SET_HANDLER(this, &Replicator::HandleReadDone);

    mReadOp.seq = mPeer->NextSeqnum();
    mReadOp.status = 0;
    mReadOp.offset = mOffset;
    mReadOp.numBytesIO = 0;
    mReadOp.checksum.clear();
    // read an MB 
    mReadOp.numBytes = 1 << 20;
    mPeer->Enqueue(&mReadOp);
}

int
Replicator::HandleReadDone(int code, void *data)
{
#ifdef DEBUG
    verifyExecutingOnEventProcessor();
#endif

    if (mReadOp.status < 0) {
        KFS_LOG_STREAM_INFO <<
            "Read from peer " << mPeer->GetLocation().ToString() <<
            " failed with error: " << mReadOp.status <<
        KFS_LOG_EOM;
    }
    if (mCancelFlag || mReadOp.status < 0) {
        Terminate();
        return 0;
    }

    delete mWriteOp.dataBuf;
    mWriteOp.Reset();
    mWriteOp.dataBuf = new IOBuffer();
    mWriteOp.numBytes = mReadOp.dataBuf->BytesConsumable();
    mWriteOp.dataBuf->Move(mReadOp.dataBuf, mWriteOp.numBytes);
    mWriteOp.offset = mOffset;
    mWriteOp.isFromReReplication = true;

    // align the writes to checksum boundaries
    if ((mWriteOp.numBytes >= CHECKSUM_BLOCKSIZE) &&
            (mWriteOp.numBytes % CHECKSUM_BLOCKSIZE) != 0) {
        // round-down so to speak; whatever is left will be picked up by the next read
        mWriteOp.numBytes = (mWriteOp.numBytes / CHECKSUM_BLOCKSIZE) * CHECKSUM_BLOCKSIZE;
    }

    SET_HANDLER(this, &Replicator::HandleWriteDone);

    if (gChunkManager.WriteChunk(&mWriteOp) < 0) {
        // abort everything
        Terminate();
        return 0;
    }
    return 0;
}

int
Replicator::HandleWriteDone(int code, void *data)
{
#ifdef DEBUG
    verifyExecutingOnEventProcessor();
#endif
    assert(
        (code == EVENT_DISK_ERROR) ||
        (code == EVENT_DISK_WROTE) ||
        (code == EVENT_CMD_DONE)
    );
    ReplicatorPtr const self = shared_from_this();

    if (mWriteOp.status < 0) {
        KFS_LOG_STREAM_ERROR <<
            "Write failed with error: " << mWriteOp.status <<
        KFS_LOG_EOM;
    }
    if (mCancelFlag || mWriteOp.status < 0) {
        Terminate();
        return 0;
    }
    mOffset += mWriteOp.numBytesIO;
    Read();
    return 0;
}

void
Replicator::Terminate()
{
#ifdef DEBUG
    verifyExecutingOnEventProcessor();
#endif
    int res = -1;
    if (mDone && ! mCancelFlag) {
        KFS_LOG_STREAM_INFO <<
            "Replication for " << mChunkId <<
            " finished from " << mPeer->GetLocation().ToString() <<
        KFS_LOG_EOM;
        // now that replication is all done, set the version appropriately
        gChunkManager.ChangeChunkVers(mFileId, mChunkId, mChunkVersion);

        SET_HANDLER(this, &Replicator::HandleReplicationDone);        

        mWriteOp.chunkVersion = mChunkVersion;
        res = gChunkManager.WriteChunkMetadata(mChunkId, &mWriteOp);
        if (res == 0) {
            return;
        } else if (res > 0) {
            res = -1;
        }
    } 
    HandleReplicationDone(EVENT_CMD_DONE, &res);
}

// logging of the chunk meta data finished; we are all done
int
Replicator::HandleReplicationDone(int code, void *data)
{
    const int status = data ? *reinterpret_cast<int*>(data) : 0;
    mOwner->status = status >= 0 ? 0 : -1;
    if (status < 0) {
        KFS_LOG_STREAM_ERROR <<
            "Replication for " << mChunkId << " from " <<
            mPeer->GetLocation().ToString() << " status = " << status <<
            (mCancelFlag ? "cancelled" : "failed") <<
        KFS_LOG_EOM;
        if (! mCancelFlag) {
            gChunkManager.DeleteChunk(mChunkId);
        }
    } else if (! mCancelFlag) {
        gChunkManager.ReplicationDone(mChunkId);
    }
    // Notify the owner of completion
    mOwner->HandleEvent(EVENT_CMD_DONE, status >= 0 ? &mChunkVersion : 0);
    return 0;
}
