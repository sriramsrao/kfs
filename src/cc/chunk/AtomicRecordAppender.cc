//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: AtomicRecordAppender.cc 3369 2011-11-28 20:05:28Z sriramr $
//
// Created 2009/03/19
//
// Copyright 2009 Quantcast Corporation.  All rights reserved.
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
// \brief Data structure for tracking record appends to chunks.
//----------------------------------------------------------------------------

/*

Fault tolerant (reliable) write append protocol overview.

At the high level the synchronous replication is the same as the replication
used in the "normal" (random) KFS write.

The client sends data to the first chunk server in the replication chain:
"append master", then the "master" sends to the first "slave", then first slave
sends the data to the next slave, and so on, until the last participant in the
chain is reached.

The replication acknowledgment is propagated trough the replication chain in
the opposite direction. Positive ACK received by the master guarantees that all
other replication participants have successfully received the data.

Each chunk server maintains protocol memory for each chunk replica opened for
"write append".

The protocol state is needed to deal with unreliable communications, including 
crash. Crash can be regarded special case of communication failure. Timeout can
be regarded as another special case.
         
The protocol state is a table (WriteIdState) that keeps track of the status of
the last write append operation for a every client that issued (or can
potentially issue write append to the chunk). In KFS terms this corresponds to
"write id allocation". Thus the table will have one entry per write id (normally
<chunk size> / <space reservation size> or 64M/1M = 64 entries).
         
From protocol point of view the append operation status in the table can be one
of the three following:
"in progress" (kErrStatusInProgress),
"success"     (kErrNone),
"fail"        (!= kErrStatusInProgress && != kErrNone).
         
The client (appender) can query the table in order to unambiguously recover from
the communication failure by first querying [write append] master for last
operation status for the given chunk, and write id.

In the case where the communication with chunk master fails, the client queries
other participating chunk servers for the append operation status.

The status inquiry has the following side effect.
The status inquiry always makes write id "read only", and un-reserves the space,
thus disallowing further appends with this write id. This guarantees that after
status inquiry returns, no other [new, or stale] appends with this write id can
succeed.

For status recovery to work correctly it is required that if "Begin make chunk
stable" is executed, then the replica is truncated to the last known
successfully replicated size ("commit offset").  See comment in
BeginMakeStable().

To reduce append status recovery latency chunk master piggies back its view of
replication status on the replication RPC. See comment in
UpdateMasterCommittedOffset().

The chunk can be in two states "unstable" -- dirty / non readable, and "stable"
-- read only. Only meta server can make a decition to transition chunk replica
into the "stable" state.

When chunk master declares write append synchronous replication failure, or
decides to stop accepting write appends for other [normal] reasons (chunk full,
no clients etc), it stops accepting write appends, sends meta server its replica
state: chunk checksum and size. "Lease Relinquish" RPC with the chunk size and
chunk checksum is used in this case.

The meta server broadcasts this information to all participants, including
master. Each participant attempts to converge its replica to the state
broadcasted.

In case of failure the chunk replica is declared "corrupted", and scheduled for
re-replication by the meta server.

The meta server keeps track of the chunk master operational status, as it does
for "normal" writes: heartbeat, and write lease renewal mechanisms.

If meta server decides that the chunk master is non operational, it takes over,
and performs recovery as follows.

First meta server instructs all chunk servers hosting the chunk to stop
accepting write appends to the chunk, and report back the chunk replica state:
size, and checksum. "Begin Make Chunk Stable" RPC is used for this purpose.

Once meta server gets sufficient # of replies, it picks the shortest replica,
and instructs all participants to converge replicas to the selected state.
"Make Chunk Stable" RPC is used to convey this information.

Then meta server updates list of chunk servers hosting the chunk based on the
"Make Chunk Stable" RPC reply status.

*/

#include <algorithm>
#include <vector>
#include <map>
#include <iomanip>
#include <sstream>
#include <cerrno>
#include <boost/pool/pool_alloc.hpp>

#include "common/log.h"
#include "libkfsIO/Globals.h"
#include "qcdio/qcdllist.h"
#include "AtomicRecordAppender.h"
#include "ChunkManager.h"
#include "LeaseClerk.h"
#include "DiskIo.h"
#include "BufferManager.h"
#include "ClientSM.h"
#include "ChunkServer.h"
#include "MetaServerSM.h"
#include "DiskIo.h"
#include "ChunkSortHelper.h"

namespace KFS {

#define WAPPEND_LOG_STREAM_PREFIX << "w_append: I" << mInstanceNum << "I "
#define WAPPEND_LOG_STREAM(pri)  KFS_LOG_STREAM(pri)  WAPPEND_LOG_STREAM_PREFIX
#define WAPPEND_LOG_STREAM_DEBUG KFS_LOG_STREAM_DEBUG WAPPEND_LOG_STREAM_PREFIX
#define WAPPEND_LOG_STREAM_WARN  KFS_LOG_STREAM_WARN  WAPPEND_LOG_STREAM_PREFIX
#define WAPPEND_LOG_STREAM_INFO  KFS_LOG_STREAM_INFO  WAPPEND_LOG_STREAM_PREFIX
#define WAPPEND_LOG_STREAM_ERROR KFS_LOG_STREAM_ERROR WAPPEND_LOG_STREAM_PREFIX
#define WAPPEND_LOG_STREAM_FATAL KFS_LOG_STREAM_FATAL WAPPEND_LOG_STREAM_PREFIX

typedef QCDLList<RecordAppendOp> AppendReplicationList;

// Leave about 15% in each chunk.  For I-files when compression is
// used, the sorted/compressed data may be higher in size compared to
// the original.  The bloat with LZO is about 15%.  For a 128M chunk
// size, this trailer space is about 30M
const off_t CHUNK_TRAILER_SPACE = (1 << 20) * 30;

RecordAppendOp::RecordAppendOp(kfsSeq_t s)
    : KfsOp(CMD_RECORD_APPEND, s),
      clientSeq(s),
      chunkId(-1),
      chunkVersion(-1),
      numBytes(0),
      writeId(-1),
      offset(-1),
      fileOffset(-1),
      numServers(0),
      checksum(0),
      servers(),
      masterCommittedOffset(-1),
      dataBuf(),
      origClnt(0),
      origSeq(s),
      replicationStartTime(0)
{
    AppendReplicationList::Init(*this);
}

RecordAppendOp::~RecordAppendOp()
{
    assert(! origClnt && ! QCDLListOp<RecordAppendOp>::IsInList(*this));
}

std::string
RecordAppendOp::Show() const
{
    std::ostringstream os;

    os << "record-append:"
       " seq: " << seq <<
       " chunkId: " << chunkId <<
       " file-offset: " << fileOffset <<
       " writeId = " << writeId <<
       " offset: " << offset <<
       " numBytes: " << numBytes <<
       " client-seq: " << clientSeq <<
       " master-committed: " << masterCommittedOffset
    ;
    /*
    os << "record-append:"
       " seq: " << seq <<
       " chunkId: " << chunkId <<
       " chunkversion: " << chunkVersion <<
       " file-offset: " << fileOffset <<
       " writeId = " << writeId <<
       " offset: " << offset <<
       " numBytes: " << numBytes <<
       " servers: " << servers <<
       " checksum: " << checksum <<
       " client-seq: " << clientSeq <<
       " master-committed: " << masterCommittedOffset
    ;
    */
    return os.str();
}

typedef QCDLList<AtomicRecordAppender> PendingFlushList;

inline AtomicRecordAppendManager::Counters& AtomicRecordAppendManager::Cntrs()
    { return mCounters; }

inline void
AtomicRecordAppendManager::IncAppendersWithWidCount()
{
    mAppendersWithWidCount++;
    assert(mAppendersWithWidCount > 0);
}

inline void
AtomicRecordAppendManager::DecAppendersWithWidCount()
{
    assert(mAppendersWithWidCount > 0);
    if (mAppendersWithWidCount > 0) {
        mAppendersWithWidCount--;
    }
}

// One per chunk
class AtomicRecordAppender : public KfsCallbackObj
{
public:
    enum
    {
        kErrNone              = 0,
        kErrParameters        = -EINVAL,
        kErrProtocolState     = -EPERM,
        kErrStatusInProgress  = -EAGAIN,
        kErrWidReadOnly       = -EROFS,
        kErrFailedState       = -EFAULT,
        kErrOutOfSpace        = -ENOSPC,
        kErrNotFound          = -ENOENT,
        kErrReplicationFailed = -EHOSTUNREACH
    };

    AtomicRecordAppender(
        const DiskIo::FilePtr& chunkFileHandle,
        kfsChunkId_t           chunkId,
        int64_t                chunkVersion,
        uint32_t               numServers,
        std::string            servers,
        ServerLocation         peerLoc,
        int                    replicationPos,
        off_t                  chunkSize,
        ChunkSortHelperPtr     &sortHelper);
    void MakeChunkStable(MakeChunkStableOp* op = 0);
    bool IsOpen() const
        { return (mState == kStateOpen); }
    bool IsChunkStable() const
    {
        return (
            mState != kStateOpen &&
            mState != kStateClosed &&
            mState != kStateReplicationFailed
        );
    }
    void Timeout();
    int GetAlignment() const
        { return (mBuffer.BytesConsumableLast() + mBufFrontPadding); }
    kfsChunkId_t GetChunkId() const
        { return mChunkId; }
    size_t SpaceReserved() const
        { return mBytesReserved; }
    bool IsMaster() const
        { return (mReplicationPos == 0); }
    bool WantsToKeepLease() const;
    void AllocateWriteId(WriteIdAllocOp *op, int replicationPos,
        ServerLocation peerLoc, const DiskIo::FilePtr& chunkFileHandle);
    int  ChangeChunkSpaceReservaton(
        int64_t writeId, size_t nBytesIn, bool releaseFlag, std::string* errMsg);
    int  InvalidateWriteId(int64_t writeId, bool declareFailureFlag);
    void AppendBegin(RecordAppendOp *op, int replicationPos,
        ServerLocation peerLoc);
    void GetOpStatus(GetRecordAppendOpStatus* op);
    void BeginMakeStable(BeginMakeChunkStableOp* op = 0);
    void CloseChunk(CloseOp* op, int64_t writeId, bool& forwardFlag);
    bool CanDoLowOnBuffersFlush() const
        { return mCanDoLowOnBuffersFlushFlag; }
    void LowOnBuffersFlush()
        { FlushFullBlocks(); }
    void UpdateFlushLimit(int flushLimit)
    {
        if (mBuffer.BytesConsumable() > flushLimit) {
            FlushFullBlocks();
        }
    }
    int  EventHandler(int code, void *data);
    void DeleteChunk();
    bool Delete();
    int  CheckParameters(
        int64_t chunkVersion, uint32_t numServers, std::string servers,
        int replicationPos, ServerLocation peerLoc,
        const DiskIo::FilePtr& fileHandle, std::string& msg);
    static bool ComputeChecksum(
        kfsChunkId_t chunkId, int64_t chunkVersion,
        off_t& chunkSize, uint32_t& chunkChecksum);
    void FatalError()
        { abort(); }

private:
    enum State
    {
        kStateNone              = 0,
        kStateOpen              = 1,
        kStateClosed            = 2,
        kStateReplicationFailed = 3,
        kStateStable            = 4,
        kStateChunkLost         = 5,
        kStatePendingDelete     = 6,
        kNumStates
    };
    struct WIdState
    {
        WIdState()
            : mBytesReserved(0),
              mLength(0),
              mOffset(-1),
              mSeq(0),
              mAppendCount(0),
              mStatus(0),
              mReadOnlyFlag(false)
            {}
        size_t   mBytesReserved;
        size_t   mLength;
        off_t    mOffset;
        kfsSeq_t mSeq;
        uint64_t mAppendCount;
        int      mStatus;
        bool     mReadOnlyFlag;
    };
    typedef std::map<int64_t, WIdState, std::less<int64_t>,
        boost::fast_pool_allocator<std::pair<const int64_t, WIdState > >
    > WriteIdState;
    typedef NetManager::Timer Timer;

    const int               mReplicationPos;
    const uint32_t          mNumServers;
    const kfsChunkId_t      mChunkId;
    const int64_t           mChunkVersion;
    // Bump file ref. count to prevent chunk manager from closing the file and
    // unloading checksums.
    DiskIo::FilePtr         mChunkFileHandle;
    const std::string       mCommitAckServers;
    const ServerLocation    mPeerLocation;
    // Protocol state.
    State                   mState;
    // The list of ops to be notified once finalize is
    // finished (as in, all dirty data is flushed out to disk and
    // the  metadata is also pushed down).
    MakeChunkStableOp*      mMakeChunkStableOp;
    MakeChunkStableOp*      mLastMakeChunkStableOp;
    BeginMakeChunkStableOp* mBeginMakeChunkStableOp;
    BeginMakeChunkStableOp* mLastBeginMakeChunkStableOp;
    // Timer
    time_t                  mLastActivityTime;
    time_t                  mLastFlushTime;
    // when records are streamed in from clients, they are
    // buffered by this object and then committed to disk.  This
    // field tracks the next offset in the file at which a record
    // should be appended.
    off_t                   mNextOffset;
    // Next committed append offset.
    off_t                   mNextCommitOffset;
    off_t                   mCommitOffsetAckSent;
    // Disk write position.
    off_t                   mNextWriteOffset;
    off_t                   mMasterCommittedOffset;
    // Buffer for holding the index
    IOBuffer                mChunkIndexBuffer;
    // Disk write buffer.
    IOBuffer                mBuffer;
    int                     mBufFrontPadding;
    int                     mIoOpsInFlight;
    int                     mReplicationsInFlight;
    size_t                  mBytesReserved;
    uint64_t                mAppendCommitCount;
    uint32_t                mChunkChecksum;
    off_t                   mChunkSize; // To report to meta server
    bool                    mStaggerRMWInFlightFlag:1;
    bool                    mRestartFlushFlag:1;
    bool                    mFlushFullBlocksFlag:1;
    bool                    mCanDoLowOnBuffersFlushFlag:1;
    bool                    mMakeStableSucceededFlag:1;
    bool                    mWritingIndexFlag:1;
    const uint64_t          mInstanceNum;
    WriteIdState            mWriteIdState;
    Timer                   mTimer;
    ChunkSortHelperPtr      mSortHelper;
    RecordAppendOp*         mReplicationList[1];
    AtomicRecordAppender*   mPrevPtr[1];
    AtomicRecordAppender*   mNextPtr[1];
    friend class QCDLListOp<AtomicRecordAppender, 0>;

    ~AtomicRecordAppender();
    static inline time_t Now()
        { return libkfsio::globalNetManager().Now(); }
    void SetState(State state, bool notifyIfLostFlag = true);
    const char* GetStateAsStr() const
        { return GetStateAsStr(mState); }
    const char* GetStateAsStr(
        State state) const
    {
        return ((state < kNumStates && state >= 0) ?
            sStateNames[state] : "invalid");
    }
    off_t GetChunkSize() const
    {
        const ChunkInfo_t* const info = gChunkManager.GetChunkInfo(mChunkId);
        return (info ? info->chunkSize : -1);
    }
    bool IsChunkOpen() const
    {
        const ChunkInfo_t* const info = gChunkManager.GetChunkInfo(mChunkId);
        return (info && (info->chunkBlockChecksum || info->chunkSize == 0));
    }
    inline void SetCanDoLowOnBuffersFlushFlag(bool flag);
    void UpdateMasterCommittedOffset(off_t masterCommittedOffset);
    void AppendCommit(RecordAppendOp *op);
    // helper function that flushes the buffered data.  the input
    // argument specifies whether the flush on the buffered data
    // should be aligned to checksum blocks.
    void FlushSelf(bool flushFullChecksumBlocks);
    void FlushFullBlocks()
        { FlushSelf(true); }
    void FlushAll()
        { FlushSelf(false); }
    int GetNextReplicationTimeout() const;
    void OpDone(WriteOp* op);
    void OpDone(RecordAppendOp* op);
    void OpDone(ReadOp* op);
    bool DeleteIfNeeded()
    {
        if (mState == kStatePendingDelete) {
            Delete();
            return true;
        }
        return false;
    }
    void CheckLeaseAndChunk(const char* prefix);
    void MetaWriteDone(int status);
    void MakeChunkStableDone();
    bool ComputeChecksum();
    void SubmitResponse(MakeChunkStableOp& op);
    void SubmitResponse(BeginMakeChunkStableOp& op);
    bool TryToCloseChunk();
    void TrimToLastCommit(const char* inMsgPtr);
    void NotifyChunkClosed();
    void SendCommitAck();
    void IncAppendersWithWidCount()
        { gAtomicRecordAppendManager.IncAppendersWithWidCount(); }
    void DecAppendersWithWidCount()
        { gAtomicRecordAppendManager.DecAppendersWithWidCount(); }

    template <typename OpT> void SubmitResponse(OpT*& listHead, OpT*& listTail)
    {
        OpT* op = listHead;
        listHead = 0;
        listTail = 0;
        while (op) {
            OpT& cur = *op;
            op = op->next;
            SubmitResponse(cur);
        }
    }
    template <typename OpT> void PushBack(
        OpT*& listHead, OpT*& listTail, OpT* op)
    {
        if (listHead) {
            listTail->next = op;
            while (listTail->next) {
                listTail = listTail->next;
            }
        } else {
            listHead = op;
            listTail = op;
        }
    }
    static std::string MakeCommitAckServers(
        uint32_t numServers, std::string servers)
    {
        std::string        ret;
        std::istringstream is(servers);
        for (uint32_t i = 0; is && i < numServers; ) {
            std::string token;
            is >> std::ws >> token; // Host
            ret += token + " ";
            is >> std::ws >> token; // Port
            ret += token + (++i < numServers ? " -1 " : " -1"); // Write id.
        }
        return ret;
    }
    static uint64_t          sInstanceNum;
    static const char* const sStateNames[kNumStates];
    static AtomicRecordAppendManager::Counters& Cntrs()
        { return gAtomicRecordAppendManager.Cntrs(); }
private:
    // No copy.
    AtomicRecordAppender(const AtomicRecordAppender&);
    AtomicRecordAppender& operator=(const AtomicRecordAppender&);
};

const char* const AtomicRecordAppender::sStateNames[kNumStates] =
{
    "none",
    "open",
    "closed",
    "replication failed",
    "stable",
    "chunk lost",
    "pending delete"
};
uint64_t AtomicRecordAppender::sInstanceNum = 10000;

inline void
AtomicRecordAppendManager::UpdatePendingFlush(AtomicRecordAppender& appender)
{
    if (appender.CanDoLowOnBuffersFlush()) {
        if (! PendingFlushList::IsInList(mPendingFlushList, appender)) {
            PendingFlushList::PushFront(mPendingFlushList, appender);
        }
    } else {
        PendingFlushList::Remove(mPendingFlushList, appender);
    }
}

inline void
AtomicRecordAppendManager::Detach(AtomicRecordAppender& appender)
{
    const size_t cnt = mAppenders.erase(appender.GetChunkId());
    if (cnt != 1) {
        WAPPEND_LOG_STREAM_FATAL <<
            "appender detach: "  << (const void*)&appender <<
            " chunkId: "         << appender.GetChunkId() <<
            " appenders count: " << mAppenders.size() <<
        KFS_LOG_EOM;
        appender.FatalError();
    }
    PendingFlushList::Remove(mPendingFlushList, appender);
}

inline void
AtomicRecordAppendManager::DecOpenAppenderCount()
{
    if (mOpenAppendersCount > 0) {
        mOpenAppendersCount--;
    }
}

inline void
AtomicRecordAppender::SetCanDoLowOnBuffersFlushFlag(bool flag)
{
    if (mCanDoLowOnBuffersFlushFlag != flag) {
        mCanDoLowOnBuffersFlushFlag = flag;
        gAtomicRecordAppendManager.UpdatePendingFlush(*this);
    }
}

AtomicRecordAppender::AtomicRecordAppender(
    const DiskIo::FilePtr& chunkFileHandle,
    kfsChunkId_t           chunkId,
    int64_t                chunkVersion,
    uint32_t               numServers,
    std::string            servers,
    ServerLocation         peerLoc,
    int                    replicationPos,
    off_t                  chunkSize,
    ChunkSortHelperPtr     &sortHelper)
    : KfsCallbackObj(),
      mReplicationPos(replicationPos),
      mNumServers(numServers),
      mChunkId(chunkId),
      mChunkVersion(chunkVersion),
      mChunkFileHandle(chunkFileHandle),
      mCommitAckServers(MakeCommitAckServers(numServers, servers)),
      mPeerLocation(peerLoc),
      mState(kStateOpen),
      mMakeChunkStableOp(0),
      mLastMakeChunkStableOp(0),
      mBeginMakeChunkStableOp(0),
      mLastBeginMakeChunkStableOp(0),
      mLastActivityTime(Now()),
      mLastFlushTime(Now()),
      mNextOffset(chunkSize),
      mNextCommitOffset(chunkSize),
      mCommitOffsetAckSent(mNextCommitOffset),
      mNextWriteOffset(chunkSize),
      mMasterCommittedOffset(-1),
      mChunkIndexBuffer(),
      mBuffer(),
      mBufFrontPadding(0),
      mIoOpsInFlight(0),
      mReplicationsInFlight(0),
      mBytesReserved(0),
      mAppendCommitCount(0),
      mChunkChecksum(0),
      mChunkSize(-1),
      mStaggerRMWInFlightFlag(false),
      mRestartFlushFlag(false),
      mFlushFullBlocksFlag(false),
      mCanDoLowOnBuffersFlushFlag(false),
      mMakeStableSucceededFlag(false),
      mWritingIndexFlag(false),
      mInstanceNum(++sInstanceNum),
      mWriteIdState(),
      mTimer(
        libkfsio::globalNetManager(),
        *this,
        gAtomicRecordAppendManager.GetCleanUpSec()
          ),
      mSortHelper(sortHelper)
{
    assert(
        chunkSize >= 0 &&
        mChunkFileHandle && mChunkFileHandle->IsOpen() &&
        IsChunkOpen()
    );
    SET_HANDLER(this, &AtomicRecordAppender::EventHandler);
    PendingFlushList::Init(*this);
    AppendReplicationList::Init(mReplicationList);
    mNextOffset = GetChunkSize();
    WAPPEND_LOG_STREAM_DEBUG <<
        "ctor" <<
        " chunk: "  << mChunkId <<
        " offset: " << mNextOffset <<
    KFS_LOG_EOM;
}

AtomicRecordAppender::~AtomicRecordAppender()
{
    assert(
        mState == kStatePendingDelete &&
        mIoOpsInFlight == 0 &&
        mReplicationsInFlight == 0 &&
        mWriteIdState.empty() &&
        AppendReplicationList::IsEmpty(mReplicationList) &&
        ! gChunkManager.IsWriteAppenderOwns(mChunkId)
    );
    WAPPEND_LOG_STREAM_DEBUG <<
        "dtor" <<
        " chunk: "  << mChunkId <<
        " offset: " << mNextOffset <<
    KFS_LOG_EOM;
    mState = kStateNone; // To catch double free;
    mChunkIndexBuffer.Clear();
}

void
AtomicRecordAppender::SetState(State state, bool notifyIfLostFlag /* = true */)
{
    if (state == mState || mState == kStatePendingDelete) {
        return;
    }
    const State prevState     = mState;
    const bool  wasStableFlag = IsChunkStable();
    mState = state;
    const bool  nowStableFlag = IsChunkStable();
    if ((wasStableFlag && ! nowStableFlag) ||
            (mState == kStateReplicationFailed && prevState != kStateOpen)) {
        // Presently transition from stable to open is not allowed.
        WAPPEND_LOG_STREAM_FATAL <<
            " invalid state transition:"
            " from: "      << GetStateAsStr(prevState) <<
            " to: "        << GetStateAsStr() <<
            " chunk: "     << mChunkId <<
            " offset: "    << mNextOffset <<
            " wid count: " << mWriteIdState.size() <<
        KFS_LOG_EOM;
        FatalError();
    }
    if (prevState == kStateOpen) {
        gAtomicRecordAppendManager.DecOpenAppenderCount();
    }
    if (wasStableFlag != nowStableFlag) {
        gAtomicRecordAppendManager.UpdateAppenderFlushLimit(this);
    }
    if (mState == kStateStable || mState == kStateChunkLost) {
        mTimer.SetTimeout(gAtomicRecordAppendManager.GetCleanUpSec());
    }
    mMakeStableSucceededFlag =
        mMakeStableSucceededFlag || mState == kStateStable;
    if (nowStableFlag) {
        mBuffer.Clear();
        mChunkFileHandle.reset(); // no more ios.
    }
    if (mState == kStateChunkLost) {
        if (notifyIfLostFlag) {
            gChunkManager.ChunkIOFailed(mChunkId, -EIO);
        }
        Cntrs().mLostChunkCount++;
    } else if (mState == kStateReplicationFailed) {
        TryToCloseChunk();
    }
}

bool
AtomicRecordAppender::Delete()
{
    if (mState != kStatePendingDelete) {
        if (int(mState) <= kStateNone || int(mState) >= kNumStates) {
            // Invalid state, most likely double free.
            FatalError();
        }
        mTimer.RemoveTimeout();
        mBuffer.Clear();
        SetCanDoLowOnBuffersFlushFlag(false);
        if (! mWriteIdState.empty()) {
            DecAppendersWithWidCount();
        }
        mWriteIdState.clear();
        gAtomicRecordAppendManager.Detach(*this);
        SetState(kStatePendingDelete);
    }
    if (mIoOpsInFlight > 0 || mReplicationsInFlight > 0) {
        return false; // wait for in flight ops to finish
    }
    delete this;
    return true;
}

int
AtomicRecordAppender::CheckParameters(
    int64_t chunkVersion, uint32_t numServers, std::string servers,
    int replicationPos, ServerLocation peerLoc,
    const DiskIo::FilePtr& fileHandle, std::string& msg)
{
    int status = 0;
    if (chunkVersion != mChunkVersion) {
        msg    = "invalid chunk version";
        status = kErrParameters;
    } else if (mReplicationPos != replicationPos) {
        status = kErrParameters;
        msg    = "invalid replication chain position";
    } else if (mPeerLocation != peerLoc) {
        status = kErrParameters;
        msg    = "invalid replication chain peer: " +
            peerLoc.ToString() + " expected: " + mPeerLocation.ToString();
    } else if (mNumServers != numServers) {
        status = kErrParameters;
        msg    = "invalid replication factor";
    } else if (mState != kStateOpen) {
        msg    = GetStateAsStr();
        status = kErrProtocolState;
    } else if (MakeCommitAckServers(numServers, servers) !=
            mCommitAckServers) {
        status = kErrParameters;
        msg    = "invalid replication chain";
    } else if (fileHandle.get() != mChunkFileHandle.get()) {
        status = kErrParameters;
        msg    = "invalid file handle";
    }
    return status;
}

void
AtomicRecordAppender::DeleteChunk()
{
    WAPPEND_LOG_STREAM_DEBUG <<
        "delete: " <<
        " chunk: "      << mChunkId <<
        " state: "      << GetStateAsStr() <<
        " offset: "     << mNextOffset <<
        " wid count: "  << mWriteIdState.size() <<
    KFS_LOG_EOM;
    if (mState == kStatePendingDelete) {
        // Only AtomicRecordAppendManager calls this method.
        // Pending delete shouldn't be in AtomicRecordAppendManager::mAppenders.
        FatalError();
    }
    // Prevent recursion:
    // SetState(kStateChunkLost) => StaleChunk() => here
    // make sure that StaleChunk() will not be invoked.
    // Never invoke Delete() here.
    SetState(kStateChunkLost, false);
}

int
AtomicRecordAppender::EventHandler(int code, void* data)
{
    switch(code) {
        case EVENT_INACTIVITY_TIMEOUT:
            Timeout();
        break;
        case EVENT_DISK_ERROR:
        case EVENT_DISK_WROTE: {
            const int status = data ? *reinterpret_cast<int*>(data) : -1;
            MetaWriteDone(
                (code == EVENT_DISK_ERROR && status > 0) ? -1 : status
            );
        }
        break;
        case EVENT_CMD_DONE: {
            KfsOp* const op = reinterpret_cast<KfsOp*>(data);
            assert(op && op->clnt == this);
            switch (op->op) {
                case CMD_WRITE:
                    OpDone(static_cast<WriteOp*>(op));
                break;
                case CMD_RECORD_APPEND:
                    OpDone(static_cast<RecordAppendOp*>(op));
                break;
                case CMD_READ:
                    OpDone(static_cast<ReadOp*>(op));
                break;
                default:
                    WAPPEND_LOG_STREAM_FATAL << "unexpected op: " << op->Show() <<
                    KFS_LOG_EOM;
                    FatalError();
                break;
            }
        }
        break;
        default:
            WAPPEND_LOG_STREAM_FATAL << "unexpected event code: " << code <<
            KFS_LOG_EOM;
            FatalError();
        break;
    }
    return 0;
}

void
AtomicRecordAppender::CheckLeaseAndChunk(const char* prefix)
{
    if (! IsChunkStable() &&
            (! mChunkFileHandle || ! mChunkFileHandle->IsOpen())) {
        WAPPEND_LOG_STREAM_WARN << (prefix ? prefix : "") <<
            ": chunk manager discarded chunk: " << mChunkId << "?" <<
            " state: " << GetStateAsStr() <<
        KFS_LOG_EOM;
        SetState(kStateChunkLost);
    } else if (mState == kStateOpen && IsMaster() &&
            ! gLeaseClerk.IsLeaseValid(mChunkId)) {
        WAPPEND_LOG_STREAM_ERROR << (prefix ? prefix : "") <<
            ": write lease has expired, no further append allowed" <<
            " chunk: " << mChunkId <<
        KFS_LOG_EOM;
        // Handle this exactly the same way as replication failure: trim to last
        // commit, and relinquish the lease.
        // Transitioning into closed won't relinquish the lease. Without
        // explicit lease release it might stay in closed state until no
        // activity timer goes off. Status inquiry is considered an
        // activity: op status recovery cannot succeed because make chunk
        // stable will not be issued until no activity timer goes off.
        Cntrs().mLeaseExpiredCount++;
        SetState(kStateReplicationFailed);
    }
}

void
AtomicRecordAppender::AllocateWriteId(
    WriteIdAllocOp *op, int replicationPos, ServerLocation peerLoc,
    const DiskIo::FilePtr& chunkFileHandle)
{
    mLastActivityTime = Now();
    if (! IsChunkStable() && chunkFileHandle != mChunkFileHandle) {
        WAPPEND_LOG_STREAM_FATAL <<
            "invalid chunk file handle: " <<
            (const void*)chunkFileHandle.get() <<
            " / " << (const void*)mChunkFileHandle.get() <<
        KFS_LOG_EOM;
        FatalError();
    }
    CheckLeaseAndChunk("allocate write id");

    int         status = 0;
    std::string msg;
    if (op->chunkId != mChunkId) {
        msg    = "invalid chunk id";
        status = kErrParameters;
    } else if (op->chunkVersion != mChunkVersion) {
        msg    = "invalid chunk version";
        status = kErrParameters;
    } else if (mReplicationPos != replicationPos) {
        status = kErrParameters;
        msg    = "invalid replication chain position";
    } else if (mPeerLocation != peerLoc) {
        status = kErrParameters;
        msg    = "invalid replication chain peer: " +
            peerLoc.ToString() + " expected: " + mPeerLocation.ToString();
    } else if (mNumServers != op->numServers) {
        status = kErrParameters;
        msg    = "invalid replication factor";
    } else if (mState != kStateOpen) {
        msg    = GetStateAsStr();
        status = kErrProtocolState;
    } else if (int(mWriteIdState.size()) >=
            gAtomicRecordAppendManager.GetMaxWriteIdsPerChunk()) {
        msg    = "too many write ids";
        status = kErrOutOfSpace;
    } else {
        const bool waEmptyFlag = mWriteIdState.empty();
        std::pair<WriteIdState::iterator, bool> res = mWriteIdState.insert(
            std::make_pair(op->writeId, WIdState()));
        if (! res.second) {
            WAPPEND_LOG_STREAM_FATAL <<
                "allocate write id: duplicate write id: " << op->writeId <<
            KFS_LOG_EOM;
            FatalError();
        } else {
            res.first->second.mSeq = op->clientSeq;
            if (waEmptyFlag) {
                IncAppendersWithWidCount();
            }
        }
    }
    op->status = status;
    if (status != 0) {
        op->statusMsg = msg;
    }
    WAPPEND_LOG_STREAM(status == 0 ?
            MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
        "allocate write id: " <<
            (status != 0 ? msg : std::string("ok")) <<
        " state: "     << GetStateAsStr() <<
        " chunk: "     << mChunkId <<
        " wid count: " << mWriteIdState.size() <<
        " offset: "    << mNextOffset <<
        " reserved: "  << mBytesReserved <<
        " writeId: "   << op->writeId <<
        " seq: "       << op->seq <<
        " cli seq: "   << op->clientSeq <<
        " status: "    << status <<
    KFS_LOG_EOM;
}

// Ideally space reservation should *not* use write id, but its own token
// "space reservation id" instead. Separate reservation token makes write append
// pipelining more efficient by reducing # of round trips required by the
// protocol: after single reservation multiple write append ops can be started.
// At the time of writing this type of request pipelining is not implemented and
// is not planned to be implemented in the near future by the client.
int
AtomicRecordAppender::ChangeChunkSpaceReservaton(
    int64_t writeId, size_t nBytes, bool releaseFlag, std::string* errMsg)
{
    mLastActivityTime = Now();
    CheckLeaseAndChunk(releaseFlag ? "space reserve" : "space release");

    int                    status       = 0;
    const char*            msg          = "ok";
    const size_t           prevReserved = mBytesReserved;
    WriteIdState::iterator it;
    if (mReplicationPos != 0) {
        msg    = "not master";
        status = kErrParameters;
    } else if (mState != kStateOpen) {
        msg    = GetStateAsStr();
        status = kErrProtocolState;
    } else if ((it = mWriteIdState.find(writeId)) == mWriteIdState.end()) {
        if (! releaseFlag) {
            msg    = "invalid write id";
            status = kErrParameters;
        }
    } else if (releaseFlag) {
        if (it->second.mReadOnlyFlag) {
            if (it->second.mBytesReserved > 0) {
                WAPPEND_LOG_STREAM_FATAL <<
                    "invalid write id state: " <<
                    it->second.mBytesReserved <<
                    " bytes reserved in read only state" <<
                KFS_LOG_EOM;
                FatalError();
            }
        } else if (it->second.mBytesReserved >= (size_t)nBytes) {
            it->second.mBytesReserved -= nBytes;
            mBytesReserved            -= nBytes;
        } else {
            mBytesReserved -= it->second.mBytesReserved;
            it->second.mBytesReserved = 0;
        }
    } else if (it->second.mReadOnlyFlag) {
        msg    = "no appends allowed with this write id";
        status = kErrParameters;
    } else {
        // Leave some room: in case of data compression, when we sort
        // and compress, the chunksize may actually grow. 
        // This line was:
        // if (mNextOffset + mBytesReserved + nBytes > off_t(CHUNKSIZE)) {
        if (mNextOffset + mBytesReserved + nBytes > 
            (off_t(CHUNKSIZE) - CHUNK_TRAILER_SPACE)) {
            msg    = "out of space";
            status = kErrOutOfSpace;
        } else {
            mBytesReserved            += nBytes;
            it->second.mBytesReserved += nBytes;
        }
    }
    if (errMsg) {
        (*errMsg) += msg;
    }
    if (mBytesReserved <= 0) {
        mTimer.ScheduleTimeoutNoLaterThanIn(
            gAtomicRecordAppendManager.GetCleanUpSec());
    }
    WAPPEND_LOG_STREAM(status == 0 ?
            MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelINFO) <<
        (releaseFlag ? "release: " : "reserve: ") << msg <<
        " state: "     << GetStateAsStr() <<
        " chunk: "    << mChunkId <<
        " writeId: "  << writeId <<
        " bytes: "    << nBytes <<
        " offset: "   << mNextOffset <<
        " reserved: " << mBytesReserved <<
        " delta: "    << ssize_t(prevReserved - mBytesReserved) <<
        " status: "   << status <<
    KFS_LOG_EOM;
    return status;
}

int
AtomicRecordAppender::InvalidateWriteId(int64_t writeId, bool declareFailureFlag)
{
    int status = 0;
    WriteIdState::iterator const it = mWriteIdState.find(writeId);
    if (it != mWriteIdState.end() &&
            it->second.mStatus == 0 &&
            it->second.mAppendCount == 0 &&
            ! it->second.mReadOnlyFlag) {
        // Entry with no appends, clean it up.
        // This is not orderly close, do not shorten close timeout.
        mWriteIdState.erase(it);
        if (mWriteIdState.empty()) {
            DecAppendersWithWidCount();
        }
        if (declareFailureFlag && mState == kStateOpen) {
            SetState(kStateReplicationFailed);
        }
    } else {
        status = kErrParameters;
    }
    WAPPEND_LOG_STREAM(status == 0 ?
            MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
        "invalidate write id" <<
        (declareFailureFlag ? " declare failure:" : ":") <<
        " wid: "        << writeId <<
        " chunk: "      << mChunkId <<
        " status: "     << status <<
        " state:  "     << GetStateAsStr() <<
        " wids count: " << mWriteIdState.size() <<
    KFS_LOG_EOM;
    return status;
}

void
AtomicRecordAppender::UpdateMasterCommittedOffset(off_t masterCommittedOffset)
{
    // Master piggy back its ack on the write append replication.
    // The ack can lag because of replication request pipelining. The ack is
    // used to determine op status if / when client has to use slave to perform
    // "get op status" in the case when the client can not communicate with the
    // master.
    // This is needed only to reduce client's failure resolution latency. By
    // comparing append end with this ack value it might be possible to
    // determine the append op status. If the ack value is greater than the
    // append end, then the append is successfully committed by all replication
    // participants.
    if (masterCommittedOffset >= mMasterCommittedOffset &&
            masterCommittedOffset <= mNextCommitOffset) {
        mMasterCommittedOffset = masterCommittedOffset;
    } else {
        WAPPEND_LOG_STREAM_ERROR <<
            "out of window master committed"
            " offset: " << masterCommittedOffset <<
            "[" << mMasterCommittedOffset <<
            "," << mNextCommitOffset << "]" <<
        KFS_LOG_EOM;
    }
}

void
AtomicRecordAppender::AppendBegin(
    RecordAppendOp *op, int replicationPos, ServerLocation peerLoc)
{
    if (op->numBytes < size_t(op->dataBuf.BytesConsumable()) ||
            op->origClnt) {
        WAPPEND_LOG_STREAM_FATAL <<
            "begin: short op buffer: " <<
            " req. size: "   << op->numBytes <<
            " buffer: "      << op->dataBuf.BytesConsumable() <<
            " or non null"
            " orig client: " << op->origClnt <<
            " " << op->Show() <<
        KFS_LOG_EOM;
        FatalError();
    }
    mLastActivityTime = Now();
    CheckLeaseAndChunk("begin");

    int         status = 0;
    std::string msg;
    ClientSM*   client = 0;
    if (op->chunkId != mChunkId) {
        status = kErrParameters;
        msg    = "invalid chunk id";
    } else if (mState != kStateOpen) {
        msg    = GetStateAsStr();
        status = kErrProtocolState;
    } else if (mPeerLocation != peerLoc) {
        status = kErrParameters;
        msg    = "invalid replication chain peer: " +
            peerLoc.ToString() + " expected: " + mPeerLocation.ToString();
    } else if (mReplicationPos != replicationPos) {
        status = kErrParameters;
        msg    = "invalid replication chain position";
    } else if (IsMaster() && op->fileOffset >= 0) {
        status = kErrParameters;
        msg    = "protocol error: offset specified for master";
    } else if (! IsMaster() && op->fileOffset < 0) {
        status = kErrParameters;
        msg    = "protocol error: offset not specified for slave";
    } else if (mNumServers != op->numServers) {
        status = kErrParameters;
        msg    = "invalid replication factor";
    } else if (mNextOffset + op->numBytes > off_t(CHUNKSIZE)) {
        msg    = "out of chunk space";
        status = kErrParameters;
    } else if (IsMaster() && op->clnt != this &&
            (client = dynamic_cast<ClientSM *>(op->clnt)) &&
            client->GetReservedSpace(mChunkId, op->writeId) < op->numBytes) {
        status = kErrParameters;
        msg    = "out of client reserved space";
    }
    if ((status != 0 || ! IsMaster()) && op->clnt == this) {
        WAPPEND_LOG_STREAM_FATAL <<
            "begin: bad internal op: " << op->Show() <<
        KFS_LOG_EOM;
        FatalError();
    }

    // Check if it is master 0 ack: no payload just commit offset.
    const bool masterAckflag = status == 0 && op->numBytes == 0 &&
        op->writeId == -1 &&
        (IsMaster() ? (op->clnt == this) : (op->masterCommittedOffset >= 0));
    WriteIdState::iterator const widIt = (masterAckflag || status != 0) ?
        mWriteIdState.end() : mWriteIdState.find(op->writeId);
    if (masterAckflag) {
        if (IsMaster()) {
            op->fileOffset = mNextOffset;
        } else if (op->fileOffset != mNextOffset) {
            // Out of order.
            msg    = "master 0 ack has invalid offset";
            status = kErrParameters;
            SetState(kStateReplicationFailed);
        } else {
            UpdateMasterCommittedOffset(op->masterCommittedOffset);
        }
    } else if (status == 0) {
        if (widIt == mWriteIdState.end()) {
            status = kErrParameters;
            msg    = "invalid write id";
        } else {
            WIdState& ws = widIt->second;
            if (! IsMaster()) {
                if (op->fileOffset != mNextOffset) {
                    // Out of order replication.
                    msg    = "invalid append offset";
                    status = kErrParameters;
                    SetState(kStateReplicationFailed);
                } else {
                    UpdateMasterCommittedOffset(op->masterCommittedOffset);
                    if (ws.mStatus == kErrStatusInProgress &&
                            mMasterCommittedOffset >=
                                ws.mOffset + off_t(ws.mLength)) {
                        ws.mStatus = 0; // Master committed.
                    }
                }
            }
            if (status != 0) {
                // Failed already.
            } if (ws.mReadOnlyFlag) {
                status = kErrWidReadOnly;
                msg    = "no appends allowed with this write id";
            } else if (ws.mStatus != 0) {
                // Force client to use multiple write ids to do request
                // pipelining, and allocate new write id after a failure.
                status = kErrParameters;
                msg    = ws.mStatus == kErrStatusInProgress ?
                    "has operation in flight" :
                    "invalid write id: previous append failed";
            }
        }
        if (status == 0) {
            const uint32_t checksum =
                ComputeBlockChecksum(&op->dataBuf, op->numBytes);
            if (op->checksum != checksum) {
                KFS_LOG_STREAM_WARN << "Problem in append---checksum mismatch: "
                    << " received: " << op->checksum 
                    << " computed: " << checksum
                    << " # of bytes on which checksum was computed: " 
                    << op->numBytes
                    << KFS_LOG_EOM;
                std::ostringstream os;
                os << "checksum mismatch: "
                    " received: " << op->checksum <<
                    " actual: " << checksum
                ;
                msg    = os.str();
                status = kErrParameters;
                Cntrs().mChecksumErrorCount++;
                if (! IsMaster()) {
                    SetState(kStateReplicationFailed);
                }
            }
        }
        if (status == 0) {
            assert(IsChunkOpen());
            if (IsMaster()) {
                // only on the write master is space reserved
                if (widIt->second.mBytesReserved < op->numBytes) {
                    status = kErrParameters;
                    msg    = "write id out of reserved space";
                } else if (mBytesReserved < op->numBytes) {
                    msg    = "out of reserved space";
                    status = kErrParameters;
                } else {
                    assert(mNextOffset + mBytesReserved <= off_t(CHUNKSIZE));
                    // Commit the execution.
                    assert(
                        widIt != mWriteIdState.end() &&
                        widIt->second.mStatus == 0
                    );
                    op->fileOffset = mNextOffset;
                    mBytesReserved -= op->numBytes;
                    widIt->second.mBytesReserved -= op->numBytes;
                    mNextOffset += op->numBytes;
                    // Decrease space reservation for this client connection.
                    // ClientSM space un-reservation in case of the subsequent
                    // failures is not needed because these failures will at
                    // least prevent any further writes with this write id.
                    if (client) {
                        client->UseReservedSpace(
                            mChunkId, op->writeId, op->numBytes);
                    }
                }
            } else {
                mNextOffset += op->numBytes;
            }
        }
    }
    // Empty appends (0 bytes) are always forwarded.
    // This is used by propagate master commit ack, and potentially can be used
    // for replication health check.
    RemoteSyncSMPtr peer;
    if (status == 0 && uint32_t(mReplicationPos + 1) < mNumServers) {
        if (! (peer = gChunkServer.FindServer(peerLoc))) {
            status = kErrReplicationFailed;
            msg    = "replication connection setup failure";
        } else {
            op->origSeq  = op->seq;
            op->seq      = peer->NextSeqnum();
            op->origClnt = op->clnt;
            op->clnt     = this;
        }
    }
    if (status == 0 && ! masterAckflag) {
        // Write id table is updated only in the case when execution is
        // committed. Otherwise the op is discarded, and treated like
        // it was never received.
        assert(widIt != mWriteIdState.end() && widIt->second.mStatus == 0);
        WIdState& ws = widIt->second;
        ws.mStatus = kErrStatusInProgress;
        ws.mLength = op->numBytes;
        ws.mOffset = op->fileOffset;
        ws.mSeq    = op->clientSeq;

        // Move blocks into the internal buffer.
        // The main reason to do this now, and not to wait for the replication
        // completion is to save io buffer space. Io buffers can be reclaimed
        // immediately after the data goes onto disk, and on the wire. Another
        // way to look at this: socket buffer space becomes an extension to
        // write appender io buffer space.
        // The price is "undoing" writes, which might be necessary in the case of
        // replication failure. Undoing writes is a simple truncate, and the the
        // failures aren't expected to be frequent enough to matter.
        const int prevNumBytes = mBuffer.BytesConsumable();
        // Always try to append to the last buffer.
        // Flush() keeps track of the write offset and "slides" buffers
        // accordingly.
        if (op->numBytes > 0) {
            assert(mBufFrontPadding == 0 || mBuffer.IsEmpty());
            IOBuffer dataBuf;

            // Copy buffer before moving data into appender's write buffer.
            // Enqueue for replication at the end.
            // Replication completion invokes AppendCommit().
            dataBuf.Copy(&op->dataBuf, op->numBytes);

            mBuffer.ReplaceKeepBuffersFull(
                &dataBuf,
                mBuffer.BytesConsumable() + mBufFrontPadding,
                op->numBytes
            );

            if (mBufFrontPadding > 0) {
                mBuffer.Consume(mBufFrontPadding);
                mBufFrontPadding = 0;
                assert(! mBuffer.IsEmpty());
            }
        }
        // Do space accounting and flush if needed.
        if (mBuffer.BytesConsumable() >=
                gAtomicRecordAppendManager.GetFlushLimit(*this,
                    mBuffer.BytesConsumable() - prevNumBytes)) {
            // Align the flush to checksum boundaries.
            FlushFullBlocks();
        } else {
            if (! mBuffer.IsEmpty()) {
                mTimer.ScheduleTimeoutNoLaterThanIn(
                    gAtomicRecordAppendManager.GetFlushIntervalSec());
            }
            SetCanDoLowOnBuffersFlushFlag(! mBuffer.IsEmpty());
        }
    }
    op->status = status;
    if (status != 0) {
        op->statusMsg = msg;
    }
    WAPPEND_LOG_STREAM(status == 0 ?
            MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
        "begin: "           << msg <<
            (masterAckflag ? " master ack" : "") <<
        " state: "        << GetStateAsStr() <<
        " reserved: "     << mBytesReserved <<
        " offset: next: " << mNextOffset <<
        " commit: "       << mNextCommitOffset <<
        " master: "       << mMasterCommittedOffset <<
        " in flight:"
        " replicaton: "   << mReplicationsInFlight <<
        " ios: "          << mIoOpsInFlight <<
        " status: "       << status <<
        " " << op->Show() <<
    KFS_LOG_EOM;
    mReplicationsInFlight++;
    op->replicationStartTime = Now();
    AppendReplicationList::PushBack(mReplicationList, *op);
    
    // fwd to the sort helper
    if (gChunkSortHelperManager.IsSorterInStreamingMode())
        mSortHelper->Enqueue(op);

    if (op->origClnt) {
        assert(status == 0);
        if (IsMaster()) {
            op->masterCommittedOffset = mNextCommitOffset;
            mCommitOffsetAckSent = mNextCommitOffset;
        }
        if (mReplicationsInFlight == 1) {
            mTimer.ScheduleTimeoutNoLaterThanIn(
                gAtomicRecordAppendManager.GetReplicationTimeoutSec());
        }
        peer->Enqueue(op);
    } else {
        OpDone(op);
    }
}

int
AtomicRecordAppender::GetNextReplicationTimeout() const
{
    if (mReplicationsInFlight <= 0 || mState != kStateOpen) {
        return -1;
    }
    const int timeout = gAtomicRecordAppendManager.GetReplicationTimeoutSec();
    if (timeout < 0) {
        return -1;
    }
    const time_t now               = Now();
    const RecordAppendOp* const op =
            AppendReplicationList::Front(mReplicationList);
    assert(op);
    const time_t end               = op->replicationStartTime + timeout;
    return (now < end ? end - now : 0);
}

void
AtomicRecordAppender::OpDone(RecordAppendOp* op)
{
    assert(
        mReplicationsInFlight > 0 &&
        AppendReplicationList::IsInList(mReplicationList, *op)
    );
    mReplicationsInFlight--;
    AppendReplicationList::Remove(mReplicationList, *op);
    if (mReplicationsInFlight > 0 && mState == kStateOpen) {
        mTimer.ScheduleTimeoutNoLaterThanIn(GetNextReplicationTimeout());
    }
    // Do not commit malformed client requests.
    const bool commitFlag = ! IsMaster() || op->origClnt || op->status == 0;
    if (op->origClnt) {
        op->seq   = op->origSeq;
        op->clnt  = op->origClnt;
        op->origClnt = 0;
    }
    if (commitFlag) {
        AppendCommit(op);
    }
    // Delete if commit ack.
    const bool deleteOpFlag = op->clnt == this;
    if (deleteOpFlag) {
        delete op;
    }
    DeleteIfNeeded();
    if (! deleteOpFlag) {
        Cntrs().mAppendCount++;
        if (op->status >= 0) {
            Cntrs().mAppendByteCount += op->numBytes;
        } else {
            Cntrs().mAppendErrorCount++;
            if (mState == kStateReplicationFailed) {
                Cntrs().mReplicationErrorCount++;
            }
        }
        KFS::SubmitOpResponse(op);
    }
}

void
AtomicRecordAppender::AppendCommit(RecordAppendOp *op)
{
    mLastActivityTime = Now();
    if (mState != kStateOpen) {
        op->status    = kErrStatusInProgress; // Don't know
        op->statusMsg = "closed for append; op status undefined; state: ";
        op->statusMsg += GetStateAsStr();
        WAPPEND_LOG_STREAM_ERROR <<
            "commit: " << op->statusMsg <<
            " status: " << op->status <<
            " " << op->Show() <<
        KFS_LOG_EOM;
        return;
    }
    // Always declare failure here if op status is non zero.
    // Theoretically it is possible to recover from errors such "write id is
    // read only", *by moving buffer append here* in AppendCommit, from
    // AppendBegin. In such case the protocol ensures that no partial
    // replication can succeed if an error status is *received*.
    // The problem is that there might be more than one replications in flight,
    // and waiting for all replications that are currently in flight to fail is
    // required to successfully recover.
    // On the other hand the price for declaring a failure is only partial
    // (non full) chunk.
    // For now assume that the replication failures are infrequent enough to
    // have any significant effect on the chunk size, and more efficient use of
    // io buffer space is more important (see comment in AppendBegin).
    if (op->status != 0) {
        op->statusMsg += " op (forwarding) failed, op status undefined"
            "; state: ";
        op->statusMsg += GetStateAsStr();
        WAPPEND_LOG_STREAM_ERROR <<
            "commit: " << op->statusMsg <<
            " status: "       << op->status <<
            " reserved: "     << mBytesReserved <<
            " offset: "       << mNextCommitOffset <<
            " nextOffset: "   << mNextOffset <<
            " " << op->Show() <<
        KFS_LOG_EOM;
        op->status = kErrStatusInProgress; // Don't know
        SetState(kStateReplicationFailed);
        return;
    }
    // AppendBegin checks if write id is read only.
    // If write id wasn't read only in the append begin, it cannot transition
    // into into read only between AppendBegin and AppendCommit, as it should
    // transition into "in progress" in the AppendBegin, and stay "in progress"
    // at least until here.
    // If the op is internally generated 0 ack verify that it has no payload.
    WriteIdState::iterator const widIt = op->clnt == this ?
        mWriteIdState.end() : mWriteIdState.find(op->writeId);
    if (op->fileOffset != mNextCommitOffset ||
            op->chunkId != mChunkId ||
            op->chunkVersion != mChunkVersion ||
            (widIt == mWriteIdState.end() ?
                ((IsMaster() ? (op->clnt != this) :
                    (op->masterCommittedOffset < 0)) ||
                op->numBytes != 0 || op->writeId != -1) :
                widIt->second.mStatus != kErrStatusInProgress)) {
        WAPPEND_LOG_STREAM_FATAL <<
            "commit: out of order or invalid op" <<
            " chunk: "        << mChunkId <<
            " chunkVersion: " << mChunkVersion <<
            " reserved: "     << mBytesReserved <<
            " offset: "       << mNextCommitOffset <<
            " nextOffset: "   << mNextOffset <<
            " " << op->Show() <<
        KFS_LOG_EOM;
        FatalError();
        return;
    }

    // Do not pay attention to the lease expiration here.
    // If lease has expired, but no make stable was received, then
    // commit the append anyway.
    op->status = 0;
    if (widIt != mWriteIdState.end()) {
        mNextCommitOffset += op->numBytes;
        mAppendCommitCount++;
        widIt->second.mAppendCount++;
    }
    if (IsMaster()) {
        // Only write master can declare a success, he is the last to commit,
        // and only in the case if all slaves committed.
        if (widIt != mWriteIdState.end()) {
            widIt->second.mStatus = 0;
        }
        // Schedule to send commit ack.
        if (mNumServers > 1 && mReplicationsInFlight <= 0 &&
                mNextCommitOffset > mCommitOffsetAckSent) {
            mTimer.ScheduleTimeoutNoLaterThanIn(
                gAtomicRecordAppendManager.GetSendCommitAckTimeoutSec());
        }
    }
    WAPPEND_LOG_STREAM_DEBUG <<
        "commit:"
        " state: "        << GetStateAsStr() <<
        " reserved: "     << mBytesReserved <<
        " offset: next: " << mNextOffset <<
        " commit: "       << mNextCommitOffset <<
        " master: "       << mMasterCommittedOffset <<
        " in flight:"
        " replication: "  << mReplicationsInFlight <<
        " ios: "          << mIoOpsInFlight <<
        " status: "       << op->status <<
        " " << op->Show() <<
    KFS_LOG_EOM;
}

void
AtomicRecordAppender::GetOpStatus(GetRecordAppendOpStatus* op)
{
    mLastActivityTime = Now();

    int         status = 0;
    const char* msg    = "ok";
    if (op->chunkId != mChunkId) {
        msg    = "invalid chunk id";
        status = kErrParameters;
    } else {
        WriteIdState::iterator const widIt = mWriteIdState.find(op->writeId);
        if (widIt == mWriteIdState.end()) {
            msg    = "no such write id";
            status = kErrNotFound;
        } else {
            WIdState& ws = widIt->second;
            assert(
                ws.mBytesReserved == 0 ||
                (IsMaster() && ! ws.mReadOnlyFlag &&
                mBytesReserved >= ws.mBytesReserved)
            );
            if (ws.mStatus == kErrStatusInProgress) {
                const off_t end = ws.mOffset + ws.mLength;
                if (mMakeStableSucceededFlag) {
                    ws.mStatus = mChunkSize >= end ? 0 : kErrFailedState;
                } else if (! IsMaster() &&  mMasterCommittedOffset >= end) {
                    ws.mStatus = 0;
                }
                if (ws.mStatus != kErrStatusInProgress) {
                    WAPPEND_LOG_STREAM_DEBUG <<
                        "get op status:"
                        " changed status from in progress"
                        " to: "               << ws.mStatus <<
                        " chunk: "            << mChunkId <<
                        " writeId: "          << op->writeId <<
                        " op end: "           << (ws.mOffset + ws.mLength) <<
                        " master committed: " << mMasterCommittedOffset <<
                        " state: "            << GetStateAsStr() <<
                        " chunk size: "       << mChunkSize <<
                    KFS_LOG_EOM;
                }
            }
            op->opStatus           = ws.mStatus;
            op->opLength           = ws.mLength;
            op->opOffset           = ws.mOffset;
            op->opSeq              = ws.mSeq;
            op->widAppendCount     = ws.mAppendCount;
            op->widBytesReserved   = ws.mBytesReserved;
            op->widWasReadOnlyFlag = ws.mReadOnlyFlag;

            op->chunkVersion       = mChunkVersion;
            op->chunkBytesReserved = mBytesReserved;
            op->remainingLeaseTime = IsMaster() ?
                gLeaseClerk.GetLeaseExpireTime(mChunkId) - Now() : -1;
            op->masterFlag         = IsMaster();
            op->stableFlag         = mState == kStateStable;
            op->appenderState      = mState;
            op->appenderStateStr   = GetStateAsStr();
            op->openForAppendFlag  = mState == kStateOpen;
            op->masterCommitOffset = mMasterCommittedOffset;
            op->nextCommitOffset   = mNextCommitOffset;

            // The status inquiry always makes write id read only, and
            // un-reserves the space, thus disallowing further appends with this
            // write id. This guarantees that after status inquiry returns, no
            // other [new, or stale] appends with this write id can succeed.
            //
            // For now this the behaviour is the same for all replication
            // participants.
            // This speeds up recovery for the clients that can not communicate
            // with the replication master.
            // The price for this is lower resistance to "DoS", where status
            // inquiry with any replication slave when append replication is
            // still in flight but haven't reached the slave can make chunks
            // less full.
            if (mBytesReserved >= ws.mBytesReserved) {
                mBytesReserved -= ws.mBytesReserved;
            } else {
                mBytesReserved = 0;
            }
            ws.mBytesReserved = 0;
            ws.mReadOnlyFlag  = true;
            op->widReadOnlyFlag = ws.mReadOnlyFlag;
        }
    }
    op->status = status;
    if (status != 0) {
        op->statusMsg = msg;
    }
    WAPPEND_LOG_STREAM(status == 0 ?
            MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelINFO) <<
        "get op status: " << msg <<
        " state: "           << GetStateAsStr() <<
        " chunk: "           << mChunkId <<
        " wid count: "       << mWriteIdState.size() <<
        " offset: "          << mNextOffset <<
        " reserved: "        << mBytesReserved <<
        " writeId: "         << op->writeId <<
        " status: "          << status <<
        " master comitted: " << mMasterCommittedOffset <<
        " " << op->Show() <<
    KFS_LOG_EOM;
}

void
AtomicRecordAppender::CloseChunk(CloseOp* op, int64_t writeId, bool& forwardFlag)
{
    mLastActivityTime = Now();

    int         status = 0;
    const char* msg    = "ok";
    if (op->chunkId != mChunkId) {
        msg    = "invalid chunk id";
        status = kErrParameters;
    } else if (op->hasWriteId) {
        WriteIdState::iterator const widIt = mWriteIdState.find(writeId);
        if (widIt == mWriteIdState.end()) {
            msg    = "no such write id";
            status = kErrNotFound;
        } else {
            if (! IsMaster()) {
                // Update master commit offset, and last op status if needed,
                // and possible.
                if (op->masterCommitted >= 0) {
                    UpdateMasterCommittedOffset(op->masterCommitted);
                }
                WIdState& ws = widIt->second;
                if (ws.mStatus == kErrStatusInProgress) {
                    const off_t end = ws.mOffset + ws.mLength;
                    if ((mMakeStableSucceededFlag ?
                            mChunkSize : mMasterCommittedOffset) >= end) {
                        ws.mStatus = 0;
                    }
                }
            }
            if (widIt->second.mStatus == kErrStatusInProgress) {
                msg    = "write id has op in flight";
                status = kErrStatusInProgress;
            } else if (widIt->second.mReadOnlyFlag) {
                msg    = "write id is read only";
                status = kErrParameters;
            } else if (widIt->second.mStatus != 0) {
                msg    = "append failed with this write id";
                status = kErrParameters;
            } else {
                // The entry is in good state, and the client indicates that he
                // does not intend to use this write id for any purpose in the
                // future. Reclaim the reserved space, and discard the entry.
                if (mBytesReserved >= widIt->second.mBytesReserved) {
                    mBytesReserved -= widIt->second.mBytesReserved;
                } else {
                    mBytesReserved = 0;
                }
                mWriteIdState.erase(widIt);
                if (mWriteIdState.empty()) {
                    DecAppendersWithWidCount();
                }
                if (IsMaster() && mState == kStateOpen &&
                        mWriteIdState.empty()) {
                    // For orderly close case shorten close timeout.
                    // Furthermore, if the chunk is nearly full, use a
                    // small timeout of 3 seconds---so that anything
                    // in-flight/pending get taken care of.
                    const bool canEarlyClose = 
                        (KFS::CHUNKSIZE - mNextCommitOffset) <= (1 << 20);
                    if (canEarlyClose) {
                        WAPPEND_LOG_STREAM_INFO << "Setting up for early close on chunk: " 
                            << mChunkId << KFS_LOG_EOM;
                        mTimer.ScheduleTimeoutNoLaterThanIn(3);
                    } else {
                        // in this case, we want to wait a bit for new
                        // writers to show up and use this chunk.
                        mTimer.ScheduleTimeoutNoLaterThanIn(Timer::MinTimeout(
                                gAtomicRecordAppendManager.GetCleanUpSec(),
                                gAtomicRecordAppendManager.GetCloseEmptyWidStateSec()
                                ));
                    }
                }
            }
        }
    }
    forwardFlag = forwardFlag && status == 0;
    op->status  = status;
    if (status != 0) {
        op->statusMsg = msg;
    }
    if (IsMaster()) {
        // Always send commit offset, it might be need to transition write id
        // into "good" state.
        // Write ids are deleted only if these are in good state, mostly for
        // extra "safety", and to simplify debugging.
        // For now do not update the acked: close is "no reply", it can be
        // simply dropped.
        op->masterCommitted = (forwardFlag && op->hasWriteId) ?
            mNextCommitOffset : -1;
    }
    WAPPEND_LOG_STREAM(status == 0 ?
            MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
        "close chunk status: " << msg <<
        " state: "     << GetStateAsStr() <<
        " chunk: "     << mChunkId <<
        " wid count: " << mWriteIdState.size() <<
        " offset: "    << mNextOffset <<
        " reserved: "  << mBytesReserved <<
        " writeId: "   << writeId <<
        " status: "    << status <<
        " " << op->Show() <<
    KFS_LOG_EOM;
}

bool
AtomicRecordAppender::ComputeChecksum()
{
    const bool ok = ComputeChecksum(
        mChunkId, mChunkVersion, mChunkSize, mChunkChecksum);
    if (! ok) {
        mChunkChecksum = 0;
        mChunkSize     = -1;
        SetState(kStateChunkLost);
    }
    return ok;
}

bool
AtomicRecordAppender::ComputeChecksum(
    kfsChunkId_t chunkId, int64_t chunkVersion,
    off_t& chunkSize, uint32_t& chunkChecksum)
{
    const ChunkInfo_t* const info = gChunkManager.GetChunkInfo(chunkId);
    if (! info ||
            (! info->chunkBlockChecksum && info->chunkSize != 0) ||
            chunkVersion != info->chunkVersion) {
        return false;
    }
    chunkSize = info->chunkSize;
    const uint32_t* checksums = info->chunkBlockChecksum;
    // Print it as text, to make byte order independent.
    std::ostringstream os;
    for (off_t i = 0; i < chunkSize; i += CHECKSUM_BLOCKSIZE) {
        os << *checksums++;
    }
    chunkChecksum = ComputeBlockChecksum(os.str().c_str(), os.str().length());
    return true;
}

void
AtomicRecordAppender::SubmitResponse(BeginMakeChunkStableOp& op)
{
    mLastActivityTime = Now();
    if (op.status == 0) {
        op.status = mState == kStateClosed ? 0 : kErrFailedState;
        if (op.status != 0) {
            op.statusMsg = "record append failure; state: ";
            op.statusMsg += GetStateAsStr();
        } else {
            if (mChunkSize < 0) {
                WAPPEND_LOG_STREAM_FATAL <<
                    "begin make stable response: "
                    " chunk: "        << mChunkId <<
                    " invalid size: " << mChunkSize <<
                KFS_LOG_EOM;
                FatalError();
            }
            op.status        = 0;
            op.chunkSize     = mChunkSize;
            op.chunkChecksum = mChunkChecksum;
        }
    }
    WAPPEND_LOG_STREAM(op.status == 0 ?
        MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
        "begin make stable done: " << op.statusMsg <<
        " "             << op.Show() <<
        " size: "       << mChunkSize <<
        " checksum: "   << mChunkChecksum <<
        " state: "      << GetStateAsStr() <<
        " wid count: "  << mWriteIdState.size() <<
    KFS_LOG_EOM;
    Cntrs().mBeginMakeStableCount++;
    if (op.status < 0) {
        Cntrs().mBeginMakeStableErrorCount++;
    }
    KFS::SubmitOpResponse(&op);
}

void
AtomicRecordAppender::BeginMakeStable(
    BeginMakeChunkStableOp* op /* = 0 */)
{
    WAPPEND_LOG_STREAM_DEBUG <<
        "begin make stable: "
        " chunk: "      << mChunkId <<
        " state: "      << GetStateAsStr() <<
        " wid count: "  << mWriteIdState.size() <<
        " offset: "     << mNextOffset <<
        " reserved: "   << mBytesReserved <<
        " in flight:"
        " replicaton: " << mReplicationsInFlight <<
        " ios: "        << mIoOpsInFlight <<
        " " << (op ? op->Show() : std::string("no op")) <<
    KFS_LOG_EOM;

    mLastActivityTime = Now();
    if (op) {
        if (mChunkVersion != op->chunkVersion) {
            op->statusMsg = "invalid chunk version";
            op->status    = kErrParameters;
            SubmitResponse(*op);
            return;
        }
        op->status = 0;
    }
    // Only meta server issues this command, when it decides that
    // the append master is not operational.
    PushBack(mBeginMakeChunkStableOp, mLastBeginMakeChunkStableOp, op);
    if (mMakeChunkStableOp) {
        return; // Wait for make stable to finish, it will send the reply.
    }
    if (mState == kStateOpen || mState == kStateReplicationFailed) {
        SetState(kStateClosed);
    }
    if (mState == kStateClosed) {
        FlushAll();
        if (mNextCommitOffset < mNextWriteOffset) {
            // Always truncate to the last commit offset.
            // This is need to properly handle status inquiry that makes write
            // id read only (prevents further ops with this write id to
            // succeed). If the append is in flight and status inquiry op
            // reaches this node first, then this will guarantee that no
            // participant will commit the in flight append, as it will never
            // receive the append (down the replicaton chain), or replication
            // succeeded status (up the replicaton chain).
            // If the status inquiry op comes in after the append op, then
            // the last op status will be "in progress" (undefined), or already
            // known.
            TrimToLastCommit("begin make stable");
            return;
        }
    }
    if (mState == kStateClosed && mIoOpsInFlight > 0) {
        return; // Completion will be invoked later.
    }
    if (mState == kStateClosed && mChunkSize < 0) {
        ComputeChecksum();
    }
    SubmitResponse(mBeginMakeChunkStableOp, mLastBeginMakeChunkStableOp);
}

bool
AtomicRecordAppender::WantsToKeepLease() const
{
    return (IsMaster() && ! IsChunkStable());
}

void
AtomicRecordAppender::Timeout()
{
    if (DeleteIfNeeded()) {
        return;
    }
    int         nextTimeout    = -1; // infinite
    const int   flushInterval  =
        gAtomicRecordAppendManager.GetFlushIntervalSec();
    // Slaves keep chunk open longer, waiting for [begin] make stable from meta.
    // Ideally the slave timeout should come from master.
    // For now assume that the master and slave have the same value.
    const int   cleanupTimeout = gAtomicRecordAppendManager.GetCleanUpSec() *
        ((IsMaster() || mState != kStateOpen) ? 1 : 4);
    const time_t now           = Now();
    if (mBuffer.BytesConsumable() >=
                gAtomicRecordAppendManager.GetFlushLimit(*this) ||
            (flushInterval >= 0 && mLastFlushTime + flushInterval <= now)) {
        FlushFullBlocks();
    } else if (! mBuffer.IsEmpty() && flushInterval >= 0) {
        nextTimeout = Timer::MinTimeout(nextTimeout,
            int(mLastFlushTime + flushInterval - now));
    }
    if (IsMaster() && mState == kStateOpen) {
        const int ackTm =
            gAtomicRecordAppendManager.GetSendCommitAckTimeoutSec();
        if (ackTm > 0 &&
                mNumServers > 1 && mReplicationsInFlight <= 0 &&
                mNextCommitOffset > mCommitOffsetAckSent) {
            if (mLastActivityTime + ackTm <= now) {
                SendCommitAck();
            } else {
                nextTimeout = Timer::MinTimeout(nextTimeout,
                    int(mLastActivityTime + ackTm - now));
            }
        }

        // If there is less than 1M free space in the chunk, can do an
        // early close---when there is no activity/no reservation.
        const bool canEarlyClose = (KFS::CHUNKSIZE - mNextCommitOffset) <= (1 << 20);
        
        const int emptyWidTimeout = canEarlyClose ? 0 :
            gAtomicRecordAppendManager.GetCloseEmptyWidStateSec();

        if (canEarlyClose) {
            WAPPEND_LOG_STREAM_INFO << "Doing possible early close on chunk: " 
                << mChunkId << KFS_LOG_EOM;
        }

        // If no activity, and no reservations, then master closes the chunk.
        const int closeTimeout = mWriteIdState.empty() ? Timer::MinTimeout(
                cleanupTimeout,
                emptyWidTimeout
            ) : cleanupTimeout;
        if (mBytesReserved <= 0 &&
                closeTimeout >= 0 && mState == kStateOpen) {
            if (mLastActivityTime + closeTimeout <= now) {
                if (! TryToCloseChunk()) {
                    // TryToCloseChunk hasn't scheduled new activity, most
                    // likely are replications in flight. If this is the case
                    // then the cleanup timeout is too short, just retry in 3
                    // sec.
                    // To avoid this "busy wait"  with short timeout an
                    // additional state is needed in the state machine.
                    assert(mReplicationsInFlight > 0);
                    if (mLastActivityTime + closeTimeout <= now) {
                        nextTimeout = Timer::MinTimeout(nextTimeout, 3);
                    }
                }
            } else {
                nextTimeout = Timer::MinTimeout(nextTimeout,
                    int(mLastActivityTime + closeTimeout - now));
            }
        }
    } else if (cleanupTimeout >= 0 &&
            mIoOpsInFlight <= 0 && mReplicationsInFlight <= 0 &&
            mLastActivityTime + cleanupTimeout <= now) {
        time_t metaUptime;
        int    minMetaUptime;
        if (! mMakeStableSucceededFlag && mState != kStateChunkLost &&
                (metaUptime = gMetaServerSM.ConnectionUptime()) <
                (minMetaUptime = std::max(
                    cleanupTimeout,
                    gAtomicRecordAppendManager.GetMetaMinUptimeSec()
                ))) {
            WAPPEND_LOG_STREAM_INFO << "timeout:"
                " short meta connection uptime;"
                " required uptime: "   << minMetaUptime << " sec."
                " last activty: "      << (now - mLastActivityTime) << " ago" <<
                " new last activity: " << metaUptime                << " ago" <<
            KFS_LOG_EOM;
            mLastActivityTime = now - std::max(time_t(0), metaUptime);
        } else {
            WAPPEND_LOG_STREAM(mMakeStableSucceededFlag ?
                    MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelWARN) <<
                "timeout: deleting write appender"
                " chunk: "     << mChunkId <<
                " state: "     << GetStateAsStr() <<
                " size: "      << mNextWriteOffset << " / " << mChunkSize <<
                " wid count: " << mWriteIdState.size() <<
            KFS_LOG_EOM;
            if (! mMakeStableSucceededFlag) {
                Cntrs().mTimeoutLostCount++;
            }
            if (mState != kStateStable) {
                SetState(kStateChunkLost);
            }
            Delete();
            return;
        }
    }
    nextTimeout = Timer::MinTimeout(nextTimeout,
        mLastActivityTime + cleanupTimeout > now ?
        int(mLastActivityTime + cleanupTimeout - now) : cleanupTimeout);
    if (mState == kStateOpen && mReplicationsInFlight > 0) {
        const int replicationTimeout = GetNextReplicationTimeout();
        if (replicationTimeout == 0) {
            const RecordAppendOp* const op =
                AppendReplicationList::Front(mReplicationList);
            WAPPEND_LOG_STREAM_ERROR <<
                "replication timeout:"
                " chunk: "   << mChunkId <<
                " state: "   << GetStateAsStr() <<
                " optime: "  << (now - op->replicationStartTime) <<
                " cmd: " << op->Show() <<
            KFS_LOG_EOM;
            Cntrs().mReplicationTimeoutCount++;
            SetState(kStateReplicationFailed);
        } else {
            nextTimeout = Timer::MinTimeout(replicationTimeout, nextTimeout);
        }
    }
    WAPPEND_LOG_STREAM_DEBUG <<
        "set timeout:"
        " chunk: "   << mChunkId <<
        " state: "   << GetStateAsStr() <<
        " timeout: " << nextTimeout <<
    KFS_LOG_EOM;
    mTimer.SetTimeout(nextTimeout);
}

void
AtomicRecordAppender::SubmitResponse(MakeChunkStableOp& op)
{
    op.status = mState == kStateStable ? 0 : kErrFailedState;
    if (op.status != 0) {
        if (! op.statusMsg.empty()) {
            op.statusMsg += " ";
        }
        op.statusMsg += "record append failure; state: ";
        op.statusMsg += GetStateAsStr();
    }
    WAPPEND_LOG_STREAM(op.status == 0 ?
            MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelERROR) <<
        "make chunk stable done:"
        " chunk: "       << mChunkId <<
        " state: "       << GetStateAsStr() <<
        " size: "        << mNextWriteOffset << " / " << mChunkSize <<
        " checksum: "    << mChunkChecksum <<
        " in flight:"
        " replication: " << mReplicationsInFlight <<
        " ios: "         << mIoOpsInFlight <<
        " " << op.Show() <<
    KFS_LOG_EOM;
    if (op.clnt == this) {
        delete &op;
    } else {
        Cntrs().mMakeStableCount++;
        if (op.status != 0) {
            Cntrs().mMakeStableErrorCount++;
        }
        if (mChunkSize >= 0) {
            if (mChunkSize < op.chunkSize && op.chunkSize >= 0) {
                Cntrs().mMakeStableLengthErrorCount++;
            }
            if (op.hasChecksum && mChunkChecksum != op.chunkChecksum) {
                Cntrs().mMakeStableChecksumErrorCount++;
            }
        }
        KFS::SubmitOpResponse(&op);
    }
}

void
AtomicRecordAppender::MakeChunkStableDone()
{
    mLastActivityTime = Now();
    SubmitResponse(mBeginMakeChunkStableOp, mLastBeginMakeChunkStableOp);
    SubmitResponse(mMakeChunkStableOp, mLastMakeChunkStableOp);
}

void
AtomicRecordAppender::OpDone(ReadOp* op)
{
    // Only read to truncate the chunk should ever get here, and it should be
    // the only one op in flight.
    if (! op ||
            ! mMakeChunkStableOp ||
            mIoOpsInFlight != 1 ||
            (mState != kStateClosed &&
                mState != kStatePendingDelete &&
                mState != kStateChunkLost) ||
            (mMakeChunkStableOp->chunkSize >= 0 &&
            mMakeChunkStableOp->chunkSize !=
                op->offset + off_t(op->numBytes)) ||
            op->offset < 0 ||
            op->numBytes <= 0 ||
            op->offset % CHECKSUM_BLOCKSIZE != 0 ||
            op->numBytes >= CHECKSUM_BLOCKSIZE) {
        WAPPEND_LOG_STREAM_FATAL <<
            "make chunk stable read:" <<
            " internal error"
            " chunk: "          << mChunkId <<
            " read op: "        << (const void*)op <<
            " make stable op: " << (const void*)mMakeChunkStableOp <<
            " in flight:"
            " replication: "    << mReplicationsInFlight <<
            " ios: "            << mIoOpsInFlight <<
        KFS_LOG_EOM;
        FatalError();
    }
    mIoOpsInFlight--;
    if (DeleteIfNeeded()) {
        delete op;
        return;
    }
    if (op->status >= 0 && ssize_t(op->numBytes) == op->numBytesIO) {
        ChunkInfo_t* const info = gChunkManager.GetChunkInfo(mChunkId);
        if (! info || (! info->chunkBlockChecksum && info->chunkSize != 0)) {
            WAPPEND_LOG_STREAM_FATAL <<
                "make chunk stable read:"
                " failed to get chunk info" <<
                " chunk: "     << mChunkId <<
                " checksums: " <<
                    (const void*)(info ? info->chunkBlockChecksum : 0) <<
                " size: "      << (info ? info->chunkSize : -1) <<
            KFS_LOG_EOM;
            FatalError();
            SetState(kStateChunkLost);
        } else {
            const off_t newSize = op->offset + op->numBytes;
            if (info->chunkSize < newSize) {
                mChunkSize = -1;
                SetState(kStateChunkLost);
            } else {
                op->dataBuf->ZeroFill(CHECKSUM_BLOCKSIZE - op->numBytes);
                info->chunkBlockChecksum[OffsetToChecksumBlockNum(newSize)] =
                    ComputeBlockChecksum(op->dataBuf,
                        op->dataBuf->BytesConsumable());
                // Truncation done, set the new size.
                info->chunkSize = newSize;
                mNextWriteOffset = newSize;
            }
        }
    } else {
        Cntrs().mReadErrorCount++;
        SetState(kStateChunkLost);
    }
    WAPPEND_LOG_STREAM(mState != kStateChunkLost ?
            MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
        "make chunk stable read:"
        " status: "      << op->status <<
        " requested: "   << op->numBytes <<
        " read: "        << op->numBytesIO <<
        " chunk: "       << mChunkId <<
        " state: "       << GetStateAsStr() <<
        " size: "        << mNextWriteOffset << " / " << mChunkSize <<
        " checksum: "    << mChunkChecksum << " / " <<
            mMakeChunkStableOp->chunkChecksum <<
        " in flight:"
        " replication: " << mReplicationsInFlight <<
        " ios: "         << mIoOpsInFlight <<
    KFS_LOG_EOM;
    delete op;
    MakeChunkStable();
}

void
AtomicRecordAppender::MakeChunkStable(MakeChunkStableOp *op /* = 0 */)
{
    mLastActivityTime = Now();
    if (op) {
        MakeChunkStableOp* eo = mMakeChunkStableOp;
        if (eo && eo->clnt == this) {
            eo = eo->next; // get "external" op
            if (eo && eo->clnt == this) {
                FatalError(); // only one "internal" op
            }
        }
        if (op->chunkId != mChunkId) {
            op->status    = kErrParameters;
            op->statusMsg = "invalid chunk id";
        } else if (op->chunkVersion != mChunkVersion) {
            op->status    = kErrParameters;
            op->statusMsg = "invalid chunk version";
        } else if (eo && (
                eo->chunkVersion != op->chunkVersion ||
                ((eo->chunkSize >= 0 && op->chunkSize >= 0 &&
                    eo->chunkSize != op->chunkSize) ||
                (eo->hasChecksum && op->hasChecksum &&
                    eo->chunkChecksum != op->chunkChecksum)))) {
            op->status    = kErrParameters;
            op->statusMsg =
                "request parameters differ from the initial request";
        }
        if (op->status != 0) {
            WAPPEND_LOG_STREAM_ERROR <<
                "make chunk stable: bad request ignored " << op->statusMsg <<
                " chunk: "   << mChunkId      <<
                " version: " << mChunkVersion <<
                " "          << op->Show() <<
            KFS_LOG_EOM;
            if (op->clnt == this) {
                FatalError();
                delete op;
            } else {
                Cntrs().mMakeStableCount++;
                Cntrs().mMakeStableErrorCount++;
                KFS::SubmitOpResponse(op);
            }
            return;
        }
        PushBack(mMakeChunkStableOp, mLastMakeChunkStableOp, op);
    }
    WAPPEND_LOG_STREAM(mMakeChunkStableOp ?
            MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelFATAL) <<
        "make chunk stable " <<
            ((mMakeChunkStableOp && mMakeChunkStableOp->clnt == this) ?
                "internal" : "external") << ":" <<
        " chunk: "       << mChunkId <<
        " state: "       << GetStateAsStr() <<
        " size: "        << mNextWriteOffset <<
        " in flight:"
        " replication: " << mReplicationsInFlight <<
        " ios: "      << mIoOpsInFlight <<
        (op ? " " : "")  << (op ? op->Show() : "") <<
    KFS_LOG_EOM;
    if (! mMakeChunkStableOp) {
        FatalError();
    }
    if (mState == kStateOpen || mState == kStateReplicationFailed) {
        SetState(kStateClosed);
        FlushAll();
    }
    // Wait for previously submitted ops to finish
    if (mIoOpsInFlight > 0) {
        return;
    }
    if (mState != kStateClosed) {
        MakeChunkStableDone();
        return;
    }
    if (mMakeChunkStableOp->chunkSize >= 0) {
        const off_t newSize = mMakeChunkStableOp->chunkSize;
        if (newSize > mNextWriteOffset) {
            SetState(kStateChunkLost);
        } else if (newSize < mNextWriteOffset) {
            WAPPEND_LOG_STREAM_INFO <<
                "make chunk stable: truncating chunk to: " << newSize <<
                " current size: " << mNextWriteOffset << " / " << mChunkSize <<
            KFS_LOG_EOM;
            if (newSize > 0 && newSize % CHECKSUM_BLOCKSIZE != 0) {
                ReadOp* const rop = new ReadOp(0);
                rop->chunkId      = mChunkId;
                rop->chunkVersion = mChunkVersion;
                rop->offset       =
                    newSize / CHECKSUM_BLOCKSIZE * CHECKSUM_BLOCKSIZE;
                rop->numBytes     = newSize - rop->offset;
                rop->clnt         = this;
                rop->dataBuf      = new IOBuffer;
                mIoOpsInFlight++;
                const int res = gChunkManager.ReadChunk(rop);
                if (res < 0) {
                    rop->status = res;
                    OpDone(rop);
                }
                return;
            }
            // No last block read and checksum update is needed.
            ChunkInfo_t* const info = gChunkManager.GetChunkInfo(mChunkId);
            if (! info || (! info->chunkBlockChecksum && info->chunkSize != 0)) {
                WAPPEND_LOG_STREAM_FATAL <<
                    "make chunk stable:"
                    " failed to get chunk info" <<
                    " chunk: "     << mChunkId <<
                    " checksums: " <<
                        (const void*)(info ? info->chunkBlockChecksum : 0) <<
                    " size: "      << (info ? info->chunkSize : -1) <<
                KFS_LOG_EOM;
                FatalError();
                SetState(kStateChunkLost);
            }
            if (info->chunkSize < newSize) {
                SetState(kStateChunkLost);
            } else {
                // Truncation done, set the new size.
                info->chunkSize = newSize;
                mNextWriteOffset = newSize;
            }
        }
    }
    if (mState == kStateClosed && (
            ! ComputeChecksum() ||
            (mMakeChunkStableOp->hasChecksum &&
                mChunkChecksum != mMakeChunkStableOp->chunkChecksum) ||
            (mMakeChunkStableOp->chunkSize >= 0 &&
                mChunkSize != mMakeChunkStableOp->chunkSize))) {
        SetState(kStateChunkLost);
    }
    if (mState != kStateClosed) {
        MakeChunkStableDone();
        return;
    }
    if (mMakeChunkStableOp->clnt == this) {
        // Internally generated op done, see if there are other ops.
        MakeChunkStableOp* const iop = mMakeChunkStableOp;
        mMakeChunkStableOp = iop->next;
        delete iop;
        if (! mMakeChunkStableOp) {
            mLastMakeChunkStableOp = 0;
            if (mBeginMakeChunkStableOp) {
                BeginMakeStable(); // Restart (send response) begin make stable.
            } else {
                // Internal make chunk stable doesn't transition into the
                // "stable" state, it only truncates the chunk, recalculates the
                // checksum, and notifies meta server that chunk append is done.
                NotifyChunkClosed();
            }
            return;
        }
        if ((mMakeChunkStableOp->hasChecksum &&
                mChunkChecksum != mMakeChunkStableOp->chunkChecksum) ||
            (mMakeChunkStableOp->chunkSize >= 0 &&
                mChunkSize != mMakeChunkStableOp->chunkSize)) {
            SetState(kStateChunkLost);
            MakeChunkStableDone();
            return;
        }
    }
    WAPPEND_LOG_STREAM_INFO <<
        "make chunk stable:"
        " starting sync of the metadata"
        " chunk: " << mChunkId <<
        " size: "  << mNextWriteOffset << " / " << GetChunkSize() <<
    KFS_LOG_EOM;
    mIoOpsInFlight++;

    // Write the index first, and then the rest of the metadata
    mWritingIndexFlag = true;
    const int res = gChunkManager.WriteChunkIndex(mChunkId, mChunkIndexBuffer, this);
    if (res < 0) {
        WAPPEND_LOG_STREAM_INFO <<
            "write of chunk index failed: " << res <<
            KFS_LOG_EOM;
        MetaWriteDone(res);
    }
}

void
AtomicRecordAppender::FlushSelf(bool flushFullChecksumBlocks)
{
    mLastFlushTime = Now();
    SetCanDoLowOnBuffersFlushFlag(false);
    if (mStaggerRMWInFlightFlag) {
        mRestartFlushFlag    = ! mBuffer.IsEmpty();
        mFlushFullBlocksFlag = mFlushFullBlocksFlag || flushFullChecksumBlocks;
        return;
    }
    mRestartFlushFlag = false;
    while (mState == kStateOpen ||
            mState == kStateClosed ||
            mState == kStateReplicationFailed) {
        const int nBytes = mBuffer.BytesConsumable();
        if (nBytes <= (flushFullChecksumBlocks ? int(CHECKSUM_BLOCKSIZE) : 0)) {
            return;
        }
        assert(! mStaggerRMWInFlightFlag);
        size_t bytesToFlush(nBytes <= int(CHECKSUM_BLOCKSIZE) ?
            nBytes : nBytes / CHECKSUM_BLOCKSIZE * CHECKSUM_BLOCKSIZE);
        // assert(IsChunkOpen()); // OK to flush deleted chunk.

        // The chunk manager write code requires writes where the # of bytes ==
        // size of checksum block to be aligned to checksum boundaries;
        // otherwise, the writes should be less than a checksum block.
        //
        // Set RMW flag to allow *only one* concurrent partial checksum block
        // write withing the same checksum block. This is need because io
        // completion order is undefined, and partial checksum block write does
        // read modify write. Obviously two such writes withing the same block
        // need to be ordered.
        //
        const int blkOffset(mNextWriteOffset % CHECKSUM_BLOCKSIZE);
        if (blkOffset > 0) {
            mStaggerRMWInFlightFlag =
                blkOffset + bytesToFlush < CHECKSUM_BLOCKSIZE;
            if (! mStaggerRMWInFlightFlag) {
                bytesToFlush = CHECKSUM_BLOCKSIZE - blkOffset;
            }
            assert(! mStaggerRMWInFlightFlag || bytesToFlush == size_t(nBytes));
        } else {
            mStaggerRMWInFlightFlag = bytesToFlush < CHECKSUM_BLOCKSIZE;
        }
        WriteOp* const wop = new WriteOp(mChunkId, mChunkVersion);
        wop->InitForRecordAppend();
        wop->clnt     = this;
        wop->offset   = mNextWriteOffset;
        wop->numBytes = bytesToFlush;
        if (bytesToFlush < CHECKSUM_BLOCKSIZE) {
            // Buffer don't have to be full and aligned, chunk manager will have
            // to do partial checksum block write, and invoke
            // ReplaceKeepBuffersFull() anyway.
            wop->dataBuf->Move(&mBuffer, bytesToFlush);
        } else {
            // Buffer size should always be multiple of checksum block size.
            assert(mNextWriteOffset % IOBufferData::GetDefaultBufferSize() == 0);
            wop->dataBuf->ReplaceKeepBuffersFull(&mBuffer, 0, bytesToFlush);
        }
        const int newLimit = gAtomicRecordAppendManager.GetFlushLimit(
            *this, mBuffer.BytesConsumable() - nBytes);

        WAPPEND_LOG_STREAM_DEBUG <<
            "flush write"
            " state: "       << GetStateAsStr() <<
            " chunk: "       << wop->chunkId <<
            " offset: "      << wop->offset <<
            " bytes: "       << wop->numBytes <<
            " buffered: "    << mBuffer.BytesConsumable() <<
            " flush limit: " << newLimit <<
            " in flight:"
            " replicaton: "  << mReplicationsInFlight <<
            " ios: "         << mIoOpsInFlight <<
        KFS_LOG_EOM;

        mNextWriteOffset += bytesToFlush;
        mBufFrontPadding = 0;
        if (bytesToFlush < CHECKSUM_BLOCKSIZE) {
            const int off(
                mNextWriteOffset % IOBufferData::GetDefaultBufferSize());
            if (off == 0) {
                mBuffer.MakeBuffersFull();
            } else {
                assert(off > 0 && mBuffer.IsEmpty());
                mBufFrontPadding = off;
            }
        }
        mIoOpsInFlight++;
        int res = gChunkManager.WriteChunk(wop);
        if (res < 0) {
            // Failed to start write, call error handler and return immediately.
            // Assume that error handler can delete this.
            wop->status = res;
            wop->HandleEvent(EVENT_DISK_ERROR, &res);
            return;
        }
    }
}

void
AtomicRecordAppender::OpDone(WriteOp *op)
{
    assert(
        op->chunkId == mChunkId && mIoOpsInFlight > 0 &&
        (mStaggerRMWInFlightFlag || op->numBytes >= CHECKSUM_BLOCKSIZE)
    );
    mIoOpsInFlight--;
    const bool failedFlag =
        op->status < 0 || size_t(op->status) < op->numBytes;
    WAPPEND_LOG_STREAM(failedFlag ?
            MsgLogger::kLogLevelERROR : MsgLogger::kLogLevelDEBUG) <<
        "write " << (failedFlag ? "FAILED" : "done") <<
        " chunk: "      << mChunkId <<
        " offset: "     << op->offset <<
        " size: "       << op->numBytes <<
        " commit: "     << mNextCommitOffset <<
        " in flight:"
        " replicaton: " << mReplicationsInFlight <<
        " ios: "        << mIoOpsInFlight <<
        " status: "     << op->status <<
        " chunk size: " << GetChunkSize() <<
    KFS_LOG_EOM;
    const off_t end = op->offset + op->numBytes;
    delete op;
    if (DeleteIfNeeded()) {
        return;
    }
    if (failedFlag) {
        Cntrs().mWriteErrorCount++;
        SetState(kStateChunkLost);
    }
    // There could be more that one write in flight, but only one stagger.
    // The stagger end, by definition, is not on checksum block boundary, but
    // all other write should end exactly on checksum block boundary.
    if (mStaggerRMWInFlightFlag && end % CHECKSUM_BLOCKSIZE != 0) {
        mStaggerRMWInFlightFlag = false;
        if (mRestartFlushFlag) {
            FlushSelf(mFlushFullBlocksFlag);
        }
    }
    if (mIoOpsInFlight <= 0 && mBeginMakeChunkStableOp) {
        BeginMakeStable();
    }
    if (mIoOpsInFlight <= 0 && mMakeChunkStableOp) {
        MakeChunkStable();
    }
}

void
AtomicRecordAppender::MetaWriteDone(int status)
{
    assert(mIoOpsInFlight > 0);
    mIoOpsInFlight--;
    WAPPEND_LOG_STREAM(status >= 0 ?
            MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
        "meta write " << (status < 0 ? "FAILED" : "done") <<
        " chunk: "      << mChunkId <<
        " in flight:"
        " replicaton: " << mReplicationsInFlight <<
        " ios: "        << mIoOpsInFlight <<
        " commit: "     << mNextCommitOffset <<
        " status: "     << status <<
    KFS_LOG_EOM;
    if (DeleteIfNeeded()) {
        return;
    }
    if (status < 0) {
        Cntrs().mWriteErrorCount++;
        SetState(kStateChunkLost);
    }
    if (mWritingIndexFlag) {
        mWritingIndexFlag = false;
        mIoOpsInFlight++;
        const int res = gChunkManager.WriteChunkMetadata(mChunkId, this);
        if (res < 0) {
            MetaWriteDone(res);
        }
        return;
    }
    if (mState == kStateClosed) {
        SetState(kStateStable);
    }
    MakeChunkStable();
}

bool
AtomicRecordAppender::TryToCloseChunk()
{
    if (! IsMaster()) {
        return false;
    }
    SendCommitAck();
    if (mState == kStateOpen && mReplicationsInFlight > 0) {
        return false;
    }
    if (mState == kStateOpen || mState == kStateReplicationFailed) {
        if (mMakeChunkStableOp || mBeginMakeChunkStableOp) {
            FatalError();
        }
        TrimToLastCommit("try to close");
    }
    return true;
}

void
AtomicRecordAppender::TrimToLastCommit(
    const char* inMsgPtr)
{
    if (mMakeChunkStableOp) {
        FatalError();
    }
    WAPPEND_LOG_STREAM_DEBUG <<
        (inMsgPtr ? inMsgPtr : "trim to last commit") <<
        " chunk: "   << mChunkId <<
        " version: " << mChunkVersion <<
        " size: "    << mNextCommitOffset <<
    KFS_LOG_EOM;
    // Trim the chunk on failure to the last committed offset, if needed.
    MakeChunkStableOp* const op = new MakeChunkStableOp(0);
    op->chunkId      = mChunkId;
    op->chunkVersion = mChunkVersion;
    op->clnt         = this;
    op->chunkSize    = mNextCommitOffset;
    MakeChunkStable(op);
}

void
AtomicRecordAppender::NotifyChunkClosed()
{
    assert(IsMaster() && mState == kStateClosed);
    WAPPEND_LOG_STREAM_DEBUG <<
        "notify closed:"
        " chunk: "    << mChunkId <<
        " size: "     << mChunkSize <<
        " checksum: " << mChunkChecksum <<
    KFS_LOG_EOM;
    gLeaseClerk.RelinquishLease(mChunkId, mChunkSize, true, mChunkChecksum);
}

void
AtomicRecordAppender::SendCommitAck()
{
    CheckLeaseAndChunk("send commit ack");
    if (! IsMaster() || mState != kStateOpen ||
            mNumServers <= 1 || mReplicationsInFlight > 0 ||
            mNextCommitOffset <= mCommitOffsetAckSent) {
        return;
    }
    WAPPEND_LOG_STREAM_DEBUG <<
        "send commit ack"
        " chunk: "    << mChunkId <<
        " last ack: " << mCommitOffsetAckSent <<
        " size: "     << mNextCommitOffset <<
        " unacked: "  << (mNextCommitOffset - mCommitOffsetAckSent) <<
    KFS_LOG_EOM;
    // Use write offset as seq. # for debugging
    RecordAppendOp* const op = new RecordAppendOp(mNextWriteOffset);
    op->clnt         = this;
    op->chunkId      = mChunkId;
    op->chunkVersion = mChunkVersion;
    op->numServers   = mNumServers;
    op->servers      = mCommitAckServers;
    op->numBytes     = 0;
    AppendBegin(op, mReplicationPos, mPeerLocation);
}

AtomicRecordAppendManager::AtomicRecordAppendManager()
    : mAppenders(),
      mCleanUpSec(300),
      mCloseEmptyWidStateSec(60),
      mFlushIntervalSec(60),
      mSendCommitAckTimeoutSec(2),
      mReplicationTimeoutSec(4 * 60),
      mMinMetaUptimeSec(8 * 60),
      mFlushLimit(1 << 20),
      mMaxAppenderBytes(0),
      mTotalBuffersBytes(0),
      mTotalPendingBytes(0),
      mActiveAppendersCount(0),
      mOpenAppendersCount(0),
      mAppendersWithWidCount(0),
      mBufferLimitRatio(0.6),
      mMaxWriteIdsPerChunk(16 << 10),
      mInstanceNum(0),
      mCounters()
{
    PendingFlushList::Init(mPendingFlushList);
    mCounters.Clear();
}

AtomicRecordAppendManager::~AtomicRecordAppendManager()
{
    assert(mAppenders.empty());
}

void
AtomicRecordAppendManager::SetParameters(const Properties& props)
{
    mCleanUpSec              = props.getValue(
        "chunkServer.recAppender.cleanupSec",          mCleanUpSec);
    mCloseEmptyWidStateSec   = props.getValue(
        "chunkServer.recAppender.closeEmptyWidStateSec",
        mCloseEmptyWidStateSec);
    mFlushIntervalSec        = props.getValue(
        "chunkServer.recAppender.flushIntervalSec",    mFlushIntervalSec),
    mSendCommitAckTimeoutSec = props.getValue(
        "chunkServer.recAppender.sendCommitAckTimeoutSec",
        mSendCommitAckTimeoutSec);
    mReplicationTimeoutSec = props.getValue(
        "chunkServer.recAppender.replicationTimeoutSec",
        mReplicationTimeoutSec);
    mMinMetaUptimeSec = props.getValue(
        "chunkServer.recAppender.minMetaUptimeSec",
        mMinMetaUptimeSec);
    mFlushLimit              = props.getValue(
        "chunkServer.recAppender.flushLimit",          mFlushLimit),
    mBufferLimitRatio        = props.getValue(
        "chunkServer.recAppender.bufferLimitRatio",    mBufferLimitRatio),
    mMaxWriteIdsPerChunk     = props.getValue(
        "chunkServer.recAppender.maxWriteIdsPerChunk", mMaxWriteIdsPerChunk);
    mTotalBuffersBytes       = 0;
    if (! mAppenders.empty()) {
        UpdateAppenderFlushLimit();
    }
}

int
AtomicRecordAppendManager::GetFlushLimit(
    AtomicRecordAppender& /* appender */, int addBytes /* = 0 */)
{
    if (addBytes != 0) {
        assert(mTotalPendingBytes + addBytes >= 0);
        mTotalPendingBytes += addBytes;
        UpdateAppenderFlushLimit();
    }
    return mMaxAppenderBytes;
}

void
AtomicRecordAppendManager::UpdateAppenderFlushLimit(const AtomicRecordAppender* appender /* = 0 */)
{
    assert(mActiveAppendersCount >= 0);
    if (appender) {
        if (appender->IsChunkStable()) {
            assert(mActiveAppendersCount > 0);
            mActiveAppendersCount--;
        } else {
            assert((size_t)mActiveAppendersCount < mAppenders.size());
            mActiveAppendersCount++;
        }
    }
    if (mTotalBuffersBytes <= 0) {
        mTotalBuffersBytes =
            int64_t(DiskIo::GetBufferManager().GetTotalCount() * mBufferLimitRatio);
        if (mTotalBuffersBytes <= 0) {
            mTotalBuffersBytes = mFlushLimit;
        }
    }
    const int prevLimit = mMaxAppenderBytes;
    mMaxAppenderBytes = std::min(int64_t(mFlushLimit),
        (mTotalBuffersBytes + mTotalPendingBytes) /
        std::max(int64_t(1), mActiveAppendersCount)
    );
    if (prevLimit / 16 - mFlushLimit > mMaxAppenderBytes) {
        PendingFlushList::Iterator it(mPendingFlushList);
        AtomicRecordAppender* appender;
        while ((appender = it.Next())) {
            appender->UpdateFlushLimit(mMaxAppenderBytes);
        }
    }
}


void
AtomicRecordAppendManager::AllocateChunk(
    AllocChunkOp* op, int replicationPos, ServerLocation peerLoc,
    const DiskIo::FilePtr& chunkFileHandle,
    ChunkSortHelperPtr &sortHelper)
{
    assert(op);
    std::pair<ARAMap::iterator, bool> const res = mAppenders.insert(
        std::make_pair(op->chunkId, (AtomicRecordAppender*)0));
    if (res.second) {
        assert(! res.first->second);
        const ChunkInfo_t* info = 0;
        if (! chunkFileHandle ||
                ! chunkFileHandle->IsOpen() ||
                ! (info = gChunkManager.GetChunkInfo(op->chunkId)) ||
                (! info->chunkBlockChecksum && info->chunkSize != 0)) {
            op->statusMsg = "chunk manager closed this chunk";
            op->status    = AtomicRecordAppender::kErrParameters;
        } else if (op->chunkVersion != info->chunkVersion) {
            op->statusMsg = "invalid chunk version";
            op->status    = AtomicRecordAppender::kErrParameters;
        }
        WAPPEND_LOG_STREAM(op->status == 0 ?
                MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
            "allocate chunk: creating new appender: " <<
                op->statusMsg <<
            " status: "         << op->status  <<
            " appender count: " << mAppenders.size() <<
            " chunk: "          << op->chunkId <<
            " checksums: "      <<
                (const void*)(info ? info->chunkBlockChecksum : 0) <<
            " size: "           << (info ? info->chunkSize : off_t(-1)) <<
            " version: "        << (info ? info->chunkVersion : (int64_t)-1) <<
            " file handle: "    << (const void*)chunkFileHandle.get() <<
            " file open: "      << (chunkFileHandle->IsOpen() ? "yes" : "no") <<
            " " << op->Show() <<
        KFS_LOG_EOM;
        if (op->status != 0) {
            mAppenders.erase(res.first);
        } else {
            res.first->second = new AtomicRecordAppender(
                chunkFileHandle, op->chunkId, op->chunkVersion, op->numServers,
                op->servers, peerLoc, replicationPos, info->chunkSize,
                sortHelper
            );
            mOpenAppendersCount++;
            UpdateAppenderFlushLimit(res.first->second);
        }
    } else if (res.first->second->IsOpen()) {
        op->status = res.first->second->CheckParameters(
            op->chunkVersion, op->numServers,
            op->servers, replicationPos, peerLoc, chunkFileHandle, op->statusMsg);
        WAPPEND_LOG_STREAM(op->status == 0 ?
                MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
            "allocate chunk: appender exists: " <<
            " chunk: "          << op->chunkId <<
            " status: "         << op->status  <<
            " appender count: " << mAppenders.size() <<
            " " << op->Show() <<
        KFS_LOG_EOM;
    } else {
        // This should not normally happen, but this could happen when meta
        // server restarts with partially (or completely) lost meta data, and
        // meta server re-uses the same chunk id.
        // Cleanup lingering appedner and retry.
        res.first->second->Delete();
        AllocateChunk(op, replicationPos, peerLoc, chunkFileHandle, sortHelper);
        return; // Tail recursion.
    }
    if (replicationPos == 0) {
        mCounters.mAppenderAllocMasterCount++;
    }
    mCounters.mAppenderAllocCount++;
    if (op->status != 0) {
        mCounters.mAppenderAllocErrorCount++;
    }
}

void
AtomicRecordAppendManager::AllocateWriteId(
    WriteIdAllocOp *op, int replicationPos, ServerLocation peerLoc,
    const DiskIo::FilePtr& chunkFileHandle)
{
    assert(op);
    ARAMap::iterator const it = mAppenders.find(op->chunkId);
    if (it == mAppenders.end()) {
        op->statusMsg = "not open for append; no appender";
        op->status    = AtomicRecordAppender::kErrParameters;
        mCounters.mWriteIdAllocNoAppenderCount++;
    } else {
        it->second->AllocateWriteId(
            op, replicationPos, peerLoc, chunkFileHandle);
    }
    mCounters.mWriteIdAllocCount++;
    if (op->status != 0) {
        mCounters.mWriteIdAllocErrorCount++;
    }
}

void
AtomicRecordAppendManager::Timeout()
{
    FlushIfLowOnBuffers();
}

void
AtomicRecordAppendManager::FlushIfLowOnBuffers()
{
    if (! DiskIo::GetBufferManager().IsLowOnBuffers()) {
        return;
    }
    PendingFlushList::Iterator it(mPendingFlushList);
    AtomicRecordAppender* appender;
    while ((appender = it.Next())) {
        appender->LowOnBuffersFlush();
    }
}

bool
AtomicRecordAppendManager::IsChunkStable(kfsChunkId_t chunkId) const
{
    // Cast until mac std::tr1::unordered_map gets "find() const"
    ARAMap::const_iterator const it =
        const_cast<ARAMap&>(mAppenders).find(chunkId);
    return (it == mAppenders.end() || it->second->IsChunkStable());
}

bool
AtomicRecordAppendManager::IsSpaceReservedInChunk(kfsChunkId_t chunkId)
{
    ARAMap::const_iterator const it = mAppenders.find(chunkId);
    return (it != mAppenders.end() && it->second->SpaceReserved() > 0);
}

int
AtomicRecordAppendManager::ChunkSpaceReserve(
    kfsChunkId_t chunkId, int64_t writeId, size_t nBytes, std::string* errMsg /* = 0 */)
{
    ARAMap::iterator const it = mAppenders.find(chunkId);
    int status;
    if (it == mAppenders.end()) {
        if (errMsg) {
            (*errMsg) += "chunk does not exist or not open for append";
        }
        status = AtomicRecordAppender::kErrParameters;
    } else {
        status = it->second->ChangeChunkSpaceReservaton(
            writeId, nBytes, false, errMsg);
    }
    mCounters.mSpaceReserveCount++;
    if (status != 0) {
        mCounters.mSpaceReserveErrorCount++;
        if (status == AtomicRecordAppender::kErrOutOfSpace) {
            mCounters.mSpaceReserveDeniedCount++;
        }
    } else {
        mCounters.mSpaceReserveByteCount += nBytes;
    }
    return status;
}

int
AtomicRecordAppendManager::ChunkSpaceRelease(
    kfsChunkId_t chunkId, int64_t writeId, size_t nBytes, std::string* errMsg /* = 0 */)
{
    ARAMap::iterator const it = mAppenders.find(chunkId);
    if (it == mAppenders.end()) {
        if (errMsg) {
            (*errMsg) += "chunk does not exist or not open for append";
        }
        return AtomicRecordAppender::kErrParameters;
    }
    return it->second->ChangeChunkSpaceReservaton(
        writeId, nBytes, true, errMsg);
}

int
AtomicRecordAppendManager::InvalidateWriteId(
    kfsChunkId_t chunkId, int64_t writeId, bool declareFailureFlag)
{
    ARAMap::const_iterator const it = mAppenders.find(chunkId);
    return (it == mAppenders.end() ? 0 :
        it->second->InvalidateWriteId(writeId, declareFailureFlag));
}

int
AtomicRecordAppendManager::GetAlignment(kfsChunkId_t chunkId) const
{
    ARAMap::const_iterator const it = mAppenders.find(chunkId);
    return (it == mAppenders.end() ? 0 : it->second->GetAlignment());
}

bool
AtomicRecordAppendManager::BeginMakeChunkStable(BeginMakeChunkStableOp* op)
{
    assert(op);
    ARAMap::iterator const it = mAppenders.find(op->chunkId);
    if (it == mAppenders.end()) {
        op->statusMsg = "chunk does not exist or not open for append";
        op->status    = AtomicRecordAppender::kErrParameters;
        WAPPEND_LOG_STREAM_ERROR <<
            "begin make stable: no write appender"
            " chunk: "  << op->chunkId   <<
            " status: " << op->status    <<
            " msg: "    << op->statusMsg <<
            " " << op->Show() <<
        KFS_LOG_EOM;
        mCounters.mBeginMakeStableCount++;
        mCounters.mBeginMakeStableErrorCount++;
        return false; // Submit response now.
    }
    it->second->BeginMakeStable(op);
    // Completion handler is already invoked or will be invoked later.
    return true;
}

bool
AtomicRecordAppendManager::CloseChunk(
    CloseOp* op, int64_t writeId, bool& forwardFlag)
{
    assert(op);
    ARAMap::iterator const it = mAppenders.find(op->chunkId);
    if (it == mAppenders.end()) {
        return false; // let chunk manager handle it
    }
    it->second->CloseChunk(op, writeId, forwardFlag);
    return true;
}

bool
AtomicRecordAppendManager::MakeChunkStable(MakeChunkStableOp* op)
{
    assert(op);
    ARAMap::iterator const it = mAppenders.find(op->chunkId);
    if (it == mAppenders.end()) {
        off_t    chunkSize     = -1;
        uint32_t chunkChecksum = 0;
        if (op->hasChecksum) {
            // The following is pretty much redundant now when write appender
            // created at the time of chunk allocation.
            if (AtomicRecordAppender::ComputeChecksum(
                    op->chunkId, op->chunkVersion, chunkSize, chunkChecksum) &&
                    chunkSize == op->chunkSize &&
                    chunkChecksum == op->chunkChecksum) {
                op->status = 0;
            } else {
                op->statusMsg = "no write appender, checksum or size mismatch";
                op->status = AtomicRecordAppender::kErrFailedState;
                gChunkManager.ChunkIOFailed(op->chunkId, -EIO);
                mCounters.mLostChunkCount++;
            }
        }
        WAPPEND_LOG_STREAM(op->status == 0 ?
                MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
            "make stable: no write appender"
            " chunk: "    << op->chunkId   <<
            " status: "   << op->status    <<
            " msg: "      << op->statusMsg <<
            " size: "     << chunkSize     <<
            " checksum: " << chunkChecksum <<
            " " << op->Show() <<
        KFS_LOG_EOM;
        mCounters.mMakeStableCount++;
        if (op->status != 0) {
            mCounters.mMakeStableErrorCount++;
            if (op->hasChecksum) {
                if (chunkSize != op->chunkSize) {
                    mCounters.mMakeStableLengthErrorCount++;
                }
                if (chunkChecksum != op->chunkChecksum) {
                    mCounters.mMakeStableChecksumErrorCount++;
                }
            }
        }
        return false; // Submit response now.
    }
    it->second->MakeChunkStable(op);
    // Completion handler is already invoked or will be invoked later.
    return true;
}

void
AtomicRecordAppendManager::AppendBegin(
    RecordAppendOp* op, int replicationPos, ServerLocation peerLoc)
{
    assert(op);
    ARAMap::iterator const it = mAppenders.find(op->chunkId);
    if (it == mAppenders.end()) {
        op->status    = AtomicRecordAppender::kErrParameters;
        op->statusMsg = "chunk does not exist or not open for append";
        mCounters.mAppendCount++;
        mCounters.mAppendErrorCount++;
        KFS::SubmitOpResponse(op);
    } else {
        it->second->AppendBegin(op, replicationPos, peerLoc);
    }
}

void
AtomicRecordAppendManager::GetOpStatus(GetRecordAppendOpStatus* op)
{
    assert(op);
    ARAMap::iterator const it = mAppenders.find(op->chunkId);
    if (it == mAppenders.end()) {
        op->status    = AtomicRecordAppender::kErrParameters;
        op->statusMsg = "chunk does not exist or not open for append";
    } else {
        it->second->GetOpStatus(op);
    }
    mCounters.mGetOpStatusCount++;
    if (op->status != 0) {
        mCounters.mGetOpStatusErrorCount++;
    } else if (op->opStatus != AtomicRecordAppender::kErrStatusInProgress) {
        mCounters.mGetOpStatusKnownCount++;
    }
}

bool
AtomicRecordAppendManager::WantsToKeepLease(kfsChunkId_t chunkId) const
{
    ARAMap::const_iterator const it = mAppenders.find(chunkId);
    return (it != mAppenders.end() && it->second->WantsToKeepLease());
}

void
AtomicRecordAppendManager:: DeleteChunk(kfsChunkId_t chunkId)
{
    ARAMap::const_iterator const it = mAppenders.find(chunkId);
    if (it != mAppenders.end()) {
        it->second->DeleteChunk();
    }
}

void
AtomicRecordAppendManager::Shutdown()
{
    while (! mAppenders.empty()) {
        mAppenders.begin()->second->Delete();
    }
}

AtomicRecordAppendManager gAtomicRecordAppendManager;

}
