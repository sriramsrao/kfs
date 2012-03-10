//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: ChunkManager.cc 3102 2011-09-21 03:23:21Z sriramr $
//
// Created 2006/03/28
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

extern "C" {
#include <dirent.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/statvfs.h>
#include <openssl/rand.h>
}

#include "common/log.h"
#include "common/kfstypes.h"

#include "ChunkManager.h"
#include "ChunkServer.h"
#include "MetaServerSM.h"
#include "LeaseClerk.h"
#include "AtomicRecordAppender.h"
#include "Utils.h"
#include "Logger.h"
#include "DiskIo.h"
#include "ChunkSortHelper.h"

#include "libkfsIO/Counter.h"
#include "libkfsIO/Checksum.h"
#include "libkfsIO/Globals.h"

#include <fstream>
#include <sstream>
#include <algorithm>
#include <string>
#include <set>

using std::ofstream;
using std::ifstream;
using std::istringstream;
using std::ostringstream;
using std::ios_base;
using std::list;
using std::min;
using std::max;
using std::endl;
using std::find_if;
using std::string;
using std::vector;
using std::set;
using std::make_pair;

using namespace KFS::libkfsio;

namespace KFS
{

ChunkManager gChunkManager;

typedef QCDLList<ChunkInfoHandle, 0> ChunkLru;
typedef QCDLList<ChunkInfoHandle, 1> PendingMetaSyncQueue;

/// Encapsulate a chunk file descriptor and information about the
/// chunk such as name and version #.
class ChunkInfoHandle : public KfsCallbackObj
{
public:
    ChunkInfoHandle()
        : KfsCallbackObj(),
          lastIOTime(0),
          readChunkMetaOp(NULL),
          isBeingReplicated(false),
          createFile(false),
          mMetaSyncPending(false),
          mMetaSyncInFlight(false),
          mDeleteFlag(false),
          mWriteAppenderOwnsFlag(false)
    {
        ChunkLru::Init(*this);
        PendingMetaSyncQueue::Init(*this);
    }
    void Delete(ChunkInfoHandle** chunkInfoLists)
    {
        ChunkLru::Remove(chunkInfoLists, *this);
        PendingMetaSyncQueue::Remove(chunkInfoLists, *this);
        if (mWriteAppenderOwnsFlag) {
            gAtomicRecordAppendManager.DeleteChunk(chunkInfo.chunkId);
        }
        if (mMetaSyncInFlight) {
            mDeleteFlag = true;
        } else {
            delete this;
        }
    }

    ChunkInfo_t chunkInfo;
    /// Chunks are stored as files in he underlying filesystem; each
    /// chunk file is named by the chunkId.  Each chunk has a header;
    /// this header is hidden from clients; all the client I/O is
    /// offset by the header amount
    DiskIo::FilePtr dataFH;
    time_t lastIOTime;  // when was the last I/O done on this chunk

    /// keep track of the op that is doing the read
    ReadChunkMetaOp *readChunkMetaOp;

    /// Handle to the sort helper to which records need to be fwd'ed
    ChunkSortHelperPtr sortHelper;

    void Release(ChunkInfoHandle** chunkInfoLists);

    bool IsFileOpen() const {
        return (dataFH && dataFH->IsOpen());
    }

    bool IsFileInUse() const {
        return (IsFileOpen() && ! dataFH.unique());
    }

    bool SyncMeta();
    inline void LruUpdate(ChunkInfoHandle** chunkInfoLists);
    inline void ScheduleSyncMeta(ChunkInfoHandle** chunkInfoLists);
    inline void SetMetaWriteInFlight(ChunkInfoHandle** chunkInfoLists, KfsOp* op);
    inline void SetWriteAppenderOwns(ChunkInfoHandle** chunkInfoLists, bool flag);
    inline bool IsWriteAppenderOwns() const;
    bool isBeingReplicated:1;  // is the chunk being replicated from
                               // another server
    bool createFile:1;

private:
    bool             mMetaSyncPending:1;
    bool             mMetaSyncInFlight:1;
    bool             mDeleteFlag:1;
    bool             mWriteAppenderOwnsFlag:1;
    ChunkInfoHandle* mPrevPtr[ChunkManager::kChunkInfoHandleListCount];
    ChunkInfoHandle* mNextPtr[ChunkManager::kChunkInfoHandleListCount];

    int HandleChunkMetaWriteDone(int code, void *data);
    virtual ~ChunkInfoHandle() {
        if (mMetaSyncInFlight) {
            // Object is the "client" of this op.
            die("attempt to delete chunk info handle "
                "with meta data write in flight");
        }
    }
    friend class QCDLListOp<ChunkInfoHandle, 0>;
    friend class QCDLListOp<ChunkInfoHandle, 1>;
private:
    ChunkInfoHandle(const  ChunkInfoHandle&);
    ChunkInfoHandle& operator=(const  ChunkInfoHandle&);
};

inline bool ChunkManager::IsInLru(const ChunkInfoHandle& cih) const {
    return ChunkLru::IsInList(mChunkInfoLists, cih);
}

inline void ChunkInfoHandle::LruUpdate(ChunkInfoHandle** chunkInfoLists) {
    lastIOTime = globalNetManager().Now();
    if (! mWriteAppenderOwnsFlag && ! isBeingReplicated && ! mMetaSyncPending) {
        ChunkLru::PushBack(chunkInfoLists, *this);
        assert(gChunkManager.IsInLru(*this));
    } else {
        ChunkLru::Remove(chunkInfoLists, *this);
        assert(! gChunkManager.IsInLru(*this));
    }
}

inline void ChunkInfoHandle::ScheduleSyncMeta(ChunkInfoHandle** chunkInfoLists) {
    mMetaSyncPending = true;
    LruUpdate(chunkInfoLists); // pretent that we've scheduled io
    PendingMetaSyncQueue::PushBack(chunkInfoLists, *this);
}

inline void ChunkInfoHandle::SetMetaWriteInFlight(ChunkInfoHandle** chunkInfoLists, KfsOp* op) {
    mMetaSyncPending = false;
    PendingMetaSyncQueue::Remove(chunkInfoLists, *this);
    mMetaSyncInFlight = op->clnt == this;
    LruUpdate(chunkInfoLists);
}

inline void ChunkInfoHandle::SetWriteAppenderOwns(ChunkInfoHandle** chunkInfoLists, bool flag) {
    if (mDeleteFlag || flag == mWriteAppenderOwnsFlag) {
        return;
    }
    mWriteAppenderOwnsFlag = flag;
    if (mWriteAppenderOwnsFlag && ! mMetaSyncInFlight && mMetaSyncPending) {
        mMetaSyncPending = false;
        PendingMetaSyncQueue::Remove(chunkInfoLists, *this);
    }
    if (mWriteAppenderOwnsFlag) {
        ChunkLru::Remove(chunkInfoLists, *this);
        assert(! gChunkManager.IsInLru(*this));
    } else {
        LruUpdate(chunkInfoLists);
    }
}

inline bool ChunkInfoHandle::IsWriteAppenderOwns() const
{
    return mWriteAppenderOwnsFlag;
}

inline void ChunkManager::LruUpdate(ChunkInfoHandle& cih) {
    cih.LruUpdate(mChunkInfoLists);
}

inline void ChunkManager::Release(ChunkInfoHandle& cih) {
    cih.Release(mChunkInfoLists);
}

inline void ChunkManager::Delete(ChunkInfoHandle& cih) {
    cih.Delete(mChunkInfoLists);
}

void
ChunkInfoHandle::Release(ChunkInfoHandle** chunkInfoLists) 
{
    chunkInfo.UnloadChecksums();
    if (! IsFileOpen()) {
        return;
    }
    if (chunkInfo.chunkIndexSize > 0) {
        KFS_LOG_STREAM_INFO << "chunk: " << chunkInfo.chunkId <<
            " has an index of size: " << chunkInfo.chunkIndexSize <<
            KFS_LOG_EOM;
        dataFH->UnReserveSpace(chunkInfo.chunkSize + KFS_CHUNK_HEADER_SIZE,
            CHUNKSIZE - chunkInfo.chunkSize);
    }

    KFS_LOG_STREAM_INFO <<
        "In release: chunk " << chunkInfo.chunkId << " is of size: " 
            << chunkInfo.chunkSize + KFS_CHUNK_HEADER_SIZE 
            << " index size: " << chunkInfo.chunkIndexSize <<
        KFS_LOG_EOM;

    std::string errMsg;
    // Don't call ftruncate in Close() if there is an index at the
    // end of chunk
    if (! dataFH->Close(
            chunkInfo.chunkIndexSize > 0 ? -1 : chunkInfo.chunkSize + KFS_CHUNK_HEADER_SIZE,
            &errMsg)) {
        KFS_LOG_STREAM_INFO <<
            "chunk " << chunkInfo.chunkId << " close error: " << errMsg <<
        KFS_LOG_EOM;
        dataFH.reset();
    }
    KFS_LOG_STREAM_INFO <<
        "Closing chunk " << chunkInfo.chunkId << " and might give up lease" <<
    KFS_LOG_EOM;
    gLeaseClerk.RelinquishLease(chunkInfo.chunkId);

    ChunkLru::Remove(chunkInfoLists, *this);
    PendingMetaSyncQueue::Remove(chunkInfoLists, *this);
    libkfsio::globals().ctrOpenDiskFds.Update(-1);
}

int
ChunkInfoHandle::HandleChunkMetaWriteDone(int code, void *data)
{
    mMetaSyncInFlight = false;
    if (mDeleteFlag) {
        delete this;
        return 0;
    }
    int status;
    if (data && (status = *reinterpret_cast<int*>(data)) < 0) {
        KFS_LOG_STREAM_INFO <<
            "failed to sync meta for chunk " << chunkInfo.chunkId <<
            " status: " << status <<
        KFS_LOG_EOM;
        if (! isBeingReplicated &&
                ! gChunkManager.IsWritePending(chunkInfo.chunkId)) {
            gChunkManager.ChunkIOFailed(chunkInfo.chunkId, -EIO);
            // "this" might have already been deleted here
            return 0;
        }
    }
    gChunkManager.LruUpdate(*this);
    return 0;
}

bool
ChunkInfoHandle::SyncMeta()
{
    if (! mMetaSyncPending || mMetaSyncInFlight ||
            isBeingReplicated || ! IsFileOpen()) {
        return mMetaSyncInFlight;
    }
    int     freeRequestCount;
    int     requestCount;
    int64_t readBlockCount;
    int64_t writeBlockCount;
    int     blockSize;
    dataFH->GetDiskQueuePendingCount(freeRequestCount, requestCount,
        readBlockCount, writeBlockCount, blockSize);
    if (freeRequestCount <= 0 || requestCount * 2 > freeRequestCount ||
            int64_t(writeBlockCount) * blockSize > (int64_t(128) << 20)) {
        KFS_LOG_STREAM_INFO << "deferring chunk meta data sync for: " <<
            chunkInfo.chunkId <<
            " requests: " << requestCount <<
            " write blocks: " << writeBlockCount <<
        KFS_LOG_EOM;
        return true;
    }
    SET_HANDLER(this, &ChunkInfoHandle::HandleChunkMetaWriteDone);
    int status = gChunkManager.WriteChunkMetadata(chunkInfo.chunkId, this);
    if (status < 0) {
        HandleChunkMetaWriteDone(EVENT_DISK_ERROR, &status);
        // "this" might have already been deleted here
        return true;
    }
    return mMetaSyncInFlight;
}

/// A Timeout interface object for taking checkpoints on the
/// ChunkManager object.
class ChunkManager::ChunkManagerTimeoutImpl : public ITimeout {
public:
    ChunkManagerTimeoutImpl(ChunkManager *mgr) : mTimeoutOp(0) {
        mChunkManager = mgr; 
        // set a checkpoint once every min.
        // SetTimeoutInterval(60*1000);
    };
    virtual void Timeout() {
        SubmitOp(&mTimeoutOp);
    }
private:
    /// Owning chunk manager
    ChunkManager	*mChunkManager;
    TimeoutOp		mTimeoutOp;
};

static int
GetMaxOpenFds()
{
    struct rlimit rlim;
    int maxOpenFds = 0;

    if (getrlimit(RLIMIT_NOFILE, &rlim) == 0) {
        maxOpenFds = rlim.rlim_cur;
        // bump the soft limit to the hard limit
        rlim.rlim_cur = rlim.rlim_max;
        if (setrlimit(RLIMIT_NOFILE, &rlim) == 0) {
            KFS_LOG_STREAM_DEBUG <<
                "setting # of open files to: " << rlim.rlim_cur <<
            KFS_LOG_EOM;
            maxOpenFds = rlim.rlim_cur;
        }
    }
    return maxOpenFds;
}

ChunkManager::ChunkManager()
{
    mTotalSpace = mUsedSpace = 0;
    mNumChunks = 0;
    mChunkManagerTimeoutImpl = 0;
    mIsChunkTableDirty = false;
    mLastDriveChosen = -1;
    time_t now = time(NULL);
    srand48(now);
    mMaxOpenChunkFiles = 1 << 10;
    mMaxIORequestSize = 4 << 20;
    ChunkLru::Init(mChunkInfoLists);
    PendingMetaSyncQueue::Init(mChunkInfoLists);
    mNextPendingMetaSyncScanTime = 0;
    mMetaSyncDelayTimeSecs = 5;
    mNextInactiveFdCleanupTime = 0;
    mInactiveFdsCleanupIntervalSecs = 300;
    mNextInactiveFdCleanupTime = 0;
    mMaxPendingWriteLruSecs = 300;
    mNextCheckpointTime = 0;
    mCheckpointIntervalSecs = 120;
    // check once every 6 hours
    mNextChunkDirsCheckTime = 0;
    mChunkDirsCheckIntervalSecs = 6 * 3600;
    // Seed write id.
    RAND_pseudo_bytes(
        reinterpret_cast<unsigned char*>(&mWriteId), int(sizeof(mWriteId)));
    mWriteId = (mWriteId < 0 ? -mWriteId : mWriteId) >> 1;
    mCounters.Clear();
}

ChunkManager::~ChunkManager()
{
    assert(mChunkTable.empty() && ! mChunkManagerTimeoutImpl);
}

void
ChunkManager::Shutdown()
{
    ScavengePendingWrites(time(0) + 2 * mMaxPendingWriteLruSecs);
    for (CMI iter = mChunkTable.begin(); iter != mChunkTable.end(); ) {
        ChunkInfoHandle * const cih = iter->second;
        if (cih->IsFileInUse()) {
            cih->SetWriteAppenderOwns(mChunkInfoLists, false);
            iter++;
            continue;
        }
        mChunkTable.erase(iter++);
        Release(*cih);
        Delete(*cih);
    }
    gAtomicRecordAppendManager.Shutdown();
    for (int i = 0; ;) {
        for (CMI iter = mChunkTable.begin(); iter != mChunkTable.end(); ) {
            ChunkInfoHandle * const cih = iter->second;
            if (cih->IsFileInUse()) {
                break;
            }
            mChunkTable.erase(iter++);
            Release(*cih);
            Delete(*cih);
        }
        const bool completionFlag = DiskIo::RunIoCompletion();
        if (mChunkTable.empty()) {
            break;
        }
        if (completionFlag) {
            continue;
        }
        if (++i > 1000) {
            KFS_LOG_STREAM_ERROR <<
                "ChunkManager::Shutdown timeout exceeded" <<
            KFS_LOG_EOM;
            break;
        }
        usleep(10000);
    }
    globalNetManager().UnRegisterTimeoutHandler(mChunkManagerTimeoutImpl);
    delete mChunkManagerTimeoutImpl;
    mChunkManagerTimeoutImpl = 0;
    string errMsg;
    if (! DiskIo::Shutdown(&errMsg)) {
        KFS_LOG_STREAM_INFO <<
            "DiskIo::Shutdown falure: " << errMsg <<
        KFS_LOG_EOM;
    }
}

bool
ChunkManager::IsWriteAppenderOwns(kfsChunkId_t chunkId) const
{
    CMI const it = mChunkTable.find(chunkId);
    return (
        it != mChunkTable.end() &&
        it->second->IsWriteAppenderOwns()
    );
}

bool 
ChunkManager::Init(const vector<string> &chunkDirs, int64_t totalSpace, const Properties& prop)
{
    mInactiveFdsCleanupIntervalSecs = prop.getValue(
        "chunkServer.inactiveFdsCleanupIntervalSecs",
        mInactiveFdsCleanupIntervalSecs);
    mMetaSyncDelayTimeSecs = std::max(1, prop.getValue(
        "chunkServer.metaSyncDelayTimeSecs",
        mMetaSyncDelayTimeSecs));
    mMaxPendingWriteLruSecs = std::max(1, prop.getValue(
        "chunkServer.maxPendingWriteLruSecs",
        mMaxPendingWriteLruSecs));
    mCheckpointIntervalSecs = std::max(1, prop.getValue(
        "chunkServer.checkpointIntervalSecs",
        mCheckpointIntervalSecs));
    mChunkDirsCheckIntervalSecs = std::max(1, prop.getValue(
        "chunkServer.chunkDirsCheckIntervalSecs",
        mChunkDirsCheckIntervalSecs));

    mTotalSpace = totalSpace;
    for (uint32_t i = 0; i < chunkDirs.size(); i++) {
        ChunkDirInfo_t c;

        c.dirname = chunkDirs[i];
        mChunkDirs.push_back(c);
    }
    string errMsg;
    if (! DiskIo::Init(prop, &errMsg)) {
        KFS_LOG_STREAM_ERROR <<
            "DiskIo::Init failure: " << errMsg <<
        KFS_LOG_EOM;
        return false;
    }
    mMaxOpenChunkFiles = std::max(128, std::min(
        GetMaxOpenFds() / DiskIo::GetFdCountPerFile(),
        prop.getValue("chunkServer.maxOpenChunkFiles", 64 << 10)));
    // force a stat of the dirs and update space usage counts
    return (GetTotalSpace(true) >= 0);
}

int
ChunkManager::AllocChunk(kfsFileId_t fileId, kfsChunkId_t chunkId, 
                         kfsSeq_t chunkVersion,
                         bool isBeingReplicated, ChunkInfoHandle **outCih)
{
    string chunkdir;
    ChunkInfoHandle *cih;
    CMI tableEntry = mChunkTable.find(chunkId);

    mIsChunkTableDirty = true;

    if (tableEntry != mChunkTable.end()) {
        // XXX: Need to make the chunk "dirty"
        cih = tableEntry->second;
        if (outCih) {
            *outCih = cih;
        }
        return ChangeChunkVers(cih->chunkInfo.fileId, cih->chunkInfo.chunkId, chunkVersion);
    }

    // Find the directory to use
    chunkdir = GetDirForChunk();
    if (chunkdir == "") {
        KFS_LOG_STREAM_INFO <<
            "No directory has space to host chunk " << chunkId <<
        KFS_LOG_EOM;
        return -ENOSPC;
    }
    
    // Chunks are dirty until they are made stable: A chunk becomes
    // stable when the write lease on the chunk expires and the
    // metaserver says the chunk is now stable.  Dirty chunks are
    // stored in a "dirty" dir; chunks in this dir will get nuked
    // on a chunkserver restart.  This provides a very simple failure
    // handling model.
    chunkdir = GetDirtyChunkPath(chunkdir);
    const string s = MakeChunkPathname(chunkdir, fileId, chunkId, chunkVersion);
    KFS_LOG_STREAM_INFO << "Creating chunk: " << s << KFS_LOG_EOM;

    CleanupInactiveFds();
    mNumChunks++;

    cih = new ChunkInfoHandle();
    cih->chunkInfo.Init(fileId, chunkId, chunkVersion);
    cih->chunkInfo.SetDirname(chunkdir);
    cih->isBeingReplicated = isBeingReplicated;
    cih->createFile = true;
    mChunkTable[chunkId] = cih;
    int ret = OpenChunk(chunkId, O_RDWR);
    if (ret < 0) {
        // open chunk failed: the entry in the chunk table is cleared and
        // Delete(*cih) is also called in OpenChunk().  Return the
        // error code
        return ret;
    }
    if (outCih) {
        *outCih = cih;
    }
    return ret;
}

void
ChunkManager::AllocChunkForAppend(AllocChunkOp* op, int replicationPos, ServerLocation peerLoc)
{
    if (IsWritePending(op->chunkId)) {
        op->statusMsg = "random write in progress";
        op->status = -EINVAL;
    }
    ChunkInfoHandle *cih = 0;
    op->status = AllocChunk(
        op->fileId, op->chunkId, op->chunkVersion, false, &cih);
    if (op->status != 0) {
        return;
    }
    assert(cih);

    cih->sortHelper = gChunkSortHelperManager.GetChunkSortHelper();

    gAtomicRecordAppendManager.AllocateChunk(
        op, replicationPos, peerLoc, cih->dataFH, cih->sortHelper);
    if (op->status == 0) {
        cih->SetWriteAppenderOwns(mChunkInfoLists, true);
        // notify the sort helper that something is coming :)
        if (gChunkSortHelperManager.IsSorterInStreamingMode())
            cih->sortHelper->Enqueue(op);
    }
}

inline static bool
EndsWith(const string& str, const string& suf)
{
    return (str.length() >= suf.length() &&
        str.compare(str.length() - suf.length(), suf.length(), suf) == 0);
}

bool
ChunkManager::IsChunkStable(const ChunkInfoHandle* cih) const
{
    // check if it is in the dirty directory
    return (
        ! EndsWith(cih->chunkInfo.GetDirname(), GetDirtyChunkPath(string())) &&
        (! cih->IsWriteAppenderOwns() ||
            gAtomicRecordAppendManager.IsChunkStable(cih->chunkInfo.chunkId))
    );
}

bool
ChunkManager::IsChunkStable(kfsChunkId_t chunkId) const
{
    CMI const tableEntry = mChunkTable.find(chunkId);
    return (
        tableEntry == mChunkTable.end() ||
        IsChunkStable(tableEntry->second)
    );
}

int
ChunkManager::MakeChunkStable(kfsChunkId_t chunkId)
{
    bool needToSortChunk;
    CMI const it = mChunkTable.find(chunkId);
    if (it == mChunkTable.end()) {
        return -EBADF;
    }
    ChunkInfoHandle* const cih = it->second;
    assert(
        cih && (
        ! cih->IsWriteAppenderOwns() ||
        gAtomicRecordAppendManager.IsChunkStable(chunkId))
    );
    
    // chunks owned by write appender need to be sorted/indexed
    needToSortChunk = cih->IsWriteAppenderOwns() && (cih->chunkInfo.chunkSize > 0);
        
    cih->SetWriteAppenderOwns(mChunkInfoLists, false);
    mPendingWrites.Delete(chunkId, cih->chunkInfo.chunkVersion);
    // strip out the "/dirty" and do the rename
    string       dirname  = cih->chunkInfo.GetDirname();
    const string dirtyDir = GetDirtyChunkPath(string());
    if (! EndsWith(dirname, dirtyDir)) {
        KFS_LOG_STREAM_INFO << "chunk: " << chunkId << " is already stable (final dir: " << dirname << ")" << KFS_LOG_EOM;
        return 0;
    }
    const string oldName = MakeChunkPathname(cih);
    dirname.erase(dirname.length() - dirtyDir.length());
    cih->chunkInfo.SetDirname(dirname);
    const string newName = MakeChunkPathname(cih);
    rename(oldName.c_str(), newName.c_str());
    KFS_LOG_STREAM_INFO << "Making chunk: " << chunkId << " stable (final dir: " << dirname << ")" << KFS_LOG_EOM;

    if (needToSortChunk) {
        KFS_LOG_STREAM_INFO << "Enqueueing request to sort chunk: " << chunkId << KFS_LOG_EOM;

        // There is a bit of hackery here: we set the indexSize to a bogus value;
        // only when the metadata is force-loaded, will this get set to the right value.
        cih->chunkInfo.chunkIndexSize = -1;
        if (gChunkSortHelperManager.IsSorterInStreamingMode()) {
            SorterFlushOp shf(cih->sortHelper->NextSeqnum());
        
            shf.fid = cih->chunkInfo.fileId;
            shf.chunkId = cih->chunkInfo.chunkId;
            shf.chunkVersion = cih->chunkInfo.chunkVersion;
            shf.chunkSize = cih->chunkInfo.chunkSize;
            shf.outputFn = MakeSortedChunkPathname(cih);
            cih->sortHelper->Enqueue(&shf);
        } else {
            SortFileOp shf(cih->sortHelper->NextSeqnum());
        
            shf.fid = cih->chunkInfo.fileId;
            shf.chunkId = cih->chunkInfo.chunkId;
            shf.chunkVersion = cih->chunkInfo.chunkVersion;
            shf.chunkSize = cih->chunkInfo.chunkSize;
            shf.inputFn = MakeChunkPathname(cih);
            shf.outputFn = MakeSortedChunkPathname(cih);
            cih->sortHelper->Enqueue(&shf);
        }

        Release(*cih);
        if (cih->IsFileOpen()) {
            KFS_LOG_STREAM_INFO << "Close on: " << chunkId << " didn't work..." 
                << KFS_LOG_EOM;
        }
    }
    return 0;
}

void
ChunkManager::ChunkSortDone(kfsChunkId_t chunkId, uint32_t indexSz)
{
    CMI const it = mChunkTable.find(chunkId);
    if (it == mChunkTable.end()) {
        return;
    }
    ChunkInfoHandle* const cih = it->second;

    const string sortedName = MakeSortedChunkPathname(cih);
    const string newName = MakeChunkPathname(cih);
    rename(sortedName.c_str(), newName.c_str());

    cih->chunkInfo.expectedIndexSize = indexSz;

    // force a reload of the filehandle and metadata
    Release(*cih);
    
    KFS_LOG_STREAM_INFO << "Sorting of chunk: " << chunkId << " is done!" 
        << " ; expect index size: " << indexSz << KFS_LOG_EOM;

    if (cih->IsFileOpen()) {
        KFS_LOG_STREAM_INFO << "Close on: " << chunkId << " didn't work..." 
            << KFS_LOG_EOM;
    }
}

int
ChunkManager::CoalesceBlock(kfsFileId_t srcFid, kfsChunkId_t srcChunkId,
                            kfsFileId_t dstFid, kfsChunkId_t dstChunkId)
{
    CMI const tableEntry = mChunkTable.find(srcChunkId);
    if (tableEntry == mChunkTable.end()) {
        return -EBADF;
    }
    ChunkInfoHandle& srcCih = *tableEntry->second;
    // Make sure that make chunk stable is done.
    if (srcCih.IsWriteAppenderOwns() || ! IsChunkStable(&srcCih)) {
        KFS_LOG_STREAM_ERROR << "coalesce block:"
            " error: source chunk is not stable" <<
            " from: <" << srcFid << "," << srcChunkId << ">"
            " to: <"   << dstFid << "," << dstChunkId << ">" <<
            (srcCih.IsWriteAppenderOwns() ? " write appender owns" : "") <<
        KFS_LOG_EOM;
        return -EINVAL;
    }
    if (srcCih.isBeingReplicated) {
        // Replicated chunk can not be stable.
        KFS_LOG_STREAM_ERROR << "coalesce block:"
            " error: source chunk is being replicated" <<
            " from: <" << srcFid << "," << srcChunkId << ">"
            " to: <"   << dstFid << "," << dstChunkId << ">" <<
        KFS_LOG_EOM;
        return -EINVAL;
    }
    if (IsWritePending(srcChunkId)) {
        // Chunk with pending writes can not be stable.
        KFS_LOG_STREAM_ERROR << "coalesce block:"
            " error: source chunk has pending write(s)" <<
            " from: <" << srcFid << "," << srcChunkId << ">"
            " to: <"   << dstFid << "," << dstChunkId << ">" <<
        KFS_LOG_EOM;
        return -EINVAL;
    }
    if (mChunkTable.find(dstChunkId) != mChunkTable.end()) {
        KFS_LOG_STREAM_ERROR << "coalesce block:"
            " error: destination chunk already exists" <<
            " from: <" << srcFid << "," << srcChunkId << ">"
            " to: <"   << dstFid << "," << dstChunkId << ">" <<
        KFS_LOG_EOM;
        return -EEXIST;
    }
    int            status  = 0;
    const kfsSeq_t version = 1;
    const string   srcPath = MakeChunkPathname(&srcCih);
    const string   dstPath = MakeChunkPathname(
        srcCih.chunkInfo.GetDirname(), dstFid, dstChunkId, version);
    // keep the old and new until the metaserver tells us it is ok to nuke the old
    if (link(srcPath.c_str(), dstPath.c_str()) != 0) {
        status = errno > 0 ? -errno : -1;
    }
    if (status == 0) {
        ChunkInfoHandle *dstCih = new ChunkInfoHandle();

        dstCih->chunkInfo.Init(dstFid, dstChunkId, version);
        dstCih->chunkInfo.SetDirname(srcCih.chunkInfo.GetDirname());
        dstCih->chunkInfo.chunkSize = srcCih.chunkInfo.chunkSize;
        // is a null blob
        dstCih->chunkInfo.UnloadChecksums();
        dstCih->isBeingReplicated = false;

        mChunkTable[dstChunkId] = dstCih;
        mIsChunkTableDirty = true;
        mNumChunks++;

        // when the coalesce succeeds, we unlink the original file. that'll get the used value back down.
        UpdateDirSpace(dstCih, dstCih->chunkInfo.chunkSize);
        mUsedSpace += dstCih->chunkInfo.chunkSize;
    }
    KFS_LOG_STREAM(status >= 0 ?
             MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelERROR) <<
        "coalesce block:"
        " from: <" << srcFid << "," << srcChunkId << "," << srcPath << ">"
        " to: <"   << dstFid << "," << dstChunkId << "," << dstPath << ">"
        " status: " << status <<
    KFS_LOG_EOM;
    return status;
}

int
ChunkManager::DeleteChunk(kfsChunkId_t chunkId)
{
    const CMI tableEntry = mChunkTable.find(chunkId);

    if (tableEntry == mChunkTable.end()) 
        return -EBADF;

    mIsChunkTableDirty = true;

    ChunkInfoHandle * const cih = tableEntry->second;

    const string fn = MakeChunkPathname(cih);
    unlink(fn.c_str());

    KFS_LOG_STREAM_INFO << "Deleting chunk: " << fn << KFS_LOG_EOM;

    mNumChunks--;
    if (mNumChunks < 0)
        mNumChunks = 0;

    UpdateDirSpace(cih, -cih->chunkInfo.chunkSize);

    mUsedSpace -= cih->chunkInfo.chunkSize;
    mPendingWrites.Delete(chunkId, cih->chunkInfo.chunkVersion);
    mChunkTable.erase(tableEntry);
    Delete(*cih);
    return 0;
}

void
ChunkManager::DumpChunkMap()
{
    ChunkInfoHandle *cih;
    ofstream ofs;

    ofs.open("chunkdump.txt");
    // Dump chunk map in the format of
    // chunkID fileID chunkSize
    for (CMI tableEntry = mChunkTable.begin(); tableEntry != mChunkTable.end();
         ++tableEntry) {
        cih = tableEntry->second;
        ofs << cih->chunkInfo.chunkId << " " << cih->chunkInfo.fileId << " " << cih->chunkInfo.chunkSize << endl;
    }

    ofs.flush();
    ofs.close();
}

void
ChunkManager::DumpChunkMap(ostringstream &ofs)
{
   ChunkInfoHandle *cih;

   // Dump chunk map in the format of
   // chunkID fileID chunkSize
   for (CMI tableEntry = mChunkTable.begin(); tableEntry != mChunkTable.end();
       ++tableEntry) {
       cih = tableEntry->second;
       ofs << cih->chunkInfo.chunkId << " " << cih->chunkInfo.fileId << " " << cih->chunkInfo.chunkSize << endl;
   }
}

int
ChunkManager::WriteChunkMetadata(kfsChunkId_t chunkId, KfsCallbackObj *cb)
{
    CMI tableEntry = mChunkTable.find(chunkId);
    int res;

    if (tableEntry == mChunkTable.end()) 
        return -EBADF;

    ChunkInfoHandle *cih = tableEntry->second;
    WriteChunkMetaOp *wcm = new WriteChunkMetaOp(chunkId, cb);
    DiskIo *d = SetupDiskIo(chunkId, wcm);
    if (d == NULL) {
        delete wcm;
        return -KFS::ESERVERBUSY;
    }

    wcm->diskIo.reset(d);
    wcm->dataBuf = new IOBuffer();
    cih->chunkInfo.Serialize(wcm->dataBuf);
    wcm->dataBuf->ZeroFillLast();
    const size_t numBytes = wcm->dataBuf->BytesConsumable();
    if (KFS_CHUNK_HEADER_SIZE < numBytes) {
        die("bad io buffer size");
    }
    LruUpdate(*cih);

    res = wcm->diskIo->Write(0, numBytes, wcm->dataBuf);
    if (res < 0) {
        delete wcm;
    } else {
        cih->SetMetaWriteInFlight(mChunkInfoLists, wcm);
    }
    return res >= 0 ? 0 : res;
}

int
ChunkManager::ScheduleWriteChunkMetadata(kfsChunkId_t chunkId)
{
    CMI tableEntry = mChunkTable.find(chunkId);

    if (tableEntry == mChunkTable.end()) 
        return -EBADF;
    ChunkInfoHandle *cih = tableEntry->second;
    if (! cih->chunkInfo.AreChecksumsLoaded())
        return -EINVAL;
    cih->ScheduleSyncMeta(mChunkInfoLists);
    return 0;
}

int
ChunkManager::ReadChunkMetadata(kfsChunkId_t chunkId, KfsOp *cb)
{
    CMI tableEntry = mChunkTable.find(chunkId);
    int res = 0;

    if (tableEntry == mChunkTable.end()) 
        return -EBADF;

    ChunkInfoHandle *cih = tableEntry->second;

    LruUpdate(*cih);

    if ((cih->chunkInfo.expectedIndexSize > 0) &&
        (cih->chunkInfo.chunkIndexSize <= 0)) {
        KFS_LOG_STREAM_INFO << "Forcing read of meta-data "
            << " chunk = " << chunkId 
            << " index size 0, but: " 
            << " expected index sz = " 
            << cih->chunkInfo.expectedIndexSize << KFS_LOG_EOM;
        // Force a read of metadata: at this point, the chunk file is
        // no longer open for writing.  Closing the FH is ok.
        cih->dataFH.reset();
        cih->chunkInfo.UnloadChecksums();
    }

    if (cih->chunkInfo.AreChecksumsLoaded())
        return cb->HandleEvent(EVENT_CMD_DONE, (void *) &res);

    if (cih->readChunkMetaOp) {
        // if we have issued a read request for this chunk's metadata,
        // don't submit another one; otherwise, we will simply drive
        // up memory usage for useless IO's
        cih->readChunkMetaOp->AddWaiter(cb);
        return 0;
    }

    ReadChunkMetaOp *rcm = new ReadChunkMetaOp(chunkId, cb);
    DiskIo *d = SetupDiskIo(chunkId, rcm);
    if (d == NULL) {
        delete rcm;
        return -KFS::ESERVERBUSY;
    }
    rcm->diskIo.reset(d);

    res = rcm->diskIo->Read(0, KFS_CHUNK_HEADER_SIZE);
    if (res < 0) {
        delete rcm;
        return res;
    }
    cih->readChunkMetaOp = rcm;
    return 0;
}

void
ChunkManager::ReadChunkMetadataDone(kfsChunkId_t chunkId)
{
    CMI tableEntry = mChunkTable.find(chunkId);

    if (tableEntry == mChunkTable.end()) 
        return;

    ChunkInfoHandle *cih = tableEntry->second;

    LruUpdate(*cih);
    cih->readChunkMetaOp = 0;
}

int
ChunkManager::SetChunkMetadata(const DiskChunkInfo_t &dci, kfsChunkId_t chunkId)
{
    ChunkInfoHandle *cih;
    int res;

    if (GetChunkInfoHandle(chunkId, &cih) < 0)
        return -EBADF;

#if 0
    // for coalesced block, we want to validate that magic #/version #
    // match.  This is because the meta-data will contain the chunkid
    // with which the file was originally created.  For non-coalesced
    // blocks, the chunkid in the meta-data should match what is
    // passed into this function.
    // if ((cih->chunkInfo.isCoalescedBlock &&
    // ((res = dci.Validate()) < 0)) ||
    // ((res = dci.Validate(chunkId)) < 0) 
#endif
    // for now, validate just the magic #/version # in the meta-data.
    // The above commented code needs to be enabled.
    if ((res = dci.Validate()) < 0) {
        KFS_LOG_STREAM_ERROR <<
            "chunk metadata validation mismatch on chunk " << chunkId <<
        KFS_LOG_EOM;
        mCounters.mBadChunkHeaderErrorCount++;
        NotifyMetaCorruptedChunk(chunkId);
        StaleChunk(chunkId);
        return res;
    }

    if ((cih->chunkInfo.chunkIndexSize < 0) &&
        (dci.chunkIndexSize == 0)) {
        // a sort is pending...don't reset index size
        KFS_LOG_STREAM_INFO <<
            "For chunk: " << chunkId <<
            " looks like sort is pending...so not resetting index size" << 
            KFS_LOG_EOM;

    } else {
        cih->chunkInfo.chunkIndexSize = dci.chunkIndexSize;
        KFS_LOG_STREAM_INFO <<
            "For chunk: " << chunkId << 
            " with size: " << cih->chunkInfo.chunkSize <<
            " setting chunk index size: " << dci.chunkIndexSize <<
            KFS_LOG_EOM;
    }
        
    cih->chunkInfo.SetChecksums(dci.chunkBlockChecksum);
    if (cih->chunkInfo.chunkSize > dci.chunkSize) {
        const off_t extra = cih->chunkInfo.chunkSize - dci.chunkSize;
        mUsedSpace -= extra;
        UpdateDirSpace(cih, -extra);
        cih->chunkInfo.chunkSize = dci.chunkSize;
    } else if (cih->chunkInfo.chunkSize < dci.chunkSize) {
        const off_t extra = dci.chunkSize - cih->chunkInfo.chunkSize;
        mUsedSpace += extra;
        UpdateDirSpace(cih, extra);
        KFS_LOG_STREAM_INFO << "Looks like chunk grew in size (possibly sort) "
            << " old size: " << cih->chunkInfo.chunkSize
            << " new size: " << dci.chunkSize << KFS_LOG_EOM;
        cih->chunkInfo.chunkSize = dci.chunkSize;

    }

    return 0;
}

bool
ChunkManager::IsChunkMetadataLoaded(kfsChunkId_t chunkId)
{
    ChunkInfoHandle *cih = 0;
    return (
        GetChunkInfoHandle(chunkId, &cih) >= 0 &&
        cih->chunkInfo.AreChecksumsLoaded()
    );
}

int
ChunkManager::ReadChunkIndex(kfsChunkId_t chunkId, KfsOp *cb)
{
    CMI tableEntry = mChunkTable.find(chunkId);
    int res = 0;

    if (tableEntry == mChunkTable.end()) 
        return -EBADF;

    ChunkInfoHandle *cih = tableEntry->second;

    if (cih->chunkInfo.chunkIndexSize == 0) {
        KFS_LOG_STREAM_INFO << "Chunk: " << chunkId << 
            " has a 0 size index!" << KFS_LOG_EOM;
        cb->HandleEvent(EVENT_DISK_READ, NULL);
        return 0;
    }
    LruUpdate(*cih);
    
    ReadChunkMetaOp *rcm = new ReadChunkMetaOp(chunkId, cb);
    rcm->SetupForIndexRead();
    DiskIo *d = SetupDiskIo(chunkId, rcm);
    if (d == NULL) {
        delete rcm;
        return -KFS::ESERVERBUSY;
    }

    std::string errMsg;

    if (!cih->dataFH->ResetFilesize(CHUNKSIZE + KFS_CHUNK_HEADER_SIZE + 
            cih->chunkInfo.chunkIndexSize, &errMsg)) {
        KFS_LOG_STREAM_INFO <<
            "chunk " << cih->chunkInfo.chunkId << " reset filesize error: " << errMsg <<
        KFS_LOG_EOM;
        cb->HandleEvent(EVENT_DISK_ERROR, &res);
        delete rcm;
        return res;
    }

    rcm->diskIo.reset(d);
    
    res = rcm->diskIo->Read(CHUNKSIZE + KFS_CHUNK_HEADER_SIZE,
        cih->chunkInfo.chunkIndexSize);
    if (res < 0) {
        delete rcm;
        cb->HandleEvent(EVENT_DISK_ERROR, &res);
        return res;
    }
    // XXX: Sriram---don't think we need to stash rcm; it should get deleted in the read done handler
    // cih->readChunkMetaOp = rcm;
    return 0;
}

int
ChunkManager::WriteChunkIndex(kfsChunkId_t chunkId, IOBuffer &chunkIndex, 
    KfsCallbackObj *cb)
{
    if (chunkIndex.BytesConsumable() == 0) {
        // need to pass back the # of bytes written
        int dummy = 0;
        cb->HandleEvent(EVENT_DISK_WROTE, &dummy);
        return 0;
    }

    CMI tableEntry = mChunkTable.find(chunkId);
    int res;

    if (tableEntry == mChunkTable.end()) 
        return -EBADF;

    ChunkInfoHandle *cih = tableEntry->second;
    WriteChunkMetaOp *wcm = new WriteChunkMetaOp(chunkId, cb);
    DiskIo *d = SetupDiskIo(chunkId, wcm);
    if (d == NULL) {
        delete wcm;
        return -KFS::ESERVERBUSY;
    }

    const size_t indexSize = chunkIndex.BytesConsumable();
    wcm->diskIo.reset(d);
    wcm->dataBuf = new IOBuffer();
    wcm->dataBuf->ReplaceKeepBuffersFull(&chunkIndex, 0, chunkIndex.BytesConsumable());
    // need the buffer to be aligned ot block size
    wcm->dataBuf->ZeroFillLast();
    const size_t numBytes = wcm->dataBuf->BytesConsumable();

    LruUpdate(*cih);

    // Allocate space at the end of the chunk to store the index
    std::string errMsg;
    if (! cih->dataFH->GrowFile(CHUNKSIZE + KFS_CHUNK_HEADER_SIZE + numBytes, &errMsg)) {
        KFS_LOG_STREAM_INFO <<
            "chunk " << cih->chunkInfo.chunkId << " grow-file error: " << errMsg <<
        KFS_LOG_EOM;
        delete wcm;
        return -1;
    }

    KFS_LOG_STREAM_INFO << "Scheduling write of chunk index on chunk: "
        << chunkId << " index size: " << numBytes 
        << KFS_LOG_EOM;

    // Write this at the end of the chunk
    res = wcm->diskIo->Write(KFS_CHUNK_HEADER_SIZE + CHUNKSIZE, numBytes, wcm->dataBuf);
    if (res < 0) {
        delete wcm;
    } 
    cih->chunkInfo.chunkIndexSize = indexSize;

    return res >= 0 ? 0 : res;
}

ChunkInfo_t*
ChunkManager::GetChunkInfo(kfsChunkId_t chunkId)
{
    CMI const it = mChunkTable.find(chunkId);
    return (it != mChunkTable.end() ? &(it->second->chunkInfo) : 0);
}

void
ChunkManager::MarkChunkStale(ChunkInfoHandle *cih)
{
    const string s = MakeChunkPathname(cih);
    string staleChunkPathname = MakeStaleChunkPathname(cih);
    rename(s.c_str(), staleChunkPathname.c_str());
    KFS_LOG_STREAM_INFO <<
        "Moving chunk " << cih->chunkInfo.chunkId << " to staleChunks dir" <<
    KFS_LOG_EOM;
}

int
ChunkManager::StaleChunk(kfsChunkId_t chunkId, bool deleteOk)
{
    CMI const it = mChunkTable.find(chunkId);
    if (it == mChunkTable.end()) {
        return -EBADF;
    }
    mIsChunkTableDirty = true;
    assert(it->second);
    ChunkInfoHandle* const cih = it->second;
    if (deleteOk) {
        // WHenever the metaserver tells us a chunk is stale, throw it away
        const string s = MakeChunkPathname(cih);
        unlink(s.c_str());
        KFS_LOG_STREAM_INFO << "Deleting stale chunk: " << s << KFS_LOG_EOM;
    } else {
        // preserve it so we can see what is wrong
        MarkChunkStale(cih);
    }
    mNumChunks--;
    assert(mNumChunks >= 0);
    if (mNumChunks < 0) {
        mNumChunks = 0;
    }
    UpdateDirSpace(cih, -cih->chunkInfo.chunkSize);
    mUsedSpace -= cih->chunkInfo.chunkSize;
    mChunkTable.erase(it);
    Delete(*cih);
    return 0;
}

int
ChunkManager::TruncateChunk(kfsChunkId_t chunkId, off_t chunkSize)
{
    string chunkPathname;
    ChunkInfoHandle *cih;
    uint32_t lastChecksumBlock;
    CMI tableEntry = mChunkTable.find(chunkId);

    // the truncated size should not exceed chunk size.
    if (chunkSize > (off_t) KFS::CHUNKSIZE)
        return -EINVAL;

    if (tableEntry == mChunkTable.end())
        return -EBADF;

    mIsChunkTableDirty = true;

    cih = tableEntry->second;
    chunkPathname = MakeChunkPathname(cih);

    // Cnunk close will truncate it to the cih->chunkInfo.chunkSize

    UpdateDirSpace(cih, -cih->chunkInfo.chunkSize);

    mUsedSpace -= cih->chunkInfo.chunkSize;
    mUsedSpace += chunkSize;
    cih->chunkInfo.chunkSize = chunkSize;

    UpdateDirSpace(cih, cih->chunkInfo.chunkSize);

    lastChecksumBlock = OffsetToChecksumBlockNum(chunkSize);

    // XXX: Could do better; recompute the checksum for this last block
    cih->chunkInfo.chunkBlockChecksum[lastChecksumBlock] = 0;

    return 0;
}

int
ChunkManager::ChangeChunkVers(kfsFileId_t fileId,
                              kfsChunkId_t chunkId, int64_t chunkVersion)
{
    CMI const tableEntry = mChunkTable.find(chunkId);
    if (tableEntry == mChunkTable.end()) {
        return -1;
    }
    ChunkInfoHandle * const cih = tableEntry->second;
    if (cih->IsWriteAppenderOwns() && ! IsChunkStable(chunkId)) {
        KFS_LOG_STREAM_WARN <<
            "attempt to change version on unstable chunk: " << chunkId <<
            " owned by write appender denied" <<
        KFS_LOG_EOM;
        return -1;
    }

    const string oldname = MakeChunkPathname(cih);

    mIsChunkTableDirty = true;

    KFS_LOG_STREAM_INFO <<
        "Chunk " << oldname << " already exists; changing version # " <<
        " from " << cih->chunkInfo.chunkVersion << " to " << chunkVersion <<
        "; checksums are " <<
            (cih->chunkInfo.AreChecksumsLoaded() ? "" : "not ") << "loaded" <<
    KFS_LOG_EOM;        

    mPendingWrites.Delete(chunkId, cih->chunkInfo.chunkVersion);

    const int64_t oldvers = cih->chunkInfo.chunkVersion;

    cih->chunkInfo.chunkVersion = chunkVersion;    
    const string newname = MakeChunkPathname(cih);
    const int ret = rename(oldname.c_str(), newname.c_str());
    if (ret < 0) {
        cih->chunkInfo.chunkVersion = oldvers;
        KFS_LOG_STREAM_INFO << "Rename from " << oldname << " to: " 
                            << newname << " failed with error code: "
                            << errno << KFS_LOG_EOM;
    } else if (! IsChunkStable(chunkId)) {
        MakeChunkStable(chunkId);
    }
    return ret;
}

void
ChunkManager::ReplicationDone(kfsChunkId_t chunkId)
{
    ChunkInfoHandle *cih;
    CMI tableEntry = mChunkTable.find(chunkId);

    if (tableEntry == mChunkTable.end()) {
        return;
    }

    cih = tableEntry->second;

#ifdef DEBUG
    KFS_LOG_STREAM_DEBUG <<
        "Replication for chunk %s is complete..." << MakeChunkPathname(cih) <<
    KFS_LOG_EOM;
#endif

    mIsChunkTableDirty = true;
    cih->isBeingReplicated = false;
    LruUpdate(*cih); // Add it to lru.
}

void
ChunkManager::Start()
{
    mChunkManagerTimeoutImpl = new ChunkManagerTimeoutImpl(this);
    globalNetManager().RegisterTimeoutHandler(mChunkManagerTimeoutImpl);
}

void
ChunkManager::UpdateDirSpace(ChunkInfoHandle *cih, off_t nbytes)
{
    for (uint32_t i = 0; i < mChunkDirs.size(); i++) {
        if (mChunkDirs[i].dirname == cih->chunkInfo.GetDirname()) {
            mChunkDirs[i].usedSpace += nbytes;
            if (mChunkDirs[i].usedSpace < 0)
                mChunkDirs[i].usedSpace = 0;
        }
    }
}

string
ChunkManager::GetDirForChunk()
{
    if (mChunkDirs.size() == 1)
        return mChunkDirs[0].dirname;

    // do weighted random, so that we can fill all drives
    off_t totalFreeSpace = 0;
    for (uint32_t i = 0; i < mChunkDirs.size(); i++) {
        if ((mChunkDirs[i].availableSpace <= 0) ||
            ((mChunkDirs[i].availableSpace - mChunkDirs[i].usedSpace) < (off_t) CHUNKSIZE)) {
            continue;
        }
        totalFreeSpace += (mChunkDirs[i].availableSpace - mChunkDirs[i].usedSpace);
    }

    bool found = false;
    int dirToUse;
    double bucketLow = 0.0, bucketHi = 0.0;
    double randVal = drand48();
    for (uint32_t i = 0; i < mChunkDirs.size(); i++) {
        if ((mChunkDirs[i].availableSpace <= 0) ||
            ((mChunkDirs[i].availableSpace - mChunkDirs[i].usedSpace) < (off_t) CHUNKSIZE)) {
            continue;
        }
        bucketHi = bucketLow + ((double) (mChunkDirs[i].availableSpace - mChunkDirs[i].usedSpace) / (double) totalFreeSpace);
        if ((bucketLow <= randVal) && (randVal <= bucketHi)) {
            dirToUse = i;
            found = true;
            break;
        }
        bucketLow = bucketHi;
    }

    if (!found) {
        // to account for rounding errors, if we didn't pick a drive, but some drive has space, use it.
        for (uint32_t i = 0; i < mChunkDirs.size(); i++) {
            if ((mChunkDirs[i].availableSpace <= 0) ||
                ((mChunkDirs[i].availableSpace - mChunkDirs[i].usedSpace) < (off_t) CHUNKSIZE)) {
                continue;
            }
            dirToUse = i;
            found = true;
            break;
        }
    }        

    if (!found) {
        KFS_LOG_INFO("All drives are full; dir allocation failed");
        return "";
    }

    mLastDriveChosen = dirToUse;
    return mChunkDirs[dirToUse].dirname;
}

string
ChunkManager::MakeSortedChunkPathname(ChunkInfoHandle *cih)
{
    return MakeChunkPathname(
        GetSortedChunkPath(cih->chunkInfo.GetDirname()),
        cih->chunkInfo.fileId,
        cih->chunkInfo.chunkId,
        cih->chunkInfo.chunkVersion
    );
}

string
ChunkManager::MakeChunkPathname(ChunkInfoHandle *cih)
{
    return MakeChunkPathname(
        cih->chunkInfo.GetDirname(),
        cih->chunkInfo.fileId,
        cih->chunkInfo.chunkId,
        cih->chunkInfo.chunkVersion
    );
}

string
ChunkManager::MakeChunkPathname(const string &chunkdir, kfsFileId_t fid, kfsChunkId_t chunkId, kfsSeq_t chunkVersion)
{
    ostringstream os;

    os << chunkdir << '/' << fid << '.' << chunkId << '.' << chunkVersion;
    return os.str();
}

string
ChunkManager::MakeStaleChunkPathname(ChunkInfoHandle *cih)
{
    return MakeChunkPathname(
        GetStaleChunkPath(cih->chunkInfo.GetDirname()),
        cih->chunkInfo.fileId,
        cih->chunkInfo.chunkId,
        cih->chunkInfo.chunkVersion
    );
}

void
ChunkManager::MakeChunkInfoFromPathname(const string &pathname, off_t filesz, ChunkInfoHandle **result)
{
    string::size_type slash = pathname.rfind('/');
    ChunkInfoHandle *cih;

    if (slash == string::npos) {
        *result = NULL;
        return;
    }
    
    string chunkFn, dirname;
    vector<string> component;

    dirname.assign(pathname, 0, slash);
    chunkFn.assign(pathname, slash + 1, string::npos);
    split(component, chunkFn, '.');
    assert(component.size() == 3);

    chunkId_t chunkId = atoll(component[1].c_str());
    if (GetChunkInfoHandle(chunkId, &cih) == 0) {
        KFS_LOG_STREAM_INFO << "Duplicate chunk " << chunkId <<
            " with path: " << pathname << KFS_LOG_EOM;
        *result = NULL;
        return;
    }

    cih = new ChunkInfoHandle();    
    cih->chunkInfo.fileId = atoll(component[0].c_str());
    cih->chunkInfo.chunkId = atoll(component[1].c_str());
    cih->chunkInfo.chunkVersion = atoll(component[2].c_str());
    if (filesz >= (off_t) KFS_CHUNK_HEADER_SIZE)
        cih->chunkInfo.chunkSize = filesz - KFS_CHUNK_HEADER_SIZE;
    cih->chunkInfo.SetDirname(dirname);
    *result = cih;
    /*
    KFS_LOG_STREAM_DEBUG << "From " << chunkFn << " restored: " <<
        cih->chunkInfo.fileId << ", " << cih->chunkInfo.chunkId << ", " <<
        cih->chunkInfo.chunkVersion <<
    KFS_LOG_EOM;
    */
}

int
ChunkManager::OpenChunk(kfsChunkId_t chunkId, 
                        int openFlags)
{
    const CMI tableEntry = mChunkTable.find(chunkId);

    if (tableEntry == mChunkTable.end()) {
        KFS_LOG_STREAM_DEBUG << "no such chunk: " << chunkId << KFS_LOG_EOM;
        return -EBADF;
    }
    ChunkInfoHandle * const cih = tableEntry->second;

    const string fn = MakeChunkPathname(cih);

    if (cih->IsFileOpen()) {
        return 0;
    }
    if (! cih->dataFH) {
        cih->dataFH.reset(new DiskIo::File());
    }
    string errMsg;
    const bool kReserveFileSpace = true;
    if (! cih->dataFH->Open(fn.c_str(),
            CHUNKSIZE + KFS_CHUNK_HEADER_SIZE,
            (openFlags & (O_WRONLY | O_RDWR)) == 0,
            kReserveFileSpace, cih->createFile, &errMsg)) {
        //
        // we are unable to open/create a file. notify the metaserver
        // of lost data so that it can re-replicate if needed.
        //
        mCounters.mOpenErrorCount++;
        NotifyMetaCorruptedChunk(chunkId);
        mChunkTable.erase(tableEntry);        
        Delete(*cih);

        KFS_LOG_STREAM_ERROR <<
            "failed to open or create chunk file: " << fn << " :" << errMsg <<
        KFS_LOG_EOM;
        return -EBADF;
    }
    globals().ctrOpenDiskFds.Update(1);
    LruUpdate(*cih);

    // the checksums will be loaded async
    return 0;
}

int
ChunkManager::CloseChunk(kfsChunkId_t chunkId)
{
    CMI tableEntry = mChunkTable.find(chunkId);

    if (tableEntry == mChunkTable.end()) {
        return -1;
    }

    ChunkInfoHandle* const cih = tableEntry->second;

    if (cih->IsWriteAppenderOwns()) {
        KFS_LOG_STREAM_INFO <<
            "Ignoring close chunk on chunk: " << chunkId <<
            " open for append " <<
        KFS_LOG_EOM;
        return -1;
    }

    // Close file if not in use.
    if (cih->IsFileOpen() && ! cih->IsFileInUse() && ! cih->SyncMeta()) {
        Release(*cih);
    } else {
        KFS_LOG_STREAM_INFO <<
            "Didn't release chunk " << cih->chunkInfo.chunkId <<
            " on close;  might give up lease" <<
        KFS_LOG_EOM;
        gLeaseClerk.RelinquishLease(cih->chunkInfo.chunkId);
    }
    return 0;
}

int
ChunkManager::ChunkSize(kfsChunkId_t chunkId, kfsFileId_t &fid, off_t *chunkSize, bool *araStableFlag)
{
    ChunkInfoHandle *cih;

    if (GetChunkInfoHandle(chunkId, &cih) < 0)
        return -EBADF;

    fid = cih->chunkInfo.fileId;
    *chunkSize = cih->chunkInfo.chunkSize;
    if (araStableFlag) {
        *araStableFlag = ! cih->IsWriteAppenderOwns() ||
            gAtomicRecordAppendManager.IsChunkStable(chunkId);
    }

    return 0;
}

void
ChunkManager::GetDriveName(ReadOp *op)
{
    ChunkInfoHandle *cih;

    if (GetChunkInfoHandle(op->chunkId, &cih) < 0)
        return;

    // provide the path to the client for telemetry
    op->driveName = cih->chunkInfo.dirname;
}

int
ChunkManager::ReadChunk(ReadOp *op)
{
    ssize_t res;
    DiskIo *d;
    ChunkInfoHandle *cih;
    off_t offset;
    size_t numBytesIO;

    if (GetChunkInfoHandle(op->chunkId, &cih) < 0)
        return -EBADF;

    // provide the path to the client for telemetry
    op->driveName = cih->chunkInfo.dirname;

    // the checksums should be loaded...
    cih->chunkInfo.VerifyChecksumsLoaded();

    if (op->chunkVersion != cih->chunkInfo.chunkVersion) {
        KFS_LOG_STREAM_INFO << "Version # mismatch (have=" <<
            cih->chunkInfo.chunkVersion << " vs asked=" << op->chunkVersion <<
            ")...failing a read" <<
        KFS_LOG_EOM;
        return -KFS::EBADVERS;
    }
    d = SetupDiskIo(op->chunkId, op);
    if (d == NULL)
        return -KFS::ESERVERBUSY;

    op->diskIo.reset(d);

    // schedule a read based on the chunk size
    if (op->offset >= cih->chunkInfo.chunkSize) {
        op->numBytesIO = 0;
    } else if ((off_t) (op->offset + op->numBytes) > cih->chunkInfo.chunkSize) {
        op->numBytesIO = cih->chunkInfo.chunkSize - op->offset;
    } else {
        op->numBytesIO = op->numBytes;
    }

    if (op->numBytesIO == 0)
        return -EIO;

    // for checksumming to work right, reads should be in terms of
    // checksum-blocks.
    offset = OffsetToChecksumBlockStart(op->offset);

    numBytesIO = OffsetToChecksumBlockEnd(op->offset + op->numBytesIO - 1) - offset;

    // Make sure we don't try to read past EOF; the checksumming will
    // do the necessary zero-padding. 
    if ((off_t) (offset + numBytesIO) > cih->chunkInfo.chunkSize)
        numBytesIO = cih->chunkInfo.chunkSize - offset;
    
    if ((res = op->diskIo->Read(offset + KFS_CHUNK_HEADER_SIZE, numBytesIO)) < 0)
        return -EIO;

    // read was successfully scheduled
    return 0;
}

int
ChunkManager::WriteChunk(WriteOp *op)
{
    ChunkInfoHandle *cih;
    int res;

    if (GetChunkInfoHandle(op->chunkId, &cih) < 0)
        return -EBADF;

    // the checksums should be loaded...
    cih->chunkInfo.VerifyChecksumsLoaded();
    
    // schedule a write based on the chunk size.  Make sure that a
    // write doesn't overflow the size of a chunk.
    op->numBytesIO = min((size_t) (KFS::CHUNKSIZE - op->offset), op->numBytes);

    if (op->numBytesIO <= 0 || op->offset < 0)
        return -EINVAL;

    const int64_t addedBytes(op->offset + op->numBytesIO - cih->chunkInfo.chunkSize);
    if (addedBytes > 0 && mUsedSpace + addedBytes >= mTotalSpace) {
        KFS_LOG_STREAM_ERROR <<
            "out of disk space: " << mUsedSpace << " + " << addedBytes <<
            " = " << (mUsedSpace + addedBytes) << " >= " << mTotalSpace <<
        KFS_LOG_EOM;
	return -ENOSPC;
    }

    off_t   offset     = op->offset;
    ssize_t numBytesIO = op->numBytesIO;
    if ((OffsetToChecksumBlockStart(offset) == offset) &&
        ((size_t) numBytesIO >= (size_t) CHECKSUM_BLOCKSIZE)) {
        if (numBytesIO % CHECKSUM_BLOCKSIZE != 0) {
            assert(numBytesIO % CHECKSUM_BLOCKSIZE != 0);
            return -EINVAL;
        }
        if (op->wpop && !op->isFromReReplication &&
                op->checksums.size() == size_t(numBytesIO / CHECKSUM_BLOCKSIZE)) {
            assert(op->checksums[0] == op->wpop->checksum || op->checksums.size() > 1);
        } else {
            op->checksums = ComputeChecksums(op->dataBuf, numBytesIO);
        }
    } else {
        if ((size_t) numBytesIO >= (size_t) CHECKSUM_BLOCKSIZE) {
            assert((size_t) numBytesIO < (size_t) CHECKSUM_BLOCKSIZE);
            return -EINVAL;
        }
        int            off     = (int)(offset % CHECKSUM_BLOCKSIZE);
        const uint32_t blkSize = (size_t(off + numBytesIO) > CHECKSUM_BLOCKSIZE) ?
            2 * CHECKSUM_BLOCKSIZE : CHECKSUM_BLOCKSIZE;

        op->checksums.clear();
        // The checksum block we are after is beyond the current
        // end-of-chunk.  So, treat that as a 0-block and splice in.
        if (offset - off >= cih->chunkInfo.chunkSize) {
            IOBuffer data;
            data.ReplaceKeepBuffersFull(op->dataBuf, off, numBytesIO);
            data.ZeroFill(blkSize - (off + numBytesIO));
            op->dataBuf->Move(&data);
        } else {
            // Need to read the data block over which the checksum is
            // computed. 
            if (op->rop == NULL) {
                // issue a read
                ReadOp *rop = new ReadOp(op, offset - off, blkSize);
                KFS_LOG_STREAM_DEBUG <<
                    "Write triggered a read for offset=" << offset <<
                KFS_LOG_EOM;
                op->rop = rop;
                rop->Execute();
                // It is possible that the both read and write ops are complete
                // at this point. This normally happens in the case of errors.
                // In such cases all error handlers are already invoked.
                // If not then the write op will be restarted once read op
                // completes.
                // Return now.
                return 0;
            }
            // If the read failed, cleanup and bail
            if (op->rop->status < 0) {
                op->status = op->rop->status;
                op->rop->wop = NULL;
                delete op->rop;
                op->rop = NULL;
                return op->HandleDone(EVENT_DISK_ERROR, NULL);
            }

            // All is good.  So, get on with checksumming
            op->rop->dataBuf->ReplaceKeepBuffersFull(op->dataBuf, off, numBytesIO);

            delete op->dataBuf;
            op->dataBuf = op->rop->dataBuf;
            op->rop->dataBuf = NULL;
            // If the buffer doesn't have a full CHECKSUM_BLOCKSIZE worth
            // of data, zero-pad the end.  We don't need to zero-pad the
            // front because the underlying filesystem will zero-fill when
            // we read a hole.
            ZeroPad(op->dataBuf);
        }

        assert(op->dataBuf->BytesConsumable() == (int) blkSize);
        op->checksums = ComputeChecksums(op->dataBuf, blkSize);

        // Trim data at the buffer boundary from the beginning, to make write
        // offset close to where we were asked from.
        int numBytes(numBytesIO);
        offset -= off;
        op->dataBuf->TrimAtBufferBoundaryLeaveOnly(off, numBytes);
        offset += off;
        numBytesIO = numBytes;
    }

    DiskIo *d = SetupDiskIo(op->chunkId, op);
    if (d == NULL)
        return -KFS::ESERVERBUSY;

    op->diskIo.reset(d);

    /*
    KFS_LOG_STREAM_DEBUG <<
        "Checksum for chunk: " << op->chunkId << ", offset=" << op->offset <<
        ", bytes=" << op->numBytesIO << ", # of cksums=" << op->checksums.size() <<
    KFS_LOG_EOM;
    */

    res = op->diskIo->Write(offset + KFS_CHUNK_HEADER_SIZE, numBytesIO, op->dataBuf);
    if (res >= 0) {
        UpdateChecksums(cih, op);
        assert(res <= numBytesIO);
        res = std::min(res, int(op->numBytesIO));
        op->numBytesIO = numBytesIO;
    }
    return res;
}

void
ChunkManager::UpdateChecksums(ChunkInfoHandle *cih, WriteOp *op)
{
    mIsChunkTableDirty = true;

    off_t endOffset = op->offset + op->numBytesIO;

    // the checksums should be loaded...
    cih->chunkInfo.VerifyChecksumsLoaded();

    for (vector<uint32_t>::size_type i = 0; i < op->checksums.size(); i++) {
        off_t offset = op->offset + i * CHECKSUM_BLOCKSIZE;
        uint32_t checksumBlock = OffsetToChecksumBlockNum(offset);

        cih->chunkInfo.chunkBlockChecksum[checksumBlock] = op->checksums[i];
    }

    if (cih->chunkInfo.chunkSize < endOffset) {

        UpdateDirSpace(cih, endOffset - cih->chunkInfo.chunkSize);

	mUsedSpace += endOffset - cih->chunkInfo.chunkSize;
        cih->chunkInfo.chunkSize = endOffset;

    }
    assert(0 <= mUsedSpace && mUsedSpace <= mTotalSpace);
}

void
ChunkManager::ReadChunkDone(ReadOp *op)
{
    ChunkInfoHandle *cih = NULL;
    
    if ((GetChunkInfoHandle(op->chunkId, &cih) < 0) ||
        (op->chunkVersion != cih->chunkInfo.chunkVersion)) {
        AdjustDataRead(op);
        if (cih) {
            KFS_LOG_STREAM_INFO << "Version # mismatch (have=" <<
                cih->chunkInfo.chunkVersion <<
                " vs asked=" << op->chunkVersion << ")" <<
            KFS_LOG_EOM;
        }
        op->status = -KFS::EBADVERS;
        return;
    }

    ZeroPad(op->dataBuf);

    assert(op->dataBuf->BytesConsumable() >= (int) CHECKSUM_BLOCKSIZE);

    // either nothing to verify or it better match

    bool mismatch = false;

    // figure out the block we are starting from and grab all the checksums
    vector<uint32_t>::size_type i, checksumBlock = OffsetToChecksumBlockNum(op->offset);
    op->checksum = ComputeChecksums(op->dataBuf, op->dataBuf->BytesConsumable());

    // the checksums should be loaded...
    if (!cih->chunkInfo.AreChecksumsLoaded()) {
        // the read took too long; the checksums got paged out.  ask the client to retry
        KFS_LOG_STREAM_INFO << "Checksums for chunk " <<
            cih->chunkInfo.chunkId  <<
            " got paged out; returning EAGAIN to client" <<
        KFS_LOG_EOM;
        op->status = -EAGAIN;
        return;
    }

    cih->chunkInfo.VerifyChecksumsLoaded();

    for (i = 0; i < op->checksum.size() &&
             checksumBlock < MAX_CHUNK_CHECKSUM_BLOCKS;
         checksumBlock++, i++) {
        if (op->checksum[i] == cih->chunkInfo.chunkBlockChecksum[checksumBlock]) {
            continue;
        }
        mismatch = true;
        break;
    }

    if (!mismatch) {
        // for checksums to verify, we did reads in multiples of
        // checksum block sizes.  so, get rid of the extra
        AdjustDataRead(op);
        return;
    }

    KFS_LOG_STREAM_ERROR <<
        "Checksum mismatch for chunk=" << op->chunkId <<
        " offset="    << op->offset <<
        " bytes="     << op->numBytesIO <<
        ": expect: "  << cih->chunkInfo.chunkBlockChecksum[checksumBlock] <<
        " computed: " << op->checksum[i] <<
    KFS_LOG_EOM;

    op->status = -KFS::EBADCKSUM;

    // Notify the metaserver that the chunk we have is "bad"; the
    // metaserver will re-replicate this chunk.
    mCounters.mReadChecksumErrorCount++;
    NotifyMetaCorruptedChunk(op->chunkId);
    
    // Take out the chunk from our side
    StaleChunk(op->chunkId);
}

void
ChunkManager::NotifyMetaCorruptedChunk(kfsChunkId_t chunkId)
{
    ChunkInfoHandle *cih;

    if (GetChunkInfoHandle(chunkId, &cih) < 0) {
        KFS_LOG_STREAM_ERROR << 
            "Unable to notify metaserver of corrupt chunk: " << chunkId <<
        KFS_LOG_EOM;
        return;
    }

    mCounters.mCorruptedChunksCount++;
    KFS_LOG_STREAM_INFO <<
        "Notifying metaserver of lost/corrupt chunk (" <<  chunkId <<
        ") in file " << cih->chunkInfo.fileId <<
        " lost total: " << mCounters.mCorruptedChunksCount <<
    KFS_LOG_EOM;

    // This op will get deleted when we get an ack from the metaserver
    CorruptChunkOp *ccop = new CorruptChunkOp(0, cih->chunkInfo.fileId, 
                                              chunkId);
    gMetaServerSM.EnqueueOp(ccop);
    // Meta server automatically cleans up leases for corrupted chunks.
    gLeaseClerk.UnRegisterLease(chunkId);
}

//
// directory with dirname is unaccessable; maybe drive failed.  so,
// notify metaserver of lost blocks.  the metaserver will then
// re-replicate.
//
void
ChunkManager::NotifyMetaChunksLost(const string &dirname)
{
    ChunkInfoHandle *cih;
    CMI iter = mChunkTable.begin();
    
    while (iter != mChunkTable.end()) {
        cih = iter->second;
        if (cih->chunkInfo.GetDirname() != dirname) {
            ++iter;
            continue;
        }
        assert(iter->first == cih->chunkInfo.chunkId);
        KFS_LOG_STREAM_INFO <<
            "Notifying metaserver of lost chunk (" << iter->first <<
            ") in file " << cih->chunkInfo.fileId <<
            " in dir " << dirname <<
        KFS_LOG_EOM;
        mCounters.mDirLostChunkCount++;
        // This op will get deleted when we get an ack from the metaserver
        CorruptChunkOp *ccop = new CorruptChunkOp(0, cih->chunkInfo.fileId, 
                                                  iter->first);
        // so that the lost chunk doesn't get accounted in the metaserver's corrupt chunks count
        ccop->isChunkLost = 1;
        gMetaServerSM.EnqueueOp(ccop);
        // get rid of chunkid from our list
        mChunkTable.erase(iter++);
        Delete(*cih);
    }
}

void
ChunkManager::ZeroPad(IOBuffer *buffer)
{
    int bytesFilled = buffer->BytesConsumable();
    if ((bytesFilled % CHECKSUM_BLOCKSIZE) == 0)
        return;

    int numToZero = CHECKSUM_BLOCKSIZE - (bytesFilled % CHECKSUM_BLOCKSIZE);
    if (numToZero > 0) {
        // pad with 0's
        buffer->ZeroFill(numToZero);
    }
}

void
ChunkManager::AdjustDataRead(ReadOp *op)
{
    op->dataBuf->Consume(
        op->offset - OffsetToChecksumBlockStart(op->offset));
    op->dataBuf->Trim(op->numBytesIO);
}

uint32_t 
ChunkManager::GetChecksum(kfsChunkId_t chunkId, off_t offset)
{
    ChunkInfoHandle *cih;

    if (offset < 0 || GetChunkInfoHandle(chunkId, &cih) < 0)
        return 0;

    const uint32_t checksumBlock = OffsetToChecksumBlockNum(offset);
    // the checksums should be loaded...
    cih->chunkInfo.VerifyChecksumsLoaded();

    assert(checksumBlock < MAX_CHUNK_CHECKSUM_BLOCKS);

    return cih->chunkInfo.chunkBlockChecksum[
        min(MAX_CHUNK_CHECKSUM_BLOCKS - 1, checksumBlock)];
}

vector<uint32_t>
ChunkManager::GetChecksums(kfsChunkId_t chunkId, off_t offset, size_t numBytes)
{
    CMI const tableEntry = mChunkTable.find(chunkId);

    if (offset < 0 || tableEntry == mChunkTable.end()) 
        return vector<uint32_t>();

    const ChunkInfoHandle * const cih = tableEntry->second;
    // the checksums should be loaded...
    cih->chunkInfo.VerifyChecksumsLoaded();

    return (vector<uint32_t>(
        cih->chunkInfo.chunkBlockChecksum +
            OffsetToChecksumBlockNum(offset),
        cih->chunkInfo.chunkBlockChecksum +
            min(MAX_CHUNK_CHECKSUM_BLOCKS,
                OffsetToChecksumBlockNum(
                    offset + numBytes + CHECKSUM_BLOCKSIZE - 1))
    ));
}

DiskIo *
ChunkManager::SetupDiskIo(kfsChunkId_t chunkId, KfsOp *op)
{
    ChunkInfoHandle *cih;
    CMI tableEntry = mChunkTable.find(chunkId);

    if (tableEntry == mChunkTable.end()) {
        return NULL;
    }

    cih = tableEntry->second;
    if (!cih->IsFileOpen()) {
        CleanupInactiveFds();
        if (OpenChunk(chunkId, O_RDWR) < 0) 
            return NULL;
    }
 
    LruUpdate(*cih);
    return new DiskIo(cih->dataFH, op);
}
    
void
ChunkManager::CancelChunkOp(KfsCallbackObj *cont, kfsChunkId_t chunkId)
{
    // Cancel the chunk operations scheduled by KfsCallbackObj on chunkId.
    // XXX: Fill it...
}

//
// dump out the contents of the chunkTable to disk
//
void
ChunkManager::Checkpoint()
{
    CheckpointOp *cop;
    // on the macs, i can't declare CMI iter;
    CMI iter = mChunkTable.begin();

    mNextCheckpointTime = globalNetManager().Now() + mCheckpointIntervalSecs;

    if (!mIsChunkTableDirty)
        return;

    // KFS_LOG_STREAM_DEBUG << "Checkpointing state" << KFS_LOG_EOM;
    cop = new CheckpointOp(1);
    
#if 0
    // there are no more checkpoints on the chunkserver...this will all go
    // we are using this to rotate logs...

    ChunkInfoHandle *cih;

    for (iter = mChunkTable.begin(); iter != mChunkTable.end(); ++iter) {
        cih = iter->second;
        // If a chunk is being replicated, then it is not yet a part
        // of the namespace.  When replication is done, it becomes a
        // part of the namespace.  This model keeps recovery simple:
        // if we die in the midst of replicating a chunk, on restart,
        // we will the chunk as an orphan and throw it away.
        if (cih->isBeingReplicated)
            continue;
        cop->data << cih->chunkInfo.fileId << ' ';
        cop->data << cih->chunkInfo.chunkId << ' ';
        cop->data << cih->chunkInfo.chunkSize << ' ';
        cop->data << cih->chunkInfo.chunkVersion << ' ';

        cop->data << MAX_CHUNK_CHECKSUM_BLOCKS << ' ';
        for (uint32_t i = 0; i < MAX_CHUNK_CHECKSUM_BLOCKS; ++i) {
            cop->data << cih->chunkInfo.chunkBlockChecksum[i] << ' ';
        }
        cop->data << endl;
    }
#endif
    
    gLogger.Submit(cop);

    // Now, everything is clean...
    mIsChunkTableDirty = false;
}

//
// Get all the chunk directory entries from all the places we can
// store the chunks into a single array.
//
int
ChunkManager::GetChunkDirsEntries(struct dirent ***namelist)
{
    struct dirent **entries;
    vector<struct dirent **> dirEntries;
    vector<int> dirEntriesCount;
    int res, numChunkFiles = 0;
    uint32_t i;

    *namelist = NULL;
    for (i = 0; i < mChunkDirs.size(); i++) {
        res = scandir(mChunkDirs[i].dirname.c_str(), &entries, 0, alphasort);
        if (res < 0) {
            KFS_LOG_STREAM_INFO <<
                "unable to open " << mChunkDirs[i].dirname <<
            KFS_LOG_EOM;
            for (i = 0; i < dirEntries.size(); i++) {
                entries = dirEntries[i];
                for (int j = 0; j < dirEntriesCount[i]; j++)
                    free(entries[j]);
                free(entries);
            }
            dirEntries.clear();
            return -1;
        }
        dirEntries.push_back(entries);
        dirEntriesCount.push_back(res);
        numChunkFiles += res;
    }
    
    // Get all the directory entries into one giganto array
    *namelist = (struct dirent **) malloc(sizeof(struct dirent **) * numChunkFiles);

    numChunkFiles = 0;
    for (i = 0; i < dirEntries.size(); i++) {
        int count = dirEntriesCount[i];
        entries = dirEntries[i];

        memcpy((*namelist) + numChunkFiles, entries, count * sizeof(struct dirent **));
        numChunkFiles += count;
    }
    return numChunkFiles;
}

void
ChunkManager::GetChunkPathEntries(vector<string> &pathnames)
{
    uint32_t i;
    struct dirent **entries = NULL;
    int res;

    for (i = 0; i < mChunkDirs.size(); i++) {
        res = scandir(mChunkDirs[i].dirname.c_str(), &entries, 0, alphasort);
        if (res < 0) {
            KFS_LOG_STREAM_INFO <<
                "unable to open " << mChunkDirs[i].dirname <<
            KFS_LOG_EOM;
            mChunkDirs[i].availableSpace = -1;
            continue;
        }
        for (int j = 0; j < res; j++) {
            string s = mChunkDirs[i].dirname + "/" + entries[j]->d_name;
            pathnames.push_back(s);
            free(entries[j]);
        }
        free(entries);
    }
}

void
ChunkManager::Restart()
{
    int version;

    version = gLogger.GetVersionFromCkpt();
    if (version == gLogger.GetLoggerVersionNum()) {
        Restore();
    } else {
        std::cout << "Unsupported version...copy out the data and copy it back in...." << std::endl;
        exit(-1);
    }

    // Write out a new checkpoint file with just version and set it at 2
    gLogger.Checkpoint(NULL);
}

//
// On a restart, whatever chunks were dirty need to be nuked: we may
// have had writes pending to them and we never flushed them to disk.
//
void
ChunkManager::RemoveDirtyChunks()
{
    uint32_t i;
    struct dirent **entries = NULL;

    for (i = 0; i < mChunkDirs.size(); i++) {
        string dirname = GetDirtyChunkPath(mChunkDirs[i].dirname);
        int nentries = scandir(dirname.c_str(), &entries, 0, alphasort);
        if (nentries < 0) {
            KFS_LOG_STREAM_INFO <<
                "unable to open " << mChunkDirs[i].dirname <<
                KFS_LOG_EOM;
            continue;
        }
        for (int j = 0; j < nentries; j++) {
            string s = dirname + "/" + entries[j]->d_name;
            struct stat buf;
            int res;

            free(entries[j]);
            res = stat(s.c_str(), &buf);

            if ((res < 0) || (!S_ISREG(buf.st_mode)))
                continue;

            KFS_LOG_STREAM_INFO <<
                "Cleaning out dirty chunk: " << s <<
            KFS_LOG_EOM;
            unlink(s.c_str());
        }
        free(entries);
    }
}

void
ChunkManager::Restore()
{
    // sort all the chunk names alphabetically in each of the
    // directories
    vector<string> chunkPathnames;
    struct stat buf;
    int res;
    uint32_t i, numChunkFiles;

    RemoveDirtyChunks();

    GetChunkPathEntries(chunkPathnames);

    numChunkFiles = chunkPathnames.size();
    // each chunk file is of the form: <fileid>.<chunkid>.<chunkversion>  
    // parse the filename to extract out the chunk info
    for (i = 0; i < numChunkFiles; ++i) {
        string s = chunkPathnames[i];
        ChunkInfoHandle *cih;
        res = stat(s.c_str(), &buf);
        if ((res < 0) || (!S_ISREG(buf.st_mode)))
            continue;
        MakeChunkInfoFromPathname(s, buf.st_size, &cih);
        if (cih != NULL)
            AddMapping(cih);
        else {
            KFS_LOG_STREAM_INFO <<
                "Deleting possibly duplicate file " << s <<
            KFS_LOG_EOM;
            unlink(s.c_str());
        }
    }
}

void
ChunkManager::AddMapping(ChunkInfoHandle *cih)
{
    mNumChunks++;
    mChunkTable[cih->chunkInfo.chunkId] = cih;
    mUsedSpace += cih->chunkInfo.chunkSize;
    UpdateDirSpace(cih, cih->chunkInfo.chunkSize);
}

void
ChunkManager::AddMapping(const ChunkInfo_t& ci)
{
    ChunkInfoHandle *cih = new ChunkInfoHandle();
    cih->chunkInfo = ci;
    mNumChunks++;
    mChunkTable[cih->chunkInfo.chunkId] = cih;
    mUsedSpace += cih->chunkInfo.chunkSize;
    UpdateDirSpace(cih, cih->chunkInfo.chunkSize);
}

void
ChunkManager::ReplayAllocChunk(kfsFileId_t fileId, kfsChunkId_t chunkId,
                               int64_t chunkVersion)
{
    ChunkInfoHandle *cih;

    mIsChunkTableDirty = true;

    if (GetChunkInfoHandle(chunkId, &cih) == 0) {
        // If the entry exists, just update the version
        cih->chunkInfo.chunkVersion = chunkVersion;
        mChunkTable[chunkId] = cih;
        return;
    }
    mNumChunks++;
    // after replay is done, when we verify entries in the table, we
    // stat the file and fix up the sizes then.  so, no need to do
    // anything here.
    cih = new ChunkInfoHandle();
    cih->chunkInfo.fileId = fileId;
    cih->chunkInfo.chunkId = chunkId;
    cih->chunkInfo.chunkVersion = chunkVersion;
    mChunkTable[chunkId] = cih;
}

void
ChunkManager::ReplayChangeChunkVers(kfsFileId_t fileId, kfsChunkId_t chunkId,
                                    int64_t chunkVersion)
{
    ChunkInfoHandle *cih;

    if (GetChunkInfoHandle(chunkId, &cih) != 0) 
        return;

    KFS_LOG_STREAM_DEBUG << "Chunk " << chunkId <<
        " already exists; changing version # to " << chunkVersion <<
    KFS_LOG_EOM;    
    // Update the version #
    cih->chunkInfo.chunkVersion = chunkVersion;
    mChunkTable[chunkId] = cih;
    mIsChunkTableDirty = true;
}

void
ChunkManager::ReplayDeleteChunk(kfsChunkId_t chunkId)
{
    ChunkInfoHandle *cih;
    CMI tableEntry = mChunkTable.find(chunkId);

    mIsChunkTableDirty = true;

    if (tableEntry != mChunkTable.end()) {
        cih = tableEntry->second;
        mUsedSpace -= cih->chunkInfo.chunkSize;
        mChunkTable.erase(chunkId);
        Delete(*cih);

        mNumChunks--;
        assert(mNumChunks >= 0);
        if (mNumChunks < 0)
            mNumChunks = 0;

    }
}

void
ChunkManager::ReplayWriteDone(kfsChunkId_t chunkId, off_t chunkSize,
                              off_t offset, vector<uint32_t> checksums)
{
    ChunkInfoHandle *cih;
    int res;

    res = GetChunkInfoHandle(chunkId, &cih);
    if (res < 0)
        return;

    mIsChunkTableDirty = true;
    mUsedSpace -= cih->chunkInfo.chunkSize;    
    cih->chunkInfo.chunkSize = chunkSize;
    mUsedSpace += cih->chunkInfo.chunkSize;
    
    for (vector<uint32_t>::size_type i = 0; i < checksums.size(); i++) {
        off_t currOffset = offset + i * CHECKSUM_BLOCKSIZE;
        size_t checksumBlock = OffsetToChecksumBlockNum(currOffset);
        
        cih->chunkInfo.chunkBlockChecksum[checksumBlock] = checksums[i];
    }
}

void
ChunkManager::ReplayTruncateDone(kfsChunkId_t chunkId, off_t chunkSize)
{
    ChunkInfoHandle *cih;
    int res;
    off_t lastChecksumBlock;

    res = GetChunkInfoHandle(chunkId, &cih);
    if (res < 0)
        return;

    mIsChunkTableDirty = true;
    mUsedSpace -= cih->chunkInfo.chunkSize;    
    cih->chunkInfo.chunkSize = chunkSize;
    mUsedSpace += cih->chunkInfo.chunkSize;

    lastChecksumBlock = OffsetToChecksumBlockNum(chunkSize);

    cih->chunkInfo.chunkBlockChecksum[lastChecksumBlock] = 0;
}

void
ChunkManager::GetHostedChunks(
    vector<ChunkInfo_t> &stable,
    vector<ChunkInfo_t> &notStable,
    vector<ChunkInfo_t> &notStableAppend)
{
    // walk thru the table and pick up the chunk-ids
    for (CMI iter = mChunkTable.begin(); iter != mChunkTable.end(); ++iter) {
        (IsChunkStable(iter->second) ? stable :
        (iter->second->IsWriteAppenderOwns() ?
            notStableAppend : notStable
        )).push_back(iter->second->chunkInfo);
    }
}

int
ChunkManager::GetChunkInfoHandle(kfsChunkId_t chunkId, ChunkInfoHandle **cih) 
{
    CMI const it = mChunkTable.find(chunkId);
    if (it == mChunkTable.end()) {
        *cih = 0;
        return -EBADF;
    }
    *cih = it->second;
    return 0;
}

int
ChunkManager::AllocateWriteId(WriteIdAllocOp *wi, int replicationPos, ServerLocation peerLoc)
{
    ChunkInfoHandle *cih = 0;

    if (GetChunkInfoHandle(wi->chunkId, &cih) < 0) {
        wi->statusMsg = "no such chunk";
        wi->status = -EBADF;
    } else if (wi->chunkVersion != cih->chunkInfo.chunkVersion) {
        wi->statusMsg = "chunk version mismatch";
        wi->status = -EINVAL;
    } else if (wi->isForRecordAppend && IsWritePending(wi->chunkId)) {
        wi->statusMsg = "random write in progress";
        wi->status = -EINVAL;
    } else if (wi->isForRecordAppend && ! IsWriteAppenderOwns(wi->chunkId)) {
        wi->statusMsg = "not open for append";
        wi->status = -EINVAL;
    } else if (! wi->isForRecordAppend && cih->IsWriteAppenderOwns()) {
        wi->statusMsg = "write append in progress";
        wi->status = -EINVAL;
    } else {
        mWriteId++;
        wi->writeId = mWriteId;
        if (wi->isForRecordAppend) {
            gAtomicRecordAppendManager.AllocateWriteId(
                wi, replicationPos, peerLoc, cih->dataFH);
        } else {
            WriteOp* const op = new WriteOp(
                wi->seq, wi->chunkId, wi->chunkVersion,
                wi->offset, wi->numBytes, NULL, mWriteId
            );
            op->enqueueTime     = globalNetManager().Now();
            op->isWriteIdHolder = true;
            mPendingWrites.push_back(op);
        }
    }
    if (wi->status != 0) {
        KFS_LOG_STREAM_ERROR <<
            "failed: " << wi->Show() <<
        KFS_LOG_EOM;
    }
    return wi->status;
}

int
ChunkManager::EnqueueWrite(WritePrepareOp *wp)
{
    WriteOp *op;
    ChunkInfoHandle *cih;

    if (GetChunkInfoHandle(wp->chunkId, &cih) < 0)
        return -EBADF;

    if (wp->chunkVersion != cih->chunkInfo.chunkVersion) {
        KFS_LOG_STREAM_INFO << "Version # mismatch (have=" <<
            cih->chunkInfo.chunkVersion << " vs asked=" << wp->chunkVersion <<
            ")...failing a write" <<
        KFS_LOG_EOM;
        return -EINVAL;
    }
    op = mPendingWrites.FindAndMoveBack(wp->writeId);
    assert(op);
    if (op->dataBuf == NULL) {
        op->dataBuf = wp->dataBuf;
    } else {
        op->dataBuf->Append(wp->dataBuf);
        delete wp->dataBuf;
    }
    wp->dataBuf = NULL;
    return 0;
}

int64_t
ChunkManager::GetChunkVersion(kfsChunkId_t c)
{
    ChunkInfoHandle *cih;

    if (GetChunkInfoHandle(c, &cih) < 0)
        return -1;

    return cih->chunkInfo.chunkVersion;
}

WriteOp *
ChunkManager::CloneWriteOp(int64_t writeId)
{
    WriteOp *op, *other;

    other = mPendingWrites.find(writeId);
    if (! other || other->status < 0) {
        // if the write is "bad" already, don't add more data to it
        if (other)
            KFS_LOG_STREAM_ERROR << "Clone write op failed due to status: " << other->status << 
                KFS_LOG_EOM;
        return NULL;
    }

    // Since we are cloning, "touch" the time
    other->enqueueTime = globalNetManager().Now();
    // offset/size/buffer are to be filled in
    op = new WriteOp(other->seq, other->chunkId, other->chunkVersion, 
                     0, 0, NULL, other->writeId);
    return op;
}

void
ChunkManager::SetWriteStatus(int64_t writeId, int status)
{
    WriteOp *op = mPendingWrites.find(writeId);
    if (! op)
        return;
    op->status = status;

    KFS_LOG_STREAM_INFO <<
        "Setting the status of writeid: " << writeId << " to " << status <<
    KFS_LOG_EOM;
}

int
ChunkManager::GetWriteStatus(int64_t writeId)
{
    WriteOp *op = mPendingWrites.find(writeId);
    if (! op)
        return -EINVAL;
    return op->status;
}


void
ChunkManager::Timeout()
{
    const time_t now = globalNetManager().Now();

#ifdef DEBUG
    verifyExecutingOnEventProcessor();
#endif

    if (now >= mNextCheckpointTime) {
        Checkpoint();
        // if any writes have been around for "too" long, remove them
        // and reclaim memory
        ScavengePendingWrites(now);
        // cleanup inactive fd's and thereby free up fd's
        CleanupInactiveFds(now);
    } else if (now > mNextPendingMetaSyncScanTime) {
        PendingMetaSyncQueue::Iterator it(mChunkInfoLists);
        int i = 0;
        ChunkInfoHandle* cih;
        while ((cih = it.Next()) &&
                cih->lastIOTime + mMetaSyncDelayTimeSecs < now) {
            KFS_LOG_STREAM_DEBUG << "[" << ++i <<
                "] starting sync for chunkId=" << cih->chunkInfo.chunkId <<
            KFS_LOG_EOM;
            cih->SyncMeta();
        }
        mNextPendingMetaSyncScanTime = now + (mMetaSyncDelayTimeSecs + 2) / 3;
    }
    if (now > mNextChunkDirsCheckTime) {
        // once in a while check that the drives hosting the chunks are good by doing disk IO.
        CheckChunkDirs();
        mNextChunkDirsCheckTime = now + mChunkDirsCheckIntervalSecs;
    }
}

void
ChunkManager::ScavengePendingWrites(time_t now)
{
    WriteOp *op;
    ChunkInfoHandle *cih;
    const time_t opExpireTime = now - mMaxPendingWriteLruSecs;

    while (! mPendingWrites.empty()) {
        op = mPendingWrites.front();
        // The list is sorted by enqueue time
        if (opExpireTime < op->enqueueTime) {
            break;
        }
        // if it exceeds 5 mins, retire the op
        KFS_LOG_STREAM_DEBUG <<
            "Retiring write with id=" << op->writeId <<
            " as it has been too long" <<
        KFS_LOG_EOM;
        mPendingWrites.pop_front();

        if (GetChunkInfoHandle(op->chunkId, &cih) == 0) {
            if (now - cih->lastIOTime >= mInactiveFdsCleanupIntervalSecs) {
                // close the chunk only if it is inactive
                CloseChunk(op->chunkId);
            }
            if (GetChunkInfoHandle(op->chunkId, &cih) == 0 &&
                    cih->IsFileOpen() && ! ChunkLru::IsInList(mChunkInfoLists, *cih)) {
                LruUpdate(*cih);
            }
        }
        delete op;
    }
}

int
ChunkManager::Sync(WriteOp *op)
{
    if (!op->diskIo) {
        return -1;
    }
    return op->diskIo->Sync(op->waitForSyncDone);
}

void
ChunkManager::CleanupInactiveFds(time_t now)
{
    const bool periodic = now > 0;
    // if we haven't cleaned up in 5 mins or if we too many fd's that
    // are open, clean up.
    if ((periodic ? now < mNextInactiveFdCleanupTime :
            (globals().ctrOpenDiskFds.GetValue() +
             globals().ctrOpenNetFds.GetValue()) <
             uint64_t(mMaxOpenChunkFiles))) {
        return;
    }

    const time_t cur = periodic ? now : globalNetManager().Now();
    // either we are periodic cleaning or we have too many FDs open
    // shorten the interval if we're out of fd.
    const time_t expireTime = cur - (periodic ?
        mInactiveFdsCleanupIntervalSecs :
        (mInactiveFdsCleanupIntervalSecs + 2) / 3);
    ChunkLru::Iterator it(mChunkInfoLists);
    ChunkInfoHandle* cih;
    while ((cih = it.Next()) && cih->lastIOTime < expireTime) {
        if (! cih->IsFileOpen() || cih->isBeingReplicated) {
            // Doesn't belong here, if / when io completes it will be added back.
            ChunkLru::Remove(mChunkInfoLists, *cih);
            continue;
        }
        bool inUse;
        bool hasLease = false;
        if ((inUse = cih->IsFileInUse()) ||
                (hasLease = gLeaseClerk.IsLeaseValid(cih->chunkInfo.chunkId)) ||
                IsWritePending(cih->chunkInfo.chunkId)) {
            KFS_LOG_STREAM_DEBUG << "cleanup: stale entry in chunk lru: "
                "fileid="    << cih->dataFH.get() <<
                " chunk="    << cih->chunkInfo.chunkId <<
                " last io= " << (now - cih->lastIOTime) << " sec. ago" <<
                (inUse ?    " file in use" : "") <<
                (hasLease ? " has lease"   : "") <<
            KFS_LOG_EOM;
            continue;
        }
        if (cih->SyncMeta()) {
            continue; // SyncMeta can delete cih
        }
        // we have a valid file-id and it has been over 5 mins since we last did I/O on it.
        KFS_LOG_STREAM_DEBUG << "cleanup: closing "
            "fileid="    << cih->dataFH.get() <<
            " chunk="    << cih->chunkInfo.chunkId <<
            " last io= " << (now - cih->lastIOTime) << " sec. ago" <<
        KFS_LOG_EOM;
        Release(*cih);
    }
    cih = ChunkLru::Front(mChunkInfoLists);
    mNextInactiveFdCleanupTime = mInactiveFdsCleanupIntervalSecs +
        ((cih && cih->lastIOTime > expireTime) ? cih->lastIOTime : cur);
}

string
GetSortedChunkPath(const string &partition)
{
    return partition + "/sorted";
}

string
GetStaleChunkPath(const string &partition)
{
    return partition + "/lost+found/";
}

string
GetDirtyChunkPath(const string partition)
{
    return partition + "/dirty";
}

#if defined(__APPLE__) || defined(__sun__) || (!defined(__i386__))
typedef struct statvfs StatVfs;
inline static int GetVfsStat(const char* path, StatVfs* stat) {
    return statvfs(path, stat);
}
#else
typedef struct statvfs64 StatVfs;
inline static int GetVfsStat(const char* path, StatVfs* stat) {
    return statvfs64(path, stat);
}
#endif

int64_t
ChunkManager::GetTotalSpace(
    bool startDiskIo) 
{
#if defined(__APPLE__)
    // On the mac, just run with 1 chunk dir.  
    for (uint32_t i = 0; i < mChunkDirs.size(); i++) {
        mChunkDirs[i].availableSpace = mTotalSpace;
        string errMsg;
        if (startDiskIo &&
                ! DiskIo::StartIoQueue(mChunkDirs[i].dirname.c_str(),
                    0 /* fsid */, mMaxOpenChunkFiles, &errMsg)) {
            KFS_LOG_STREAM_ERROR <<
                "Failed to start disk queue for: " << mChunkDirs[i].dirname <<
                " dev: << " << 0 << " :" << errMsg <<
            KFS_LOG_EOM;
            DiskIo::Shutdown();
            return -1;
        }
    }
    if (startDiskIo) {
        mMaxIORequestSize = DiskIo::GetMaxRequestSize();
    }
    return mTotalSpace;
#endif

    int64_t availableSpace = 0;
    set<dev_t> seenDrives;

    for (uint32_t i = 0; i < mChunkDirs.size(); i++) {
        // report the space based on availability
        StatVfs result;
	struct stat statbuf;

        if (mChunkDirs[i].availableSpace < 0)
            // drive is flagged as being down; move on
            continue;

        if (GetVfsStat(mChunkDirs[i].dirname.c_str(), &result) < 0 ||
	    stat(mChunkDirs[i].dirname.c_str(), &statbuf) < 0) {
            int err = errno;
            KFS_LOG_STREAM_INFO <<
                "statvfs (or stat) failed on " << mChunkDirs[i].dirname <<
                " with error: " << err << " " << strerror(err) <<
            KFS_LOG_EOM;
            mChunkDirs[i].availableSpace = -1;
            if ((err == EIO) || (err == ENOENT)) {
                // We can't stat the directory.
                // Notify metaserver that all blocks on this
                // drive are lost
                mCounters.mChunkDirLostCount++;
                NotifyMetaChunksLost(mChunkDirs[i].dirname);
            }
            continue;
        }

        string errMsg;
        if (startDiskIo &&
                ! DiskIo::StartIoQueue(mChunkDirs[i].dirname.c_str(),
                    result.f_fsid, mMaxOpenChunkFiles, &errMsg)) {
            KFS_LOG_STREAM_ERROR <<
                "Failed to start disk queue for: " << mChunkDirs[i].dirname <<
                " dev: << " << result.f_fsid << " :" << errMsg <<
            KFS_LOG_EOM;
            DiskIo::Shutdown();
            return -1;
        }
        if (seenDrives.find(statbuf.st_dev) != seenDrives.end()) {
            // if we have seen the drive where this directory is, then
            // we have already accounted for how much is free on the drive
            availableSpace += mChunkDirs[i].usedSpace;
            mChunkDirs[i].availableSpace = result.f_bavail * result.f_frsize + mChunkDirs[i].usedSpace;
        } else {
            // result.* is how much is available on disk; mUsedSpace is how
            // much we used up with chunks; so, the total storage available on
            // the drive is the sum of the two.  if we don't add mUsedSpace,
            // then all the chunks we write will get to use the space on disk and
            // won't get acounted for in terms of drive space.
            mChunkDirs[i].availableSpace = result.f_bavail * result.f_frsize + mChunkDirs[i].usedSpace;
            availableSpace += result.f_bavail * result.f_frsize + mChunkDirs[i].usedSpace;	    
            seenDrives.insert(statbuf.st_dev);
        }
        KFS_LOG_STREAM_DEBUG <<
            "Dir: " << mChunkDirs[i].dirname <<
            " has space " << mChunkDirs[i].availableSpace <<
        KFS_LOG_EOM;
    }
    if (startDiskIo) {
        mMaxIORequestSize = DiskIo::GetMaxRequestSize();
    }
    // we got all the info...so report true value
    return min(availableSpace, mTotalSpace);
}

long
ChunkManager::GetNumWritableChunks() const
{
    return (long)mPendingWrites.GetChunkIdCount();
}

int
ChunkManager::GetUsableChunkDirs() const
{
    int count = 0;

    for (uint32_t i = 0; i < mChunkDirs.size(); i++) {
        if (mChunkDirs[i].availableSpace <= 0)
            continue;
        count++;
    }
    return count;
}

void
ChunkManager::CheckChunkDirs()
{
    struct dirent **entries = NULL;
    KFS_LOG_STREAM_INFO << "Checking chunkdirs..." << KFS_LOG_EOM;
    for (uint32_t i = 0; i < mChunkDirs.size(); i++) {
        if (mChunkDirs[i].availableSpace < 0) {
            continue;
        }
        const int nentries = scandir(mChunkDirs[i].dirname.c_str(), &entries, 0, alphasort);
        if (nentries < 0) {
            KFS_LOG_STREAM_INFO <<
                "unable to open " << mChunkDirs[i].dirname <<
            KFS_LOG_EOM;
            mCounters.mChunkDirLostCount++;
            NotifyMetaChunksLost(mChunkDirs[i].dirname);            
            mChunkDirs[i].availableSpace = -1;
            continue;
        }
        for (int j = 0; j < nentries; j++)
            free(entries[j]);
        free(entries);
    }
}

} // namespace KFS
