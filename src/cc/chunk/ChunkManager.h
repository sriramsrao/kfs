//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: ChunkManager.h 1857 2011-02-13 07:20:40Z sriramr $
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
// \file ChunkManager.h
// \brief Handles all chunk related ops.
//
//----------------------------------------------------------------------------

#ifndef _CHUNKMANAGER_H
#define _CHUNKMANAGER_H

#include <tr1/unordered_map>
#include <vector>
#include <string>
#include <set>
#include <map>
#include <boost/pool/pool_alloc.hpp> 
#include <boost/static_assert.hpp>

#include "Chunk.h"
#include "KfsOps.h"
#include "common/cxxutil.h"
#include "qcdio/qcdllist.h"

namespace KFS
{

/// We allow a chunk header upto 16K in size
const size_t KFS_CHUNK_HEADER_SIZE = 16384;

class ChunkInfoHandle;

class DiskIo;
class Properties;
struct ChunkDirInfo_t {
    ChunkDirInfo_t() : usedSpace(0), availableSpace(0) { }
    std::string dirname;
    int64_t usedSpace;
    int64_t availableSpace;
};

/// The chunk manager writes out chunks as individual files on disk.
/// The location of the chunk directory is defined by chunkBaseDir.
/// The file names of chunks is a string representation of the chunk
/// id.  The chunk manager performs disk I/O asynchronously---that is,
/// it schedules disk requests to the Disk manager which uses aio() to
/// perform the operations.
///
class ChunkManager {
public:
    struct Counters
    {
        typedef int64_t Counter;

        Counter mBadChunkHeaderErrorCount;
        Counter mReadChecksumErrorCount;
        Counter mReadErrorCount;
        Counter mWriteErrorCount;
        Counter mOpenErrorCount;
        Counter mCorruptedChunksCount;
        Counter mDirLostChunkCount;
        Counter mChunkDirLostCount;

        void Clear()
        {
            mBadChunkHeaderErrorCount = 0;
            mReadChecksumErrorCount   = 0;
            mReadErrorCount           = 0;
            mWriteErrorCount          = 0;
            mOpenErrorCount           = 0;
            mCorruptedChunksCount     = 0;
            mDirLostChunkCount        = 0;
            mChunkDirLostCount        = 0;
        }
    };

    ChunkManager();
    ~ChunkManager();

    /// Init function to configure the chunk manager object.
    bool Init(const std::vector<std::string> &chunkDirs, int64_t totalSpace, const Properties& prop);

    /// Allocate a file to hold a chunk on disk.  The filename is the
    /// chunk id itself.
    /// @param[in] fileId  id of the file that has chunk chunkId
    /// @param[in] chunkId id of the chunk being allocated.
    /// @param[in] chunkVersion  the version assigned by the metaserver to this chunk
    /// @param[in] isBeingReplicated is the allocation for replicating a chunk?
    /// @retval status code
    int 	AllocChunk(kfsFileId_t fileId, kfsChunkId_t chunkId, 
                           int64_t chunkVersion,
                           bool isBeingReplicated = false,
                           ChunkInfoHandle **cih = 0);
    void    AllocChunkForAppend(
        AllocChunkOp* op, int replicationPos, ServerLocation peerLoc);
    /// Delete a previously allocated chunk file.
    /// @param[in] chunkId id of the chunk being deleted.
    /// @retval status code
    int		DeleteChunk(kfsChunkId_t chunkId);

    /// Dump chunk map with information about chunkID and chunkSize    
    void    DumpChunkMap();

    /// Dump chunk map with information about chunkID and chunkSize
    /// to a string stream
    void    DumpChunkMap(std::ostringstream &ofs);

    /// For chunks for which there is a per-chunk index, retrieve
    /// the index from disk.
    int  ReadChunkIndex(kfsChunkId_t chunkId, KfsOp *cb);

    /// For chunks for which there is a per-chunk index, write out
    /// the index to disk.
    int  WriteChunkIndex(kfsChunkId_t chunkId, IOBuffer &chunkIndex,
        KfsCallbackObj *cb);

    /// A previously created dirty chunk should now be made "stable".
    /// Move that chunk out of the dirty dir.
    int	    MakeChunkStable(kfsChunkId_t chunkId);
    bool    IsChunkStable(kfsChunkId_t chunkId) const;

    /// Make <dstFid, dstChunkId> share the same chunk as <srcFid, srcChunkid>
    int   CoalesceBlock(kfsFileId_t srcFid, kfsChunkId_t srcChunkId,
                        kfsFileId_t dstFid, kfsChunkId_t dstChunkId);

    /// A previously created chunk is stale; move it to stale chunks
    /// dir only if we want to preserve it; otherwise, delete
    ///
    /// @param[in] chunkId id of the chunk being moved
    /// @retval status code
    int		StaleChunk(kfsChunkId_t chunkId, bool deleteOk = false);

    /// Truncate a chunk to the specified size
    /// @param[in] chunkId id of the chunk being truncated.
    /// @param[in] chunkSize  size to which chunk should be truncated.
    /// @retval status code
    int		TruncateChunk(kfsChunkId_t chunkId, off_t chunkSize);

    /// Change a chunk's version # to what the server says it should be.
    /// @param[in] fileId  id of the file that has chunk chunkId
    /// @param[in] chunkId id of the chunk being allocated.
    /// @param[in] chunkVersion  the version assigned by the metaserver to this chunk
    /// @retval status code
    int 	ChangeChunkVers(kfsFileId_t fileId, kfsChunkId_t chunkId, 
                           int64_t chunkVersion);

    /// Open a chunk for I/O.
    /// @param[in] chunkId id of the chunk being opened.
    /// @param[in] openFlags  O_RDONLY, O_WRONLY
    /// @retval status code
    int		OpenChunk(kfsChunkId_t chunkId, int openFlags);

    /// Close a previously opened chunk and release resources.
    /// @param[in] chunkId id of the chunk being closed.
    /// @retval 0 if the close was accepted; -1 otherwise
    int		CloseChunk(kfsChunkId_t chunkId);

    /// Utility function that returns a pointer to mChunkTable[chunkId].
    /// @param[in] chunkId  the chunk id for which we want info
    /// @param[out] cih  the resulting pointer from mChunkTable[chunkId]
    /// @retval  0 on success; -EBADF if we can't find mChunkTable[chunkId]
    int GetChunkInfoHandle(kfsChunkId_t chunkId, ChunkInfoHandle **cih);

    /// Given a byte range, return the checksums for that range.
    std::vector<uint32_t> GetChecksums(kfsChunkId_t chunkId, off_t offset, size_t numBytes);

    /// For telemetry purposes, provide the driveName where the chunk
    /// is stored and pass that back to the client. 
    void  GetDriveName(ReadOp *op);

    /// Schedule a read on a chunk.
    /// @param[in] op  The read operation being scheduled.
    /// @retval 0 if op was successfully scheduled; -1 otherwise
    int		ReadChunk(ReadOp *op);

    /// Schedule a write on a chunk.
    /// @param[in] op  The write operation being scheduled.
    /// @retval 0 if op was successfully scheduled; -1 otherwise
    int		WriteChunk(WriteOp *op);

    /// Write/read out/in the chunk meta-data and notify the cb when the op
    /// is done.
    /// @retval 0 if op was successfully scheduled; -errno otherwise
    int		WriteChunkMetadata(kfsChunkId_t chunkId, KfsCallbackObj *cb);
    int		ScheduleWriteChunkMetadata(kfsChunkId_t chunkId);
    int		ReadChunkMetadata(kfsChunkId_t chunkId, KfsOp *cb);
    
    /// Notification that read is finished
    void	ReadChunkMetadataDone(kfsChunkId_t chunkId);

    /// We read the chunk metadata out of disk; we update the chunk
    /// table with this info.
    /// @retval 0 if successful (i.e., valid chunkid); -EBADCKSUM otherwise
    int		SetChunkMetadata(const DiskChunkInfo_t &dci, kfsChunkId_t chunkId);
    bool	IsChunkMetadataLoaded(kfsChunkId_t chunkId);

    /// A previously scheduled write op just finished.  Update chunk
    /// size and the amount of used space.
    /// @param[in] op  The write op that just finished
    ///
    void	ReadChunkDone(ReadOp *op);
    void	ReplicationDone(kfsChunkId_t chunkId);
    /// Determine the size of a chunk.
    /// @param[in] chunkId  The chunk whose size is needed
    /// @param[out] fid     Return the file-id that owns the chunk
    /// @param[out] chunkSize  The size of the chunk
    /// @retval status code
    int 	ChunkSize(kfsChunkId_t chunkId, kfsFileId_t &fid, off_t *chunkSize, bool *araStableFlag = 0);

    /// Cancel a previously scheduled chunk operation.
    /// @param[in] cont   The callback object that scheduled the
    ///  operation
    /// @param[in] chunkId  The chunk on which ops were scheduled
    void 	CancelChunkOp(KfsCallbackObj *cont, kfsChunkId_t chunkId);

    /// Register a timeout handler with the net manager for taking
    /// checkpoints.  Also, get the logger going
    void	Start();
    
    /// Write out the chunk table data structure to disk
    void	Checkpoint();

    /// Read the chunk table from disk following a restart.  See
    /// comments in the method for issues relating to validation (such
    /// as, checkpoint contains a chunk name, but the associated file
    /// is not there on disk, etc.).
    void	Restart();

    /// On a restart following a dirty shutdown, do log replay.  This
    /// involves updating the Chunk table map to reflect operations
    /// that are in the log.

    /// When a checkpoint file is read, update the mChunkTable[] to
    /// include a mapping for cih->chunkInfo.chunkId.
    void AddMapping(ChunkInfoHandle *cih);
    void AddMapping(const ChunkInfo_t& ci);

    /// Replay a chunk allocation.
    /// 
    /// @param[in] fileId  id of the file that has chunk chunkId
    /// @param[in] chunkId  Update the mChunkTable[] to include this
    /// chunk id
    /// @param[in] chunkVersion  the version assigned by the
    /// metaserver to this chunk. 
    void ReplayAllocChunk(kfsFileId_t fileId, kfsChunkId_t chunkId,
                          int64_t chunkVersion);

    /// Replay a chunk version # change.
    /// 
    /// @param[in] fileId  id of the file that has chunk chunkId
    /// @param[in] chunkId  Update the mChunkTable[] with the changed
    /// version # for this chunkId
    /// @param[in] chunkVersion  the version assigned by the
    /// metaserver to this chunk. 
    void ReplayChangeChunkVers(kfsFileId_t fileId, kfsChunkId_t chunkId,
                               int64_t chunkVersion);

    /// Replay a chunk deletion
    /// @param[in] chunkId  Update the mChunkTable[] to remove this
    /// chunk id
    void ReplayDeleteChunk(kfsChunkId_t chunkId);


    /// Replay a write done on a chunk.
    /// @param[in] chunkId  Update the size of chunk to reflect the
    /// completion of a write.
    /// @param[in] chunkSize The new size of the chunk
    void ReplayWriteDone(kfsChunkId_t chunkId, off_t chunkSize,
                         off_t offset, std::vector<uint32_t> checksum);

    /// Replay a truncation done on a chunk.
    /// @param[in] chunkId  Update the size of chunk to reflect the
    /// completion of a truncation
    /// @param[in] chunkSize The new size of the chunk
    void ReplayTruncateDone(kfsChunkId_t chunkId, off_t chunkSize);

    /// Retrieve the chunks hosted on this chunk server.
    /// @param[out] result  A vector containing info of all chunks
    /// hosted on this server.
    void GetHostedChunks(
        std::vector<ChunkInfo_t> &stable,
        std::vector<ChunkInfo_t> &notStable,
        std::vector<ChunkInfo_t> &notStableAppend);

    /// Return the total space that is exported by this server.  If
    /// chunks are stored in a single directory, we use statvfs to
    /// determine the total space avail; we report the min of statvfs
    /// value and the configured mTotalSpace.
    int64_t GetTotalSpace() { return GetTotalSpace(false); }
    int64_t GetUsedSpace() const { return mUsedSpace; };
    long GetNumChunks() const { return mNumChunks; };
    long GetNumWritableChunks() const;

    /// From the list of chunk dirs, return a count of the # drives that are usable.
    int GetUsableChunkDirs() const;

    /// For a write, the client is defining a write operation.  The op
    /// is queued and the client pushes data for it subsequently.
    /// @param[in] wi  The op that defines the write
    /// @retval status code
    int AllocateWriteId(WriteIdAllocOp *wi, int replicationPos, ServerLocation peerLoc);

    /// For a write, the client has pushed data to us.  This is queued
    /// for a commit later on.
    /// @param[in] wp  The op that needs to be queued
    /// @retval status code
    int EnqueueWrite(WritePrepareOp *wp);

    /// Check if a write is pending to a chunk.
    /// @param[in] chunkId  The chunkid for which we are checking for
    /// pending write(s). 
    /// @retval True if a write is pending; false otherwise
    bool IsWritePending(kfsChunkId_t chunkId) {
        return mPendingWrites.HasChunkId(chunkId);
    }

    /// Given a chunk id, return its version
    int64_t GetChunkVersion(kfsChunkId_t c);

    /// if the chunk exists and has a valid version #, then we need to page in the chunk meta-data.
    bool NeedToReadChunkMetadata(kfsChunkId_t c) {
        return GetChunkVersion(c) > 0;
    }

    /// Retrieve the write op given a write id.
    /// @param[in] writeId  The id corresponding to a previously
    /// enqueued write.
    /// @retval WriteOp if one exists; NULL otherwise
    WriteOp *GetWriteOp(int64_t writeId);

    /// The model with writes: allocate a write id (this causes a
    /// write-op to be created); then, push data for writes (which
    /// retrieves the write-op and then sends writes down to disk).
    /// The "clone" method makes a copy of a previously created
    /// write-op.
    /// @param[in] writeId the write id that was previously assigned
    /// @retval WriteOp if one exists; NULL otherwise
    WriteOp *CloneWriteOp(int64_t writeId);

    /// Set the status for a given write id
    void SetWriteStatus(int64_t writeId, int status);
    int  GetWriteStatus(int64_t writeId);
    
    /// Is the write id a valid one
    bool IsValidWriteId(int64_t writeId) {
        return mPendingWrites.find(writeId);
    }

    void Timeout();

    /// Push the changes from the write out to disk
    int Sync(WriteOp *op);

    ChunkInfo_t* GetChunkInfo(kfsChunkId_t chunkId);
    
    /// Called when sorting service is done with sorting key/value pairs in a chunk
    void ChunkSortDone(kfsChunkId_t chunkId, uint32_t indexSz);

    void ChunkIOFailed(kfsChunkId_t chunkId, int err) {
        if (err == -EIO) {
            NotifyMetaCorruptedChunk(chunkId);
            StaleChunk(chunkId);
        }
    }
    size_t GetMaxIORequestSize() const {
        return mMaxIORequestSize;
    }
    void Shutdown();
    bool IsWriteAppenderOwns(kfsChunkId_t chunkId) const;

    inline void LruUpdate(ChunkInfoHandle& cih);
    inline bool IsInLru(const ChunkInfoHandle& cih) const;
    enum { kChunkInfoHandleListCount = 2 };
    void GetCounters(Counters& counters)
        { counters = mCounters; }

private:
    class PendingWrites
    {
    public:
        PendingWrites()
           : mWriteIds(), mChunkIds(), mLru(), mKeyOp(0, 0)
            {}
        bool empty() const
            { return (mWriteIds.empty()); }
        bool push_front(WriteOp* op)
            { return Insert(op, true); }
        bool push_back(WriteOp* op)
            { return Insert(op, false); }
        bool pop_front()
            { return Remove(true); }
        bool pop_back()
            { return Remove(false); }
        size_t size() const
            { return mWriteIds.size(); }
        WriteOp* front() const
            { return mLru.front().mWriteIdIt->mOp; }
        WriteOp* back() const
            { return mLru.back().mWriteIdIt->mOp; }
        WriteOp* find(int64_t writeId) const
        {
            WriteOp& op = GetKeyOp();
            op.writeId = writeId;
            WriteIdSet::const_iterator const i =
                mWriteIds.find(WriteIdEntry(&op));
            return (i == mWriteIds.end() ? 0 : i->mOp);
        }
        bool HasChunkId(kfsChunkId_t chunkId) const
            { return (mChunkIds.find(chunkId) != mChunkIds.end()); }
        bool erase(WriteOp* op)
        {
            const WriteIdSet::iterator i = mWriteIds.find(WriteIdEntry(op));
            return (i != mWriteIds.end() && op == i->mOp && Erase(i));
        }
        bool erase(int64_t writeId)
        {
            WriteOp& op = GetKeyOp();
            op.writeId = writeId;
            WriteIdSet::const_iterator const i =
                mWriteIds.find(WriteIdEntry(&op));
            return (i != mWriteIds.end() && Erase(i));
        }
        bool Delete(kfsChunkId_t chunkId, kfsSeq_t chunkVersion)
        {
            ChunkIdMap::iterator i = mChunkIds.find(chunkId);
            if (i == mChunkIds.end()) {
                return true;
            }
            ChunkWrites& wr = i->second;
            for (ChunkWrites::iterator w = wr.begin(); w != wr.end(); ) {
                Lru::iterator const c = w->GetLruIterator();
                if (c->mWriteIdIt->mOp->chunkVersion == chunkVersion) {
                    WriteOp* const op = c->mWriteIdIt->mOp;
                    mWriteIds.erase(c->mWriteIdIt);
                    mLru.erase(c);
                    w = wr.erase(w);
                    delete op;
                } else {
                    ++w;
                }
            }
            if (wr.empty()) {
                mChunkIds.erase(i);
                return true;
            }
            return false;
        }
        WriteOp* FindAndMoveBack(int64_t writeId)
        {
            mKeyOp.writeId = writeId;
            const WriteIdSet::iterator i =
                mWriteIds.find(WriteIdEntry(&mKeyOp));
            if (i == mWriteIds.end()) {
                return 0;
            }
            // splice: "All iterators remain valid including iterators that
            // point to elements of x." x == mLru
            mLru.splice(mLru.end(), mLru, i->GetLruIterator());
            return i->mOp;
        }
        size_t GetChunkIdCount() const
            { return mChunkIds.size(); }
    private:
        class LruIterator;
        class OpListEntry
        {
            private:
                struct { // Make it struct aligned.
                    char  mArray[sizeof(std::list<void*>::iterator)];
                } mLruIteratorStorage;
            public:
                inline OpListEntry();
                inline ~OpListEntry();
                // Set iterator prohibit node mutation, because the node is the
                // key, and changing the key can potentially change the order.
                // In this particular case order only depends on mOp->writeId.
                // The following hack is also needed to get around type dependency
                // cycle with Lru::iterator, and WriteIdEntry.
                LruIterator& GetLruIterator() const
                {
                    return *reinterpret_cast<LruIterator*>(
                        &const_cast<OpListEntry*>(this)->mLruIteratorStorage);
                }
        };
        struct WriteIdEntry : public OpListEntry
        {
        public:
            inline WriteIdEntry(WriteOp* op = 0);
            WriteOp* mOp;
        };
        struct WriteIdCmp
        {
            bool operator()(const WriteIdEntry& x, const WriteIdEntry& y) const
                { return (x.mOp->writeId < y.mOp->writeId); }
        };
        typedef std::set<WriteIdEntry, WriteIdCmp,
            boost::fast_pool_allocator<WriteIdEntry>
        > WriteIdSet;
        typedef std::list<OpListEntry,
            boost::fast_pool_allocator<OpListEntry> > ChunkWrites;
        typedef std::map<kfsChunkId_t, ChunkWrites, std::less<kfsChunkId_t>,
            boost::fast_pool_allocator<
                std::pair<const kfsChunkId_t, ChunkWrites> >
        > ChunkIdMap;
        struct LruEntry
        {
            LruEntry()
                : mWriteIdIt(), mChunkIdIt(), mChunkWritesIt()
                {}
            LruEntry(
                WriteIdSet::iterator  writeIdIt,
                ChunkIdMap::iterator  chunkIdIt,
                ChunkWrites::iterator chunkWritesIt)
                : mWriteIdIt(writeIdIt),
                  mChunkIdIt(chunkIdIt),
                  mChunkWritesIt(chunkWritesIt)
                {}
            WriteIdSet::iterator  mWriteIdIt;
            ChunkIdMap::iterator  mChunkIdIt;
            ChunkWrites::iterator mChunkWritesIt;
        };
        typedef std::list<LruEntry, boost::fast_pool_allocator<LruEntry> > Lru;
        class LruIterator : public Lru::iterator
        {
        public:
            LruIterator& operator=(const Lru::iterator& it)
            {
                Lru::iterator::operator=(it);
                return *this;
            }
        };

        WriteIdSet mWriteIds;
        ChunkIdMap mChunkIds;
        Lru        mLru;
        WriteOp    mKeyOp;

        bool Insert(WriteOp* op, bool front)
        {
            if (! op) {
                return false;
            }
            std::pair<WriteIdSet::iterator, bool> const w =
                mWriteIds.insert(WriteIdEntry(op));
            if (! w.second) {
                return false;
            }
            ChunkIdMap::iterator const c = mChunkIds.insert(
                std::make_pair(op->chunkId, ChunkWrites())).first;
            ChunkWrites::iterator const cw =
                c->second.insert(c->second.end(), OpListEntry());
            w.first->GetLruIterator() = mLru.insert(
                front ? mLru.begin() : mLru.end(),
                LruEntry(w.first, c, cw));
            cw->GetLruIterator() = w.first->GetLruIterator();
            return true;
        }
        bool Remove(bool front)
        {
            if (mLru.empty()) {
                return false;
            }
            LruEntry& c = front ? mLru.front() : mLru.back();
            mWriteIds.erase(c.mWriteIdIt);
            c.mChunkIdIt->second.erase(c.mChunkWritesIt);
            if (c.mChunkIdIt->second.empty()) {
                mChunkIds.erase(c.mChunkIdIt);
            }
            if (front) {
                mLru.pop_front();
            } else {
                mLru.pop_back();
            }
            return true;
        }
        bool Erase(WriteIdSet::iterator i)
        {
            const Lru::iterator c = i->GetLruIterator();
            c->mChunkIdIt->second.erase(c->mChunkWritesIt);
            if (c->mChunkIdIt->second.empty()) {
                mChunkIds.erase(c->mChunkIdIt);
            }
            mLru.erase(c);
            mWriteIds.erase(i);
            return true;
        }
        WriteOp& GetKeyOp() const
            { return *const_cast<WriteOp*>(&mKeyOp); }
    private:
        PendingWrites(const PendingWrites&);
        PendingWrites& operator=(const PendingWrites&);
    };

    struct ChunkDirInfo_t {
        ChunkDirInfo_t() : usedSpace(0), availableSpace(0) { }
        std::string dirname;
        int64_t usedSpace;
        int64_t availableSpace;
    };

    /// Map from a chunk id to a chunk handle
    ///
    typedef std::tr1::unordered_map<kfsChunkId_t, ChunkInfoHandle*,
        std::tr1::hash<kfsChunkId_t>,
        std::equal_to<kfsChunkId_t>,
        boost::fast_pool_allocator<
            std::pair<const kfsChunkId_t, ChunkInfoHandle*> >
    > CMap;
    typedef CMap::const_iterator CMI;
    /// Periodically write out the chunk manager state to disk
    class ChunkManagerTimeoutImpl;

    /// How long should a pending write be held in LRU
    int mMaxPendingWriteLruSecs;
    /// take a checkpoint once every 2 mins
    int mCheckpointIntervalSecs;

    /// space available for allocation 
    int64_t	mTotalSpace;
    /// how much is used up by chunks
    int64_t	mUsedSpace;

    /// how many chunks are we hosting
    long	mNumChunks;

    time_t      mNextCheckpointTime;
    int         mMaxOpenChunkFiles;
    
    /// directories for storing the chunks
    std::vector<ChunkDirInfo_t> mChunkDirs;

    /// index of the last directory/drive that we used for placing a
    /// chunk
    int mLastDriveChosen;

    /// See the comments in KfsOps.cc near WritePrepareOp related to write handling
    int64_t mWriteId;
    PendingWrites mPendingWrites;

    /// on a timeout, the timeout interface will force a checkpoint
    /// and query the disk manager for data
    ChunkManagerTimeoutImpl	*mChunkManagerTimeoutImpl;

    /// when taking checkpoints, write one out only if the chunk table
    /// is dirty. 
    bool mIsChunkTableDirty;
    /// table that maps chunkIds to their associated state
    CMap   mChunkTable;
    size_t mMaxIORequestSize;
    /// Chunk lru, and chunks with delayed meta data write.
    ChunkInfoHandle* mChunkInfoLists[kChunkInfoHandleListCount];
    time_t           mNextPendingMetaSyncScanTime;
    int              mMetaSyncDelayTimeSecs;

    /// Periodically do an IO and check the chunk dirs and identify failed drives
    time_t	     mNextChunkDirsCheckTime;
    int              mChunkDirsCheckIntervalSecs;

    // Cleanup fds on which no I/O has been done for the past N secs
    int    mInactiveFdsCleanupIntervalSecs;
    time_t mNextInactiveFdCleanupTime;

    Counters mCounters;

    inline void Delete(ChunkInfoHandle& cih);
    inline void Release(ChunkInfoHandle& cih);

    /// Given a chunk file name, extract out the
    /// fileid/chunkid/chunkversion from it and build a chunkinfo structure
    void MakeChunkInfoFromPathname(const std::string &pathname, off_t filesz, ChunkInfoHandle **result);

    /// Of the various directories this chunkserver is configured with, find the directory to store a chunk file.  
    /// This method does a "directory allocation".
    std::string GetDirForChunk();

    void CheckChunkDirs();

    /// Utility function that given a chunkId, returns the full path
    /// to the chunk filename.
    std::string MakeChunkPathname(ChunkInfoHandle *cih);
    std::string MakeChunkPathname(const std::string &chunkdir, kfsFileId_t fid, kfsChunkId_t chunkId, kfsSeq_t chunkVersion);

    /// Utility function that given a chunkId, returns the full path
    /// to the chunk filename in the "stalechunks" dir
    std::string MakeStaleChunkPathname(ChunkInfoHandle *cih);

    /// Utility function that given a chunkId, returns the full path
    /// to the chunk filename in the "sorted" dir
    std::string MakeSortedChunkPathname(ChunkInfoHandle *cih);

    /// update the used space in the directory where the chunk resides by nbytes.
    void UpdateDirSpace(ChunkInfoHandle *cih, off_t nbytes);

    /// Utility function that sets up a disk connection for an
    /// I/O operation on a chunk.
    /// @param[in] chunkId  Id of the chunk on which we are doing I/O
    /// @param[in] op   The KfsOp that is being on the chunk
    /// @retval A disk connection pointer allocated via a call to new;
    /// it is the caller's responsibility to free the memory
    DiskIo *SetupDiskIo(kfsChunkId_t chunkId, KfsOp *op);

    /// Checksums are computed on 64K blocks.  To verify checksums on
    /// reads, reads are aligned at 64K boundaries and data is read in
    /// 64K blocks.  So, for reads that are un-aligned/read less data,
    /// adjust appropriately.
    void AdjustDataRead(ReadOp *op);

    /// Pad the buffer with sufficient 0's so that checksumming works
    /// out.
    /// @param[in/out] buffer  The buffer to be padded with 0's
    void ZeroPad(IOBuffer *buffer);

    /// Given a chunkId and offset, return the checksum of corresponding
    /// "checksum block"---i.e., the 64K block that contains offset.
    uint32_t GetChecksum(kfsChunkId_t chunkId, off_t offset);

    /// For any writes that have been held for more than 2 mins,
    /// scavenge them and reclaim memory.
    void ScavengePendingWrites(time_t now);

    /// If we have too many open fd's close out whatever we can.  When
    /// periodic is set, we do a scan and clean up.
    void CleanupInactiveFds(time_t now = 0);

    /// Notify the metaserver that chunk chunkId is corrupted; the
    /// metaserver will re-replicate this chunk and for now, won't
    /// send us traffic for this chunk.
    void NotifyMetaCorruptedChunk(kfsChunkId_t chunkId);

    /// For some reason, dirname is not accessable (for instance, the
    /// drive may have failed); in this case, notify metaserver that
    /// all the blocks on that dir are lost and the metaserver can
    /// then re-replicate.
    void NotifyMetaChunksLost(const std::string &dirname);

    /// Get all the chunk filenames into a single array.
    /// @retval on success, # of entries in the array;
    ///         on failures, -1
    int GetChunkDirsEntries(struct dirent ***namelist);
    /// Get all the chunk pathnames into a single vector
    void GetChunkPathEntries(std::vector<std::string> &pathnames);

    /// Helper function to move a chunk to the stale dir
    void MarkChunkStale(ChunkInfoHandle *cih);

    /// On a restart, nuke out all the dirty chunks
    void RemoveDirtyChunks();

    /// Scan the chunk dirs and rebuild the list of chunks that are hosted on this server
    void Restore();
    /// Restore the chunk meta-data from the specified file name.
    void RestoreChunkMeta(const std::string &chunkMetaFn);
    
    /// Update the checksums in the chunk metadata based on the op.
    void UpdateChecksums(ChunkInfoHandle *cih, WriteOp *op);
    int64_t GetTotalSpace(bool startDiskIo);
    bool IsChunkStable(const ChunkInfoHandle* cih) const;
};

inline ChunkManager::PendingWrites::OpListEntry::OpListEntry()
{
    BOOST_STATIC_ASSERT(sizeof(mLruIteratorStorage) >= sizeof(LruIterator));
    LruIterator* const i =
        ::new (static_cast<void*>(&mLruIteratorStorage)) LruIterator();
    assert(i == &GetLruIterator());
    (void)i;
}

inline ChunkManager::PendingWrites::OpListEntry::~OpListEntry()
{  GetLruIterator().~LruIterator(); }

inline ChunkManager::PendingWrites::WriteIdEntry::WriteIdEntry(WriteOp* op)
    : OpListEntry(), mOp(op)
{}

extern ChunkManager gChunkManager;

/// Given a partition that holds chunks, get the path to the directory
/// that is used to keep the stale chunks (from this partition)
std::string GetStaleChunkPath(const std::string &partition);
/// Given a partition that holds chunks, get the path to the directory
/// that is used to keep the dirty chunks (from this partition)
std::string GetDirtyChunkPath(const std::string partition);
/// Given a partition that holds chunks, get the path to the directory
/// that is used to keep the sorted chunks (from this partition)
std::string GetSortedChunkPath(const std::string &partition);

}

#endif // _CHUNKMANAGER_H
