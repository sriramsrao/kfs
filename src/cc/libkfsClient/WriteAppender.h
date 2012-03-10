//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: WriteAppender.h 3375 2011-11-28 23:29:28Z sriramr $
//
// Created 2009/05/20
//
// Copyright 2009 Quantcast Corp.
// Copyright 2011 Yahoo! Corp.
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

#ifndef WRITE_APPENDER_H
#define WRITE_APPENDER_H

#include "KfsNetClient.h"
#include "common/kfstypes.h"
#include "libkfsIO/Checksum.h"

#include <set>
#include <stdint.h>
#include <ostream>

namespace KFS
{

class IOBuffer;

class WriteAppender
{
public:
    class Completion
    {
    public:
        virtual void Done(
            WriteAppender& inAppender,
            int            inStatusCode) = 0;
        virtual void Unregistered(
            WriteAppender& /* inAppender */)
            {}
    protected:
        Completion()
            {}
        Completion(
            const Completion&)
            {}
        virtual ~Completion()
            {}
    };
    struct Stats
    {
        typedef int64_t Counter;
        Stats()
            : mMetaOpsQueuedCount(0),
              mMetaOpsCancelledCount(0),
              mSleepTimeSec(0),
              mChunkAllocCount(0),
              mReserveSpaceCount(0),
              mReserveSpaceDeniedCount(0),
              mOpsRecAppendCount(0),
              mAllocRetriesCount(0),
              mRetriesCount(0),
              mBufferCompactionCount(0),
              mAppendCount(0),
              mAppendByteCount(0)
            {}
        void Clear()
            { *this = Stats(); }
        Stats& Add(
            const Stats& inStats)
        {
            mMetaOpsQueuedCount      += inStats.mMetaOpsQueuedCount;
            mMetaOpsCancelledCount   += inStats.mMetaOpsCancelledCount;
            mSleepTimeSec            += inStats.mSleepTimeSec;
            mChunkAllocCount         += inStats.mChunkAllocCount;
            mReserveSpaceCount       += inStats.mReserveSpaceCount;
            mReserveSpaceDeniedCount += inStats.mReserveSpaceDeniedCount;
            mOpsRecAppendCount       += inStats.mOpsRecAppendCount;
            mAllocRetriesCount       += inStats.mAllocRetriesCount;
            mRetriesCount            += inStats.mRetriesCount;
            mBufferCompactionCount   += inStats.mBufferCompactionCount;
            mAppendCount             += inStats.mAppendCount;
            mAppendByteCount         += inStats.mAppendByteCount;
            return *this;
        }
        std::ostream& Display(
            std::ostream& inStream,
            const char*   inSeparatorPtr = 0,
            const char*   inDelimiterPtr = 0) const
        {
            const char* const theSeparatorPtr =
                inSeparatorPtr ? inSeparatorPtr : " ";
            const char* const theDelimiterPtr =
                inDelimiterPtr ? inDelimiterPtr : ": ";
            inStream <<
                "MetaOpsQueued"              << theDelimiterPtr <<
                    mMetaOpsQueuedCount      << theSeparatorPtr <<
                "MetaOpsCancelled"           << theDelimiterPtr <<
                    mMetaOpsCancelledCount   << theSeparatorPtr <<
                "SleepTimeSec"               << theDelimiterPtr <<
                    mSleepTimeSec            << theSeparatorPtr <<
                "ChunkAlloc"                 << theDelimiterPtr <<
                    mChunkAllocCount         << theSeparatorPtr <<
                "ReserveSpace"               << theDelimiterPtr <<
                    mReserveSpaceCount       << theSeparatorPtr <<
                "ReserveSpaceDenied"         << theDelimiterPtr <<
                    mReserveSpaceDeniedCount << theSeparatorPtr <<
                "OpsRecAppend"               << theDelimiterPtr <<
                    mOpsRecAppendCount       << theSeparatorPtr <<
                "AllocRetries"               << theDelimiterPtr <<
                    mAllocRetriesCount       << theSeparatorPtr <<
                "Retries"                    << theDelimiterPtr <<
                    mRetriesCount            << theSeparatorPtr <<
                "BufferCompaction"           << theDelimiterPtr <<
                    mBufferCompactionCount   << theSeparatorPtr <<
                "AppendCount"                << theDelimiterPtr <<
                    mAppendCount             << theSeparatorPtr <<
                "AppendByteCount"            << theDelimiterPtr <<
                    mAppendByteCount
            ;
            return inStream;
        }
        Counter mMetaOpsQueuedCount;
        Counter mMetaOpsCancelledCount;
        Counter mSleepTimeSec;
        Counter mChunkAllocCount;
        Counter mReserveSpaceCount;
        Counter mReserveSpaceDeniedCount;
        Counter mOpsRecAppendCount;
        Counter mAllocRetriesCount;
        Counter mRetriesCount;
        Counter mBufferCompactionCount;
        Counter mAppendCount;
        Counter mAppendByteCount;
    };
    typedef KfsNetClient MetaServer;
    WriteAppender(
        MetaServer& inMetaServer,
        Completion* inCompletionPtr               = 0,
        const char* inLogPrefixPtr                = 0,
        int         inRackId                      = -1,
        int         inMaxRetryCount               = 6,
        int         inWriteThreshold              = KFS::CHECKSUM_BLOCKSIZE,
        int         inTimeSecBetweenRetries       = 15,
        int         inDefaultSpaceReservationSize = 1 << 20,
        int         inPreferredAppendSize         = KFS::CHECKSUM_BLOCKSIZE,
        int         inMaxPartialBuffersCount      = 16,
        int         inOpTimeoutSec                = 30,
        int         inIdleTimeoutSec              = 5 * 30,
        int64_t     inChunkServerInitialSeqNum    = 1,
        bool        inPreAllocationFlag           = true,
        // Semantics of atomic record append: The KFS atomic record
        // append protocol supports replication.  The protocol also
        // guarantees that when the record is appended successfully,
        // it is at the same location on the replicas. Hence, use
        // EXACTLY ONCE semantics for such settings.  On the other
        // hand, for non-replicated settings, use AT LEAST ONCE
        // semantics.  If an append fails on a chunkserver, the system
        // will retry the append on a possibly different server.  This
        // causes records to be duplicated and the duplicate records
        // will need to be filtered out (which is the app's responsibility).
        //
        bool        inAtleastOnceFlag             = true);
    virtual ~WriteAppender();
    int Open(
        const char* inFileNamePtr,
        int         inNumReplicas  = 3,
        bool        inMakeDirsFlag = false);
    int Open(
        kfsFileId_t inFileId,
        const char* inFileNamePtr);
    int Close();
    int Append(
        IOBuffer& inBuffer,
        int       inLength);
    int Append(
        IOBuffer& inBuffer,
        int       inLength,
        IOBuffer* inKeyBuffer,
        int       inKeyLength);
    void Shutdown();
    bool IsOpen()     const;
    bool IsOpening()  const;
    bool IsClosing()  const;
    bool IsSleeping() const;
    bool IsActive()   const;
    int GetPendingSize() const;
    int GetErrorCode() const;
    int SetWriteThreshold(
        int inThreshold);
    void Register(
        Completion* inCompletionPtr);
    bool Unregister(
        Completion* inCompletionPtr);
    void GetStats(
        Stats&               outStats,
        KfsNetClient::Stats& outChunkServersStats);
    std::string GetServerLocation() const;
    int SetPreAllocation(
        bool inFlag);
    bool GetPreAllocation() const;
    int SetAtleastOnce(
        bool inFlag);
    bool GetAtleastOnce() const;
    void SetForcedAllocationInterval(
        int inInterval);
    // set of chunks to which we appended records to
    std::set<kfsChunkId_t> GetChunksAppended();
private:
    class Impl;
    Impl& mImpl;
private:
    WriteAppender(
        const WriteAppender& inAppender);
    WriteAppender& operator=(
        const WriteAppender& inAppender);
};
}

#endif /* WRITE_APPENDER_H */
