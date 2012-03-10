//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: DiskIo.cc 1575 2011-01-10 22:26:35Z sriramr $
//
// Created 2009/01/17
//
// Copyright 2009 Quantcast Corp.
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

#include <cerrno>
#include <algorithm>
#include <limits>

#include "DiskIo.h"
#include "BufferManager.h"

#include "libkfsIO/IOBuffer.h"
#include "libkfsIO/Globals.h"
#include "common/properties.h"
#include "common/log.h"
#include "common/kfstypes.h"

#include "qcdio/qcdllist.h"
#include "qcdio/qcmutex.h"
#include "qcdio/qcstutils.h"
#include "qcdio/qcutils.h"
#include "qcdio/qciobufferpool.h"
#include "qcdio/qcdebug.h"

namespace KFS
{
static void DiskIoReportError(
    const char* inMsgPtr = 0);
static void DiskIoReportError(
    std::string inMsg)
{ DiskIoReportError(inMsg.c_str()); }

class DiskQueue : public QCDiskQueue
{
public:
    typedef QCDLList<DiskQueue, 0> DiskQueueList;

    DiskQueue(
        DiskQueue**   inListPtr,
        unsigned long inDeviceId,
        const char*   inFileNamePrefixPtr)
        : QCDiskQueue(),
          mFileNamePrefixes(inFileNamePrefixPtr ? inFileNamePrefixPtr : ""),
          mDeviceId(inDeviceId)
    {
        mFileNamePrefixes.append(1, (char)0);
        DiskQueueList::Init(*this);
        DiskQueueList::PushBack(inListPtr, *this);
    }
    void Delete(
        DiskQueue** inListPtr)
    {
        DiskQueueList::Remove(inListPtr, *this);
        delete this;
    }
    bool IsFileNamePrefixMatches(
        const char* inFileNamePtr) const
    {
        const char* const theFileNamePtr = inFileNamePtr ? inFileNamePtr : "";
        const char*       thePtr         = theFileNamePtr;
        const char*       thePrefPtr     = mFileNamePrefixes.data();
        const char* const thePrefsEndPtr = thePrefPtr +
            mFileNamePrefixes.length();
        while (thePrefPtr < thePrefsEndPtr) {
            while (*thePtr && *thePrefPtr && *thePtr == *thePrefPtr) {
                thePtr++;
                thePrefPtr++;
            }
            if (*thePrefPtr == 0) {
                return true;
            }
            while (*thePrefPtr++)
            {}
            thePtr = theFileNamePtr;
        }
        return false;
    }
    unsigned long GetDeviceId() const
        { return mDeviceId; }
    void AddFileNamePrefix(
        const char* inFileNamePtr)
    {
        mFileNamePrefixes.append(inFileNamePtr ? inFileNamePtr : "");
        mFileNamePrefixes.append(1, (char)0);
    }
private:
    std::string         mFileNamePrefixes;
    const unsigned long mDeviceId;
    DiskQueue*          mPrevPtr[1];
    DiskQueue*          mNextPtr[1];

     ~DiskQueue()
        {}
   friend class QCDLListOp<DiskQueue, 0>;
private:
    DiskQueue(
        const DiskQueue& inQueue);
    DiskQueue& operator=(
        const DiskQueue& inQueue);
};

class DiskIoQueues : private ITimeout
{
private:
    typedef DiskQueue::DiskQueueList DiskQueueList;

public:
    enum { kDiskQueueIdNone = -1 };

    typedef QCDLList<DiskIo, 0> DoneQueue;
    typedef DiskIo::Counters Counters;

    DiskIoQueues(
            const Properties& inConfig)
        : ITimeout(),
          mDiskQueueThreadCount(inConfig.getValue(
            "chunkServer.diskQueue.threadCount", 2)),
          mDiskQueueMaxQueueDepth(inConfig.getValue(
            "chunkServer.diskQueue.maxDepth", 4 << 10)),
          mDiskQueueMaxBuffersPerRequest(inConfig.getValue(
            "chunkServer.diskQueue.maxBuffersPerRequest", 1 << 8)),
          mDiskQueueMaxEnqueueWaitNanoSec(inConfig.getValue(
            "chunkServer.diskQueue.maxEnqueueWaitTimeMilliSec", 0) * 1000000),
          mBufferPoolPartitionCount(inConfig.getValue(
            "chunkServer.ioBufferPool.partitionCount", 1)),
          mBufferPoolPartitionBufferCount(inConfig.getValue(
            "chunkServer.ioBufferPool.partitionBufferCount",
                (sizeof(size_t) < 8 ? 32 : 192) << 10)),
          mBufferPoolBufferSize(inConfig.getValue(
            "chunkServer.ioBufferPool.bufferSize", 4 << 10)),
          mBufferPoolLockMemoryFlag(inConfig.getValue(
            "chunkServer.ioBufferPool.lockMemory", false)),
          mDiskOverloadedPendingRequestCount(inConfig.getValue(
            "chunkServer.diskIo.OverloadedPendingRequestCount",
                mDiskQueueMaxQueueDepth * 3 / 4)),
          mDiskClearOverloadedPendingRequestCount(inConfig.getValue(
            "chunkServer.diskIo.ClearOverloadedPendingRequestCount",
                mDiskOverloadedPendingRequestCount * 3 / 4)),
          mDiskOverloadedMinFreeBufferCount(inConfig.getValue(
            "chunkServer.diskIo.OverloadedMinFreeBufferCount",
                int(int64_t(mBufferPoolPartitionCount) *
                    mBufferPoolPartitionBufferCount / 16))),
          mDiskClearOverloadedMinFreeBufferCount(inConfig.getValue(
            "chunkServer.diskIo.OverloadedClearMinFreeBufferCount",
                mDiskOverloadedMinFreeBufferCount * 2 / 3)),
          mDiskOverloadedPendingWriteByteCount(inConfig.getValue(
            "chunkServer.diskIo.OverloadedPendingWriteByteCount",
                int64_t(mBufferPoolPartitionBufferCount) *
                mBufferPoolBufferSize * mBufferPoolPartitionCount / 4)),
          mDiskClearOverloadedPendingWriteByteCount(inConfig.getValue(
            "chunkServer.diskIo.ClearOverloadedPendingWriteByteCount",
            mDiskOverloadedPendingWriteByteCount * 2 / 3)),
          mCrashOnErrorFlag(inConfig.getValue(
            "chunkServer.diskIo.crashOnError", false)),
          mBufferManagerMaxRatio(inConfig.getValue(
            "chunkServer.bufferManager.maxRatio", 0.4)),
          mOverloadedFlag(false),
          mMaxRequestSize(0),
          mWriteCancelWaiterPtr(0),
          mReadPendingBytes(0),
          mWritePendingBytes(0),
          mReadReqCount(0),
          mWriteReqCount(0),
          mMutex(),
          mBufferAllocator(),
          mBufferManager(inConfig.getValue(
            "chunkServer.bufferManager.enabled", true))
    {
        mCounters.Clear();
        DoneQueue::Init(mIoQueuesPtr);
        DiskQueueList::Init(mDiskQueuesPtr);
        // Call Timeout() every time NetManager goes trough its work loop.
        ITimeout::SetTimeoutInterval(0);
    }
    ~DiskIoQueues()
        { DiskIoQueues::Shutdown(); }
    bool Start(
        std::string* inErrMessagePtr)
    {
        int theSysError = GetBufferPool().Create(
            mBufferPoolPartitionCount,
            mBufferPoolPartitionBufferCount,
            mBufferPoolBufferSize,
            mBufferPoolLockMemoryFlag
        );
        if (theSysError) {
            if (inErrMessagePtr) {
                *inErrMessagePtr = QCUtils::SysError(theSysError);
            }
        } else {
            if (! libkfsio::SetIOBufferAllocator(&GetBufferAllocator())) {
                DiskIoReportError("failed to set buffer allocator");
                if (inErrMessagePtr) {
                    *inErrMessagePtr = "failed to set buffer allocator";
                    theSysError = -1;
                }
            } else {
                // Make sure that allocator works, and it isn't possible to
                // change it:
                IOBufferData theAllocatorTest;
            }
        }
        if (theSysError) {
            GetBufferPool().Destroy();
        } else {
            int64_t const theMaxReqSize =
                int64_t(mBufferAllocator.GetBufferSize()) *
                mDiskQueueMaxBuffersPerRequest * mDiskQueueMaxQueueDepth / 2;
            if (theMaxReqSize > 0 &&
                    int64_t(mMaxRequestSize = size_t(theMaxReqSize)) <
                    theMaxReqSize) {
                mMaxRequestSize = std::numeric_limits<size_t>::max();
            }
            libkfsio::globalNetManager().RegisterTimeoutHandler(this);
            mBufferManager.Init(
                &GetBufferPool(),
                int64_t(mBufferManagerMaxRatio * mBufferAllocator.GetBufferSize() *
                    mBufferPoolPartitionCount * mBufferPoolPartitionBufferCount),
                KFS::CHUNKSIZE,
                mDiskOverloadedPendingWriteByteCount /
                    mBufferAllocator.GetBufferSize()
            );
        }
        return (! theSysError);
    }
    bool Shutdown(
        std::string* inErrMsgPtr = 0)
    {
        while (! DiskQueueList::IsEmpty(mDiskQueuesPtr)) {
            DiskQueueList::Front(mDiskQueuesPtr)->Delete(mDiskQueuesPtr);
        }
        delete mWriteCancelWaiterPtr;
        mWriteCancelWaiterPtr = 0;
        libkfsio::globalNetManager().UnRegisterTimeoutHandler(this);
        mMaxRequestSize = 0;
        if (DoneQueue::IsEmpty(mIoQueuesPtr)) {
            return true;
        }
        DiskIoReportError("io completion queue is not empty");
        if (inErrMsgPtr) {
            *inErrMsgPtr = "io completion queue is not empty: "
                "call RunIoCompletion()";
        }
        return false;
    }
    bool RunCompletion()
    {
        bool    theRet = false;
        DiskIo* thePtr;
        while ((thePtr = Get())) {
            thePtr->RunCompletion();
            theRet = true;
        }
        return theRet;
    }
    void Put(
        DiskIo& inIo)
    {
        {
            QCStMutexLocker theLocker(mMutex);
            DoneQueue::PushBack(mIoQueuesPtr, inIo);
        }
        libkfsio::globalNetManager().Wakeup();
    }
    DiskIo* Get()
    {
        QCStMutexLocker theLocker(mMutex);
        return DoneQueue::PopFront(mIoQueuesPtr);
    }
    bool Cancel(
        DiskIo& inIo)
    {
        if (! QCDiskQueue::IsValidRequestId(inIo.mRequestId)) {
            return false;
        }
        DiskQueue* const theQueuePtr = inIo.mFilePtr->GetDiskQueuePtr();
        if (theQueuePtr) {
            if (inIo.mReadLength <= 0 && ! inIo.mIoBuffers.empty()) {
                // Hold on to the write buffers, while waiting for write to
                // complete.
                WritePending(-int64_t(inIo.mIoBuffers.size() *
                    GetBufferAllocator().GetBufferSize()));
                if (! mWriteCancelWaiterPtr) {
                    mWriteCancelWaiterPtr = new WriteCancelWaiter();
                }
                mWriteCancelWaiterPtr->mIoBuffers = inIo.mIoBuffers;
                if (theQueuePtr->CancelOrSetCompletionIfInFlight(
                        inIo.mRequestId,
                        mWriteCancelWaiterPtr) == mWriteCancelWaiterPtr) {
                    mWriteCancelWaiterPtr = 0;
                } else {
                    mWriteCancelWaiterPtr->mIoBuffers.clear();
                }
            } else {
                if (inIo.mReadLength > 0) {
                    ReadPending(-int64_t(inIo.mReadLength));
                }
                // When read completes it can just discard buffers.
                // Sync doesn't have any buffers attached.
                theQueuePtr->CancelOrSetCompletionIfInFlight(
                    inIo.mRequestId, 0);
            }
        }
        QCStMutexLocker theLocker(mMutex);
        DoneQueue::Remove(mIoQueuesPtr, inIo);
        inIo.mRequestId = QCDiskQueue::kRequestIdNone;
        return true;
    }
    DiskQueue* FindDiskQueue(
        const char* inFileNamePtr)
    {
        DiskQueueList::Iterator theItr(mDiskQueuesPtr);
        DiskQueue* thePtr;
        while ((thePtr = theItr.Next()) &&
                ! thePtr->IsFileNamePrefixMatches(inFileNamePtr))
            {}
        return thePtr;
    }
    DiskQueue* FindDiskQueue(
        unsigned long inDeviceId)
    {
        DiskQueueList::Iterator theItr(mDiskQueuesPtr);
        DiskQueue* thePtr;
        while ((thePtr = theItr.Next()) && thePtr->GetDeviceId() != inDeviceId)
            {}
        return thePtr;
    }
    bool AddDiskQueue(
        const char*   inDirNamePtr,
        unsigned long inDeviceId,
        int           inMaxOpenFiles,
        std::string*  inErrMessagePtr)
    {
        if (FindDiskQueue(inDirNamePtr)) {
            return true; // already has one.
        }
        DiskQueue* theQueuePtr = FindDiskQueue(inDeviceId);
        if (theQueuePtr) {
            theQueuePtr->AddFileNamePrefix(inDirNamePtr);
            return true;
        }
        theQueuePtr = new DiskQueue(mDiskQueuesPtr, inDeviceId, inDirNamePtr);
        const int theSysErr = theQueuePtr->Start(
            mDiskQueueThreadCount,
            mDiskQueueMaxQueueDepth,
            mDiskQueueMaxBuffersPerRequest,
            inMaxOpenFiles,
            0, // FileNamesPtr
            GetBufferPool()
        );
        if (theSysErr) {
            theQueuePtr->Delete(mDiskQueuesPtr);
            const std::string theErrMsg = QCUtils::SysError(theSysErr);
            DiskIoReportError("failed to start queue" + theErrMsg);
            if (inErrMessagePtr) {
                *inErrMessagePtr = theErrMsg;
            }
            return false;
        }
        return true;
    }
    DiskQueue::Time GetMaxEnqueueWaitTimeNanoSec() const
        { return mDiskQueueMaxEnqueueWaitNanoSec; }
    libkfsio::IOBufferAllocator& GetBufferAllocator()
        { return mBufferAllocator; }
    BufferManager& GetBufferManager()
        { return mBufferManager; }
    void ReportError(
        const char* inMsgPtr)
    {
        if (mCrashOnErrorFlag) {
            QCUtils::FatalError(inMsgPtr, errno);
        }
    }
    size_t GetMaxRequestSize() const
        { return mMaxRequestSize; }
    void ReadPending(
        int64_t inReqBytes,
        ssize_t inRetCode = 0)
    {
        if (inReqBytes == 0) {
            return;
        }
        if (inReqBytes < 0) {
            mCounters.mReadCount++;
            if (inRetCode >= 0) {
                mCounters.mReadByteCount += inRetCode;
            } else {
                mCounters.mReadErrorCount++;
            }
        }
        mReadPendingBytes += inReqBytes;
        mReadReqCount += inReqBytes > 0 ? 1 : -1;
        QCASSERT(mReadPendingBytes >= 0 && mReadReqCount >= 0);
        CheckIfOverloaded();
    }
    void WritePending(
        int64_t inReqBytes,
        ssize_t inRetCode = 0)
    {
        if (inReqBytes == 0) {
            return;
        }
        if (inReqBytes < 0) {
            mCounters.mWriteCount++;
            if (inRetCode >= 0) {
                mCounters.mWriteByteCount += inRetCode;
            } else {
                mCounters.mWriteErrorCount++;
            }
        }
        mWritePendingBytes += inReqBytes;
        mWriteReqCount += inReqBytes > 0 ? 1 : -1;
        QCASSERT(mWritePendingBytes >= 0 && mWriteReqCount >= 0);
        CheckIfOverloaded();
    }
    void SyncDone(
        ssize_t inRetCode)
    {
        if (inRetCode >= 0) {
            mCounters.mSyncCount++;
        } else {
            mCounters.mSyncErrorCount++;
        }
    }
    int GetFdCountPerFile() const
        { return mDiskQueueThreadCount; }
    void GetCounters(
        Counters& outCounters)
        { outCounters = mCounters; }
private:
    typedef DiskIo::IoBuffers IoBuffers;
    class WriteCancelWaiter : public QCDiskQueue::IoCompletion
    {
    public:
        WriteCancelWaiter()
            : QCDiskQueue::IoCompletion(),
              mIoBuffers()
            {}
        virtual bool Done(
            QCDiskQueue::RequestId      /* inRequestId */,
            QCDiskQueue::FileIdx        /* inFileIdx */,
            QCDiskQueue::BlockIdx       /* inStartBlockIdx */,
            QCDiskQueue::InputIterator& /* inBufferItr */,
            int                         /* inBufferCount */,
            QCDiskQueue::Error          /* inCompletionCode */,
            int                         /* inSysErrorCode */,
            int64_t                     /* inIoByteCount */)
        {
            delete this; // This might release buffers.
            return true; // Tell the caller not to release buffers.
        }
        IoBuffers mIoBuffers;
    };
    class BufferAllocator : public libkfsio::IOBufferAllocator
    {
    public:
        BufferAllocator()
            : mBufferPool()
            {}
        virtual size_t GetBufferSize() const
            { return mBufferPool.GetBufferSize(); }
        virtual char* Allocate()
        {
            char* const theBufPtr = mBufferPool.Get();
            if (! theBufPtr) {
                QCUtils::FatalError("out of io buffers", 0);
            }
            return theBufPtr;
        }
        virtual void Deallocate(
            char* inBufferPtr)
            { mBufferPool.Put(inBufferPtr); }
        QCIoBufferPool& GetBufferPool()
            { return mBufferPool; }
    private:
        QCIoBufferPool mBufferPool;

    private:
        BufferAllocator(
            const BufferAllocator& inAllocator);
        BufferAllocator& operator=(
            const BufferAllocator& inAllocator);
    };

    const int             mDiskQueueThreadCount;
    const int             mDiskQueueMaxQueueDepth;
    const int             mDiskQueueMaxBuffersPerRequest;
    const DiskQueue::Time mDiskQueueMaxEnqueueWaitNanoSec;
    const int             mBufferPoolPartitionCount;
    const int             mBufferPoolPartitionBufferCount;
    const int             mBufferPoolBufferSize;
    const int             mBufferPoolLockMemoryFlag;
    const int             mDiskOverloadedPendingRequestCount;
    const int             mDiskClearOverloadedPendingRequestCount;
    const int             mDiskOverloadedMinFreeBufferCount;
    const int             mDiskClearOverloadedMinFreeBufferCount;
    const int64_t         mDiskOverloadedPendingWriteByteCount;
    const int64_t         mDiskClearOverloadedPendingWriteByteCount;
    const bool            mCrashOnErrorFlag;
    const double          mBufferManagerMaxRatio;
    bool                  mOverloadedFlag;
    size_t                mMaxRequestSize;
    WriteCancelWaiter*    mWriteCancelWaiterPtr;
    int64_t               mReadPendingBytes;
    int64_t               mWritePendingBytes;
    int                   mReadReqCount;
    int                   mWriteReqCount;
    QCMutex               mMutex;
    BufferAllocator       mBufferAllocator;
    BufferManager         mBufferManager;
    DiskIo*               mIoQueuesPtr[1];
    DiskQueue*            mDiskQueuesPtr[1];
    Counters              mCounters;

    QCIoBufferPool& GetBufferPool()
        { return mBufferAllocator.GetBufferPool(); }
    virtual void Timeout() // ITimeout
        { RunCompletion(); }
    void CheckIfOverloaded()
    {
        const int theReqCount = mReadReqCount + mWriteReqCount;
        SetOverloaded(mOverloadedFlag ? // Hysteresis
            mWritePendingBytes > mDiskClearOverloadedPendingWriteByteCount ||
            theReqCount        > mDiskClearOverloadedPendingRequestCount   ||
            (mWritePendingBytes > 0 &&
                mBufferAllocator.GetBufferPool().GetFreeBufferCount() <
                mDiskClearOverloadedMinFreeBufferCount
            )
        :
            mWritePendingBytes > mDiskOverloadedPendingWriteByteCount ||
            theReqCount        > mDiskOverloadedPendingRequestCount   ||
            (mWritePendingBytes > 0 &&
                mBufferAllocator.GetBufferPool().GetFreeBufferCount() <
                mDiskOverloadedMinFreeBufferCount
            )
        );
    }
    void SetOverloaded(
        bool inFlag)
    {
        if (mOverloadedFlag == inFlag) {
            return;
        }
        mOverloadedFlag = inFlag;
        KFS_LOG_VA_INFO("%s disk overloaded state: "
            "pending read: %d bytes: %.0f write: %d bytes: %.0f",
            mOverloadedFlag ? "Setting" : "Clearing",
            int(mReadReqCount), double(mReadPendingBytes),
            int(mWriteReqCount), double(mWritePendingBytes));
        KFS::libkfsio::globalNetManager().ChangeDiskOverloadState(inFlag);
    }
};

static DiskIoQueues* sDiskIoQueuesPtr;

    /* static */ bool
DiskIo::Init(
    const Properties& inProperties,
    std::string*      inErrMessagePtr /* = 0 */)
{
    if (sDiskIoQueuesPtr) {
        *inErrMessagePtr = "already initialized";
        return false;
    }
    sDiskIoQueuesPtr = new DiskIoQueues(inProperties);
    if (! sDiskIoQueuesPtr->Start(inErrMessagePtr)) {
        delete sDiskIoQueuesPtr;
        sDiskIoQueuesPtr = 0;
        return false;
    }
    return (sDiskIoQueuesPtr != 0);
}

static void DiskIoReportError(
    const char* inMsgPtr)
{
    if (sDiskIoQueuesPtr) {
        sDiskIoQueuesPtr->ReportError(inMsgPtr);
    }
}

    /* static */ bool
DiskIo::StartIoQueue(
    const char*   inDirNamePtr,
    unsigned long inDeviceId,
    int           inMaxOpenFiles,
    std::string*  inErrMessagePtr /* = 0 */)
{
    if (! sDiskIoQueuesPtr) {
        if (inErrMessagePtr) {
            *inErrMessagePtr = "not initialized";
        }
        return false;
    }
    return sDiskIoQueuesPtr->AddDiskQueue(
        inDirNamePtr, inDeviceId, inMaxOpenFiles, inErrMessagePtr);
}

    /* static */ bool
DiskIo::Shutdown(
    std::string* inErrMessagePtr /* = 0 */)
{
    if (! sDiskIoQueuesPtr) {
        return true;
    }
    const bool theOkFlag = sDiskIoQueuesPtr->Shutdown(inErrMessagePtr);
    delete sDiskIoQueuesPtr;
    sDiskIoQueuesPtr = 0;
    return theOkFlag;
}

    /* static */ int
DiskIo::GetFdCountPerFile()
{
    return (sDiskIoQueuesPtr ? sDiskIoQueuesPtr->GetFdCountPerFile() : -1);
}

    /* static */ bool
DiskIo::RunIoCompletion()
{
    return (sDiskIoQueuesPtr && sDiskIoQueuesPtr->RunCompletion());
}

    /* static */ size_t
DiskIo::GetMaxRequestSize()
{
    return (sDiskIoQueuesPtr ? sDiskIoQueuesPtr->GetMaxRequestSize() : 0);
}

    /* static */ BufferManager&
DiskIo::GetBufferManager()
{
    QCRTASSERT(sDiskIoQueuesPtr);
    return (sDiskIoQueuesPtr->GetBufferManager());
}

    /* static */ void
DiskIo::GetCounters(
    Counters& outCounters)
{
    if (! sDiskIoQueuesPtr) {
        outCounters.Clear();
        return;
    }
    sDiskIoQueuesPtr->GetCounters(outCounters);
}

    bool
DiskIo::File::Open(
    const char*  inFileNamePtr,
    off_t        inMaxFileSize          /* = -1 */,
    bool         inReadOnlyFlag         /* = false */,
    bool         inReserveFileSpaceFlag /* = false */,
    bool         inCreateFlag           /* = false */,
    std::string* inErrMessagePtr        /* = 0 */)
{
    const char* theErrMsgPtr = 0;
    if (IsOpen()) {
       theErrMsgPtr = "file is already open";
    } else if (! inFileNamePtr) {
        theErrMsgPtr = "file name is null";
    } else if (! sDiskIoQueuesPtr) {
        theErrMsgPtr = "disk queues are not initialized";
    } else {
        Reset();
        if (! (mQueuePtr = sDiskIoQueuesPtr->FindDiskQueue(inFileNamePtr))) {
            theErrMsgPtr = "failed to find disk queue";
        }
    }
    if (theErrMsgPtr) {
        if (inErrMessagePtr) {
            *inErrMessagePtr = theErrMsgPtr;
        }
        DiskIoReportError(theErrMsgPtr);
        return false;
    }
    mReadOnlyFlag      = inReadOnlyFlag;
    mSpaceReservedFlag = ! mReadOnlyFlag &&
        inMaxFileSize > 0 && inReserveFileSpaceFlag;
    QCDiskQueue::OpenFileStatus const theStatus = mQueuePtr->OpenFile(
        inFileNamePtr, mReadOnlyFlag ? -1 : inMaxFileSize, mReadOnlyFlag,
        mSpaceReservedFlag, inCreateFlag);
    if (theStatus.IsError()) {
        if (inErrMessagePtr) {
            *inErrMessagePtr =
                QCUtils::SysError(theStatus.GetSysError()) + " " +
                QCDiskQueue::ToString(theStatus.GetError());
        }
        Reset();
        DiskIoReportError(QCUtils::SysError(theStatus.GetSysError()) + " " +
            QCDiskQueue::ToString(theStatus.GetError()));
        return false;
    }
    mFileIdx = theStatus.GetFileIdx();
    return true;
}

    bool
DiskIo::File::Close(
    off_t        inFileSize,     /* = -1 */
    std::string* inErrMessagePtr /* = 0  */)
{
    if (mFileIdx < 0 || ! mQueuePtr) {
        Reset();
        return true;
    }
    QCDiskQueue::CloseFileStatus theStatus = mQueuePtr->CloseFile(
        mFileIdx, mReadOnlyFlag ? -1 : inFileSize);
    if (theStatus.IsError()) {
        if (inErrMessagePtr) {
            *inErrMessagePtr =
                QCUtils::SysError(theStatus.GetSysError()) + " " +
                QCDiskQueue::ToString(theStatus.GetError());
        }
        DiskIoReportError(QCUtils::SysError(theStatus.GetSysError()) + " " +
            QCDiskQueue::ToString(theStatus.GetError()));
        // Remains open.
        return false;
    }
    Reset();
    return true;
}

    void
DiskIo::File::GetDiskQueuePendingCount(
    int&     outFreeRequestCount,
    int&     outRequestCount,
    int64_t& outReadBlockCount,
    int64_t& outWriteBlockCount,
    int&     outBlockSize)
{
    if (mQueuePtr) {
        mQueuePtr->GetPendingCount(
            outFreeRequestCount,
            outRequestCount,
            outReadBlockCount,
            outWriteBlockCount);
        outBlockSize = mQueuePtr->GetBlockSize();
    } else {
        outFreeRequestCount = 0;
        outRequestCount     = 0;
        outReadBlockCount   = 0;
        outWriteBlockCount  = 0;
        outBlockSize        = 0;
    }
}

    bool
DiskIo::File::ReserveSpace(
    std::string* inErrMessagePtr)
{
    if (mSpaceReservedFlag) {
        return true; // Already done.
    }
    if (! IsOpen()) {
        if (inErrMessagePtr) {
            *inErrMessagePtr = "closed";
        }
        return false;
    }
    if (IsReadOnly()) {
        if (inErrMessagePtr) {
            *inErrMessagePtr = "read only";
        }
        return false;
    }
    if (! mQueuePtr) {
        if (inErrMessagePtr) {
            *inErrMessagePtr = "no queue";
        }
        return false;
    }
    const DiskQueue::Status theStatus = mQueuePtr->AllocateFileSpace(mFileIdx);
    if (theStatus.IsError() && inErrMessagePtr) {
        if (theStatus.GetError() != QCDiskQueue::kErrorNone) {
            *inErrMessagePtr = QCDiskQueue::ToString(theStatus.GetError());
        } else {
            *inErrMessagePtr = QCUtils::SysError(theStatus.GetSysError());
        }
    }
    mSpaceReservedFlag = theStatus.IsGood();
    return mSpaceReservedFlag;
}

bool
DiskIo::File::GrowFile(off_t inTargetSz, std::string *inErrMessagePtr)
{
    const DiskQueue::Status theStatus = mQueuePtr->GrowFile(mFileIdx, inTargetSz);    

    if (theStatus.IsError() && inErrMessagePtr) {
        if (theStatus.GetError() != QCDiskQueue::kErrorNone) {
            *inErrMessagePtr = QCDiskQueue::ToString(theStatus.GetError());
        } else {
            *inErrMessagePtr = QCUtils::SysError(theStatus.GetSysError());
        }
    }

    mSpaceReservedFlag = theStatus.IsGood();
    return mSpaceReservedFlag;
}

bool
DiskIo::File::ResetFilesize(off_t inTargetSz, std::string *inErrMessagePtr)
{
    const DiskQueue::Status theStatus = mQueuePtr->ResetFilesize(mFileIdx, inTargetSz);    

    if (theStatus.IsError() && inErrMessagePtr) {
        if (theStatus.GetError() != QCDiskQueue::kErrorNone) {
            *inErrMessagePtr = QCDiskQueue::ToString(theStatus.GetError());
        } else {
            *inErrMessagePtr = QCUtils::SysError(theStatus.GetSysError());
        }
    }
    return theStatus.IsGood();
}

void
DiskIo::File::UnReserveSpace(off_t inStartOffset, off_t inLen) 
{
    if (mSpaceReservedFlag) {
        mQueuePtr->UnReserveFileSpace(mFileIdx, inStartOffset, inLen);
    }
}

    void
DiskIo::File::Reset()
{
    mQueuePtr          = 0;
    mFileIdx           = -1;
    mReadOnlyFlag      = false;
    mSpaceReservedFlag = false;
}

DiskIo::DiskIo(
    DiskIo::FilePtr inFilePtr,
    KfsCallbackObj* inCallBackObjPtr)
    : mCallbackObjPtr(inCallBackObjPtr),
      mFilePtr(inFilePtr),
      mRequestId(QCDiskQueue::kRequestIdNone),
      mIoBuffers(),
      mReadBufOffset(0),
      mReadLength(0),
      mIoRetCode(0),
      mCompletionRequestId(QCDiskQueue::kRequestIdNone),
      mCompletionCode(QCDiskQueue::kErrorNone)
{
    QCRTASSERT(mCallbackObjPtr && mFilePtr.get());
    DiskIoQueues::DoneQueue::Init(*this);
}

DiskIo::~DiskIo()
{
    DiskIo::Close();
}

    void
DiskIo::Close()
{
    if (sDiskIoQueuesPtr) {
        sDiskIoQueuesPtr->Cancel(*this);
    }
}

    ssize_t
DiskIo::Read(
    off_t  inOffset,
    size_t inNumBytes)
{
    if (inOffset < 0 ||
            mRequestId != QCDiskQueue::kRequestIdNone || ! mFilePtr->IsOpen()) {
        KFS_LOG_VA_ERROR("file: %d %s read request: %d offset: %.0f",
            (int)mFilePtr->GetFileIdx(), mFilePtr->IsOpen() ? "open" : "closed",
            (int)mRequestId, (double)inOffset);
        errno = -EINVAL;
        DiskIoReportError("DiskIo::Read: bad parameters");
        return -1;
    }
    mIoBuffers.clear();
    DiskQueue* const theQueuePtr = mFilePtr->GetDiskQueuePtr();
    if (! theQueuePtr) {
        KFS_LOG_ERROR("read: no queue");
        errno = -EINVAL;
        DiskIoReportError("DiskIo::Read: no queue");
        return -1;
    }
    if (inNumBytes <= 0) {
        return 0; // Io completion will not be called in this case.
    }
    const int theBlockSize = theQueuePtr->GetBlockSize();
    if (theBlockSize <= 0) {
        KFS_LOG_VA_ERROR("bad block size %d", theBlockSize);
        errno = -EINVAL;
        DiskIoReportError("DiskIo::Read: bad block size");
        return -1;
    }
    mIoRetCode     = 0;
    mReadBufOffset = inOffset % theBlockSize;
    mReadLength    = inNumBytes;
    const int theBufferCnt =
        (mReadLength + mReadBufOffset + theBlockSize - 1) / theBlockSize;
    mIoBuffers.reserve(theBufferCnt);
    mCompletionRequestId = QCDiskQueue::kRequestIdNone;
    mCompletionCode      = QCDiskQueue::kErrorNone;
    const DiskQueue::EnqueueStatus theStatus = theQueuePtr->Read(
        mFilePtr->GetFileIdx(),
        inOffset / theBlockSize,
        0, // inBufferIteratorPtr // allocate buffers just beofre read
        theBufferCnt,
        this,
        sDiskIoQueuesPtr->GetMaxEnqueueWaitTimeNanoSec()
    );
    if (theStatus.IsGood()) {
        sDiskIoQueuesPtr->ReadPending(inNumBytes);
        mRequestId = theStatus.GetRequestId();
        QCRTASSERT(mRequestId != QCDiskQueue::kRequestIdNone);
        return inNumBytes;
    }
    errno = -EINVAL;
    const std::string theErrMsg(QCDiskQueue::ToString(theStatus.GetError()));
    KFS_LOG_VA_ERROR("read queuing error: %s", theErrMsg.c_str());
    DiskIoReportError("DiskIo::Read: " + theErrMsg);
    return -1;
}

    ssize_t
DiskIo::Write(
    off_t     inOffset,
    size_t    inNumBytes,
    IOBuffer* inBufferPtr)
{
    if (inOffset < 0 || ! inBufferPtr ||
            mRequestId != QCDiskQueue::kRequestIdNone || ! mFilePtr->IsOpen()) {
        KFS_LOG_VA_ERROR("file: %d %s write request: %d "
            "offset: %.0f, buffer: %lx",
            (int)mFilePtr->GetFileIdx(), mFilePtr->IsOpen() ? "open" : "closed",
            (int)mRequestId, (double)inOffset, (long)inBufferPtr);
        errno = -EINVAL;
        DiskIoReportError("DiskIo::Write: bad parameters");
        return -1;
    }
    mReadLength    = 0;
    mIoRetCode     = 0;
    mReadBufOffset = 0;
    mIoBuffers.clear();
    if (mFilePtr->IsReadOnly()) {
        KFS_LOG_ERROR("write: read only mode");
        errno = -EINVAL;
        DiskIoReportError("DiskIo::Write: read only mode");
        return -1;
    }
    DiskQueue* const theQueuePtr = mFilePtr->GetDiskQueuePtr();
    if (! theQueuePtr) {
        KFS_LOG_ERROR("write: no queue");
        errno = -EINVAL;
        DiskIoReportError("DiskIo::Write: no queue");
        return -1;
    }
    const int theBlockSize = theQueuePtr->GetBlockSize();
    if (inOffset % theBlockSize != 0) {
        KFS_LOG_VA_ERROR("file: %d write: bad offset: %.0f",
            (int)mFilePtr->GetFileIdx(), (double)inOffset);
        errno = -EINVAL;
        DiskIoReportError("DiskIo::Write: bad offset");
        return -1;
    }
    const size_t kBlockAlignMask = (4 << 10) - 1;
    size_t       theNWr          = inNumBytes;
    for (IOBuffer::iterator
            theIt = inBufferPtr->begin();
            theIt != inBufferPtr->end() && theNWr > 0;
            ++theIt) {
        const IOBufferData& theBuf = *theIt;
        if (theBuf.IsEmpty()) {
            continue;
        }
        if (theNWr < (size_t)theBlockSize ||
                theBlockSize != theBuf.BytesConsumable() ||
                (theBuf.Consumer() - (char*)0) & kBlockAlignMask) {
            KFS_LOG_VA_ERROR("file: %d write offset: %.0f "
                "io buffer: %lx size: %d",
                (int)mFilePtr->GetFileIdx(), (long)theBuf.Consumer(),
                std::min((int)theNWr, (int)theBuf.BytesConsumable()));
            mIoBuffers.clear();
            errno = -EINVAL;
            DiskIoReportError("DiskIo::Write: bad buffer");
            return -1;
        }
        theNWr -= theBlockSize;
        mIoBuffers.push_back(*theIt);
    }
    if (mIoBuffers.empty()) {
        return 0;
    }
    struct BufIterator : public QCDiskQueue::InputIterator
    {
        BufIterator(
            IoBuffers& inBufs)
            : mCur(inBufs.begin()),
              mEnd(inBufs.end())
            {}
            virtual char* Get()
                { return (mEnd == mCur ? 0 : (mCur++)->Consumer()); }
        IoBuffers::iterator       mCur;
        IoBuffers::iterator const mEnd;
    };
    BufIterator theBufItr(mIoBuffers);
    mCompletionRequestId = QCDiskQueue::kRequestIdNone;
    mCompletionCode      = QCDiskQueue::kErrorNone;
    const DiskQueue::EnqueueStatus theStatus = theQueuePtr->Write(
        mFilePtr->GetFileIdx(),
        inOffset / theBlockSize,
        &theBufItr,
        mIoBuffers.size(),
        this,
        sDiskIoQueuesPtr->GetMaxEnqueueWaitTimeNanoSec()
    );
    if (theStatus.IsGood()) {
        sDiskIoQueuesPtr->WritePending(inNumBytes - theNWr);
        mRequestId = theStatus.GetRequestId();
        QCRTASSERT(mRequestId != QCDiskQueue::kRequestIdNone);
        return (inNumBytes - theNWr);
    }
    errno = -EINVAL;
    const std::string theErrMsg = QCDiskQueue::ToString(theStatus.GetError());
    KFS_LOG_VA_ERROR("write queuing error: %s", theErrMsg.c_str());
    DiskIoReportError("DiskIo::Write: " + theErrMsg);
    return -1;
}

    int
DiskIo::Sync(
    bool inNotifyDoneFlag)
{
    if (mRequestId != QCDiskQueue::kRequestIdNone || ! mFilePtr->IsOpen()) {
        KFS_LOG_VA_ERROR("file: %d %s sync request: %d notify: %s",
            (int)mFilePtr->GetFileIdx(), mFilePtr->IsOpen() ? "open" : "closed",
            (int)mRequestId, inNotifyDoneFlag ? "yes" : "no");
        errno = -EINVAL;
        DiskIoReportError("DiskIo::Sync: bad parameters");
        return -1;
    }
    mIoBuffers.clear();
    DiskQueue* const theQueuePtr = mFilePtr->GetDiskQueuePtr();
    if (! theQueuePtr) {
        KFS_LOG_ERROR("sync: no queue");
        errno = -EINVAL;
        DiskIoReportError("DiskIo::Sync: no queue");
        return -1;
    }
    mCompletionRequestId = QCDiskQueue::kRequestIdNone;
    mCompletionCode      = QCDiskQueue::kErrorNone;
    const DiskQueue::EnqueueStatus theStatus = theQueuePtr->Sync(
        mFilePtr->GetFileIdx(),
        inNotifyDoneFlag ? this : 0,
        sDiskIoQueuesPtr->GetMaxEnqueueWaitTimeNanoSec()
    );
    if (theStatus.IsGood()) {
        if (inNotifyDoneFlag) {
            mRequestId = theStatus.GetRequestId();
            QCRTASSERT(mRequestId != QCDiskQueue::kRequestIdNone);
        }
        return 0;
    }
    errno = -EINVAL;
    const std::string theErrMsg(QCDiskQueue::ToString(theStatus.GetError()));
    KFS_LOG_VA_ERROR("sync queuing error: %s", theErrMsg.c_str());
    DiskIoReportError("DiskIo::Sync: " + theErrMsg);
    return -1;
}

    /* virtual */ bool
DiskIo::Done(
    QCDiskQueue::RequestId      inRequestId,
    QCDiskQueue::FileIdx        inFileIdx,
    QCDiskQueue::BlockIdx       /* inStartBlockIdx */,
    QCDiskQueue::InputIterator& inBufferItr,
    int                         inBufferCount,
    QCDiskQueue::Error          inCompletionCode,
    int                         inSysErrorCode,
    int64_t                     inIoByteCount)
{
    QCASSERT(sDiskIoQueuesPtr);
    bool theOwnBuffersFlag = false;
    mCompletionRequestId = inRequestId;
    mCompletionCode      = inCompletionCode;
    if (mCompletionCode != QCDiskQueue::kErrorNone) {
        if (inSysErrorCode != 0) {
            mIoRetCode = -inSysErrorCode;
        } else {
            if (mCompletionCode == QCDiskQueue::kErrorOutOfBuffers) {
                mIoRetCode = -ENOMEM;
            } else {
                mIoRetCode = -EIO;
            }
        }
        if (mIoRetCode > 0) {
            mIoRetCode = -mIoRetCode;
        } else if (mIoRetCode == 0) {
            mIoRetCode = -1000;
        }
        // If this is read failure, then tell caller to free the buffers.
        theOwnBuffersFlag = mReadLength <= 0;
    } else {
        mIoRetCode = ssize_t(inIoByteCount);
        if (mReadLength <= 0) {
            theOwnBuffersFlag = true;  // Write or sync done.
        } else if (inIoByteCount <= 0) {
            theOwnBuffersFlag = false; // empty read, free buffers if any.
        } else {
            const int theBufSize =
                sDiskIoQueuesPtr->GetBufferAllocator().GetBufferSize();
            QCRTASSERT(inBufferCount * theBufSize >= inIoByteCount);
            int   theCnt         = inBufferCount;
            char* thePtr;
            while (theCnt-- > 0 && (thePtr = inBufferItr.Get())) {
                mIoBuffers.push_back(IOBufferData(
                    thePtr, 0, theBufSize,
                    sDiskIoQueuesPtr->GetBufferAllocator()));
            }
            QCRTASSERT(
                (inBufferCount - (theCnt + 1)) * theBufSize >= inIoByteCount);
            theOwnBuffersFlag = true;
        }
    }
    sDiskIoQueuesPtr->Put(*this);
    return theOwnBuffersFlag;
}

    void
DiskIo::RunCompletion()
{
    QCASSERT(mCompletionRequestId == mRequestId && sDiskIoQueuesPtr);
    mRequestId = QCDiskQueue::kRequestIdNone;
    if (mReadLength > 0) {
        sDiskIoQueuesPtr->ReadPending(-int64_t(mReadLength), mIoRetCode);
    } else if (! mIoBuffers.empty()) {
        sDiskIoQueuesPtr->WritePending(-int64_t(mIoBuffers.size() *
            sDiskIoQueuesPtr->GetBufferAllocator().GetBufferSize()),
            mIoRetCode);
    } else {
        sDiskIoQueuesPtr->SyncDone(mIoRetCode);
    }
    int theNumRead(mIoRetCode);
    QCRTASSERT(theNumRead == mIoRetCode);
    if (mIoRetCode < 0) {
        std::string theErrMsg(QCDiskQueue::ToString(mCompletionCode));
        theErrMsg += " ";
        theErrMsg += QCUtils::SysError(-theNumRead);
        KFS_LOG_VA_ERROR("%s (%d %d) error: %d %s",
            mReadLength > 0 ? "read" : (mIoBuffers.empty() ? "sync" : "write"),
            int(mReadLength), int(mIoBuffers.size()),
            theNumRead, theErrMsg.c_str()
        );
    }
    if (mIoRetCode < 0 || mReadLength <= 0) {
        const bool theSyncFlag = mReadLength == 0 && mIoBuffers.empty();
        mIoBuffers.clear();
        IoCompletion(0, theNumRead, theSyncFlag);
        return;
    }
    // Read. Skip/trim first/last buffers if needed.
    if (mIoBuffers.empty()) {
        QCRTASSERT(theNumRead == 0);
        theNumRead = 0;
    } else {
        const size_t  theBufSize = mIoBuffers.front().BytesConsumable();
        const ssize_t theSize(mIoBuffers.size() * theBufSize);
        QCRTASSERT(theSize >= theNumRead);
        const int theConsumed = mIoBuffers.front().Consume(mReadBufOffset);
        QCRTASSERT(theConsumed == (int)mReadBufOffset);
        theNumRead -= std::min(theNumRead, theConsumed);
        if (theNumRead > (int)mReadLength) {
            const int theToTrimTo(theBufSize - (theNumRead - mReadLength));
            const int theTrimmedSize = mIoBuffers.back().Trim(theToTrimTo);
            QCRTASSERT(theToTrimTo == theTrimmedSize);
            theNumRead = mReadLength;
        }
    }
    IOBuffer theIoBuffer;
    for (IoBuffers::iterator theItr = mIoBuffers.begin();
            theItr != mIoBuffers.end();
            ++theItr) {
        if (theItr->IsEmpty()) {
            continue;
        }
        theIoBuffer.Append(*theItr);
    }
    mIoBuffers.clear();
    IoCompletion(&theIoBuffer, theNumRead);
}

    void
DiskIo::IoCompletion(
    IOBuffer*   inBufferPtr,
    int         inRetCode,
    bool        inSyncFlag /* = false */)
{
    if (inRetCode < 0) {
        mCallbackObjPtr->HandleEvent(EVENT_DISK_ERROR, &inRetCode);
    } else if (inSyncFlag) {
        mCallbackObjPtr->HandleEvent(EVENT_SYNC_DONE, 0);
    } else if (inBufferPtr) {
        libkfsio::globals().ctrDiskBytesRead.Update(int(mIoRetCode));
        mCallbackObjPtr->HandleEvent(EVENT_DISK_READ, inBufferPtr);
    } else {
        libkfsio::globals().ctrDiskBytesWritten.Update(int(mIoRetCode));
        mCallbackObjPtr->HandleEvent(EVENT_DISK_WROTE, &inRetCode);
    }
}

} /* namespace KFS */
