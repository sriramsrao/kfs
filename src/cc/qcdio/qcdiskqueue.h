//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: qcdiskqueue.h 1575 2011-01-10 22:26:35Z sriramr $
//
// Created 2008/11/11
//
// Copyright 2008,2009 Quantcast Corp.
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

#ifndef QCDISKQUEUE_H
#define QCDISKQUEUE_H

#include <stdint.h>
#include <unistd.h>
#include "qciobufferpool.h"
#include "qcmutex.h"

class QCDiskQueue
{
public:
    enum ReqType
    {
        kReqTypeNone  = 0,
        kReqTypeRead  = 1,
        kReqTypeWrite = 2
    };
    enum Error
    {
        kErrorNone                 = 0,
        kErrorRead                 = 1,
        kErrorWrite                = 2,
        kErrorCancel               = 3,
        kErrorSeek                 = 4,
        kErrorEnqueue              = 5,
        kErrorOutOfBuffers         = 6,
        kErrorParameter            = 7,
        kErrorQueueStopped         = 8,
        kErrorFileIdxOutOfRange    = 9,
        kErrorBlockIdxOutOfRange   = 10,
        kErrorBlockCountOutOfRange = 11,
        kErrorOutOfRequests        = 12,
        kErrorOpen                 = 13,
        kErrorClose                = 14,
        kErrorHasPendingRequests   = 15,
        kErrorSpaceAlloc           = 16
    };
    static const char* ToString(
        Error inErrorCode);
    enum { kRequestIdNone = -1 };
    typedef int      RequestId;
    typedef int      FileIdx;
    typedef int64_t  BlockIdx;
    typedef QCIoBufferPool::InputIterator  InputIterator;
    typedef QCIoBufferPool::OutputIterator OutputIterator;
    typedef QCMutex::Time                  Time;

    class Status
    {
    public:
        Status(
            Error inError    = kErrorNone,
            int   inSysError = 0)
            : mError(inError),
              mSysError(inSysError)
            {}
        bool IsError() const
            { return (mError != kErrorNone || mSysError != 0); }
        bool IsGood() const
            { return (! IsError()); }
        Error GetError() const
            { return mError; }
        int GetSysError() const
            { return mSysError; }
    private:
        Error mError;
        int   mSysError;
    };

    class CompletionStatus : public Status
    {
    public:
        CompletionStatus(
            Error   inError       = kErrorNone,
            int     inSysError    = 0,
            int64_t inIoByteCount = 0)
            : Status(inError, inSysError),
              mIoByteCount(inIoByteCount)
            {}
            int GetIoByteCount() const
                { return mIoByteCount; }
        private:
            int64_t mIoByteCount;
    };

    typedef Status CloseFileStatus;

    class EnqueueStatus
    {
    public:
        EnqueueStatus(
            RequestId inRequestId = kRequestIdNone,
            Error     inError     = kErrorNone)
            : mRequestId(inRequestId),
              mError(inError)
            {}
        bool IsError() const
            { return (mError != kErrorNone || mRequestId == kRequestIdNone); }
        bool IsGood() const
            { return (! IsError()); }
        Error GetError() const
            { return mError; }
        RequestId GetRequestId() const
            { return mRequestId; }
    private:
        RequestId mRequestId;
        Error     mError;
    };

    class OpenFileStatus : public Status
    {
    public:
        OpenFileStatus(
            FileIdx inFileIdx  = -1,
            Error   inError    = kErrorParameter,
            int     inSysError = 0)
            : Status(inError, inSysError),
              mFileIdx(inFileIdx)
            {}
        FileIdx GetFileIdx() const
            { return mFileIdx; }
    private:
        FileIdx mFileIdx;
    };

    static bool IsValidRequestId(
        RequestId inReqId)
        { return (inReqId != kRequestIdNone); }
    class IoCompletion
    {
    public:
        // If returns false then caller will free buffers.
        virtual bool Done(
            RequestId      inRequestId,
            FileIdx        inFileIdx,
            BlockIdx       inStartBlockIdx,
            InputIterator& inBufferItr,
            int            inBufferCount,
            Error          inCompletionCode,
            int            inSysErrorCode,
            int64_t        inIoBytes) = 0;
    protected:
        IoCompletion()
            {}
        virtual ~IoCompletion()
            {}
    };

    QCDiskQueue();
    ~QCDiskQueue();

    int Start(
        int             inThreadCount,
        int             inMaxQueueDepth,
        int             inMaxBuffersPerRequestCount,
        int             inFileCount,
        const char**    inFileNamesPtr,
        QCIoBufferPool& inBufferPool);
    void Stop();
    EnqueueStatus Enqueue(
        ReqType        inReqType,
        FileIdx        inFileIdx,
        BlockIdx       inStartBlockIdx,
        InputIterator* inBufferIteratorPtr,
        int            inBufferCount,
        IoCompletion*  inIoCompletionPtr,
        Time           inTimeWaitNanoSec = -1);
    EnqueueStatus Read(
        FileIdx        inFileIdx,
        BlockIdx       inStartBlockIdx,
        InputIterator* inBufferIteratorPtr,
        int            inBufferCount,
        IoCompletion*  inIoCompletionPtr,
        Time           inTimeWaitNanoSec = -1)
    {
        return Enqueue(
            kReqTypeRead,
            inFileIdx,
            inStartBlockIdx,
            inBufferIteratorPtr,
            inBufferCount,
            inIoCompletionPtr,
            inTimeWaitNanoSec);
    }
    EnqueueStatus Write(
        FileIdx        inFileIdx,
        BlockIdx       inStartBlockIdx,
        InputIterator* inBufferIteratorPtr,
        int            inBufferCount,
        IoCompletion*  inIoCompletionPtr,
        Time           inTimeWaitNanoSec = -1)
    {
        return Enqueue(
            kReqTypeWrite,
            inFileIdx,
            inStartBlockIdx,
            inBufferIteratorPtr,
            inBufferCount,
            inIoCompletionPtr,
            inTimeWaitNanoSec);
    }
    CompletionStatus SyncIo(
        ReqType         inReqType,
        FileIdx         inFileIdx,
        BlockIdx        inStartBlockIdx,
        InputIterator*  inBufferIteratorPtr,
        int             inBufferCount,
        OutputIterator* inOutBufferIteratroPtr);
    CompletionStatus SyncRead(
        FileIdx         inFileIdx,
        BlockIdx        inStartBlockIdx,
        InputIterator*  inBufferIteratorPtr,
        int             inBufferCount,
        OutputIterator* inOutBufferIteratroPtr = 0)
    {
        return SyncIo(
            kReqTypeRead,
            inFileIdx,
            inStartBlockIdx,
            inBufferIteratorPtr,
            inBufferCount,
            inOutBufferIteratroPtr);
    }
    CompletionStatus SyncWrite(
        FileIdx         inFileIdx,
        BlockIdx        inStartBlockIdx,
        InputIterator*  inBufferIteratorPtr,
        int             inBufferCount,
        OutputIterator* inOutBufferIteratroPtr = 0)
    {
        return SyncIo(
            kReqTypeWrite,
            inFileIdx,
            inStartBlockIdx,
            inBufferIteratorPtr,
            inBufferCount,
            inOutBufferIteratroPtr);
    }
    EnqueueStatus Sync(
        FileIdx       inFileIdx,
        IoCompletion* inIoCompletionPtr,
        Time          inTimeWaitNanoSec = -1);
    bool Cancel(
        RequestId inRequestId);
    IoCompletion* CancelOrSetCompletionIfInFlight(
        RequestId     inRequestId,
        IoCompletion* inCompletionIfInFlightPtr);
    void GetPendingCount(
        int&     outFreeRequestCount,
        int&     outRequestCount,
        int64_t& outReadBlockCount,
        int64_t& outWriteBlockCount);
    OpenFileStatus OpenFile(
        const char* inFileNamePtr,
        int64_t     inMaxFileSize           = -1,
        bool        inReadOnlyFlag          = false,
        bool        inAllocateFileSpaceFlag = false,
        bool        inCreateFlag            = false);
    CloseFileStatus CloseFile(
        FileIdx inFileIdx,
        int64_t inFileSize = -1);
    int GetBlockSize() const;
    Status AllocateFileSpace(
        FileIdx inFileIdx);
    Status GrowFile(FileIdx inFileIdx, off_t inTargetSz);
    Status ResetFilesize(FileIdx inFileIdx, off_t inTargetSz);
    Status UnReserveFileSpace(FileIdx inFileIdx, off_t inStartOffset, 
        off_t inLen);

private:
    class Queue;
    class RequestWaiter;
    Queue* mQueuePtr;

private:
    QCDiskQueue(
        QCDiskQueue& inQueue);
    QCDiskQueue& operator=(
        const QCDiskQueue& inQueue);
};

#endif /* QCDISKQUEUE_H */
