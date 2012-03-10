//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: KfsProtocolWorker.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2009/10/10
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

#include "KfsProtocolWorker.h"

#include <algorithm>
#include <map>
#include <string>
#include <sstream>
#include <cerrno>

#include <boost/pool/pool_alloc.hpp>
#include <boost/random/mersenne_twister.hpp>
#include "boost/date_time/posix_time/posix_time.hpp" 

#include "libkfsIO/IOBuffer.h"
#include "libkfsIO/NetConnection.h"
#include "libkfsIO/NetManager.h"
#include "libkfsIO/ITimeout.h"
#include "common/kfstypes.h"
#include "common/kfsdecls.h"
#include "qcdio/qcutils.h"
#include "qcdio/qcthread.h"
#include "qcdio/qcmutex.h"
#include "qcdio/qcutils.h"
#include "qcdio/qcstutils.h"
#include "qcdio/qcdllist.h"
#include "qcdio/qcdebug.h"

#include "WriteAppender.h"

namespace KFS
{

using std::string;
using std::make_pair;
using std::ostringstream;

class KfsProtocolWorker::Impl :
    public QCRunnable,
    public ITimeout
{
public:
    typedef WriteAppender::MetaServer MetaServer;
    typedef QCDLList<Request, 0>      WorkQueue;

    Impl(
        string      inMetaHost                    ,
        int         inMetaPort                    ,
        int         inMetaMaxRetryCount           ,
        int         inMetaTimeSecBetweenRetries   ,
        int         inMetaOpTimeoutSec            ,
        int         inMetaIdleTimeoutSec          ,
        int64_t     inMetaInitialSeqNum           ,
        const char* inMetaLogPrefixPtr            ,
        int         inMaxRetryCount               ,
        int         inWriteThreshold              ,
        int         inTimeSecBetweenRetries       ,
        int         inDefaultSpaceReservationSize ,
        int         inPreferredAppendSize         ,
        int         inOpTimeoutSec                ,
        int         inIdleTimeoutSec              ,
        const char* inLogPrefixPtr                ,
        int64_t     inChunkServerInitialSeqNum    ,
        bool        inPreAllocateFlag             )
        : QCRunnable(),
          ITimeout(),
          mNetManager(),
          mMetaServer(
            mNetManager,
            inMetaHost,
            inMetaPort,
            inMetaMaxRetryCount,
            inMetaTimeSecBetweenRetries,
            inMetaOpTimeoutSec,
            inMetaIdleTimeoutSec,
            inMetaInitialSeqNum > 0 ? inMetaInitialSeqNum : GetInitalSeqNum(),
            inMetaLogPrefixPtr ? inMetaLogPrefixPtr : "PWM"
          ),
          mAppenders(),
          mMaxRetryCount(inMaxRetryCount),
          mWriteThreshold(inWriteThreshold),
          mTimeSecBetweenRetries(inTimeSecBetweenRetries),
          mDefaultSpaceReservationSize(inDefaultSpaceReservationSize),
          mPreferredAppendSize(inPreferredAppendSize),
          mOpTimeoutSec(inOpTimeoutSec),
          mIdleTimeoutSec(inIdleTimeoutSec),
          mLogPrefixPtr(inLogPrefixPtr ? inLogPrefixPtr : "PW"),
          mPreAllocateFlag(inPreAllocateFlag),
          mChunkServerInitialSeqNum(
            inChunkServerInitialSeqNum > 0 ? inChunkServerInitialSeqNum :
                GetInitalSeqNum(0x19885a10)),
          mDoNotDeallocate(),
          mStopRequest(),
          mWorker(this, "KfsProtocolWorker"),
          mMutex()
    {
        WorkQueue::Init(mWorkQueue);
        FreeSyncRequests::Init(mFreeSyncRequests);
        CleanupList::Init(mCleanupList);
    }
    virtual ~Impl()
        { Impl::Stop(); }
    virtual void Run()
    {
        mNetManager.RegisterTimeoutHandler(this);
        mNetManager.MainLoop();
        mNetManager.UnRegisterTimeoutHandler(this);
    }
    void Start()
    {
        if (mWorker.IsStarted()) {
            return;
        }
        mStopRequest.mState = Request::kStateNone;
        const int kStackSize = 64 << 10;
        mWorker.Start(this, kStackSize);
    }
    void Stop()
    {
        {
            QCStMutexLocker lock(mMutex);
            if (mStopRequest.mState == Request::kStateNone) {
                Enqueue(mStopRequest);
            }
        }
        mWorker.Join();
        {
            QCStMutexLocker lock(mMutex);
            QCRTASSERT(
                mAppenders.empty() &&
                CleanupList::IsEmpty(mCleanupList)
            );
            SyncRequest* theReqPtr;
            while ((theReqPtr =
                    FreeSyncRequests::PopFront(mFreeSyncRequests))) {
                delete theReqPtr;
            }
        }
    }
    virtual void Timeout()
    {
        Request* theWorkQueue[1];
        {
            QCStMutexLocker theLock(mMutex);
            theWorkQueue[0] = mWorkQueue[0];
            WorkQueue::Init(mWorkQueue);
        }
        bool theShutdownFlag = false;
        Request* theReqPtr;
        while ((theReqPtr = WorkQueue::PopFront(theWorkQueue))) {
            Request& theReq = *theReqPtr;
            QCRTASSERT(theReq.mState == Request::kStateInFlight);
            theShutdownFlag = theShutdownFlag || &mStopRequest == theReqPtr;
            if (theShutdownFlag) {
                Done(theReq, kErrShutdown);
                continue;
            }
            AppenderKey const theKey(theReq.mFileInstance, theReq.mFileId);
            Appenders::iterator theIt;
            if (IsAppend(theReq)) {
                theIt = mAppenders.insert(
                    make_pair(theKey, (Appender*)0)).first;
                if (! theIt->second && ! NewAppender(theReq, theIt)) {
                    continue;
                }
            } else {
                theIt = mAppenders.find(theKey);
                if (theIt == mAppenders.end()) {
                    Done(
                        theReq,
                        theReq.mRequestType == kRequestTypeWriteAppendShutdown ?
                            kErrNone : kErrProtocol
                    );
                    continue;
                }
            }
            QCRTASSERT(
                IsAppend(theReq) ||
                theReq.mRequestType == kRequestTypeWriteAppendClose ||
                theReq.mRequestType == kRequestTypeWriteAppendShutdown ||
                theReq.mRequestType == kRequestTypeWriteAppendSetWriteThreshold
            );
            theIt->second->Process(theReq);
        }
        while (! CleanupList::IsEmpty(mCleanupList)) {
            delete CleanupList::Front(mCleanupList);
        }
        if (theShutdownFlag) {
            Appenders theAppenders;
            theAppenders.swap(mAppenders);
            for (Appenders::iterator theIt = theAppenders.begin();
                    theIt != theAppenders.end();
                    ) {
                delete theIt++->second;
            }
            QCASSERT(mAppenders.empty());
            mNetManager.Shutdown();
        }
    }
    int Execute(
        RequestType  inRequestType,
        FileInstance inFileInstance,
        FileId       inFileId,
        string       inPathName,
        void*        inBufferPtr,
        int          inSize,
        int          inMaxPending)
    {
        if (inRequestType == kRequestTypeWriteAppendAsync) {
            Enqueue(AsyncRequest::Create(
                inRequestType,
                inFileInstance,
                inFileId,
                inPathName,
                inBufferPtr,
                inSize
            ));
            return 0;
        } else {
            SyncRequest& theReq = GetSyncRequest(
                inRequestType,
                inFileInstance,
                inFileId,
                inPathName,
                inBufferPtr,
                inSize,
                inMaxPending
            );
            const int theRet = theReq.Execute(*this);
            PutSyncRequest(theReq);
            return theRet;
        }
    }
    void Enqueue(
        Request& inRequest)
    {
        if (! IsValid(inRequest)) {
            inRequest.mState = Request::kStateDone;
            inRequest.Done(kErrParameters);
            return;
        }
        if (inRequest.mRequestType == kRequestTypeWriteAppendAsync &&
                inRequest.mSize <= 0) {
            inRequest.mState = Request::kStateDone;
            inRequest.Done(kErrNone);
            return;
        }
        {
            QCStMutexLocker theLock(mMutex);
            if (mStopRequest.mState != Request::kStateNone) {
                theLock.Unlock();
                Done(inRequest, kErrShutdown);
                return;
            }
            QCRTASSERT(inRequest.mState != Request::kStateInFlight);
            inRequest.mState = Request::kStateInFlight;
            WorkQueue::PushBack(mWorkQueue, inRequest);
        }
        mNetManager.Wakeup();
    }
    static bool IsAppend(
        const Request& inRequest)
    {
        switch (inRequest.mRequestType) {
            case kRequestTypeWriteAppend:
            case kRequestTypeWriteAppendThrottle:
                return true;
            case kRequestTypeWriteAppendClose:
                return (inRequest.mSize > 0);
            case kRequestTypeWriteAppendAsync:
                return (inRequest.mSize > 0 ||
                    inRequest.mMaxPendingOrEndPos >= 0);
            default:
                break;
        }
        return false;
    }
    static bool IsValid(
        const Request& inRequest)
    {
        if (inRequest.mFileId <= 0 || inRequest.mPathName.empty()) {
            return false;
        }
        switch (inRequest.mRequestType) {
            case kRequestTypeWriteAppend:
            case kRequestTypeWriteAppendClose:
            case kRequestTypeWriteAppendAsync:
            case kRequestTypeWriteAppendThrottle:
                return (inRequest.mBufferPtr || inRequest.mSize <= 0);
            case kRequestTypeWriteAppendShutdown:
                return (inRequest.mSize == 0);
            case kRequestTypeWriteAppendSetWriteThreshold:
                return true;
            default:
                break;
        }
        return false;
    }
private:
    class StopRequest : public Request
    {
    public:
        StopRequest()
            : Request(kRequestTypeWriteAppendClose, 0, 1, "stop")
            {}
        virtual void Done(
            int Status)
            {}
    };
    class AsyncRequest : public Request
    {
    public:
        static AsyncRequest& Create(
            RequestType  inRequestType,
            FileInstance inFileInstance,
            FileId       inFileId,
            string       inPathName,
            void*        inBufferPtr,
            int          inSize)
        {
            char* const theAllocPtr = new char[
                sizeof(AsyncRequest) + std::max(0, inSize)];
            AsyncRequest* theRetPtr = new(theAllocPtr) AsyncRequest(
                inRequestType,
                inFileInstance,
                inFileId,
                inPathName,
                inSize > 0 ? memcpy(theAllocPtr + sizeof(AsyncRequest),
                    inBufferPtr, inSize) : 0,
                inSize
            );
            QCASSERT(reinterpret_cast<char*>(theRetPtr) == theAllocPtr);
            return *theRetPtr;
        }
        virtual void Done(
            int Status)
        {
            this->~AsyncRequest();
            delete [] reinterpret_cast<char*>(this);
        }
    private:
        AsyncRequest(
            RequestType  inRequestType,
            FileInstance inFileInstance,
            FileId       inFileId,
            string       inPathName,
            void*        inBufferPtr,
            int          inSize)
            : Request(
                inRequestType,
                inFileInstance,
                inFileId,
                inPathName,
                inBufferPtr,
                inSize)
            {}
        virtual ~AsyncRequest()
            {}
    private:
        AsyncRequest(
            const AsyncRequest& inReq);
        AsyncRequest& operator=(
            const AsyncRequest& inReq);
    };
    class SyncRequest : public Request
    {
    public:
        SyncRequest(
            RequestType  inRequestType  = kRequestTypeUnknown,
            FileInstance inFileInstance = 0,
            FileId       inFileId       = -1,
            string       inPathName     = string(),
            void*        inBufferPtr    = 0,
            int          inSize         = 0,
            int          inMaxPending   = -1)
            : Request(
                inRequestType,
                inFileInstance,
                inFileId,
                inPathName,
                inBufferPtr,
                inSize,
                inMaxPending),
              mMutex(),
              mCond(),
              mRetStatus(0),
              mWaitingFlag(false)
            { FreeSyncRequests::Init(*this); }
        SyncRequest& Reset(
            RequestType  inRequestType  = kRequestTypeUnknown,
            FileInstance inFileInstance = 0,
            FileId       inFileId       = -1,
            string       inPathName     = string(),
            void*        inBufferPtr    = 0,
            int          inSize         = 0,
            int          inMaxPending   = -1)
        {
            QCRTASSERT(! mWaitingFlag);
            Request::Reset(
                inRequestType,
                inFileInstance,
                inFileId,
                inPathName,
                inBufferPtr,
                inSize,
                inMaxPending
            );
            mRetStatus   = 0;
            mWaitingFlag = 0;
            return *this;
        }
        virtual ~SyncRequest()
            { QCRTASSERT(! mWaitingFlag); }
        virtual void Done(
            int inStatus)
        {
            QCStMutexLocker theLock(mMutex);
            mRetStatus   = inStatus;
            mWaitingFlag = false;
            mCond.Notify();
        }
        int Execute(
            Impl& inWorker)
        {
            mWaitingFlag = true;
            inWorker.Enqueue(*this);
            QCStMutexLocker theLock(mMutex);
            while (mWaitingFlag && mCond.Wait(mMutex))
                {}
            return mRetStatus;
        }
    private:
        QCMutex      mMutex;
        QCCondVar    mCond;
        int          mRetStatus;
        bool         mWaitingFlag;
        SyncRequest* mPrevPtr[1];
        SyncRequest* mNextPtr[1];
        friend class QCDLListOp<SyncRequest, 0>;
    private:
        SyncRequest(
            const SyncRequest& inReq);
        SyncRequest& operator=(
            const SyncRequest& inReq);
    };
    typedef QCDLList<SyncRequest, 0> FreeSyncRequests;
    class DoNotDeallocate : public libkfsio::IOBufferAllocator
    {
    public:
        DoNotDeallocate()
            : mCurBufSize(0)
            {}
        virtual size_t GetBufferSize() const
            { return mCurBufSize; }
        virtual char* Allocate()
        {
            QCRTASSERT(! "unexpected invocation");
            return 0;
        }
        virtual void Deallocate(
            char* /* inBufferPtr */)
            {}
        libkfsio::IOBufferAllocator& Get(
            size_t inBufSize)
        {
            mCurBufSize = inBufSize;
            return *this;
        }
    private:
        size_t mCurBufSize;
    };
    class Appender;
    typedef QCDLList<Appender, 0> CleanupList;
    typedef std::pair<FileInstance, FileId> AppenderKey;
    typedef std::map<AppenderKey, Appender*, std::less<AppenderKey>,
        boost::fast_pool_allocator<std::pair<const AppenderKey, Appender*> >
    > Appenders;
    class Appender : public WriteAppender::Completion
    {
        enum { kNoBufferCompaction = -1 };
    public:
        typedef KfsProtocolWorker::Impl Owner;
        Appender(
            Owner&              inOwner,
            const char*         inLogPrefixPtr,
            Appenders::iterator inAppendersIt)
            : WriteAppender::Completion(),
              mWAppender(
                inOwner.mMetaServer,
                this,
                inLogPrefixPtr,
                inOwner.mMaxRetryCount,
                inOwner.mWriteThreshold,
                inOwner.mTimeSecBetweenRetries,
                inOwner.mDefaultSpaceReservationSize,
                inOwner.mPreferredAppendSize,
                kNoBufferCompaction, // no buffer compaction
                inOwner.mOpTimeoutSec,
                inOwner.mIdleTimeoutSec,
                inOwner.mChunkServerInitialSeqNum,
                inOwner.mPreAllocateFlag
              ),
              mOwner(inOwner),
              mWriteThreshold(inOwner.mWriteThreshold),
              mPending(0),
              mCurPos(0),
              mDonePos(0),
              mLastSyncReqPtr(0),
              mCloseReqPtr(0),
              mAppendersIt(inAppendersIt)
        {
            WorkQueue::Init(mWorkQueue);
            CleanupList::Init(*this);
        }
        ~Appender()
        {
            Appender::Shutdown();
            QCRTASSERT(WorkQueue::IsEmpty(mWorkQueue));
            CleanupList::Remove(mOwner.mCleanupList, *this);
            mWAppender.Unregister(this);
        }
        int Open(
            FileId inFileId,
            string inPathName)
            { return mWAppender.Open(inFileId, inPathName.c_str()); }
        virtual void Done(
            WriteAppender& inAppender,
            int            inStatusCode)
        {
            const int theRem = inStatusCode == 0 ?
                mWAppender.GetPendingSize() : 0;
            QCRTASSERT(&inAppender == &mWAppender && theRem <= mPending);
            const int theDone = mPending - theRem;
            mPending = theRem;
            Done(theDone, inStatusCode);
        }
        void Process(
            Request& inRequest)
        {
            const bool theShutdownFlag =
                inRequest.mRequestType == kRequestTypeWriteAppendShutdown;
            if (theShutdownFlag) {
                mWAppender.Shutdown();
            } else if (inRequest.mRequestType ==
                    kRequestTypeWriteAppendSetWriteThreshold) {
                mWriteThreshold = inRequest.mSize;
                mOwner.Done(inRequest, mWAppender.SetWriteThreshold(
                    mLastSyncReqPtr != 0 ? 0 : mWriteThreshold));
            } else {
                QCRTASSERT(
                    (IsAppend(inRequest) ||
                    inRequest.mRequestType == kRequestTypeWriteAppendClose
                    ) &&
                    inRequest.mState == Request::kStateInFlight &&
                    (inRequest.mBufferPtr || inRequest.mSize <= 0)
                );
                const bool theCloseFlag =
                    inRequest.mRequestType == kRequestTypeWriteAppendClose;
                const bool theFlushFlag =
                    ! theCloseFlag &&
                    inRequest.mRequestType != kRequestTypeWriteAppendAsync &&
                    (inRequest.mRequestType != kRequestTypeWriteAppendThrottle ||
                        (inRequest.mMaxPendingOrEndPos >= 0 &&
                        inRequest.mMaxPendingOrEndPos <
                            mPending + std::max(0, inRequest.mSize)));
                if (theFlushFlag) {
                    mLastSyncReqPtr = &inRequest;
                }
                if (inRequest.mSize <= 0) {
                    inRequest.mMaxPendingOrEndPos = mCurPos;
                    if (theCloseFlag) {
                        if (mCloseReqPtr || ! mWAppender.IsOpen()) {
                            if (&inRequest != mCloseReqPtr) {
                                mOwner.Done(inRequest, kErrProtocol);
                            }
                        } else {
                            mCloseReqPtr = &inRequest;
                            const int theStatus = mWAppender.Close();
                            if (theStatus != 0 && &inRequest == mCloseReqPtr) {
                                mCloseReqPtr = 0;
                                mOwner.Done(inRequest, theStatus);
                            }
                        }
                    }
                    if (theFlushFlag) {
                        if (mPending > 0) {
                            WorkQueue::PushBack(mWorkQueue, inRequest);
                        } else {
                            mLastSyncReqPtr = 0;
                            // SetWriteThreshold() should be effectively a
                            // no op, it is here to get status.
                            mOwner.Done(inRequest,
                                mWAppender.SetWriteThreshold(mWriteThreshold));
                        }
                    } else if (inRequest.mRequestType ==
                            kRequestTypeWriteAppendThrottle) {
                        const int theStatus = mWAppender.GetErrorCode();
                        mOwner.Done(inRequest,
                            theStatus == 0 ? mPending : theStatus);
                    }
                } else {
                    IOBuffer theBuf;
                    const bool theAsyncThrottle = ! theFlushFlag &&
                        inRequest.mRequestType ==
                            kRequestTypeWriteAppendThrottle;
                    if (theAsyncThrottle) {
                        theBuf.CopyIn(
                            reinterpret_cast<char*>(inRequest.mBufferPtr),
                            inRequest.mSize
                        );
                    } else {
                        theBuf.Append(IOBufferData(
                            reinterpret_cast<char*>(inRequest.mBufferPtr),
                            0,
                            inRequest.mSize,
                            mOwner.mDoNotDeallocate.Get(inRequest.mSize)
                        ));
                        WorkQueue::PushBack(mWorkQueue, inRequest);
                    }
                    mCurPos += inRequest.mSize;
                    inRequest.mMaxPendingOrEndPos = mCurPos;
                    const int theStatus =
                        mWAppender.Append(theBuf, inRequest.mSize);
                    if (theStatus <= 0) {
                        if (! theAsyncThrottle) {
                            WorkQueue::Remove(mWorkQueue, inRequest);
                        }
                        mOwner.Done(inRequest, theStatus);
                    } else {
                        QCRTASSERT(theStatus == inRequest.mSize);
                        mPending = mWAppender.GetPendingSize();
                        if (theAsyncThrottle) {
                            // Tell the caller the pending size.
                            mOwner.Done(inRequest, mPending);
                        }
                    }
                    if (theCloseFlag) {
                        mWAppender.Close();
                    }
                }
                if (theFlushFlag && mLastSyncReqPtr && mWriteThreshold > 0) {
                    mWAppender.SetWriteThreshold(0);
                }
            }
            if (! mWAppender.IsActive()) {
                const int thePending = mPending;
                mPending = 0;
                Done(thePending, kErrShutdown);
            }
            if (theShutdownFlag) {
                mOwner.Done(inRequest, kErrNone);
            }
        }
        void Done(
            int inDone,
            int inStatus)
        {
            QCRTASSERT(inDone >= 0 && mDonePos + inDone <= mCurPos);
            mDonePos += inDone;
            const bool theHadSynRequestFlag = mLastSyncReqPtr != 0;
            Request* theReqPtr;
            while ((theReqPtr = WorkQueue::Front(mWorkQueue)) &&
                    theReqPtr->mMaxPendingOrEndPos <= mDonePos) {
                Request& theReq = *theReqPtr;
                QCRTASSERT(theReq.mMaxPendingOrEndPos >= 0);
                if (&theReq == mLastSyncReqPtr) {
                    mLastSyncReqPtr = 0;
                }
                WorkQueue::PopFront(mWorkQueue);
                mOwner.Done(theReq, inStatus);
            }
            if (mCloseReqPtr &&
                    WorkQueue::IsEmpty(mWorkQueue) && ! mWAppender.IsActive()) {
                Request& theReq = *mCloseReqPtr;
                mCloseReqPtr = 0;
                // Schedule delete.
                mOwner.mAppenders.erase(mAppendersIt);
                CleanupList::PushBack(mOwner.mCleanupList, *this);
                mOwner.Done(theReq, inStatus);
                return;
            }
            if (theHadSynRequestFlag && mWriteThreshold > 0 &&
                    ! mLastSyncReqPtr) {
                mWAppender.SetWriteThreshold(mWriteThreshold);
            }
        }
        void Shutdown()
        {
            mWAppender.Shutdown();
            QCASSERT(! mWAppender.IsActive());
            const int thePending = mPending;
            mPending = 0;
            Done(thePending, kErrShutdown);
        }
    private:
        WriteAppender       mWAppender;
        Owner&              mOwner;
        int                 mWriteThreshold;
        int                 mPending;
        int64_t             mCurPos;
        int64_t             mDonePos;
        Request*            mLastSyncReqPtr;
        Request*            mCloseReqPtr;
        Request*            mWorkQueue[1];
        Appender*           mPrevPtr[1];
        Appender*           mNextPtr[1];
        Appenders::iterator mAppendersIt;
        friend class QCDLListOp<Appender, 0>;
    };
    friend class Appender;

    NetManager        mNetManager;
    MetaServer        mMetaServer;
    Appenders         mAppenders;
    const int         mMaxRetryCount;
    const int         mWriteThreshold;
    const int         mTimeSecBetweenRetries;
    const int         mDefaultSpaceReservationSize;
    const int         mPreferredAppendSize;
    const int         mOpTimeoutSec;
    const int         mIdleTimeoutSec;
    const char* const mLogPrefixPtr;
    const bool        mPreAllocateFlag;
    int64_t           mChunkServerInitialSeqNum;
    DoNotDeallocate   mDoNotDeallocate;
    StopRequest       mStopRequest;
    QCThread          mWorker;
    QCMutex           mMutex;
    Request*          mWorkQueue[1];
    SyncRequest*      mFreeSyncRequests[1];
    Appender*         mCleanupList[1];

    void Done(
        Request& inRequest,
        int      inStatus)
    {
        if (inRequest.mState == Request::kStateDone) {
            return;
        }
        QCRTASSERT(inRequest.mState == Request::kStateInFlight);
        inRequest.mState  = Request::kStateDone;
        inRequest.mStatus = inStatus;
        inRequest.Done(inStatus);
    }
    void Done(
        Request& inRequest)
        { Done(inRequest, inRequest.mStatus); }
    bool NewAppender(
        Request&             inRequest,
        Appenders::iterator& inAppendersIt)
    {
        string theName;
        size_t thePos = inRequest.mPathName.find_last_of('/');
        if (thePos != string::npos &&
                ++thePos < inRequest.mPathName.length()) {
            theName = inRequest.mPathName.substr(thePos);
        }
        ostringstream theStream;
        theStream <<
            mLogPrefixPtr <<
            " " << inRequest.mFileId <<
            "," << inRequest.mFileInstance <<
            "," << theName
        ;
        Appender* const    theRetPtr    = new Appender(
            *this, theStream.str().c_str(), inAppendersIt);
        const int          theStatus    = theRetPtr->Open(
            inRequest.mFileId, inRequest.mPathName);
        if (theStatus != kErrNone) {
            mAppenders.erase(inAppendersIt);
            delete theRetPtr;
            Done(inRequest, theStatus);
            return false;
        }
        inAppendersIt->second = theRetPtr;
        mChunkServerInitialSeqNum += 100000;
        return true;
    }
    SyncRequest& GetSyncRequest(
        RequestType  inRequestType,
        FileInstance inFileInstance,
        FileId       inFileId,
        string       inPathName,
        void*        inBufferPtr,
        int          inSize,
        int          inMaxPending)
    {
        QCStMutexLocker lock(mMutex);
        SyncRequest* theReqPtr = FreeSyncRequests::PopFront(mFreeSyncRequests);
        return (theReqPtr ? theReqPtr->Reset(
            inRequestType,
            inFileInstance,
            inFileId,
            inPathName,
            inBufferPtr,
            inSize,
            inMaxPending
        ) : *(new SyncRequest(
            inRequestType,
            inFileInstance,
            inFileId,
            inPathName,
            inBufferPtr,
            inSize,
            inMaxPending
        )));
    }
    void PutSyncRequest(
        SyncRequest& inRequest)
    {
        QCStMutexLocker lock(mMutex);
        FreeSyncRequests::PushFront(mFreeSyncRequests, inRequest);
    }
    static int64_t GetInitalSeqNum(
        int64_t inSeed = 0)
    {
        boost::mt19937 sRandom(boost::mt19937::result_type(
            (boost::posix_time::microsec_clock::universal_time() -
            boost::posix_time::ptime(
                boost::gregorian::date(1970, boost::date_time::Jan, 1)
            )).ticks() + inSeed
        ));
        const int64_t theRet(
            (int64_t)sRandom() | ((int64_t)sRandom() << 32));
        return ((theRet < 0 ? -theRet : theRet) >> 1);
    }
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

KfsProtocolWorker::Request::Request(
    KfsProtocolWorker::RequestType  inRequestType  /* = kRequestTypeUnknown */,
    KfsProtocolWorker::FileInstance inFileInstance /* = 0 */,
    KfsProtocolWorker::FileId       inFileId       /* = -1 */,
    string inPathName                              /* = string() */,
    void*       inBufferPtr                        /* = 0 */,
    int         inSize                             /* = 0 */,
    int         inMaxPending                       /* = -1 */)
    : mRequestType(inRequestType),
      mFileInstance(inFileInstance),
      mFileId(inFileId),
      mPathName(inPathName),
      mBufferPtr(inBufferPtr),
      mSize(inSize),
      mState(KfsProtocolWorker::Request::kStateNone),
      mStatus(0),
      mMaxPendingOrEndPos(inMaxPending)
{
    KfsProtocolWorker::Impl::WorkQueue::Init(*this);
}

    void
KfsProtocolWorker::Request::Reset(
    KfsProtocolWorker::RequestType  inRequestType  /* = kRequestTypeUnknown */,
    KfsProtocolWorker::FileInstance inFileInstance /* = 0 */,
    KfsProtocolWorker::FileId       inFileId       /* = -1 */,
    string inPathName                              /* = string() */,
    void*       inBufferPtr                        /* = 0 */,
    int         inSize                             /* = 0 */,
    int         inMaxPending                       /* = -1 */)
{
    mRequestType        = inRequestType;
    mFileInstance       = inFileInstance;
    mFileId             = inFileId;
    mPathName           = inPathName;
    mBufferPtr          = inBufferPtr;
    mSize               = inSize;
    mMaxPendingOrEndPos = inMaxPending;
    mState              = KfsProtocolWorker::Request::kStateNone;
    mStatus             = 0;
}

    /* virtual */
KfsProtocolWorker::Request::~Request()
{
    QCRTASSERT(mState != kStateInFlight);
}

KfsProtocolWorker::KfsProtocolWorker(
        string     inMetaHost,
        int         inMetaPort,
        int         inMetaMaxRetryCount           /* = 3 */,
        int         inMetaTimeSecBetweenRetries   /* = 10 */,
        int         inMetaOpTimeoutSec            /* = 3 * 60 */,
        int         inMetaIdleTimeoutSec          /* = 5 * 60 */,
        int64_t     inMetaInitialSeqNum           /* = 0 */,
        const char* inMetaLogPrefixPtr            /* = 0 */,
        int         inMaxRetryCount               /* = 10 */,
        int         inWriteThreshold              /* = KFS::CHECKSUM_BLOCKSIZE */,
        int         inTimeSecBetweenRetries       /* = 15 */,
        int         inDefaultSpaceReservationSize /* = 1 << 20 */,
        int         inPreferredAppendSize         /* = KFS::CHECKSUM_BLOCKSIZE */,
        int         inOpTimeoutSec                /* = 120 */,
        int         inIdleTimeoutSec              /* = 5 * 30 */,
        const char* inLogPrefixPtr                /* = 0 */,
        int64_t     inChunkServerInitialSeqNum    /* = 0 */,
        bool        inPreAllocateFlag             /* = false */)
    : mImpl(*(new Impl(
        inMetaHost                    ,
        inMetaPort                    ,
        inMetaMaxRetryCount           ,
        inMetaTimeSecBetweenRetries   ,
        inMetaOpTimeoutSec            ,
        inMetaIdleTimeoutSec          ,
        inMetaInitialSeqNum           ,
        inMetaLogPrefixPtr            ,
        inMaxRetryCount               ,
        inWriteThreshold              ,
        inTimeSecBetweenRetries       ,
        inDefaultSpaceReservationSize ,
        inPreferredAppendSize         ,
        inOpTimeoutSec                ,
        inIdleTimeoutSec              ,
        inLogPrefixPtr                ,
        inChunkServerInitialSeqNum    ,
        inPreAllocateFlag
    )))
{
}

KfsProtocolWorker::~KfsProtocolWorker()
{
    delete &mImpl;
}

    void
KfsProtocolWorker::Start()
{
    mImpl.Start();
}

    void
KfsProtocolWorker::Stop()
{
    mImpl.Stop();
}

    int
KfsProtocolWorker::Execute(
    RequestType  inRequestType,
    FileInstance inFileInstance,
    FileId       inFileId,
    string       inPathName,
    void*        inBufferPtr,
    int          inSize,
    int          inMaxPending)
{
    return mImpl.Execute(inRequestType, inFileInstance,
        inFileId, inPathName, inBufferPtr, inSize, inMaxPending);
}

    void
KfsProtocolWorker::Enqueue(
    Request& inRequest)
{
    mImpl.Enqueue(inRequest);
}

} /* namespace KFS */
