//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: KfsNetClient.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2009/05/20
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

#include "KfsNetClient.h"

#include <sstream>
#include <algorithm>
#include <map>
#include <deque>
#include <string>
#include <cerrno>

#include <stdint.h>
#include <time.h>

#include <boost/pool/pool_alloc.hpp>

#include "libkfsIO/IOBuffer.h"
#include "libkfsIO/NetConnection.h"
#include "libkfsIO/NetManager.h"
#include "libkfsIO/ITimeout.h"
#include "common/kfstypes.h"
#include "common/kfsdecls.h"
#include "common/log.h"
#include "qcdio/qcutils.h"
#include "qcdio/qcdllist.h"
#include "KfsOps.h"
#include "Utils.h"

namespace KFS {

class KfsNetClient::Impl : public KfsCallbackObj, private ITimeout
{
public:
    Impl(
        std::string inHost,
        int         inPort,
        int         inMaxRetryCount,
        int         inTimeSecBetweenRetries,
        int         inOpTimeoutSec,
        int         inIdleTimeoutSec,
        kfsSeq_t    inInitialSeqNum,
        const char* inLogPrefixPtr,
        NetManager& inNetManager)
        : KfsCallbackObj(),
          ITimeout(),
          mServerLocation(inHost, inPort),
          mPendingOpQueue(),
          mQueueStack(),
          mConnPtr(),
          mNextSeqNum(
            (inInitialSeqNum < 0 ? -inInitialSeqNum : inInitialSeqNum) >> 1),
          mReadHeaderDoneFlag(false),
          mSleepingFlag(false),
          mDataReceivedFlag(false),
          mDataSentFlag(false),
          mAllDataSentFlag(false),
          mRetryConnectOnlyFlag(false),
          mIdleTimeoutFlag(false),
          mTimeSecBetweenRetries(inTimeSecBetweenRetries),
          mOpTimeoutSec(inOpTimeoutSec),
          mIdleTimeoutSec(inIdleTimeoutSec),
          mRetryCount(0),
          mContentLength(0),
          mMaxRetryCount(inMaxRetryCount),
          mProperties(),
          mStats(),
          mEventObserverPtr(0),
          mLogPrefix((inLogPrefixPtr && inLogPrefixPtr[0]) ?
                (inLogPrefixPtr + std::string(" ")) : std::string()),
          mNetManager(inNetManager)
    {
        SET_HANDLER(this, &KfsNetClient::Impl::EventHandler);
    }
    ~Impl()
    {
        if (mConnPtr) {
            mConnPtr->Close();
        }
        if (mSleepingFlag) {
            mNetManager.UnRegisterTimeoutHandler(this);
        }
    }
    bool IsConnected() const
        { return (mConnPtr && mConnPtr->IsGood()); }

    bool Start() 
    {
        EnsureConnected();
        return (mSleepingFlag || IsConnected());
    }
    bool Start(
        std::string  inServerName,
        int          inServerPort,
        std::string* inErrMsgPtr,
        bool         inRetryPendingOpsFlag,
        int          inMaxRetryCount,
        int          inTimeSecBetweenRetries,
        bool         inRetryConnectOnlyFlag)
    {
        if (! inRetryPendingOpsFlag) {
            Cancel();
        }
        mRetryConnectOnlyFlag  = inRetryConnectOnlyFlag;
        mMaxRetryCount         = inMaxRetryCount;
        mTimeSecBetweenRetries = inTimeSecBetweenRetries;
        return SetServer(ServerLocation(inServerName, inServerPort), false);
    }
    bool SetServer(
        const ServerLocation& inLocation,
        bool                  inCancelPendingOpsFlag = true)
    {
        if (inLocation == mServerLocation) {
            EnsureConnected();
            return (mSleepingFlag || IsConnected());
        }
        if (inCancelPendingOpsFlag) {
            Cancel();
        }
        if (mSleepingFlag || IsConnected()) {
            Reset();
        }
        mServerLocation = inLocation;
        mRetryCount = 0;
        mNextSeqNum += 100;
        EnsureConnected();
        return (mSleepingFlag || IsConnected());
    }
    void Stop()
    {
        if (mSleepingFlag || IsConnected()) {
            Reset();
        }
        mConnPtr.reset();
        Cancel();
    }
    int GetMaxRetryCount() const
        { return mMaxRetryCount; }
    void SetMaxRetryCount(
        int inMaxRetryCount)
        { mMaxRetryCount = inMaxRetryCount; }
    int GetOpTimeoutSec() const
        { return mOpTimeoutSec; }
    void SetOpTimeoutSec(
        int inTimeout)
    {
        mOpTimeoutSec = inTimeout;
        if (IsConnected() && ! mPendingOpQueue.empty()) {
            mConnPtr->SetInactivityTimeout(mOpTimeoutSec);
        }
    }
    int GetIdleTimeoutSec() const
        { return mIdleTimeoutSec; }
    void SetIdleTimeoutSec(
        int inTimeout)
    {
        mIdleTimeoutSec = inTimeout;
        if (IsConnected() && mPendingOpQueue.empty()) {
            mConnPtr->SetInactivityTimeout(mIdleTimeoutSec);
        }
    }
    int GetTimeSecBetweenRetries() const
        { return mTimeSecBetweenRetries; }
    void SetTimeSecBetweenRetries(
        int inTimeSec)
        { mTimeSecBetweenRetries = inTimeSec; }
    bool IsAllDataSent() const
        { return (mDataSentFlag && mAllDataSentFlag); }
    bool IsDataReceived() const
        { return mDataReceivedFlag; }
    bool IsDataSent() const
        { return mDataSentFlag; }
    bool IsRetryConnectOnly() const
        { return mRetryConnectOnlyFlag; }
    bool WasDisconnected() const
        { return ((mDataSentFlag || mDataReceivedFlag) && ! IsConnected()); }
    void SetRetryConnectOnly(
        bool inFlag)
        { mRetryConnectOnlyFlag = inFlag; }
    void SetOpTimeout(
        int inOpTimeoutSec)
        { mOpTimeoutSec = inOpTimeoutSec; }
    void GetStats(
        Stats& outStats) const
        { outStats = mStats; }
    std::string GetServerLocation() const
        { return mServerLocation.ToString(); }
    bool Enqueue(
        KfsOp*    inOpPtr,
        OpOwner*  inOwnerPtr,
        IOBuffer* inBufferPtr = 0)
    {
        mStats.mOpsQueuedCount++;
        const bool theOkFlag = EnqueueSelf(inOpPtr, inOwnerPtr, inBufferPtr, 0);
        EnsureConnected();
        return theOkFlag;
    }
    bool Cancel(
        KfsOp*   inOpPtr,
        OpOwner* inOwnerPtr)
    {
        if (! inOpPtr) {
            return true; // Nothing to do.
        }
        OpQueue::iterator theIt = mPendingOpQueue.find(inOpPtr->seq);
        if (theIt == mPendingOpQueue.end()) {
            for (QueueStack::iterator theStIt = mQueueStack.begin();
                    theStIt != mQueueStack.end();
                    ++theStIt) {
                if ((theIt = theStIt->find(inOpPtr->seq)) != theStIt->end()) {
                    if (theIt->second.mOwnerPtr != inOwnerPtr ||
                            theIt->second.mOpPtr != inOpPtr) {
                        return false;
                    }
                    theIt->second.Cancel();
                    break;
                }
            }
            return true;
        }
        if (theIt->second.mOwnerPtr != inOwnerPtr ||
                theIt->second.mOpPtr != inOpPtr) {
            return false;
        }
        Cancel(theIt);
        return true;
    }
    bool Cancel()
    {
        if (mPendingOpQueue.empty()) {
            return false;
        }
        QueueStack::iterator const theIt =
            mQueueStack.insert(mQueueStack.end(), OpQueue());
        OpQueue& theQueue = *theIt;
        theQueue.swap(mPendingOpQueue);
        for (OpQueue::iterator theIt = theQueue.begin();
                theIt != theQueue.end();
                ++theIt) {
            if (! theIt->second.mOpPtr) {
                continue;
            }
            mStats.mOpsCancelledCount++;
            theIt->second.Cancel();
        }
        mQueueStack.erase(theIt);
        return true;
    }
    int EventHandler(
        int   inCode,
        void* inDataPtr)
    {
        if (mEventObserverPtr && mEventObserverPtr->Event(inCode, inDataPtr)) {
            return 0;
        }

        const char* theReasonPtr = "network error";
        switch (inCode) {
            case EVENT_NET_READ: {
                    assert(inDataPtr && mConnPtr);
                    IOBuffer& theBuffer = *reinterpret_cast<IOBuffer*>(inDataPtr);
                    mRetryCount       = 0;
                    mDataReceivedFlag = mDataReceivedFlag || ! theBuffer.IsEmpty();
                    HandleResponse(theBuffer);
                }
	        break;

            case EVENT_NET_WROTE:
                assert(inDataPtr && mConnPtr);
                mRetryCount   = 0;
                mDataSentFlag = true;
	        break;

            case EVENT_INACTIVITY_TIMEOUT:
                if (! mIdleTimeoutFlag &&
                        IsConnected() && mPendingOpQueue.empty()) {
                    mConnPtr->SetInactivityTimeout(mIdleTimeoutSec);
                    mIdleTimeoutFlag = true;
                    break;
                }
    	        theReasonPtr = "inactivity timeout";
                // Fall through.
            case EVENT_NET_ERROR:
                if (mConnPtr) {
                    mAllDataSentFlag = ! mConnPtr->IsWriteReady();
	            KFS_LOG_STREAM(mPendingOpQueue.empty() ?
                            MsgLogger::kLogLevelDEBUG :
                            MsgLogger::kLogLevelERROR) << mLogPrefix <<
                        "closing connection: " << mConnPtr->GetSockName() <<
                        " to: " <<
                        mServerLocation.ToString() <<
                        " due to " << theReasonPtr <<
                        " error: " <<
                            QCUtils::SysError(mConnPtr->GetSocketError()) <<
                        " pending read: " << mConnPtr->GetNumBytesToRead() <<
                        " write: " << mConnPtr->GetNumBytesToWrite() <<
                        " ops: " << mPendingOpQueue.size() <<
                    KFS_LOG_EOM;
                    Reset();
                }
                if (mIdleTimeoutFlag) {
                    mStats.mConnectionIdleTimeoutCount++;
                } else if (mDataSentFlag || mDataReceivedFlag) {
                    mStats.mNetErrorCount++;
                    if (inCode == EVENT_INACTIVITY_TIMEOUT) {
                        mStats.mResponseTimeoutCount++;
                    }
                } else {
                    mStats.mConnectFailureCount++;
                }
                if (! mPendingOpQueue.empty()) {
                    RetryConnect();
                }
	        break;

            default:
	        assert(!"Unknown event");
	        break;
        }
        OpsTimeout();
        return 0;
    }
    void SetEventObserver(
        EventObserver* inEventObserverPtr)
        { mEventObserverPtr = inEventObserverPtr; }
    time_t Now() const
        { return mNetManager.Now(); }
    NetManager& GetNetManager()
        { return mNetManager; }
    const NetManager& GetNetManager() const
        { return mNetManager; }
private:
    struct OpQueueEntry
    {
        OpQueueEntry(
            KfsOp*    inOpPtr     = 0,
            OpOwner*  inOwnerPtr  = 0,
            IOBuffer* inBufferPtr = 0)
            : mOpPtr(inOpPtr),
              mOwnerPtr(inOwnerPtr),
              mBufferPtr(inBufferPtr),
              mTime(0),
              mRetryCount(0)
            {}
        void Cancel()
            { OpDone(true); }
        void Done()
            { OpDone(false); }
        void OpDone(
            bool inCanceledFlag)
        {
            if (mOwnerPtr) {
                if (mOpPtr) {
                    KfsOp* const theOpPtr = mOpPtr;
                    mOpPtr = 0;
                    mOwnerPtr->OpDone(theOpPtr, inCanceledFlag, mBufferPtr);
                }
                mOwnerPtr  = 0;
                mBufferPtr = 0;
            } else {
                delete mOpPtr;
                delete mBufferPtr;
                mBufferPtr = 0;
                mOpPtr     = 0;
            }
        }
        void Clear()
        {
            mOpPtr     = 0;
            mOwnerPtr  = 0;
            mBufferPtr = 0;
        }
        KfsOp*    mOpPtr;
        OpOwner*  mOwnerPtr;
        IOBuffer* mBufferPtr;
        time_t    mTime;
        int       mRetryCount;
    };
    typedef std::map<kfsSeq_t, OpQueueEntry, std::less<kfsSeq_t>,
        boost::fast_pool_allocator<std::pair<const kfsSeq_t, OpQueueEntry> >
    > OpQueue;
    typedef std::list<OpQueue,
        boost::fast_pool_allocator<OpQueue >
    > QueueStack;
    enum { kMaxReadAhead = 4 << 10 };

    ServerLocation    mServerLocation;
    OpQueue           mPendingOpQueue;
    QueueStack        mQueueStack;
    NetConnectionPtr  mConnPtr;
    kfsSeq_t          mNextSeqNum;
    bool              mReadHeaderDoneFlag;
    bool              mSleepingFlag;
    bool              mDataReceivedFlag;
    bool              mDataSentFlag;
    bool              mAllDataSentFlag;
    bool              mRetryConnectOnlyFlag;
    bool              mIdleTimeoutFlag;
    int               mTimeSecBetweenRetries;
    int               mOpTimeoutSec;
    int               mIdleTimeoutSec;
    int               mRetryCount;
    int               mContentLength;
    int               mMaxRetryCount;
    Properties        mProperties;
    Stats             mStats;
    EventObserver*    mEventObserverPtr;
    const std::string mLogPrefix;
    NetManager&       mNetManager;

    bool EnqueueSelf(
        KfsOp*    inOpPtr,
        OpOwner*  inOwnerPtr,
        IOBuffer* inBufferPtr,
        int       inRetryCount)
    {
        if (! inOpPtr) {
            return false;
        }
        mIdleTimeoutFlag = false;
        inOpPtr->seq = mNextSeqNum++;
        std::pair<OpQueue::iterator, bool> const theRes =
            mPendingOpQueue.insert(std::make_pair(
                inOpPtr->seq, OpQueueEntry(inOpPtr, inOwnerPtr, inBufferPtr)
            ));
        if (theRes.second && IsConnected()) {
            IOBuffer::OStream theOutStream;
            inOpPtr->Request(theOutStream);
            mConnPtr->Write(&theOutStream);
            if (inOpPtr->op == CMD_RECORD_APPEND) {
                RecordAppendOp *rop = (RecordAppendOp *) inOpPtr;
                if (rop->keyLength > 0) {
                    mConnPtr->WriteCopy(rop->keyBufferPtr, rop->keyLength);
                }
            }
            if (inOpPtr->contentLength > 0) {
                if (inOpPtr->contentBuf && inOpPtr->contentBufLen > 0) {
                    assert(inOpPtr->contentBufLen >= inOpPtr->contentLength);
                    mConnPtr->Write(
                        inOpPtr->contentBuf, inOpPtr->contentLength);
                } else if (inBufferPtr) {
                    assert(size_t(inBufferPtr->BytesConsumable()) >=
                        inOpPtr->contentLength);
                    mConnPtr->WriteCopy(inBufferPtr, inOpPtr->contentLength);
                }
            }
            // Start the timer.
            mConnPtr->SetInactivityTimeout(mOpTimeoutSec);
            theRes.first->second.mTime       = Now();
            theRes.first->second.mRetryCount = inRetryCount;
        }
        return theRes.second;
    }
    void HandleResponse(
       IOBuffer& inBuffer)
    {
        for (; ;) {
            if (! mReadHeaderDoneFlag && ! ReadHeader(inBuffer)) {
                return;
            }
            if (mContentLength > inBuffer.BytesConsumable()) {
                if (mConnPtr) {
                    mConnPtr->SetMaxReadAhead(std::max(int(kMaxReadAhead),
                        mContentLength - inBuffer.BytesConsumable()));
                }
                return;
            }
            // Get ready for next op.
            if (mConnPtr) {
                mConnPtr->SetMaxReadAhead(kMaxReadAhead);
            }
            mReadHeaderDoneFlag = false;
            kfsSeq_t const theSeq = mProperties.getValue("Cseq", kfsSeq_t(-1));
            OpQueue::iterator const theIt = mPendingOpQueue.find(theSeq);
            if (theIt == mPendingOpQueue.end()) {
                KFS_LOG_STREAM_INFO << mLogPrefix <<
                    "no operation found with seq: " << theSeq <<
                    ", discarding response" <<
                KFS_LOG_EOM;
                inBuffer.Consume(mContentLength);
                mContentLength = 0;
                // Don't rely on compiler to properly handle tail recursion,
                // use for loop instead.
                continue;
            }
            KfsOp& theOp = *theIt->second.mOpPtr;
            theOp.ParseResponseHeader(mProperties);
            mProperties.clear();
            if (mContentLength > 0) {
                if (theIt->second.mBufferPtr) {
                    theIt->second.mBufferPtr->Move(&inBuffer, mContentLength);
                } else {
                    if (theOp.contentBufLen < size_t(mContentLength) ||
                            ! theOp.contentBuf) {
                        delete [] theOp.contentBuf;
                        theOp.contentBuf = new char [mContentLength];
                    }
                    theOp.contentBufLen = mContentLength;
                    inBuffer.CopyOut(theOp.contentBuf, mContentLength);
                }
                mContentLength = 0;
            }
            HandleOp(theIt);
        }
    }
    bool ReadHeader(
        IOBuffer& inBuffer)
    {
        const int theIdx = inBuffer.IndexOf(0, "\r\n\r\n");
        if (theIdx < 0) {
            if (inBuffer.BytesConsumable() > MAX_RPC_HEADER_LEN) {
               KFS_LOG_STREAM_ERROR << mLogPrefix <<
                    "error: " << mServerLocation.ToString() <<
                    ": exceeded max. response header size: " <<
                    MAX_RPC_HEADER_LEN << "; got " <<
                    inBuffer.BytesConsumable() << " resetting connection" <<
                KFS_LOG_EOM;
                Reset();
                RetryAll();
            }
            return false;
        }
        const int theHdrLen = theIdx + 4;
        mProperties.clear();
        {
            IOBuffer::IStream theStream(inBuffer, theHdrLen);
            const char        theSeparator     = ':';
            const bool        theMultiLineFlag = false;
            mProperties.loadProperties(
                theStream, theSeparator, theMultiLineFlag);
        }
        inBuffer.Consume(theHdrLen);
        mReadHeaderDoneFlag = true;
        mContentLength = mProperties.getValue("Content-length", 0);
        return true;
    }
    void EnsureConnected(
        std::string* inErrMsgPtr = 0)
    {
        if (mSleepingFlag || IsConnected()) {
            return;
        }
        mDataReceivedFlag = false;
        mDataSentFlag     = false;
        mAllDataSentFlag  = true;
        mIdleTimeoutFlag  = false;
        mConnPtr.reset();
        mStats.mConnectCount++;
        const bool theNonBlockingFlag = true;
        TcpSocket& theSocket          = *(new TcpSocket());
        const int theErr              = theSocket.Connect(
            mServerLocation, theNonBlockingFlag);
        if (theErr && theErr != -EINPROGRESS) {
            if (inErrMsgPtr) {
                *inErrMsgPtr = QCUtils::SysError(-theErr);
            }
            KFS_LOG_STREAM_ERROR << mLogPrefix <<
                "failed to connect to server " << mServerLocation.ToString() <<
                " : " << QCUtils::SysError(-theErr) <<
            KFS_LOG_EOM;
            delete &theSocket;
            mStats.mConnectFailureCount++;
            RetryConnect();
            return;
        }
        KFS_LOG_STREAM_DEBUG << mLogPrefix <<
            "connecting to server: " << mServerLocation.ToString() <<
        KFS_LOG_EOM;
        mConnPtr.reset(new NetConnection(&theSocket, this));
        mConnPtr->EnableReadIfOverloaded();
        mConnPtr->SetDoingNonblockingConnect();
        mConnPtr->SetMaxReadAhead(kMaxReadAhead);
        mConnPtr->SetInactivityTimeout(mOpTimeoutSec);
        // Add connection to the poll vector
        mNetManager.AddConnection(mConnPtr);
        RetryAll();
    }
    void RetryAll()
    {
        if (mPendingOpQueue.empty()) {
            return;
        }
        mNextSeqNum += 1000; // For debugging to see retries.
        QueueStack::iterator const theIt =
            mQueueStack.insert(mQueueStack.end(), OpQueue());
        OpQueue& theQueue = *theIt;
        theQueue.swap(mPendingOpQueue);
        for (OpQueue::iterator theIt = theQueue.begin();
                theIt != theQueue.end();
                ++theIt) {
            OpQueueEntry& theEntry = theIt->second;
            if (! theEntry.mOpPtr) {
                continue;
            }
            if (theIt->second.mOwnerPtr) {
                if (theEntry.mRetryCount >= mMaxRetryCount) {
                    mStats.mOpsTimeoutCount++;
                    theEntry.mOpPtr->status = kErrorMaxRetryReached;
                    theEntry.Done();
                } else {
                    mStats.mOpsRetriedCount++;
                    EnqueueSelf(theEntry.mOpPtr, theEntry.mOwnerPtr,
                        theEntry.mBufferPtr, theEntry.mRetryCount);
                    theEntry.Clear();
                }
            } else {
                mStats.mOpsCancelledCount++;
                theEntry.Cancel();
            }
        }
        mQueueStack.erase(theIt);
    }
    void Reset()
    {
        if (! mConnPtr) {
            return;
        }
        if (mSleepingFlag) {
            mNetManager.UnRegisterTimeoutHandler(this);
            mSleepingFlag = false;
        }
        mConnPtr->Close();
        mConnPtr.reset();
        mReadHeaderDoneFlag = false;
        mContentLength = 0;
    }
    void HandleOp(
        OpQueue::iterator inIt,
        bool              inCanceledFlag = false)
    {
        if (inCanceledFlag) {
            mStats.mOpsCancelledCount++;
        }
        OpQueueEntry theOpEntry = inIt->second;
        mPendingOpQueue.erase(inIt);
        theOpEntry.OpDone(inCanceledFlag);
    }
    void Cancel(
        OpQueue::iterator inIt)
        { HandleOp(inIt, true); }
    virtual void Timeout()
    {
        if (mSleepingFlag) {
            mNetManager.UnRegisterTimeoutHandler(this);
            mSleepingFlag = false;
        }
        if (mPendingOpQueue.empty()) {
            return;
        }
        EnsureConnected();
    }
    void RetryConnect()
    {
        if (mSleepingFlag) {
            return;
        }
        if (mRetryCount < mMaxRetryCount && (! mRetryConnectOnlyFlag ||
                (! mDataSentFlag && ! mDataReceivedFlag))) {
            mRetryCount++;
            if (mTimeSecBetweenRetries > 0) {
                KFS_LOG_STREAM_INFO << mLogPrefix <<
                    "retry attempt " << mRetryCount <<
                    " of " << mMaxRetryCount <<
                    ", will retry " << mPendingOpQueue.size() <<
                    " pending operation(s) in " << mTimeSecBetweenRetries <<
                    " seconds" <<
                KFS_LOG_EOM;
                mStats.mSleepTimeSec += mTimeSecBetweenRetries;
                SetTimeoutInterval(mTimeSecBetweenRetries * 1000, true);
                mSleepingFlag = true;
                mNetManager.RegisterTimeoutHandler(this);
            } else {
                Timeout();
            }
        } else {
            QueueStack::iterator const theIt =
                mQueueStack.insert(mQueueStack.end(), OpQueue());
            OpQueue& theQueue = *theIt;
            theQueue.swap(mPendingOpQueue);
            for (OpQueue::iterator theIt = theQueue.begin();
                    theIt != theQueue.end();
                    ++theIt) {
                if (! theIt->second.mOpPtr) {
                    continue;
                }
                theIt->second.mOpPtr->status = kErrorMaxRetryReached;
                theIt->second.Done();
            }
            mQueueStack.erase(theIt);
        }
    }
    void OpsTimeout()
    {
        if (mOpTimeoutSec <= 0 || ! IsConnected()) {
            return;
        }
        // Timeout ops waiting for response.
        // The ops in the queue are ordered by op seq. number.
        // The code ensures (initial seq. number in ctor) that seq. numbers
        // never wrap around, and are monotonically increase, so that the last
        // (re)queued operation seq. number is always the largest.
        const time_t theNow = Now();
        const time_t theExpireTime = theNow - mOpTimeoutSec;
        for (OpQueue::iterator theIt = mPendingOpQueue.begin();
                theIt != mPendingOpQueue.end() &&
                theIt->second.mTime < theExpireTime; ) {
            OpQueueEntry theEntry = theIt->second;
            mPendingOpQueue.erase(theIt++);
            KFS_LOG_STREAM_INFO << mLogPrefix <<
                "op timed out: seq: " << theEntry.mOpPtr->seq <<
                " "                   << theEntry.mOpPtr->Show() <<
                " retry count: "      << theEntry.mRetryCount <<
                " wait time: "        << (theNow - theEntry.mTime) <<
            KFS_LOG_EOM;
            mStats.mOpsTimeoutCount++;
            if (theEntry.mRetryCount >= mMaxRetryCount) {
                theEntry.mOpPtr->status = kErrorMaxRetryReached;
                theEntry.Done();
            } else {
                mStats.mOpsRetriedCount++;
                EnqueueSelf(theEntry.mOpPtr, theEntry.mOwnerPtr,
                    theEntry.mBufferPtr, theEntry.mRetryCount + 1);
            }
        }
    }
private:
    Impl(
        const Impl& inClient);
    Impl& operator=(
        const Impl& inClient);
};

KfsNetClient::KfsNetClient(
        NetManager& inNetManager,
        std::string inHost                  /* = std::string() */,
        int         inPort                  /* = 0 */,
        int         inMaxRetryCount         /* = 0 */,
        int         inTimeSecBetweenRetries /* = 10 */,
        int         inOpTimeoutSec          /* = 5  * 60 */,
        int         inIdleTimeoutSec        /* = 30 * 60 */,
        int64_t     inInitialSeqNum         /* = 1 */,
        const char* inLogPrefixPtr          /* = 0 */)
    : mImpl(*new Impl(
        inHost,
        inPort,
        inMaxRetryCount,
        inTimeSecBetweenRetries,
        inOpTimeoutSec,
        inIdleTimeoutSec,
        kfsSeq_t(inInitialSeqNum),
        inLogPrefixPtr,
        inNetManager
    ))
{
}

    /* virtual */
KfsNetClient::~KfsNetClient()
{
    delete &mImpl;
}

    bool
KfsNetClient::IsConnected() const
{
    return mImpl.IsConnected();
}

bool
KfsNetClient::Start()
{
    return mImpl.Start();
}

    bool
KfsNetClient::Start(
    std::string  inServerName,
    int          inServerPort,
    std::string* inErrMsgPtr,
    bool         inRetryPendingOpsFlag,
    int          inMaxRetryCount,
    int          inTimeSecBetweenRetries,
    bool         inRetryConnectOnlyFlag)
{
    return mImpl.Start(
        inServerName,
        inServerPort,
        inErrMsgPtr,
        inRetryPendingOpsFlag,
        inMaxRetryCount,
        inTimeSecBetweenRetries,
        inRetryConnectOnlyFlag
    );
}

    bool
KfsNetClient::SetServer(
    const ServerLocation& inLocation,
    bool                  inCancelPendingOpsFlag /* = true */)
{
    return mImpl.SetServer(inLocation, inCancelPendingOpsFlag);
}

    void
KfsNetClient::Stop()
{
    mImpl.Stop();
}

    int
KfsNetClient::GetMaxRetryCount() const
{
    return mImpl.GetMaxRetryCount();
}

    void
KfsNetClient::SetMaxRetryCount(
    int inMaxRetryCount)
{
    mImpl.SetMaxRetryCount(inMaxRetryCount);
}

    int
KfsNetClient::GetOpTimeoutSec() const
{
    return mImpl.GetOpTimeoutSec();
}

    void
KfsNetClient::SetOpTimeoutSec(
    int inTimeout)
{
    mImpl.SetOpTimeoutSec(inTimeout);
}

    int
KfsNetClient::GetIdleTimeoutSec() const
{
    return mImpl.GetIdleTimeoutSec();
}

    void
KfsNetClient::SetIdleTimeoutSec(
    int inTimeout)
{
    mImpl.SetIdleTimeoutSec(inTimeout);
}

    int
KfsNetClient::GetTimeSecBetweenRetries()
{
    return mImpl.GetTimeSecBetweenRetries();
}

    void
KfsNetClient::SetTimeSecBetweenRetries(
    int inTimeSec)
{
    mImpl.SetTimeSecBetweenRetries(inTimeSec);
}

    bool
KfsNetClient::IsAllDataSent() const
{
    return mImpl.IsAllDataSent();
}

    bool
KfsNetClient::IsDataReceived() const
{
    return mImpl.IsDataReceived();
}

    bool
KfsNetClient::IsDataSent() const
{
    return mImpl.IsDataSent();
}

    bool
KfsNetClient::IsRetryConnectOnly() const
{
    return mImpl.IsRetryConnectOnly();
}

    bool
KfsNetClient::WasDisconnected() const
{
    return mImpl.WasDisconnected();
}

    void
KfsNetClient::SetRetryConnectOnly(
    bool inFlag)
{
    mImpl.SetRetryConnectOnly(inFlag);
}

    void
KfsNetClient::SetOpTimeout(
    int inOpTimeoutSec)
{
    mImpl.SetOpTimeout(inOpTimeoutSec);
}

    void
KfsNetClient::GetStats(
    Stats& outStats) const
{
    mImpl.GetStats(outStats);
}

    bool
KfsNetClient::Enqueue(
    KfsOp*    inOpPtr,
    OpOwner*  inOwnerPtr,
    IOBuffer* inBufferPtr /* = 0 */)
{
    return mImpl.Enqueue(inOpPtr, inOwnerPtr, inBufferPtr);
}

    bool
KfsNetClient::Cancel(
    KfsOp*   inOpPtr,
    OpOwner* inOwnerPtr)
{
    return mImpl.Cancel(inOpPtr, inOwnerPtr);
}

    bool
KfsNetClient::Cancel()
{
    return mImpl.Cancel();
}

    std::string
KfsNetClient::GetServerLocation() const
{
    return mImpl.GetServerLocation();
}

    void
KfsNetClient::SetEventObserver(
    KfsNetClient::EventObserver* inEventObserverPtr)
{
    return mImpl.SetEventObserver(inEventObserverPtr);
}

    NetManager&
KfsNetClient::GetNetManager()
{
    return mImpl.GetNetManager();
}

    const NetManager&
KfsNetClient::GetNetManager() const
{
    return mImpl.GetNetManager();
}

}
