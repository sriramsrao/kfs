//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: BufferManager.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2009/06/06
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

#include <algorithm>

#include "BufferManager.h"
#include "qcdio/qcutils.h"
#include "libkfsIO/NetManager.h"
#include "libkfsIO/Globals.h"

namespace KFS
{


BufferManager::Client::Client()
    : mManagerPtr(0),
      mByteCount(0),
      mWaitingForByteCount(0)
{
    WaitQueue::Init(*this);
}

BufferManager::BufferManager(
    bool inEnabledFlag /* = true */)
    : ITimeout(),
      mTotalCount(0),
      mMaxClientQuota(0),
      mRemainingCount(0),
      mWaitingByteCount(0),
      mGetRequestCount(0),
      mPutRequestCount(0),
      mClientsWihtBuffersCount(0),
      mMinBufferCount(0),
      mWaitingCount(0),
      mInitedFlag(false),
      mEnabledFlag(inEnabledFlag),
      mCounters()
{
    WaitQueue::Init(mWaitQueuePtr);
    mCounters.Clear();
}

BufferManager::~BufferManager()
{
    QCRTASSERT(WaitQueue::IsEmpty(mWaitQueuePtr));
    libkfsio::globalNetManager().UnRegisterTimeoutHandler(this);
}

    void
BufferManager::Init(
    QCIoBufferPool*          inBufferPoolPtr,
    BufferManager::ByteCount inTotalCount,
    BufferManager::ByteCount inMaxClientQuota,
    int                      inMinBufferCount)
{
    QCRTASSERT(! mInitedFlag);
    mInitedFlag              = true;
    mWaitingCount            = 0;
    mWaitingByteCount        = 0;
    mGetRequestCount         = 0;
    mPutRequestCount         = 0;
    mClientsWihtBuffersCount = 0;
    mBufferPoolPtr           = inBufferPoolPtr;
    mTotalCount              = inTotalCount;
    mRemainingCount          = mTotalCount;
    mMinBufferCount          = inMinBufferCount;
    mMaxClientQuota          = std::min(mTotalCount, inMaxClientQuota);
    libkfsio::globalNetManager().RegisterTimeoutHandler(this);
}

    bool
BufferManager::Modify(
    BufferManager::Client&   inClient,
    BufferManager::ByteCount inByteCount)
{
    if (! mEnabledFlag) {
        return true;
    }
    assert(inClient.mByteCount >= 0 && inClient.mWaitingForByteCount >= 0);
    assert(inClient.mManagerPtr ||
        inClient.mWaitingForByteCount + inClient.mByteCount == 0);
    assert(! inClient.mManagerPtr || inClient.mManagerPtr == this);
    assert(inClient.IsWaiting() || inClient.mWaitingForByteCount == 0);
    assert(mRemainingCount + inClient.mByteCount <= mTotalCount);

    const bool theHadBuffersFlag = inClient.mByteCount > 0;
    mRemainingCount += inClient.mByteCount;
#if 0
    std::cout << reinterpret_cast<const void*>(&inClient) <<
        " mod: "      << inByteCount <<
        " used: "     << inClient.mByteCount <<
        " waiting: "  << inClient.mWaitingForByteCount <<
    std::endl;
#endif
    if (inByteCount < 0) {
        mPutRequestCount++;
        inClient.mByteCount += inByteCount;
        if (inClient.mByteCount < 0) {
            inClient.mByteCount = 0;
        }
        mRemainingCount -= inClient.mByteCount;
        if (theHadBuffersFlag && inClient.mByteCount <= 0) {
            mClientsWihtBuffersCount--;
        }
        return true;
    }
    mCounters.mRequestCount++;
    mCounters.mRequestByteCount += inByteCount;
    mGetRequestCount++;
    inClient.mManagerPtr = this;
    const ByteCount theReqCount    =
        inClient.mWaitingForByteCount + inClient.mByteCount + inByteCount;
    const bool      theGrantedFlag = ! inClient.IsWaiting() && (
        theReqCount <= 0 || (
            (! mBufferPoolPtr ||
                mBufferPoolPtr->GetFreeBufferCount() >= mMinBufferCount) &&
            theReqCount < mRemainingCount &&
            ! IsOverQuota(inClient)
        )
    );
    if (theGrantedFlag) {
        inClient.mByteCount = theReqCount;
        mRemainingCount -= theReqCount;
        mCounters.mRequestGrantedCount++;
        mCounters.mRequestGrantedByteCount += inByteCount;
    } else {
        mCounters.mRequestDeniedCount++;
        mCounters.mRequestDeniedByteCount += inByteCount;
        // If already waiting leave him in the same place in the queue.
        if (! inClient.IsWaiting()) {
            WaitQueue::PushBack(mWaitQueuePtr, inClient);
            mWaitingCount++;
        }
        mWaitingByteCount += inByteCount;
        mRemainingCount -= inClient.mByteCount;
        inClient.mWaitingForByteCount += inByteCount;
    }
    assert(mRemainingCount >= 0 && mRemainingCount <= mTotalCount);
    assert(inClient.IsWaiting() || inClient.mWaitingForByteCount == 0);
    if (! theHadBuffersFlag && inClient.mByteCount > 0) {
        mClientsWihtBuffersCount++;
    }
    return theGrantedFlag;
}

    void
BufferManager::Unregister(
    BufferManager::Client& inClient)
{
    if (! inClient.mManagerPtr) {
        return;
    }
    QCRTASSERT(inClient.mManagerPtr == this);
    if (IsWaiting(inClient)) {
        mWaitingCount--;
        mWaitingByteCount -= inClient.mWaitingForByteCount;
    }
    WaitQueue::Remove(mWaitQueuePtr, inClient);
    inClient.mWaitingForByteCount = 0;
    Put(inClient, inClient.mByteCount);
    assert(! inClient.IsWaiting() && inClient.mByteCount == 0);
}

    void
BufferManager::CancelRequest(
    Client& inClient)
{
    if (! inClient.mManagerPtr) {
        return;
    }
    QCRTASSERT(inClient.mManagerPtr == this);
    if (! IsWaiting(inClient)) {
        assert(inClient.mWaitingForByteCount == 0);
        return;
    }
    WaitQueue::Remove(mWaitQueuePtr, inClient);
    mWaitingCount--;
    mWaitingByteCount -= inClient.mWaitingForByteCount;
    inClient.mWaitingForByteCount = 0;
}

    bool
BufferManager::IsLowOnBuffers() const
{
    return (mBufferPoolPtr->GetFreeBufferCount() < mMinBufferCount);
}

    /* virtual */ void
BufferManager::Timeout()
{
    while (! mBufferPoolPtr ||
            mBufferPoolPtr->GetFreeBufferCount() > mMinBufferCount) {
        WaitQueue::Iterator theIt(mWaitQueuePtr);
        Client*             theClientPtr;
        while ((theClientPtr = theIt.Next())) {
            // Skip all that are over quota.
            if (! IsOverQuota(*theClientPtr)) {
                break;
            }
        }
        if (! theClientPtr ||
                theClientPtr->mWaitingForByteCount > mRemainingCount) {
            break;
        }
        WaitQueue::Remove(mWaitQueuePtr, *theClientPtr);
        mWaitingCount--;
        const ByteCount theGrantedCount = theClientPtr->mWaitingForByteCount;
        assert(theGrantedCount > 0);
        mRemainingCount -= theGrantedCount;
        assert(mRemainingCount <= mTotalCount);
        mWaitingByteCount -= theGrantedCount;
        if (theClientPtr->mByteCount <= 0 && theGrantedCount > 0) {
            mClientsWihtBuffersCount++;
        }
        mCounters.mRequestGrantedCount++;
        mCounters.mRequestGrantedByteCount += theGrantedCount;
        theClientPtr->mByteCount += theGrantedCount;
        theClientPtr->mWaitingForByteCount = 0;
        theClientPtr->Granted(theGrantedCount);
    }
}

} /* namespace KFS */
