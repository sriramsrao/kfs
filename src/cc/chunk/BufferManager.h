//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: BufferManager.h 1552 2011-01-06 22:21:54Z sriramr $
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

#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

#include <stdint.h>
#include "qcdio/qcdllist.h"
#include "qcdio/qciobufferpool.h"
#include "libkfsIO/ITimeout.h"

namespace KFS
{

class BufferManager : private ITimeout
{
public:
    typedef int64_t ByteCount;
    typedef int64_t RequestCount;
    struct Counters
    {
        typedef int64_t Counter;

        Counter mRequestCount;
        Counter mRequestByteCount;
        Counter mRequestDeniedCount;
        Counter mRequestDeniedByteCount;
        Counter mRequestGrantedCount;
        Counter mRequestGrantedByteCount;

        void Clear()
        {
            mRequestCount            = 0;
            mRequestByteCount        = 0;
            mRequestDeniedCount      = 0;
            mRequestDeniedByteCount  = 0;
            mRequestGrantedCount     = 0;
            mRequestGrantedByteCount = 0;
        }
    };

    class Client
    {
    public:
        typedef BufferManager::ByteCount ByteCount;

        virtual void Granted(
            ByteCount inByteCount) = 0;
        // virtual int EmeregencyRelease(
        //    ByteCount inByteCount)
        //    { return 0; }
        ByteCount GetByteCount() const
            { return mByteCount; }
        ByteCount GetWaitingForByteCount() const
            { return mWaitingForByteCount; }
        bool IsWaiting() const
            { return (mManagerPtr && mManagerPtr->IsWaiting(*this)); }
        void CancelRequest()
        {
            if (mManagerPtr) {
                mManagerPtr->CancelRequest(*this);
            }
        }
        void Unregister()
        {
            if (mManagerPtr) {
                mManagerPtr->Unregister(*this);
            }
        }
    protected:
        Client();
        virtual ~Client()
            { Client::Unregister(); }
    private:
        Client*        mPrevPtr[1];
        Client*        mNextPtr[1];
        BufferManager* mManagerPtr;
        ByteCount      mByteCount;
        ByteCount      mWaitingForByteCount;

        friend class BufferManager;
        friend class QCDLListOp<Client, 0>;
    };
    BufferManager(
        bool inEnabledFlag);
    ~BufferManager();
    void Init(
        QCIoBufferPool* inBufferPoolPtr,
        ByteCount       inTotalCount,
        ByteCount       inMaxClientQuota,
        int             inMinBufferCount);
    ByteCount GetMaxClientQuota() const
        { return mMaxClientQuota; }
    bool IsOverQuota(
        Client&   inClient,
        ByteCount inByteCount = 0)
    {
        return (mMaxClientQuota <
            inClient.mByteCount + inClient.mWaitingForByteCount + inByteCount);
    }
    bool Get(
        Client&   inClient,
        ByteCount inByteCount)
        { return (inByteCount > 0 && Modify(inClient, inByteCount)); }
    bool Put(
        Client&   inClient,
        ByteCount inByteCount)
        { return (inByteCount > 0 && Modify(inClient, -inByteCount)); }
    ByteCount GetTotalCount() const
        { return mTotalCount; }
    bool IsLowOnBuffers() const;
    virtual void Timeout();
    bool IsWaiting(
        const Client& inClient) const
        { return WaitQueue::IsInList(mWaitQueuePtr, inClient); }
    void Unregister(
        Client& inClient);
    void CancelRequest(
        Client& inClient);
    ByteCount GetTotalByteCount() const
        { return mTotalCount; }
    ByteCount GetRemainingByteCount() const
        { return mRemainingCount; }
    ByteCount GetUsedByteCount() const
        { return (mTotalCount - mRemainingCount); }
    int GetFreeBufferCount() const
        { return (mBufferPoolPtr ? mBufferPoolPtr->GetFreeBufferCount() : 0); }
    int GetMinBufferCount() const
        { return mMinBufferCount; }
    int GetTotalBufferCount() const
    {
        const int theSize = mBufferPoolPtr ? mBufferPoolPtr->GetBufferSize() : 0;
        return (theSize > 0 ? mTotalCount / theSize : 0);
    }
    int GetWaitingCount() const
        { return mWaitingCount; }
    int GetWaitingByteCount() const
        { return mWaitingByteCount; }
    RequestCount GetGetRequestCount() const
        { return mGetRequestCount; }
    RequestCount GetPutRequestCount() const
        { return mPutRequestCount; }
    int GetClientsWihtBuffersCount() const
        { return mClientsWihtBuffersCount; }
    void GetCounters(
        Counters& outCounters) const
        { outCounters = mCounters; }
private:
    typedef QCDLList<Client, 0> WaitQueue;
    Client*         mWaitQueuePtr[1];
    QCIoBufferPool* mBufferPoolPtr;
    ByteCount       mTotalCount;
    ByteCount       mMaxClientQuota;
    ByteCount       mRemainingCount;
    ByteCount       mWaitingByteCount;
    RequestCount    mGetRequestCount;
    RequestCount    mPutRequestCount;
    int             mClientsWihtBuffersCount;
    int             mMinBufferCount;
    int             mWaitingCount;
    bool            mInitedFlag;
    const bool      mEnabledFlag;
    Counters        mCounters;

    bool Modify(
        Client&   inClient,
        ByteCount inByteCount);

    BufferManager(
        const BufferManager& inManager);
    BufferManager& operator=(
        const BufferManager& inManager);
};

} /* namespace KFS */
#endif /* BUFFER_MANAGER_H */
