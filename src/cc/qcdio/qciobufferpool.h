//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: qciobufferpool.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2008/11/01
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

#ifndef QCIOBUFFERPOOL_H
#define QCIOBUFFERPOOL_H

#include "qcmutex.h"


class QCIoBufferPool
{
public:
    typedef int RefillReqId;
    enum
    {
        kRefillReqIdUndefined = -1,
        kRefillReqIdRead      = 1
    };

    class Client
    {
    public:
        // FIXME: Currently invoked without dropping the lock: deadlock prone,
        // fix later if required.
        virtual void Release(
            RefillReqId inReqId,
            int         inBufCount) = 0;
        virtual bool Unregister();
        QCIoBufferPool* GetPoolPtr() const
            { return mPoolPtr; }
        bool IsRegistered()
            { return (mPoolPtr != 0); }
    protected:
        Client();
        virtual ~Client()
            { Client::Unregister(); }
    private:
        QCIoBufferPool* mPoolPtr;
        Client*         mPrevPtr[1];
        Client*         mNextPtr[1];
    friend class QCIoBufferPool;
    template<typename, unsigned int> friend class QCDLListOp;
    };

    class InputIterator
    {
    public:
        virtual char* Get() = 0;
    protected:
        InputIterator()
            {}
        virtual ~InputIterator()
            {}
    };

    class OutputIterator
    {
    public:
        virtual void Put(
            char* inBufPtr) = 0;
    protected:
        OutputIterator()
            {}
        virtual ~OutputIterator()
            {}
    };

    QCIoBufferPool();
    ~QCIoBufferPool();
    int Create(
        int          inPartitionCount,
        int          inPartitionBufferCount,
        int          inBufferSize,
        bool         inLockMemoryFlag);
    void Destroy();
    char* Get(
        RefillReqId inRefillReqId = kRefillReqIdUndefined);
    bool Get(
        OutputIterator& inIt,
        int             inBufCnt,
        RefillReqId     inRefillReqId = kRefillReqIdUndefined);
    void Put(
        char* inBufPtr);
    void Put(
        InputIterator& inIt,
        int            inBufCnt);
    bool Register(
        Client& inClient);
    bool UnRegister(
        Client& inClient);
    int GetBufferSize() const
        { return mBufferSize; }
    int GetFreeBufferCount();

private:
    class Partition;
    QCMutex    mMutex;
    Client*    mClientListPtr[1];
    Partition* mPartitionListPtr[1];
    int        mBufferSize;
    int        mFreeCnt;

    bool TryToRefill(
        RefillReqId inReqId,
        int         inBufCnt);
    void PutSelf(
        char* inBufPtr);

public:
    // No copies.
    QCIoBufferPool(
        const QCIoBufferPool& inPool);
    QCIoBufferPool& operator=(
        const QCIoBufferPool& inPool);
};

#endif /* QCIOBUFFERPOOL_H */
