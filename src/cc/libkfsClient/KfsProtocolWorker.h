//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: KfsProtocolWorker.h 1552 2011-01-06 22:21:54Z sriramr $
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

#ifndef FKS_PROTOCOL_WORKER_H
#define FKS_PROTOCOL_WORKER_H

#include <cerrno>

#include "common/kfstypes.h"
#include "libkfsIO/Checksum.h"
#include "qcdio/qcdllist.h"

namespace KFS
{

class KfsProtocolWorker
{
private:
    class Impl;
public:
    enum
    {
        kErrNone       = 0,
        kErrParameters = -EINVAL,
        kErrProtocol   = -EBADF,
        kErrShutdown   = -91010
    };
    enum RequestType
    {
        kRequestTypeUnknown                      = 0,
        kRequestTypeWriteAppend                  = 1,
        kRequestTypeWriteAppendClose             = 2,
        kRequestTypeWriteAppendShutdown          = 3,
        kRequestTypeWriteAppendSetWriteThreshold = 4,
        kRequestTypeWriteAppendAsync             = 5,
        kRequestTypeWriteAppendThrottle          = 6,
    };
    typedef kfsFileId_t  FileId;
    typedef unsigned int FileInstance;
    class Request
    {
    public:
        Request(
            RequestType  inOpType       = kRequestTypeUnknown,
            FileInstance inFileInstance = 0,
            FileId       inFileId       = -1,
            std::string  inPathName     = std::string(),
            void*        inBufferPtr    = 0,
            int          inSize         = 0,
            int          inMaxPending   = -1);
        void Reset(
            RequestType  inOpType       = kRequestTypeUnknown,
            FileInstance inFileInstance = 0,
            FileId       inFileId       = -1,
            std::string  inPathName     = std::string(),
            void*        inBufferPtr    = 0,
            int          inSize         = 0,
            int          inMaxPending   = -1);
        virtual void Done(
            int status) = 0;
    protected:
        virtual ~Request();
    private:
        enum State
        {
            kStateNone     = 0,
            kStateInFlight = 1,
            kStateDone     = 2
        };
        RequestType  mRequestType;
        FileInstance mFileInstance;
        FileId       mFileId;
        std::string  mPathName;
        void*        mBufferPtr;
        int          mSize;
        State        mState;
        int          mStatus;
        int64_t      mMaxPendingOrEndPos;
    private:
        Request* mPrevPtr[1];
        Request* mNextPtr[1];
        friend class QCDLListOp<Request, 0>;
        friend class Impl;
    private:
        Request(
            const Request& inReq);
        Request& operator=(
            const Request& inReq);
    };
    KfsProtocolWorker(
        std::string inMetaHost,
        int         inMetaPort,
        int         inMetaMaxRetryCount           = 3,
        int         inMetaTimeSecBetweenRetries   = 10,
        int         inMetaOpTimeoutSec            = 3 * 60,
        int         inMetaIdleTimeoutSec          = 5 * 60,
        int64_t     inMetaInitialSeqNum           = 0,
        const char* inMetaLogPrefixPtr            = 0,
        int         inMaxRetryCount               = 10,
        int         inWriteThreshold              = KFS::CHECKSUM_BLOCKSIZE,
        int         inTimeSecBetweenRetries       = 15,
        int         inDefaultSpaceReservationSize = 1 << 20,
        int         inPreferredAppendSize         = KFS::CHECKSUM_BLOCKSIZE,
        int         inOpTimeoutSec                = 120,
        int         inIdleTimeoutSec              = 5 * 30,
        const char* inLogPrefixPtr                = 0,
        int64_t     inChunkServerInitialSeqNum    = 0,
        bool        inPreAllocateFlag             = false);
    ~KfsProtocolWorker();
    int Execute(
        RequestType  inRequestType,
        FileInstance inFileInstance,
        FileId       inFileId,
        std::string  inPathName   = std::string(),
        void*        inBufferPtr  = 0,
        int          inSize       = 0,
        int          inMaxPending = -1);
    void Enqueue(
        Request& inRequest);
    void Start();
    void Stop();
private:
    Impl& mImpl;
private:
    KfsProtocolWorker(
        const KfsProtocolWorker& inWorker);
    KfsProtocolWorker& operator=(
        const KfsProtocolWorker& inWorker);
};

}

#endif /* FKS_PROTOCOL_WORKER_H */
