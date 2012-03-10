//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: RemoteSyncSM.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2006/09/27
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
// 
//----------------------------------------------------------------------------

#ifndef CHUNKSERVER_REMOTESYNCSM_H
#define CHUNKSERVER_REMOTESYNCSM_H

#include <list>

#include "libkfsIO/KfsCallbackObj.h"
#include "libkfsIO/NetConnection.h"
#include "KfsOps.h"
#include "Chunk.h"
#include "libkfsIO/ITimeout.h"
#include "meta/queue.h"

#include <sys/types.h>
#include <time.h>

#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>

namespace KFS
{

class RemoteSyncSMTimeoutImpl;

// State machine that asks a remote server to commit a write
class RemoteSyncSM : public KfsCallbackObj,
                     public boost::enable_shared_from_this<RemoteSyncSM>
{
public:

    RemoteSyncSM(const ServerLocation &location);

    ~RemoteSyncSM();

    bool Connect();

    kfsSeq_t NextSeqnum();

    void Enqueue(KfsOp *op);

    void Finish();

    // void Dispatch();

    int HandleEvent(int code, void *data);

    ServerLocation GetLocation() const {
        return mLocation;
    }
    static void SetTraceRequestResponse(bool flag) {
        sTraceRequestResponse = flag;
    }
    static void SetResponseTimeoutSec(int timeoutSec) {
        sOpResponseTimeoutSec = timeoutSec;
    }
    static int GetResponseTimeoutSec() {
        return sOpResponseTimeoutSec;
    }

private:
    NetConnectionPtr mNetConnection;

    ServerLocation mLocation;

    /// Assign a sequence # for each op we send to the remote server
    kfsSeq_t mSeqnum;

    /// Queue of outstanding ops sent to remote server.
    std::list<KfsOp *> mDispatchedOps;

    kfsSeq_t mReplySeqNum;
    int      mReplyNumBytes;
    time_t   mLastRecvTime;

    /// We (may) have got a response from the peer.  If we are doing
    /// re-replication, then we need to wait until we got all the data
    /// for the op; in such cases, we need to know if we got the full
    /// response. 
    /// @retval 0 if we got the response; -1 if we need to wait
    int HandleResponse(IOBuffer *iobuf, int cmdLen);
    void FailAllOps();
    inline void UpdateRecvTimeout();
    static bool sTraceRequestResponse;
    static int  sOpResponseTimeoutSec;
};

typedef boost::shared_ptr<RemoteSyncSM> RemoteSyncSMPtr;

    RemoteSyncSMPtr FindServer(std::list<RemoteSyncSMPtr> &remoteSyncers, const ServerLocation &location, 
                               bool connect);
    
    void RemoveServer(std::list<RemoteSyncSMPtr> &remoteSyncers, RemoteSyncSM *target);

    void ReleaseAllServers(std::list<RemoteSyncSMPtr> &remoteSyncers);
}

#endif // CHUNKSERVER_REMOTESYNCSM_H
