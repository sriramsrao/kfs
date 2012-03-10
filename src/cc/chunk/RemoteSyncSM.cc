//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: RemoteSyncSM.cc 3369 2011-11-28 20:05:28Z sriramr $
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

#include "RemoteSyncSM.h"
#include "Utils.h"
#include "ChunkServer.h"
#include "libkfsIO/NetManager.h"
#include "libkfsIO/Globals.h"

#include "common/log.h"
#include "common/properties.h"

#include <openssl/rand.h>

#include <cerrno>
#include <sstream>
#include <string>
#include <algorithm>
using std::find_if;
using std::for_each;
using std::istringstream;
using std::ostringstream;
using std::list;
using std::string;

using namespace KFS;
using namespace KFS::libkfsio;

const int kMaxCmdHeaderLength = 2 << 10;
bool RemoteSyncSM::sTraceRequestResponse = false;
int  RemoteSyncSM::sOpResponseTimeoutSec = 5 * 60; // 5 min op response timeout

inline static kfsSeq_t
InitialSeqNo()
{
    kfsSeq_t ret = 1;
    RAND_pseudo_bytes(reinterpret_cast<unsigned char*>(&ret), int(sizeof(ret)));
    return ((ret < 0 ? -ret : ret) >> 1);
}

static kfsSeq_t
NextSeq()
{
    static kfsSeq_t sSeqno = InitialSeqNo();
    return sSeqno++;
}

inline void
RemoteSyncSM::UpdateRecvTimeout()
{
    if (sOpResponseTimeoutSec < 0 || ! mNetConnection) {
        return;
    }
    const time_t now = globalNetManager().Now();
    const time_t end = mLastRecvTime + sOpResponseTimeoutSec;
    mNetConnection->SetInactivityTimeout(end > now ? end - now : 0);
}

RemoteSyncSM::RemoteSyncSM(const ServerLocation &location)         
    : mLocation(location),
      mReplySeqNum(-1),
      mReplyNumBytes(0),
      mLastRecvTime(0)
{
    mSeqnum = NextSeq();
}

kfsSeq_t
RemoteSyncSM::NextSeqnum()
{
    mSeqnum = NextSeq();
    return mSeqnum;
}

RemoteSyncSM::~RemoteSyncSM()
{
    if (mNetConnection)
        mNetConnection->Close();
    assert(mDispatchedOps.size() == 0);
}

bool
RemoteSyncSM::Connect()
{
    TcpSocket *sock;
    int res;

    assert(! mNetConnection);

    KFS_LOG_VA_DEBUG("Trying to connect to: %s", mLocation.ToString().c_str());

    sock = new TcpSocket();
    // do a non-blocking connect
    res = sock->Connect(mLocation, true);
    if ((res < 0) && (res != -EINPROGRESS)) {
        KFS_LOG_VA_INFO("Connect to remote server (%s) failed: code = %d",
                        mLocation.ToString().c_str(), res);
        delete sock;
        return false;
    }

    KFS_LOG_VA_INFO("Connect to remote server (%s) succeeded (res = %d)...",
                    mLocation.ToString().c_str(), res);

    SET_HANDLER(this, &RemoteSyncSM::HandleEvent);

    mNetConnection.reset(new NetConnection(sock, this));
    mNetConnection->SetDoingNonblockingConnect();
    mNetConnection->SetMaxReadAhead(kMaxCmdHeaderLength);
    mLastRecvTime = globalNetManager().Now();
    
    // If there is no activity on this socket, we want
    // to be notified, so that we can close connection.
    mNetConnection->SetInactivityTimeout(sOpResponseTimeoutSec);
    // Add this to the poll vector
    globalNetManager().AddConnection(mNetConnection);

    return true;
}

void
RemoteSyncSM::Enqueue(KfsOp *op)
{
    if (!mNetConnection) {
        if (!Connect()) {
            KFS_LOG_VA_INFO("Connect to peer %s failed; failing ops", mLocation.ToString().c_str());
            mDispatchedOps.push_back(op);
            FailAllOps();
            return;
        }
    }
    if (!mNetConnection->IsGood()) {
        KFS_LOG_VA_INFO("Lost the connection to peer %s; failing ops", mLocation.ToString().c_str());
        mDispatchedOps.push_back(op);
        FailAllOps();
        mNetConnection->Close();
        mNetConnection.reset();
        return;
    }
    if (mDispatchedOps.empty()) {
        mLastRecvTime = globalNetManager().Now();
    }
    IOBuffer::OStream os;
    op->Request(os);
    if (sTraceRequestResponse) {
        IOBuffer::IStream is(os, os.BytesConsumable());
        char buf[128];
        KFS_LOG_STREAM_DEBUG << reinterpret_cast<void*>(this) <<
            " send to: " << mLocation.ToString() <<
        KFS_LOG_EOM;
        while (is.getline(buf, sizeof(buf))) {
            KFS_LOG_STREAM_DEBUG << reinterpret_cast<void*>(this) <<
                " request: " << buf <<
            KFS_LOG_EOM;
        }
    }
    mNetConnection->Write(&os);
    if (op->op == CMD_CLOSE) {
        // fire'n'forget
        op->status = 0;
        KFS::SubmitOpResponse(op); 
    }
    else if (op->op == CMD_WRITE_PREPARE_FWD) {
        // send the data as well
        WritePrepareFwdOp *wpfo = static_cast<WritePrepareFwdOp *>(op);        
        mNetConnection->Write(wpfo->dataBuf, wpfo->dataBuf->BytesConsumable());
        // fire'n'forget
        op->status = 0;
        KFS::SubmitOpResponse(op);            
    }
    else {
        if (op->op == CMD_RECORD_APPEND) {
            KFS_LOG_STREAM_DEBUG << "Fwd'ing record append: " << op->Show() << KFS_LOG_EOM;
            // send the append over; we'll get an ack back
            RecordAppendOp *ra = static_cast<RecordAppendOp *>(op);
            mNetConnection->Write(&ra->dataBuf, ra->numBytes);
        }
        mDispatchedOps.push_back(op);
    }
    UpdateRecvTimeout();
    if (mNetConnection) {
        mNetConnection->StartFlush();
    }
}


int
RemoteSyncSM::HandleEvent(int code, void *data)
{
    IOBuffer *iobuf;
    int msgLen = 0;
    // take a ref to prevent the object from being deleted
    // while we are still in this function.
    RemoteSyncSMPtr self = shared_from_this();
    const char *reason = "error";

#ifdef DEBUG
    verifyExecutingOnNetProcessor();
#endif    

    switch (code) {
    case EVENT_NET_READ:
        mLastRecvTime = globalNetManager().Now();
	// We read something from the network.  Run the RPC that
	// came in if we got all the data for the RPC
	iobuf = (IOBuffer *) data;
	while ((mReplyNumBytes > 0 || IsMsgAvail(iobuf, &msgLen)) &&
	        HandleResponse(iobuf, msgLen) >= 0)
	    {}
        UpdateRecvTimeout();
	break;

    case EVENT_NET_WROTE:
	// Something went out on the network.  For now, we don't
	// track it. Later, we may use it for tracking throttling
	// and such.
        UpdateRecvTimeout();
	break;

        
    case EVENT_INACTIVITY_TIMEOUT:
    	reason = "inactivity timeout";
    case EVENT_NET_ERROR:
        // If there is an error or there is no activity on the socket
        // for N mins, we close the connection. 
	KFS_LOG_VA_INFO("Closing connection to peer: %s due to %s", mLocation.ToString().c_str(), reason);

	if (mNetConnection)
	    mNetConnection->Close();

        // we are done...
        Finish();

	break;

    default:
	assert(!"Unknown event");
	break;
    }
    return 0;

}

int
RemoteSyncSM::HandleResponse(IOBuffer *iobuf, int msgLen)
{
    list<KfsOp *>::iterator i = mDispatchedOps.end();
    int nAvail = iobuf->BytesConsumable();

    if (mReplyNumBytes <= 0) {
        assert(msgLen >= 0 && msgLen <= nAvail);
        if (sTraceRequestResponse) {
            IOBuffer::IStream is(*iobuf, msgLen);
            const string      loc(mLocation.ToString());
            string line;
            while (getline(is, line)) {
                KFS_LOG_STREAM_DEBUG << reinterpret_cast<void*>(this) <<
                    loc << " response: " << line <<
                KFS_LOG_EOM;
            }
        }
        Properties prop;
        {
            const char separator(':');
            IOBuffer::IStream is(*iobuf, msgLen);
            prop.loadProperties(is, separator, false);
        }
        iobuf->Consume(msgLen);
        mReplySeqNum = prop.getValue("Cseq", (kfsSeq_t) -1);
        if (mReplySeqNum < 0) {
            KFS_LOG_STREAM_ERROR <<
                "invalid or missing Cseq header: " << mReplySeqNum <<
                ", resetting connection" <<
            KFS_LOG_EOM;
            HandleEvent(EVENT_NET_ERROR, 0);
        }
        mReplyNumBytes = prop.getValue("Content-length", (long long) 0);
        nAvail -= msgLen;
        i = find_if(mDispatchedOps.begin(), mDispatchedOps.end(), 
                    OpMatcher(mReplySeqNum));
        KfsOp* const op = i != mDispatchedOps.end() ? *i : 0;
        if (op) {
            op->status = prop.getValue("Status", -1);
            if (op->op == CMD_WRITE_ID_ALLOC) {
                WriteIdAllocOp *wiao = static_cast<WriteIdAllocOp *>(op);
                wiao->writeIdStr = prop.getValue("Write-id", "");
            } else if (op->op == CMD_READ) {
                ReadOp *rop = static_cast<ReadOp *> (op);
                const int checksumEntries = prop.getValue("Checksum-entries", 0);
                if (checksumEntries > 0) {
                    istringstream is(prop.getValue("Checksums", ""));
                    uint32_t cks;
                    for (int i = 0; i < checksumEntries; i++) {
                        is >> cks;
                        rop->checksum.push_back(cks);
                    }
                }
                const int off(rop->offset % IOBufferData::GetDefaultBufferSize());
                if (off > 0) {
                    IOBuffer buf;
                    buf.ReplaceKeepBuffersFull(iobuf, off, nAvail);
                    iobuf->Move(&buf);
                    iobuf->Consume(off);
                } else {
                    iobuf->MakeBuffersFull(); 
                }
            } else if (op->op == CMD_SIZE) {
                SizeOp *sop = static_cast<SizeOp *>(op);
                sop->size = prop.getValue("Size", 0);
            } else if (op->op == CMD_GET_CHUNK_METADATA) {
                GetChunkMetadataOp *gcm = static_cast<GetChunkMetadataOp *>(op);
                gcm->chunkVersion = prop.getValue("Chunk-version", 0);
                gcm->chunkSize = prop.getValue("Size", (off_t) 0);
                gcm->indexSize = prop.getValue("Index-size", (int) 0);
            }
        }
    }

    // if we don't have all the data for the write, hold on...
    if (nAvail < mReplyNumBytes) {
        // the data isn't here yet...wait...
        if (mNetConnection) {
            mNetConnection->SetMaxReadAhead(mReplyNumBytes - nAvail);
        }
        return -1;
    }

    // now, we got everything...
    if (mNetConnection) {
        mNetConnection->SetMaxReadAhead(kMaxCmdHeaderLength);
    }

    // find the matching op
    if (i == mDispatchedOps.end()) {
        i = find_if(mDispatchedOps.begin(), mDispatchedOps.end(), 
                    OpMatcher(mReplySeqNum));
    }
    if (i != mDispatchedOps.end()) {
        KfsOp *const op = *i;
        mDispatchedOps.erase(i);
        if (op->op == CMD_READ) {
            ReadOp *rop = static_cast<ReadOp *> (op);
            if (rop->dataBuf == NULL)
                rop->dataBuf = new IOBuffer();
            rop->dataBuf->Move(iobuf, mReplyNumBytes);
            rop->numBytesIO = mReplyNumBytes;
        } else if (op->op == CMD_GET_CHUNK_METADATA) {
            GetChunkMetadataOp *gcm = static_cast<GetChunkMetadataOp *>(op);
            if (gcm->dataBuf == NULL)
                gcm->dataBuf = new IOBuffer();
            gcm->dataBuf->Move(iobuf, mReplyNumBytes);            
        }  else if (op->op == CMD_GET_CHUNK_INDEX) {
            GetChunkIndexOp *gci = static_cast<GetChunkIndexOp *>(op);
            gci->dataBuf->Move(iobuf, mReplyNumBytes);
        }
        mReplyNumBytes = 0;
        // op->HandleEvent(EVENT_DONE, op);
        KFS::SubmitOpResponse(op);
    } else {
        KFS_LOG_VA_DEBUG("Discarding a reply for unknown seq #: %d", mReplySeqNum);
        mReplyNumBytes = 0;
    }
    return 0;
}

// Helper functor that fails an op with an error code.
class OpFailer {
    int errCode;
public:
    OpFailer(int c) : errCode(c) { };
    void operator() (KfsOp *op) {
        op->status = errCode;
        // op->HandleEvent(EVENT_DONE, op);
        KFS::SubmitOpResponse(op);
    }
};


void
RemoteSyncSM::FailAllOps()
{
    if (mDispatchedOps.empty())
        return;

    // There is a potential recursive call: if a client owns this
    // object and the client got a network error, the client will call
    // here to fail the outstandnig ops; when an op the client is
    // notified and the client calls to close out this object.  We'll
    // be right back here trying  to fail an op and will core.  To
    // avoid, swap out the ops and try.
    list<KfsOp *> opsToFail;

    mDispatchedOps.swap(opsToFail);
    for_each(opsToFail.begin(), opsToFail.end(),
             OpFailer(-EHOSTUNREACH));
    opsToFail.clear();
}

void
RemoteSyncSM::Finish()
{
#ifdef DEBUG
    verifyExecutingOnEventProcessor();
#endif    

    FailAllOps();
    if (mNetConnection) {
	mNetConnection->Close();
        mNetConnection.reset();
    }
    // if the object was owned by the chunkserver, have it release the reference
    gChunkServer.RemoveServer(this);
}

//
// Utility functions to operate on a list of remotesync servers
//

class RemoteSyncSMMatcher {
    ServerLocation myLoc;
public:
    RemoteSyncSMMatcher(const ServerLocation &loc) :
        myLoc(loc) { }
    bool operator() (RemoteSyncSMPtr other) {
        return other->GetLocation() == myLoc;
    }
};

RemoteSyncSMPtr
KFS::FindServer(list<RemoteSyncSMPtr> &remoteSyncers, const ServerLocation &location, 
                bool connect)
{
    list<RemoteSyncSMPtr>::iterator i;
    RemoteSyncSMPtr peer;

    i = find_if(remoteSyncers.begin(), remoteSyncers.end(),
                RemoteSyncSMMatcher(location));
    if (i != remoteSyncers.end()) {
        peer = *i;
        return peer;
    }
    if (!connect)
        return peer;

    peer.reset(new RemoteSyncSM(location));
    if (peer->Connect()) {
        remoteSyncers.push_back(peer);
    } else {
        // we couldn't connect...so, force destruction
        peer.reset();
    }
    return peer;
}

void
KFS::RemoveServer(list<RemoteSyncSMPtr> &remoteSyncers, RemoteSyncSM *target)
{
    list<RemoteSyncSMPtr>::iterator i;

    i = find_if(remoteSyncers.begin(), remoteSyncers.end(),
                RemoteSyncSMMatcher(target->GetLocation()));
    if (i != remoteSyncers.end()) {
        RemoteSyncSMPtr r = *i;
        if (r.get() == target) {
            remoteSyncers.erase(i);
        }
    }
}

void
KFS::ReleaseAllServers(list<RemoteSyncSMPtr> &remoteSyncers)
{
    list<RemoteSyncSMPtr>::iterator i;
    while (1) {
        i = remoteSyncers.begin();
        if (i == remoteSyncers.end())
            break;
        RemoteSyncSMPtr r = *i;

        remoteSyncers.erase(i);
        r->Finish();
    }
}
