//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: MetaServerSM.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2006/06/07
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
// \file MetaServerSM.cc
// \brief Handle interactions with the meta server.
//
//----------------------------------------------------------------------------

#include "common/log.h"
#include "MetaServerSM.h"
#include "ChunkManager.h"
#include "ChunkServer.h"
#include "Utils.h"
#include "LeaseClerk.h"
#include "Replicator.h"

#include "libkfsIO/NetManager.h"
#include "libkfsIO/Globals.h"
#include "qcdio/qcutils.h"

#include <unistd.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <openssl/rand.h>

#include <algorithm>
#include <sstream>

using std::ostringstream;
using std::istringstream;
using std::find_if;
using std::list;
using std::string;

using namespace KFS;
using namespace KFS::libkfsio;

MetaServerSM KFS::gMetaServerSM;

inline static kfsSeq_t
InitialSeqNo()
{
    kfsSeq_t ret = 1;
    RAND_pseudo_bytes(reinterpret_cast<unsigned char*>(&ret), int(sizeof(ret)));
    return ((ret < 0 ? -ret : ret) >> 1);
}

MetaServerSM::MetaServerSM()
    : KfsCallbackObj(),
      ITimeout(),
      mCmdSeq(InitialSeqNo()),
      mRackId(-1),
      mSentHello(false),
      mHelloOp(NULL),
      mInactivityTimeout(3 * 60),
      mMaxReadAhead(4 << 10),
      mLastRecvCmdTime(0),
      mLastConnectTime(0),
      mConnectedTime(0),
      mCounters()
{
    // Force net manager construction here, to insure that net manager
    // destructor is called after gMetaServerSM destructor.
    globalNetManager();
    SET_HANDLER(this, &MetaServerSM::HandleRequest);
    mCounters.Clear();
}

MetaServerSM::~MetaServerSM()
{
    globalNetManager().UnRegisterTimeoutHandler(this);
    delete mHelloOp;
}

void 
MetaServerSM::SetMetaInfo(const ServerLocation &metaLoc, const char *clusterKey, 
                          int rackId, const string &md5sum, const Properties& prop)
{
    mLocation = metaLoc;
    mClusterKey = clusterKey;
    mRackId = rackId;
    mMD5Sum = md5sum;
    mInactivityTimeout = prop.getValue(
        "chunkServer.meta.inactivityTimeout", mInactivityTimeout);
    mMaxReadAhead      = prop.getValue(
        "chunkServer.meta.maxReadAhead",      mMaxReadAhead);
}

void
MetaServerSM::Init(int chunkServerPort, string chunkServerHostname)
{
    globalNetManager().RegisterTimeoutHandler(this);
    mChunkServerPort = chunkServerPort;
    mChunkServerHostname = chunkServerHostname;
}

void
MetaServerSM::Timeout()
{
    if (! IsConnected()) {
        if (mHelloOp) {
            if (! mSentHello) {
                return; // Wait for hello to come back.
            }
            delete mHelloOp;
            mHelloOp   = 0;
            mSentHello = false;
        }
        const time_t now = libkfsio::globalNetManager().Now();
        if (mLastConnectTime + 1 < now) {
            mLastConnectTime = now;
            Connect();
        }
        return;
    }
    if (! IsHandshakeDone()) {
        return;
    }
    DispatchOps();
    DispatchResponse();
    mNetConnection->StartFlush();
}

time_t
MetaServerSM::ConnectionUptime() const
{
    return (IsUp() ?
        (libkfsio::globalNetManager().Now() - mLastConnectTime) : 0);
}

int
MetaServerSM::Connect()
{
    if (mHelloOp) {
        return 0;
    }
    mCounters.mConnectCount++;
    mSentHello = false;
    TcpSocket * const sock = new TcpSocket();
    const bool nonBlocking = true;
    const int  ret         = sock->Connect(mLocation, nonBlocking);
    if (ret < 0 && ret != -EINPROGRESS) {
        KFS_LOG_STREAM_ERROR <<
            "connection to meter server failed:"
            " error: " << QCUtils::SysError(-ret) <<
        KFS_LOG_EOM;
        delete sock;
        return -1;
    }
    KFS_LOG_STREAM_INFO <<
        (ret < 0 ? "connecting" : "connected") <<
            " to metaserver " << mLocation.ToString() <<
    KFS_LOG_EOM;
    mNetConnection.reset(new NetConnection(sock, this));
    if (ret != 0) {
        mNetConnection->SetDoingNonblockingConnect();
    }
    // when the system is overloaded, we still want to add this
    // connection to the poll vector for reads; this ensures that we
    // get the heartbeats and other RPCs from the metaserver
    mNetConnection->EnableReadIfOverloaded();
    mNetConnection->SetInactivityTimeout(mInactivityTimeout);
    mNetConnection->SetMaxReadAhead(mMaxReadAhead);
    // Add this to the poll vector
    globalNetManager().AddConnection(mNetConnection);
    if (ret == 0) {
        SendHello();
    }
    return 0;
}

int
MetaServerSM::SendHello()
{
    if (mHelloOp) {
        return 0;
    }
#ifdef DEBUG
    verifyExecutingOnNetProcessor();
#endif

    if (! IsConnected()) {
        KFS_LOG_STREAM_DEBUG <<
            "unable to connect to meta server" <<
        KFS_LOG_EOM;
        return -1;
    }
    struct hostent *hent = 0;
    if (mChunkServerHostname.empty()) {
        char hostname[256];
        gethostname(hostname, sizeof(hostname));
        // switch to IP address so we can avoid repeated DNS lookups
        hent = gethostbyname(hostname);
    } else {
        // switch to IP address so we can avoid repeated DNS lookups
        hent = gethostbyname(mChunkServerHostname.c_str());
    }
    if (! hent) {
        hent = gethostbyname("localhost");
    }
    if (! hent) {
        die("Unable to resolve hostname");
    }
    in_addr ipaddr;
    memcpy(&ipaddr, hent->h_addr, hent->h_length);

    ServerLocation loc(inet_ntoa(ipaddr), mChunkServerPort);
    mHelloOp = new HelloMetaOp(nextSeq(), loc, mClusterKey, mMD5Sum, mRackId);
    mHelloOp->clnt = this;
    // send the op and wait for it comeback
    KFS::SubmitOp(mHelloOp);
    return 0;
}

void
MetaServerSM::DispatchHello()
{
#ifdef DEBUG
    verifyExecutingOnNetProcessor();
#endif

    if (! IsConnected()) {
        // don't have a connection...so, need to start the process again...
        delete mHelloOp;
        mHelloOp = 0;
        mSentHello = false;
        return;
    }
    mSentHello = true;
    IOBuffer::OStream os;
    mHelloOp->Request(os);
    mNetConnection->Write(&os);
    mNetConnection->StartFlush();
    KFS_LOG_STREAM_INFO <<
        "Sent hello to meta server: " << mHelloOp->Show() <<
    KFS_LOG_EOM;
}

///
/// Generic event handler.  Decode the event that occurred and
/// appropriately extract out the data and deal with the event.
/// @param[in] code: The type of event that occurred
/// @param[in] data: Data being passed in relative to the event that
/// occurred.
/// @retval 0 to indicate successful event handling; -1 otherwise.
///
int
MetaServerSM::HandleRequest(int code, void *data)
{

#ifdef DEBUG
    verifyExecutingOnNetProcessor();
#endif

    switch (code) {
    case EVENT_NET_READ: {
	    // We read something from the network.  Run the RPC that
	    // came in.
	    IOBuffer * const iobuf = (IOBuffer *) data;
            bool hasMsg;
            int  cmdLen = 0;
	    while ((hasMsg = IsMsgAvail(iobuf, &cmdLen))) {
                // if we don't have all the data for the command, bail
	        if (! HandleMsg(iobuf, cmdLen)) {
                    break;
                }
	    }
            int hdrsz;
            if (! hasMsg &&
                    (hdrsz = iobuf->BytesConsumable()) > MAX_RPC_HEADER_LEN) {
                KFS_LOG_STREAM_ERROR <<
                    "exceeded max request header size: " << hdrsz <<
                    ">" << MAX_RPC_HEADER_LEN <<
                    " closing connection: " << (IsConnected() ?
                        mNetConnection->GetPeerName() :
                        string("not connected")) <<
                KFS_LOG_EOM;
                iobuf->Clear();
                return HandleRequest(EVENT_NET_ERROR, 0);
            }
        }
	break;

    case EVENT_NET_WROTE:
        if (! mSentHello && ! mHelloOp) {
            SendHello();
        }
	// Something went out on the network.  For now, we don't
	// track it. Later, we may use it for tracking throttling
	// and such.
	break;

    case EVENT_CMD_DONE: {
	    // An op finished execution.  Send a response back
	    KfsOp* const op = (KfsOp *) data;
            if (op->op == CMD_META_HELLO) {
                DispatchHello();
                break;
            }
            // the op will be deleted after we send the response.
	    EnqueueResponse(op);
        }
	break;

    case EVENT_INACTIVITY_TIMEOUT:
    case EVENT_NET_ERROR:
	if (mNetConnection) {
            KFS_LOG_STREAM_DEBUG <<
                mLocation.ToString() <<
                " closing meta server connection " <<
            KFS_LOG_EOM;
            mNetConnection->Close();
            // Drop all leases.
            gLeaseClerk.UnregisterAllLeases();
            // Meta server will fail all replication requests on
            // disconnect anyway.
            Replicator::CancelAll();
        }
	break;

    default:
	assert(!"Unknown event");
	break;
    }
    return 0;
}

bool
MetaServerSM::HandleMsg(IOBuffer *iobuf, int msgLen)
{
    char buf[3];
    if (iobuf->CopyOut(buf, 3) == 3 &&
            buf[0] == 'O' && buf[1] == 'K' && (buf[2] & 0xFF) <= ' ') {
        // This is a response to some op we sent earlier
        return HandleReply(iobuf, msgLen);
    } else {
        // is an RPC from the server
        return HandleCmd(iobuf, msgLen);
    }
}

bool
MetaServerSM::HandleReply(IOBuffer *iobuf, int msgLen)
{
    Properties prop;
    {
        IOBuffer::IStream is(*iobuf, msgLen);
        const char separator = ':';
        prop.loadProperties(is, separator, false);
    }
    iobuf->Consume(msgLen);

    const kfsSeq_t seq    = prop.getValue("Cseq",  (kfsSeq_t)-1);
    const int      status = prop.getValue("Status",          -1);
    if (status == -EBADCLUSTERKEY) {
        KFS_LOG_STREAM_FATAL <<
            "Aborting due to cluster key mismatch; our key: " << mClusterKey <<
        KFS_LOG_EOM;
	exit(-1);
    }
    if (mHelloOp) {
        mCounters.mHelloCount++;
        const bool err = seq != mHelloOp->seq || status != 0;
        if (err) {
            KFS_LOG_STREAM_ERROR <<
                " bad hello response:"
                " seq: "    << seq << "/" << mHelloOp->seq <<
                " status: " << status <<
            KFS_LOG_EOM;
            mCounters.mHelloErrorCount++;
        }
        delete mHelloOp;
        mHelloOp = 0;
        if (err) {
            HandleRequest(EVENT_NET_ERROR, 0);
            return false;
        }
        mConnectedTime = libkfsio::globalNetManager().Now();
        ResubmitOps();
        return true;
    }
    list<KfsOp *>::iterator const iter = find_if(
        mDispatchedOps.begin(), mDispatchedOps.end(), OpMatcher(seq));
    if (iter == mDispatchedOps.end()) {
        string reply;
        prop.getList(reply, string(), string(" "));
        KFS_LOG_STREAM_DEBUG << "meta reply:"
            " no op found for: " << reply <<
        KFS_LOG_EOM;        
        return true;
    }
    KfsOp * const op = *iter;
    mDispatchedOps.erase(iter);
    op->status = status;
    KFS_LOG_STREAM_DEBUG <<
        "recv meta reply:"
        " seq: "    << seq <<
        " status: " << status <<
        " "         << op->Show() <<
    KFS_LOG_EOM;        
    // The op will be gotten rid of by this call.
    KFS::SubmitOpResponse(op);
    return true;
}

///
/// We have a command in a buffer.  It is possible that we don't have
/// everything we need to execute it (for example, for a stale chunks
/// RPC, we may not have received all the chunkids).  So, parse
/// out the command and if we have everything execute it.
/// 

bool
MetaServerSM::HandleCmd(IOBuffer *iobuf, int cmdLen)
{
    IOBuffer::IStream is(*iobuf, cmdLen);
    KfsOp*            op = 0;
    if (ParseCommand(is, &op) != 0) {
        is.Rewind(cmdLen);
        const string peer = IsConnected() ?
            mNetConnection->GetPeerName() : string("not connected");
        string line;
        int numLines = 32;
        while (--numLines >= 0 && getline(is, line)) {
            KFS_LOG_STREAM_ERROR << peer <<
                " invalid meta request: " << line <<
            KFS_LOG_EOM;
        }
        iobuf->Clear();
        HandleRequest(EVENT_NET_ERROR, 0);
        // got a bogus command
        return false;
    }

    const int contentLength = op->GetContentLength();
    const int remLen = cmdLen + contentLength - iobuf->BytesConsumable();
    if (remLen > 0) {
        // if we don't have all the data wait...
        if (remLen > mMaxReadAhead && mNetConnection) {
            mNetConnection->SetMaxReadAhead(remLen);
        }
        delete op;
        return false;
    }
    if (mNetConnection) {
        mNetConnection->SetMaxReadAhead(mMaxReadAhead);
    }
    iobuf->Consume(cmdLen);
    is.Rewind(contentLength);
    if (! op->ParseContent(is)) {
        KFS_LOG_STREAM_ERROR <<
            (IsConnected() ?  mNetConnection->GetPeerName() : "") <<
            " invalid content: " << op->statusMsg <<
            " cmd: " << op->Show() <<
        KFS_LOG_EOM;
        delete op;
        HandleRequest(EVENT_NET_ERROR, 0);
        return false;
    }
    iobuf->Consume(contentLength);
    mLastRecvCmdTime = libkfsio::globalNetManager().Now();
    op->clnt = this;
    if (op->op != CMD_HEARTBEAT) {
        KFS_LOG_STREAM_DEBUG <<
            "recv meta cmd:"
            " seq: " << op->seq <<
            " "      << op->Show() <<
            KFS_LOG_EOM; 
    }
    KFS::SubmitOp(op);
    return true;
}



void
MetaServerSM::EnqueueOp(KfsOp *op)
{
    op->seq = nextSeq();
    mPendingOps.enqueue(op);
    globalNetManager().Wakeup();
}

///
/// Queue the response to the meta server request.  The response is
/// generated by MetaRequest as per the protocol.
/// @param[in] op The request for which we finished execution.
///

void
MetaServerSM::EnqueueResponse(KfsOp *op)
{
    mPendingResponses.enqueue(op);
    globalNetManager().Wakeup();
}

void
MetaServerSM::DispatchOps()
{
#ifdef DEBUG
    verifyExecutingOnNetProcessor();
#endif

    while (IsHandshakeDone()) {
        if (! IsConnected()) {
            KFS_LOG_STREAM_INFO <<
                "meta handshake is not done, will dispatch later" <<
            KFS_LOG_EOM;
            return;
        }
        KfsOp* const op = mPendingOps.dequeue_nowait();
        if (! op) {
            return;
        }
        assert(op->op != CMD_META_HELLO);
        mDispatchedOps.push_back(op);
        KFS_LOG_STREAM_DEBUG <<
            "send meta cmd:"
            " seq: " << op->seq <<
            " "      << op->Show() <<
        KFS_LOG_EOM;
        IOBuffer::OStream os;
        op->Request(os);
        mNetConnection->Write(&os);
    }
}

void
MetaServerSM::DispatchResponse()
{
    KfsOp *op;

#ifdef DEBUG
    verifyExecutingOnNetProcessor();
#endif

    while (IsHandshakeDone() &&
            (op = mPendingResponses.dequeue_nowait())) {
        // fire'n'forget.
        if (op->op != CMD_HEARTBEAT) {
            KFS_LOG_STREAM_DEBUG <<
                "send meta reply:"
                " seq: "     << op->seq <<
                (op->statusMsg.empty() ? "" : " msg: ") << op->statusMsg <<
                " status: "  << op->status <<
                " "          << op->Show() <<
                KFS_LOG_EOM;
        }
        if (op->op == CMD_ALLOC_CHUNK) {
            mCounters.mAllocCount++;
            if (op->status < 0) {
                mCounters.mAllocErrorCount++;
            }
        }
        IOBuffer::OStream os;
        op->Response(os);
        mNetConnection->Write(&os);
        delete op;
    }
}

class OpDispatcher {
    NetConnectionPtr conn;
public:
    OpDispatcher(NetConnectionPtr &c) : conn(c) { }
    void operator() (KfsOp *op) {
        IOBuffer::OStream os;
        op->Request(os);
        conn->Write(&os);
    }
};

// After re-establishing connection to the server, resubmit the ops.
void
MetaServerSM::ResubmitOps()
{
    for_each(mDispatchedOps.begin(), mDispatchedOps.end(),
             OpDispatcher(mNetConnection));
}
