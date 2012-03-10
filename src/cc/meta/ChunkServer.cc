//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: ChunkServer.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2006/06/06
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

#include "ChunkServer.h"
#include "LayoutManager.h"
#include "NetDispatch.h"
#include "util.h"
#include "libkfsIO/Globals.h"
#include "qcdio/qcutils.h"

using namespace KFS;
using namespace libkfsio;

#include <cassert>
#include <string>
#include <sstream>
using std::string;
using std::istringstream;
using std::max;

#include <boost/lexical_cast.hpp>
#include <openssl/rand.h>

#include "common/log.h"

static inline time_t TimeNow()
{
	return libkfsio::globalNetManager().Now();
}

int ChunkServer::sHeartbeatTimeout     = 60;
int ChunkServer::sHeartbeatInterval    = 30;
int ChunkServer::sHeartbeatLogInterval = 100;
const int kMaxReadAhead             = 4 << 10;
const int kMaxRequestResponseHeader = 64 << 10;

void ChunkServer::SetParameters(const Properties& prop)
{
	sHeartbeatTimeout  = prop.getValue(
		"metaServer.chunkServer.heartbeatTimeout",
                sHeartbeatTimeout);
	sHeartbeatInterval = max(3, prop.getValue(
		"metaServer.chunkServer.heartbeatInterval",
                sHeartbeatInterval));
        sHeartbeatLogInterval = prop.getValue(
		"metaServer.chunkServer.heartbeatLogInterval",
                sHeartbeatLogInterval);
}

static seq_t RandomSeqNo()
{
    seq_t ret = 0;
    RAND_pseudo_bytes(
        reinterpret_cast<unsigned char*>(&ret), int(sizeof(ret)));
    return ((ret < 0 ? -ret : ret) >> 1);
}

ChunkServer::ChunkServer() :
	mSeqNo(RandomSeqNo()),
	mHelloDone(false), mDown(false), mHeartbeatSent(false),
	mHeartbeatSkipped(false), mLastHeartbeatSent(TimeNow()),
	mCanBeChunkMaster(false),
	mIsRetiring(false), mRackId(-1), 
	mNumCorruptChunks(0), mTotalSpace(0), mUsedSpace(0), mAllocSpace(0), 
	mNumChunks(0), mCpuLoadAvg(0.0), mNumDrives(0), mNumChunkWrites(0),
        mNumAppendsWithWid(0),
	mNumChunkWriteReplications(0), mNumChunkReadReplications(0),
	mLostChunks(0), mUptime(0), mHeartbeatProperties(),
	mRestartScheduledFlag(false), mRestartQueuedFlag(false),
	mRestartScheduledTime(0), mLastHeartBeatLoggedTime(0), mDownReason()
{
	// this is used in emulation mode...

}

ChunkServer::ChunkServer(NetConnectionPtr &conn) :
	mSeqNo(RandomSeqNo()), mNetConnection(conn), 
	mHelloDone(false), mDown(false), mHeartbeatSent(false),
	mHeartbeatSkipped(false), mLastHeartbeatSent(TimeNow()),
	mCanBeChunkMaster(false), mIsRetiring(false), mRackId(-1), 
	mNumCorruptChunks(0), mTotalSpace(0), mUsedSpace(0), mAllocSpace(0), 
	mNumChunks(0), mCpuLoadAvg(0.0), mNumDrives(0), mNumChunkWrites(0), 
        mNumAppendsWithWid(0),
	mNumChunkWriteReplications(0), mNumChunkReadReplications(0),
        mLostChunks(0), mUptime(0), mHeartbeatProperties(),
	mRestartScheduledFlag(false), mRestartQueuedFlag(false),
	mRestartScheduledTime(0), mLastHeartBeatLoggedTime(0), mDownReason()
{
	assert(mNetConnection);
	SET_HANDLER(this, &ChunkServer::HandleRequest);
	mNetConnection->SetInactivityTimeout(sHeartbeatInterval);
	mNetConnection->SetMaxReadAhead(kMaxReadAhead);
	KFS_LOG_STREAM_INFO <<
		"new ChunkServer " << (const void*)this << " " <<
		mNetConnection->GetPeerName() <<
	KFS_LOG_EOM;
}

ChunkServer::~ChunkServer()
{
	KFS_LOG_STREAM_DEBUG << ServerID() <<
		" ~ChunkServer " << (const void*)this <<
	KFS_LOG_EOM;
        if (mNetConnection) {
                mNetConnection->Close();
        }
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
ChunkServer::HandleRequest(int code, void *data)
{
	IOBuffer             *iobuf;
	int                  msgLen;
	MetaChunkRequest     *op;
	bool                 gotMsgHdr;
	ChunkServerPtr const doNotDelete(shared_from_this());

	switch (code) {
	case EVENT_NET_READ:
		// We read something from the network.  It is
		// either an RPC (such as hello) or a reply to
		// an RPC we sent earlier.
		iobuf = (IOBuffer *) data;
		while ((gotMsgHdr = IsMsgAvail(iobuf, &msgLen))) {
			const int retval = HandleMsg(iobuf, msgLen);
			if (retval < 0) {
				iobuf->Clear();
				Error(mHelloDone ?
					"request or response parse error" :
					"failed to parse hello message");
				return 0;
			}
			if (retval > 0) {
				break; // Need more data
			}
		}
		if (! gotMsgHdr && iobuf->BytesConsumable() >
				kMaxRequestResponseHeader) {
			iobuf->Clear();
			Error(mHelloDone ?
				"request or response header length"
					" exceeds max allowed" :
				"hello message header length"
					" exceeds max allowed");
			return 0;
		}
		break;

	case EVENT_CMD_DONE:
		assert(mHelloDone && data);
		op = (MetaChunkRequest *) data;
		if (!mDown) {
			SendResponse(op);
		}	
		// nothing left to be done...get rid of it
		delete op;
		break;

	case EVENT_NET_WROTE:
		// Something went out on the network.  
		break;

	case EVENT_INACTIVITY_TIMEOUT:
		// Check heartbeat timeout.
		if (mHelloDone || mLastHeartbeatSent + sHeartbeatTimeout >
				TimeNow()) {
			break;
		}
		Error("hello timeout");
		return 0;

	case EVENT_NET_ERROR:
		Error("communication error");
		return 0;

	default:
		assert(!"Unknown event");
		return -1;
	}
	if (mHelloDone) {
		Heartbeat();
	} else if (code != EVENT_INACTIVITY_TIMEOUT) {
		mLastHeartbeatSent = TimeNow();
	}
	return 0;
}

void
ChunkServer::ForceDown()
{
	if (mDown) {
		return;
	}
	KFS_LOG_STREAM_ERROR <<
		"forcing chunk server " << ServerID() <<
		"/" << (mNetConnection ? mNetConnection->GetPeerName() :
			string("not connected")) <<
		" down" <<
	KFS_LOG_EOM;
	if (mNetConnection) {
		mNetConnection->Close();
		mNetConnection.reset();
	}
	mDown       = true;
	// Take out the server from write-allocation
	mTotalSpace = 0;
	mAllocSpace = 0;
	mUsedSpace  = 0;
	FailDispatchedOps();
        gNetDispatch.GetChunkServerFactory()->RemoveServer(this);
}

void
ChunkServer::Error(const char* errorMsg)
{
	const int socketErr = (mNetConnection && mNetConnection->IsGood()) ?
		mNetConnection->GetSocketError() : 0;
	KFS_LOG_STREAM_ERROR <<
		"chunk server " << ServerID() <<
		"/" << (mNetConnection ? mNetConnection->GetPeerName() :
			string("not connected")) <<
		" down" <<
		(mRestartQueuedFlag ? " restart" : "") <<
		" reason: " << (errorMsg ? errorMsg : "unspecified") <<
		" socket error: " << QCUtils::SysError(socketErr) <<
	KFS_LOG_EOM;
	if (mNetConnection) {
		mNetConnection->Close();
		mNetConnection.reset();
	}
	if (mDownReason.empty() && mRestartQueuedFlag) {
		mDownReason = "restart";
	}
	mDown       = true;
	// Take out the server from write-allocation
	mTotalSpace = 0;
	mAllocSpace = 0;
	mUsedSpace  = 0;
	FailDispatchedOps();
	if (mHelloDone) {
		// force the server down thru the main loop to avoid races
		MetaBye* const mb = new MetaBye(0, shared_from_this());
		mb->clnt = this;
		gNetDispatch.GetChunkServerFactory()->RemoveServer(this);
		submit_request(mb);
	} else {
		gNetDispatch.GetChunkServerFactory()->RemoveServer(this);
	}
}

///
/// We have a message from the chunk server.  The message we got is one
/// of:
///  -  a HELLO message from the server 
///  -  it is a response to some command we previously sent
///  -  is an RPC from the chunkserver
/// 
/// Of these, for the first and third case,create an op and
/// send that down the pike; in the second case, retrieve the op from
/// the pending list, attach the response, and push that down the pike.
///
/// @param[in] iobuf: Buffer containing the command
/// @param[in] msgLen: Length of the command in the buffer
/// @retval 0 if we handled the message properly; -1 on error; 
///   1 if there is more data needed for this message and we haven't
///   yet received the data.
int
ChunkServer::HandleMsg(IOBuffer *iobuf, int msgLen)
{
	if (!mHelloDone) {
		return HandleHelloMsg(iobuf, msgLen);
	}
	char buf[3];
	if (iobuf->CopyOut(buf, 3) == 3 &&
			buf[0] == 'O' && buf[1] == 'K' && (buf[2] & 0xFF) <= ' ') {
		return HandleReply(iobuf, msgLen);
	}
	return HandleCmd(iobuf, msgLen);
}

MetaRequest*
ChunkServer::GetOp(IOBuffer& iobuf, int msgLen, const char* errMsgPrefix)
{
        MetaRequest *op = 0;
        IOBuffer::IStream is(iobuf, msgLen);
        if (ParseCommand(is, &op) >= 0) {
		return op;
	}
	const string loc      =
		ServerID() + "/" + mNetConnection->GetPeerName();
	int          maxLines = 64;
	const char*  prefix   = errMsgPrefix ? errMsgPrefix : "";
        string       line;
	is.Rewind(msgLen);
	while (--maxLines >= 0 && getline(is, line)) {
		KFS_LOG_STREAM_ERROR <<
			loc << " " << prefix << ": " << line <<
		KFS_LOG_EOM;
	}
	iobuf.Consume(msgLen);
	return 0;
}

/// Case #1: Handle Hello message from a chunkserver that
/// just connected to us.
int
ChunkServer::HandleHelloMsg(IOBuffer *iobuf, int msgLen)
{
        assert(!mHelloDone);

        MetaRequest * const op = GetOp(*iobuf, msgLen, "invalid hello");
	if (! op) {
		return -1;
	}
	if (op->op != META_HELLO ||
			iobuf->BytesConsumable() >= msgLen +
			static_cast<const MetaHello*>(op)->contentLength) {
        	IOBuffer::IStream is(*iobuf, msgLen);
		const string      loc      = mNetConnection->GetPeerName();
		int               maxLines = 64;
        	string            line;
		while (--maxLines >= 0 && getline(is, line)) {
			string::iterator last = line.end();
			if (last != line.begin() && *--last == '\r') {
				line.erase(last);
			}
			KFS_LOG_STREAM((op->op == META_HELLO) ?
					MsgLogger::kLogLevelINFO :
					MsgLogger::kLogLevelERROR) <<
				"new: " << loc << " " << line <<
			KFS_LOG_EOM;
		}
	}
        // We should only get a HELLO message here; anything else is bad.
        if (op->op != META_HELLO) {
		KFS_LOG_STREAM_ERROR << mNetConnection->GetPeerName() <<
	    		" unexpected request, expected hello" <<
		KFS_LOG_EOM;
		iobuf->Consume(msgLen);
		delete op;
		return -1;
        }

        MetaHello * const helloOp = static_cast<MetaHello *> (op);
        op->clnt = this;
        helloOp->server = shared_from_this();
        // make sure we have the chunk ids...
        if (helloOp->contentLength > 0) {
            const int nAvail = iobuf->BytesConsumable() - msgLen;
            if (nAvail < helloOp->contentLength) {
                // need to wait for data...
		mNetConnection->SetMaxReadAhead(
			std::max(kMaxReadAhead, helloOp->contentLength - nAvail));
                delete op;
                return 1;
            }
            // we have everything
            iobuf->Consume(msgLen);
            // get the chunkids
            IOBuffer::IStream is(*iobuf, helloOp->contentLength);
	    helloOp->chunks.clear();
	    helloOp->notStableChunks.clear();
            helloOp->notStableAppendChunks.clear();
            const size_t numStable(max(0, helloOp->numChunks));
	    helloOp->chunks.reserve(helloOp->numChunks);
            const size_t nonStableAppendNum(max(0, helloOp->numNotStableAppendChunks));
	    helloOp->notStableAppendChunks.reserve(nonStableAppendNum);
            const size_t nonStableNum(max(0, helloOp->numNotStableChunks));
	    helloOp->notStableChunks.reserve(nonStableNum);
	    for (int j = 0; j < 3; ++j) {
	    	vector<ChunkInfo>& chunks = j == 0 ?
			helloOp->chunks : (j == 1 ?
			helloOp->notStableAppendChunks :
			helloOp->notStableChunks);
		int i = j == 0 ?
			helloOp->numChunks : (j == 1 ?
			helloOp->numNotStableAppendChunks :
			helloOp->numNotStableChunks);
        	while (i-- > 0) {
                    ChunkInfo c;
                    if (! (is >> c.allocFileId >> c.chunkId >> c.chunkVersion)) {
			    break;
		    }
                    chunks.push_back(c);
        	}
	    }
            iobuf->Consume(helloOp->contentLength);
	    if (helloOp->chunks.size() != numStable ||
	    		helloOp->notStableAppendChunks.size() !=
			    nonStableAppendNum ||
	    		helloOp->notStableChunks.size() !=
			    nonStableNum) {
		KFS_LOG_STREAM_ERROR << mNetConnection->GetPeerName() <<
	    		" invalid or short chunk list:"
			" expected: " << helloOp->numChunks <<
			"/"           << helloOp->numNotStableAppendChunks <<
			"/"           << helloOp->numNotStableChunks <<
			" got: "      << helloOp->chunks.size() <<
			"/"           << helloOp->notStableAppendChunks.size() <<
			"/"           << helloOp->notStableChunks.size() <<
			" last good chunk: "     <<
				(helloOp->chunks.empty() ? -1 :
					helloOp->chunks.back().chunkId) <<
			"/" <<
				(helloOp->notStableAppendChunks.empty() ? -1 :
					helloOp->notStableAppendChunks.back().chunkId) <<
			"/" <<
				(helloOp->notStableChunks.empty() ? -1 :
					helloOp->notStableChunks.back().chunkId) <<
			" content length: " << helloOp->contentLength <<
		KFS_LOG_EOM;
		delete op;
		return -1;
	    }
        } else {
            // Message is ready to be pushed down.  So remove it.
            iobuf->Consume(msgLen);
        }
	mNetConnection->SetMaxReadAhead(kMaxReadAhead);
	// Hello message successfully processed.  Setup to handle RPCs
	SET_HANDLER(this, &ChunkServer::HandleRequest);
        // Hello done.
        mHelloDone = true;
        mLastHeard = TimeNow();
	helloOp->peerName = mNetConnection->GetPeerName();
	mUptime = helloOp->uptime;
	mNumAppendsWithWid = helloOp->numAppendsWithWid;
        mHeartbeatSent     = true;
        mLastHeartbeatSent = mLastHeard;
        Enqueue(new MetaChunkHeartbeat(NextSeq(), this));
        mNumChunkWrites = (int)(helloOp->notStableAppendChunks.size() +
            helloOp->notStableChunks.size());
        submit_request(op);
        return 0;
}

///
/// Case #2: Handle an RPC from a chunkserver.
///
int
ChunkServer::HandleCmd(IOBuffer *iobuf, int msgLen)
{
        assert(mHelloDone);

        MetaRequest * const op = GetOp(*iobuf, msgLen, "invalid request");
	if (! op) {
		return -1;
	}
        // Message is ready to be pushed down.  So remove it.
        iobuf->Consume(msgLen);
	if (op->op == META_CHUNK_CORRUPT) {
		MetaChunkCorrupt *ccop = static_cast<MetaChunkCorrupt *>(op);
		ccop->server = shared_from_this();
	}
        op->clnt = this;
        submit_request(op);
        return 0;
}

///
/// Case #3: Handle a reply from a chunkserver to an RPC we
/// previously sent.
///
int
ChunkServer::HandleReply(IOBuffer *iobuf, int msgLen)
{
        assert(mHelloDone);

        // We got a response for a command we previously
        // sent.  So, match the response to its request and
        // resume request processing.
        IOBuffer::IStream is(*iobuf, msgLen);
        Properties        prop;
        if (! ParseResponse(is, prop)) {
		return -1;
	}
        // Message is ready to be pushed down.  So remove it.
        iobuf->Consume(msgLen);

        const seq_t             cseq = prop.getValue("Cseq", (seq_t) -1);
        MetaChunkRequest* const op   = FindMatchingRequest(cseq);
	if (! op) {
		// Uh-oh...this can happen if the server restarts between sending
		// the message and getting reply back
		// assert(!"Unable to find command for a response");
		KFS_LOG_STREAM_WARN << ServerID() <<
	    		" unable to find command for response cseq: " << cseq <<
		KFS_LOG_EOM;
		return 0;
	}

	mLastHeard = TimeNow();
        op->statusMsg = prop.getValue("Status-message", "");
        op->status    = prop.getValue("Status",         -1);
	op->handleReply(prop);
        if (op->op == META_CHUNK_HEARTBEAT) {
		mTotalSpace        = prop.getValue("Total-space",     (long long) 0);
		mUsedSpace         = prop.getValue("Used-space",      (long long) 0);
		mNumChunks         = prop.getValue("Num-chunks",                  0);
		mCpuLoadAvg        = prop.getValue("CPU-load-avg",              0.0);
		mNumDrives         = prop.getValue("Num-drives",                  0);
                mUptime            = prop.getValue("Uptime",          (long long) 0);
                mLostChunks        = prop.getValue("Chunk-corrupted", (long long) 0);
                mNumChunkWrites    = max(0,
                                     prop.getValue("Num-writable-chunks",         0));
                mNumAppendsWithWid = prop.getValue("Num-appends-with-wids", (long long)0);
		mAllocSpace        = mUsedSpace + mNumChunkWrites * CHUNKSIZE;
		mHeartbeatSent     = false;
		mHeartbeatSkipped = mLastHeartbeatSent + sHeartbeatInterval < TimeNow();
                mHeartbeatProperties.swap(prop);
                if (sHeartbeatLogInterval > 0 &&
                            mLastHeartBeatLoggedTime +
                                sHeartbeatLogInterval <= mLastHeard) {
                        mLastHeartBeatLoggedTime = mLastHeard;
                        string hbp;
                        mHeartbeatProperties.getList(hbp, " ", "");
                        KFS_LOG_STREAM_DEBUG <<
                            "===chunk=server: " << mLocation.hostname <<
                            ":" << mLocation.port <<
                            " responsive=" << IsResponsiveServer() <<
                            " retiring="  << IsRetiring() <<
                            " restarting=" << IsRestartScheduled() <<
                            hbp <<
                        KFS_LOG_EOM;
                }
	}
        op->resume();
        return 0;
}

///
/// The response sent by a chunkserver is of the form:
/// OK \r\n
/// Cseq: <seq #>\r\n
/// Status: <status> \r\n
/// {<other header/value pair>\r\n}*\r\n
///
/// @param[in] buf Buffer containing the response
/// @param[in] bufLen length of buf
/// @param[out] prop  Properties object with the response header/values
/// 
bool
ChunkServer::ParseResponse(std::istream& is, Properties &prop)
{
        string token;
        is >> token;
        // Response better start with OK
        if (token.compare("OK") != 0) {
		int maxLines = 32;
		do {
                	KFS_LOG_STREAM_ERROR << ServerID() <<
				" bad response header: " << token <<
			KFS_LOG_EOM;
		} while (--maxLines > 0 && getline(is, token));
                return false;
        }
        const char separator = ':';
        prop.loadProperties(is, separator, false);
	return true;
}

// Helper functor that matches ops by sequence #'s
class OpMatch {
	const seq_t myseq;
public:
	OpMatch(seq_t s) : myseq(s) { }
	bool operator() (const MetaChunkRequest *r) {
		return (r->opSeqno == myseq);
	}
};

///
/// Request/responses are matched based on sequence #'s.
///
MetaChunkRequest *
ChunkServer::FindMatchingRequest(seq_t cseq)
{
        list<MetaChunkRequest *>::iterator const iter = find_if(
		mDispatchedReqs.begin(), mDispatchedReqs.end(), OpMatch(cseq));
	if (iter == mDispatchedReqs.end()) {
		return 0;
	}
	MetaChunkRequest* const op = *iter;
	mDispatchedReqs.erase(iter);
	return op;
}

int
ChunkServer::TimeSinceLastHeartbeat() const
{
    return (TimeNow() - mLastHeartbeatSent);
}

///
/// Queue an RPC request
///
void
ChunkServer::Enqueue(MetaChunkRequest *r) 
{
	if (mDown) {
		r->status = -EIO;
		r->resume();
		return;
	}
        mPendingReqs.enqueue(r);
	Dispatch();
}

int
ChunkServer::AllocateChunk(MetaAllocate *r, int64_t leaseId)
{
        mAllocSpace += CHUNKSIZE;
	mNumChunkWrites++;
        Enqueue(new MetaChunkAllocate(NextSeq(), r, this, leaseId));
        return 0;
}

int
ChunkServer::DeleteChunk(chunkId_t chunkId)
{
        mAllocSpace = max((int64_t)0, mAllocSpace - (int64_t)CHUNKSIZE);
	if (IsRetiring()) {
		EvacuateChunkDone(chunkId);
	}
	Enqueue(new MetaChunkDelete(NextSeq(), this, chunkId));
	return 0;
}

int
ChunkServer::TruncateChunk(chunkId_t chunkId, off_t s)
{
        mAllocSpace = max((int64_t)0, mAllocSpace - ((int64_t)CHUNKSIZE - s));
	Enqueue(new MetaChunkTruncate(NextSeq(), this, chunkId, s));
	return 0;
}

int
ChunkServer::GetChunkSize(fid_t fid, chunkId_t chunkId, const string &pathname)
{
	Enqueue(new MetaChunkSize(NextSeq(), this, fid, chunkId, pathname));
	return 0;
}

int
ChunkServer::BeginMakeChunkStable(fid_t fid, chunkId_t chunkId, seq_t chunkVersion)
{
	Enqueue(new MetaBeginMakeChunkStable(
		NextSeq(), this, mLocation, fid, chunkId, chunkVersion
	));
	return 0;
}

int
ChunkServer::MakeChunkStable(fid_t fid, chunkId_t chunkId, seq_t chunkVersion,
    off_t chunkSize, bool hasChunkChecksum, uint32_t chunkChecksum, bool addPending)
{
	Enqueue(new MetaChunkMakeStable(
		NextSeq(), shared_from_this(),
		fid, chunkId, chunkVersion,
		chunkSize, hasChunkChecksum, chunkChecksum, addPending
	));
	return 0;
}

int
ChunkServer::ReplicateChunk(fid_t fid, chunkId_t chunkId, seq_t chunkVersion,
				const ServerLocation &loc)
{
	MetaChunkReplicate * const r = new MetaChunkReplicate(
		NextSeq(), this, fid, chunkId, chunkVersion, loc);
	r->server = shared_from_this();
	mNumChunkWriteReplications++;
	mNumChunkWrites++;
        mAllocSpace += CHUNKSIZE;
	Enqueue(r);
	return 0;
}

void
ChunkServer::Heartbeat()
{
	if (! mHelloDone || mDown) {
		return;
	}
	assert(mNetConnection);
	const time_t now           = TimeNow();
	const int    timeSinceSent = (int)(now - mLastHeartbeatSent);
	if (mHeartbeatSent) {
		if (sHeartbeatTimeout >= 0 &&
				timeSinceSent >= sHeartbeatTimeout) {
			ostringstream os;
			os << "heartbeat timed out, sent: " <<
				timeSinceSent << " sec. ago";
			Error(os.str().c_str());
			return;
		}
		// If a request is outstanding, don't send one more
		if (! mHeartbeatSkipped &&
				mLastHeartbeatSent + sHeartbeatInterval < now) {
			mHeartbeatSkipped = true;
			KFS_LOG_STREAM_INFO << ServerID() <<
				" skipping heartbeat send,"
				" last sent " << timeSinceSent << " sec. ago" <<
			KFS_LOG_EOM;
		}
		mNetConnection->SetInactivityTimeout(sHeartbeatTimeout < 0 ?
			sHeartbeatTimeout :
			sHeartbeatTimeout - timeSinceSent
		);
		return;
	}
	if (timeSinceSent >= sHeartbeatInterval) {
		KFS_LOG_STREAM_DEBUG << ServerID() <<
			" sending heartbeat,"
			" last sent " << timeSinceSent << " sec. ago" <<
		KFS_LOG_EOM;
		mHeartbeatSent     = true;
		mLastHeartbeatSent = now;
		mNetConnection->SetInactivityTimeout(
			(sHeartbeatTimeout >= 0 &&
				sHeartbeatTimeout < sHeartbeatInterval) ?
			sHeartbeatTimeout : sHeartbeatInterval
		);
        	Enqueue(new MetaChunkHeartbeat(NextSeq(), this));
		return;
	}
	mNetConnection->SetInactivityTimeout(sHeartbeatInterval - timeSinceSent);
}

void
ChunkServer::NotifyStaleChunks(const vector<chunkId_t> &staleChunkIds)
{
	mAllocSpace = max((int64_t)0, mAllocSpace - (int64_t)(CHUNKSIZE * staleChunkIds.size()));
	MetaChunkStaleNotify * const r = new MetaChunkStaleNotify(NextSeq(), this);
	r->staleChunkIds = staleChunkIds;
	Enqueue(r);

}

void
ChunkServer::NotifyStaleChunk(chunkId_t staleChunkId)
{
	mAllocSpace = max((int64_t)0, mAllocSpace - (int64_t)CHUNKSIZE);
	MetaChunkStaleNotify * const r = new MetaChunkStaleNotify(NextSeq(), this);
	r->staleChunkIds.push_back(staleChunkId);
	Enqueue(r);
}

void
ChunkServer::NotifyChunkVersChange(fid_t fid, chunkId_t chunkId, seq_t chunkVers)
{
	Enqueue(new MetaChunkVersChange(NextSeq(), this, fid, chunkId, chunkVers));
}

void
ChunkServer::SetRetiring()
{
	mIsRetiring = true;
	mRetireStartTime = TimeNow();
	KFS_LOG_STREAM_INFO << ServerID() <<
		" initiation of retire for " << mNumChunks << " chunks" <<
	KFS_LOG_EOM;
}

void
ChunkServer::EvacuateChunkDone(chunkId_t chunkId)
{
	if (!mIsRetiring) {
		return;
	}
	mEvacuatingChunks.erase(chunkId);
	if (mEvacuatingChunks.empty()) {
		KFS_LOG_STREAM_INFO << ServerID() <<
			" evacuation of chunks done, retiring" <<
		KFS_LOG_EOM;
		Retire();
	}
}

void
ChunkServer::Retire()
{
	Enqueue(new MetaChunkRetire(NextSeq(), this));
}

void
ChunkServer::SetProperties(const Properties& props)
{
	Enqueue(new MetaChunkSetProperties(NextSeq(), this, props));
}

void
ChunkServer::Restart()
{
	mRestartQueuedFlag = true;
	Enqueue(new MetaChunkServerRestart(NextSeq(), this));
}

void
ChunkServer::Dispatch()
{
	ChunkServerPtr const doNotDelete(shared_from_this());
	PendingReqs::Queue reqs;
	mPendingReqs.swap(reqs);
	IOBuffer::OStream os;
	for (PendingReqs::Queue::const_iterator it = reqs.begin();
			it != reqs.end();
			++it) {
		MetaChunkRequest& r = **it;
		if (! mNetConnection) {
			// Server is dead...so, drop the op
			r.status = -EIO;
			r.resume();
			continue;
		}
        	// Get the request into string format
		r.request(os);
		// Send it on its merry way
		if (r.op == META_CHUNK_REPLICATE) {
			KFS_LOG_STREAM_INFO << ServerID() <<
				" dispatched re-replication request: "
				" seq: " << r.opSeqno << " " << r.Show() <<
			KFS_LOG_EOM;
		}
		// Notify the server the op is dispatched
		Dispatched(&r);
	}
	if (mNetConnection) {
		mNetConnection->Write(&os);
		mNetConnection->StartFlush();
	}
}

// Helper functor that fails an op with an error code.
class OpFailer {
	const int errCode;
public:
	OpFailer(int c) : errCode(c) { };
	void operator() (MetaChunkRequest *op) {
                op->status = errCode;
                op->resume();
	}
};

void
ChunkServer::FailDispatchedOps()
{
	DispatchedReqs reqs;
	mDispatchedReqs.swap(reqs);
	for_each(reqs.begin(), reqs.end(), OpFailer(-EIO));
}

void
ChunkServer::FailPendingOps()
{
	PendingReqs::Queue reqs;
	mPendingReqs.swap(reqs);
	for_each(reqs.begin(), reqs.end(), OpFailer(-EIO));
}

void
ChunkServer::GetRetiringStatus(string &result)
{
	if (!mIsRetiring) {
		return;
	}
	ostringstream ost;
	char timebuf[64];
	ctime_r(&mRetireStartTime, timebuf);
	char* const cr = strchr(timebuf, '\n');
	if (cr) {
		*cr = '\0';
	}
	ost << "s=" << mLocation.hostname << ", p=" << mLocation.port 
		<< ", started=" << timebuf 
		<< ", numLeft=" << mEvacuatingChunks.size() << ", numDone=" 
		<< mNumChunks - mEvacuatingChunks.size() << '\t';
	result += ost.str();
}

void
ChunkServer::Ping(string &result)
{
	// for nodes taken out of write allocation, send the info back; this allows
	// the UI to color these nodes differently
	const bool     isOverloaded = GetSpaceUtilization() > MAX_SERVER_SPACE_UTIL_THRESHOLD;
	const time_t   now          = TimeNow();
	const uint64_t freeSpace    = mTotalSpace - mUsedSpace;
        const double   div          = double(1L << ((mTotalSpace < (1L << 30)) ? 20 : 30));
        const char*    mult         = (mTotalSpace < (1L << 30)) ? "MB" : "GB";
        ostringstream ost;
	ost << "s=" << mLocation.hostname << ", p=" << mLocation.port 
		<< ", rack=" << mRackId 
		<< ", used=" << (mUsedSpace / div)
	    	<< "(" << mult << "), free=" << (freeSpace / div)
		<< "(" << mult << "), util=" << GetSpaceUtilization() * 100.0 
		<< "%, nblocks=" << mNumChunks 
		<< ", lastheard=" << now - mLastHeard << " (sec)"
		<< ", ncorrupt=" << mNumCorruptChunks
		<< ", nchunksToMove=" << mChunksToMove.size()
		<< ", numDrives=" << mNumDrives
                << (isOverloaded ? ", overloaded=1" : "")
		<< "\t"
	;
	result += ost.str();
}

void
ChunkServer::SendResponse(MetaChunkRequest *op)
{
        IOBuffer::OStream os;
        op->response(os);
        mNetConnection->Write(&os);
        mNetConnection->StartFlush();
}

bool
ChunkServer::ScheduleRestart(int64_t gracefulRestartTimeout, int64_t gracefulRestartAppendWithWidTimeout)
{
	if (mDown) {
		return true;
	}
	if (! mRestartScheduledFlag) {
		mRestartScheduledTime = TimeNow();
		mRestartScheduledFlag = true;
	}
	if ((mNumChunkWrites <= 0 &&
			mNumAppendsWithWid <= 0 &&
			mDispatchedReqs.empty() &&
			mPendingReqs.empty()) ||
			mRestartScheduledTime +
				(mNumAppendsWithWid <= 0 ?
					gracefulRestartTimeout :
					max(gracefulRestartTimeout,
						gracefulRestartAppendWithWidTimeout))
				< TimeNow()) {
                mDownReason = "restarting";
		Error("reconnect before restart");
		return true;
	}
	return false;
}
