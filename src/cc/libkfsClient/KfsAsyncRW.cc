//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: KfsAsyncRW.cc 3072 2011-09-16 21:31:44Z sriramr $
//
// Created 2009/08/06
//
// Copyright 2009 Quantcast Corporation.  All rights reserved.
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
//----------------------------------------------------------------------------

#include "libkfsIO/Checksum.h"
#include "libkfsIO/Globals.h"
#include "libkfsIO/IOBuffer.h"
#include "KfsAsyncRW.h"
#include "KfsClientInt.h"

#include <istream>

using std::min;
using std::istringstream;

using namespace KFS;

static const int MAX_READOP_RPC_HDR_LEN = 1024;
static const int MAX_CONCURRENT_PREFETCHES = 16;
// if there is no data for over 90 secs, kill the worker
static const int READ_WORKER_INACTIVITY_TIMEOUT = 90;
// a prefetch worker shouldn't run for too long.  This is to handle
// the case where data is trickling in from the server.  The max
// lifetime we are giving it: 2 mins + 90 secs
static const int READ_WORKER_LIFETIME = 120;

static bool
VerifyChecksum(ReadOp &readOp)
{
    for (size_t pos = 0; pos < readOp.contentLength; pos += CHECKSUM_BLOCKSIZE) {
        size_t len = min(CHECKSUM_BLOCKSIZE, (uint32_t) (readOp.contentLength - pos));
        uint32_t cksum = ComputeBlockChecksum(readOp.contentBuf + pos, len);
        uint32_t cksumIndex = pos / CHECKSUM_BLOCKSIZE;
        if (readOp.checksums.size() < cksumIndex) {
            // didn't get all the checksums
            KFS_LOG_VA_DEBUG("Didn't get checksum for offset: %lld",
                             readOp.offset + pos);
            continue;
        }

        uint32_t serverCksum = readOp.checksums[cksumIndex];
        if (serverCksum != cksum) {
            KFS_LOG_VA_INFO("Checksum mismatch: got = %u; computed = %u", serverCksum, cksum);
            return false;
        }
    }
    return true;
}

void
Asyncer::Start()
{
    mStopFlag = false;
    const int kStackSize = 128 << 10;
    mThread.Start(this, kStackSize);
}

void
Asyncer::Stop()
{
    AsyncReadReq stopReq;

    if (mStopFlag)
        return;

    mStopFlag = true;
    stopReq.fd = -1;
    Enqueue(&stopReq);
    mThread.Join();
}

void
Asyncer::Run()
{
    mNumRunning = 0;
    libkfsio::globalNetManager().RegisterTimeoutHandler(this);
    libkfsio::globalNetManager().MainLoop();
}

void
Asyncer::Enqueue(AsyncGetIndexReq *req)
{
    req->inQ = true;
    if (!mStopFlag) {
        KFS_LOG_VA_DEBUG("+> Async get-index of fd=%d, chunk=%ld, offset=%ld",
                         req->fd, req->chunkId, req->offset);
    }
    mGetIndexRequest.enqueue(req);
    libkfsio::globalNetManager().Wakeup();
}

void
Asyncer::Dequeue(AsyncGetIndexReq **req)
{
    *req = mGetIndexResponse.dequeue();
    (*req)->inQ = false;
}

void
Asyncer::Done(AsyncGetIndexReq *req)
{
    mGetIndexResponse.enqueue(req);
}

void
Asyncer::Enqueue(AsyncReadReq *req)
{
    req->inQ = true;
    if (!mStopFlag) {
        mNumRunning++;
        KFS_LOG_VA_DEBUG("+> Async read of fd=%d, chunk=%ld, offset=%ld, nbytes=%d",
                         req->fd, req->chunkId, req->offset, (int) req->length);
    }
    mReadRequest.enqueue(req);
    libkfsio::globalNetManager().Wakeup();
}

void
Asyncer::Dequeue(AsyncReadReq **req)
{
    mNumRunning--;
    if (mNumRunning < 0) {
        KFS_LOG_STREAM_INFO << "Resetting # running since its value is: "
            << mNumRunning << KFS_LOG_EOM;
        // sanity
        mNumRunning = 0;
        if (mReadResponse.empty()) {
            *req = NULL;
            KFS_LOG_STREAM_INFO << "To avoid deadlock...returning NULL in dequeue"
                << KFS_LOG_EOM;
            return;
        }
    }

    *req = mReadResponse.dequeue();
    (*req)->inQ = false;
}

bool
Asyncer::IsReadPending() const
{
    return mNumRunning > 0;
}

void
Asyncer::Done(AsyncReadReq *req)
{
    mReadResponse.enqueue(req);
    // if any work is pending, start it
    // Timeout();
}

void
Asyncer::Enqueue(AsyncWriteReq *req)
{
    req->inQ = true;
    KFS_LOG_VA_INFO("+> Async write of fd=%d, chunk=%ld, offset=%ld, nbytes=%d",
                    req->fd, req->chunkId, req->offset, (int) req->length);
    mWriteRequest.enqueue(req);
    libkfsio::globalNetManager().Wakeup();
}

void
Asyncer::Dequeue(AsyncWriteReq **req)
{
    *req = mWriteResponse.dequeue();
    (*req)->inQ = false;
}

void
Asyncer::Done(AsyncWriteReq *req)
{
    mWriteResponse.enqueue(req);
}

void
Asyncer::Timeout()
{
    while (1) {
        AsyncWriteReq *req = mWriteRequest.dequeue_nowait();
        if (req == NULL)
            break;
        AsyncWriteWorker *worker = new AsyncWriteWorker(this, req);
        worker->IssueRequest();
    }

    while (1) {
        AsyncGetIndexReq *req = mGetIndexRequest.dequeue_nowait();
        if (req == NULL)
            break;
        AsyncGetIndexWorker *worker = new AsyncGetIndexWorker(this, req);
        worker->IssueRequest();
    }

    while (1) {
        AsyncReadReq *req = mReadRequest.dequeue_nowait();
        if (req == NULL)
            return;

        if (req->fd < 0) {
            KFS_LOG_INFO("Asyncer: Shutting down the netmanager");
            libkfsio::globalNetManager().Shutdown();
            return;
        }
        // Create a new worker
        AsyncReadWorker *worker = new AsyncReadWorker(this, req);
        worker->IssueRequest();
    }
}

AsyncWorker::AsyncWorker(Asyncer *p, AsyncReq *r) : 
    mAsyncer(p), mReq(r)
{
    mStartTime = time(NULL);
    mConn.reset(new NetConnection(mReq->sock.get(), this, false, false));
    mReq->mServerName = mConn->GetPeerName();
    mReq->mOurName = mConn->GetSockName();    
    // if we don't get a response back in N secs, fail the read
    mConn->SetInactivityTimeout(READ_WORKER_INACTIVITY_TIMEOUT);
    libkfsio::globalNetManager().AddConnection(mConn);
}

AsyncWorker::~AsyncWorker()
{
    mConn.reset();
}

void
AsyncWorker::Done()
{
    // take out the connection from the poll loop before we return the
    // request; otherwise, if the fd gets re-used, we will take stuff
    // out of the poll loop incorrectly
    mConn->Close();

    mReq->done = true;
    KFS_LOG_VA_DEBUG(" <- Async done: server = %s, fd=%d, chunk=%ld, offset=%ld, nbytes=%d, res=%d",
                    mReq->mServerName.c_str(), mReq->fd, mReq->chunkId, mReq->offset, (int) mReq->length, (int) mReq->numDone);

    DoneSelf();

    delete this;
}

bool
AsyncWorker::GotHeader(IOBuffer *buffer, int *hdrLen, KfsOp *op)
{
    const int idx = buffer->IndexOf(0, "\r\n\r\n");
    if (idx < 0) {
        if (buffer->BytesConsumable() >= MAX_READOP_RPC_HDR_LEN) {
            // this is all the data we are going to read; if the response isn't valid terminate
            KFS_LOG_VA_INFO("On fd = %d, Got an invalid response of len: %d", 
                            mReq->fd, buffer->BytesConsumable());
            op->status = -1;
            return true;
        }
        return false;
    }
    // +4 to get the length

    *hdrLen = idx + 4;
    IOBuffer::IStream is(*buffer, *hdrLen);
    Properties prop;
    const char kSeparator = ':';
    kfsSeq_t seq;

    prop.loadProperties(is, kSeparator, false);
    seq = prop.getValue("Cseq", (kfsSeq_t) -1);
    if (seq != op->seq) {
        KFS_LOG_VA_INFO("On fd = %d, Seq # mismatch: expect = %d, got = %d", 
            mReq->fd, op->seq, seq);
        op->status = -1;
        return true;
    }
    op->ParseResponseHeader(prop);
    
    if (op->status < 0) {
        KFS_LOG_VA_INFO("Async worker failure for fd = %d: bogus response for %s", 
            mReq->fd, op->Show().c_str());
        // something bad happened; we got a header and we need to end
        return true;
    }
    op->seq++;

    return true;
}

AsyncGetIndexWorker::AsyncGetIndexWorker(Asyncer *p, AsyncGetIndexReq *r) : 
    AsyncWorker(p, r), mGetIndexOp(r->seqNo, r->chunkId)
{
    KFS_LOG_VA_DEBUG("+> Async get index: seq = %d fd = %d (%s->%s), creating index worker for chunk=%ld",
        mGetIndexOp.seq, mReq->fd, mReq->mOurName.c_str(), mReq->mServerName.c_str(), 
        mGetIndexOp.chunkId);
}

void
AsyncGetIndexWorker::DoneSelf()
{
    if (mGetIndexOp.status < 0)
        mReq->numDone = -1;

    mGetIndexOp.ReleaseContentBuf();
    mAsyncer->Done((AsyncGetIndexReq* ) mReq);
}

void
AsyncGetIndexWorker::IssueRequest()
{
    SET_HANDLER(this, &AsyncGetIndexWorker::ReadResponseHeader);
    mConn->SetMaxReadAhead(MAX_READOP_RPC_HDR_LEN);

    IOBuffer::OStream os;
    mGetIndexOp.Request(os);
    mConn->Write(&os);
}

int
AsyncGetIndexWorker::ReadResponseHeader(int code, void *data)
{
    IOBuffer *buffer = (IOBuffer *) data;
    int headerLen;

    switch (code) {
        case EVENT_NET_WROTE:
            KFS_LOG_VA_DEBUG("Async read: seq = %d fd = %d (%s->%s), writing out done on request for chunk=%ld",
                mGetIndexOp.seq, mReq->fd, mReq->mOurName.c_str(), mReq->mServerName.c_str(), mReq->chunkId);
            break;
        case EVENT_NET_READ:
            if (GotHeader(buffer, &headerLen, &mGetIndexOp)) {
                if (mGetIndexOp.status < 0) {
                    // got a header that we can't parse out; we are done
                    KFS_LOG_VA_INFO("Async get index failure: got back status: %d", 
                                    mGetIndexOp.status);                    
                    Done();
                    break;
                }
                int ntodo = SetupBufferForRead(buffer, headerLen);
                if (ntodo > 0) {
                    mConn->SetMaxReadAhead(ntodo);
                    SET_HANDLER(this, &AsyncGetIndexWorker::ReadData);                
                } else {
                    // caller has to free it
                    mReq->buf = (unsigned char *) mGetIndexOp.contentBuf;
                    mGetIndexOp.ReleaseContentBuf();
                    // we got everything...so, done
                    Done();
                }
            }
            break;

        case EVENT_NET_ERROR:
        default:
            KFS_LOG_VA_INFO("Async worker for fd = %d (%s->%s) chunk=%ld terminating due to error (code = %d)", 
                            mReq->fd, mReq->mOurName.c_str(), mReq->mServerName.c_str(), mReq->chunkId, code);
            mGetIndexOp.status = -1;
            Done();
            break;
    }
    return 0;
}

int
AsyncGetIndexWorker::ReadData(int code, void *data)
{
    IOBuffer *buffer = (IOBuffer *) data;

    switch (code) {
        case EVENT_NET_WROTE:
            break;
        case EVENT_NET_READ:
            // if we got all the data, we are done
            if (buffer->IsLastFull()) {
                KFS_LOG_VA_DEBUG("Async read finished for seq = %d fd = %d (%s->%s), chunk=%ld, server sent back= %d",
                                 mGetIndexOp.seq, mReq->fd, mReq->mOurName.c_str(), mReq->mServerName.c_str(), mReq->chunkId, 
                                 mGetIndexOp.contentLength);

                mReq->numDone += buffer->BytesConsumable();
                // caller has to free it
                mReq->buf = (unsigned char *) mGetIndexOp.contentBuf;
                mGetIndexOp.ReleaseContentBuf();
                buffer->Clear();
                // We are done at this point
                Done();
            } else {
                // dial down read ahead since we got some data back
                mConn->SetMaxReadAhead(buffer->SpaceAvailableLast());
            }
            break;
        case EVENT_NET_ERROR:
        default:
            KFS_LOG_VA_INFO("Async worker for fd = %d (%s->%s) terminating due to error",
                mReq->fd, mReq->mOurName.c_str(), mReq->mServerName.c_str());
            mConn->DiscardRead();
            mGetIndexOp.status = -1;
            Done();
            break;
    }
    return 0;
}

int
AsyncGetIndexWorker::SetupBufferForRead(IOBuffer *buffer, int hdrLen)
{
    buffer->Consume(hdrLen);
    
    mGetIndexOp.contentBuf = new char[mGetIndexOp.indexSize];
    int navail = buffer->BytesConsumable();
    buffer->CopyOut(mGetIndexOp.contentBuf, navail);
    buffer->Clear();
    mReq->numDone += navail;

    int ntodo = mGetIndexOp.contentLength - navail;

    if (ntodo == 0) {
        mGetIndexOp.status = 0;
        // we got everything we were supposed to get
        return 0;
    }

    // setup the IOBuffer with backing store provided by the memory
    // allocated in the prefetch request
    mAllocator.SetBufferSize(ntodo);
    IOBufferData bdata(mGetIndexOp.contentBuf + navail, 0, 0, mAllocator);
    buffer->Append(bdata);
    
    return ntodo;
}

AsyncReadWorker::AsyncReadWorker(Asyncer *p, AsyncReadReq *r) : 
    AsyncWorker(p, r), mReadOp(r->seqNo, r->chunkId, r->chunkVersion)
{
    // salt the thing...so that we can figure out who is causing
    // the seq # mismatches; is it from here or due to the KFS client code
    mReadOp.seq += 10000;

    KFS_LOG_VA_DEBUG("+> Async read: seq = %d fd = %d (%s->%s), creating worker for chunk=%ld, offset=%ld, numbytes=%d",
                    mReadOp.seq, mReq->fd, mReq->mOurName.c_str(), mReq->mServerName.c_str(), 
                    mReadOp.chunkId, mReq->offset, mReq->length);
}

void
AsyncReadWorker::DoneSelf()
{
    if (mReadOp.status < 0)
        mReq->numDone = -1;

    // pass back when we were done
    mReq->reqDoneTime = time(0);

    mReadOp.ReleaseContentBuf();
    mAsyncer->Done((AsyncReadReq* ) mReq);
}

void
AsyncReadWorker::IssueRequest()
{
    time_t now = time(NULL);

    if (!mConn->IsGood()) {
        mReadOp.status = -EAGAIN;
        KFS_LOG_STREAM_INFO << "---> Async worker terminating: no connection on fd:"
            << mReq->fd << KFS_LOG_EOM;
        Done();
        return;
    }
    if (now - mStartTime > READ_WORKER_LIFETIME) {
        KFS_LOG_VA_INFO("-->Async worker terminating because it has been around too long: now=%d, start=%d",
                        now, mStartTime);
        mReadOp.status = -ETIMEDOUT;
    }

    if ((now - mStartTime > READ_WORKER_LIFETIME) ||
        ((size_t) mReq->numDone >= mReq->length) || (mReadOp.status < 0)) {
        Done();
        return;
    }

    unsigned char *ptr = mReq->buf + mReq->numDone;

    mReadOp.offset = mReq->offset + mReq->numDone;
    mReadOp.numBytes = 1 << 20;
    const size_t blkOff(mReadOp.offset % KFS::CHECKSUM_BLOCKSIZE);
    // if the read isn't block aligned, do the minimal read in this call; then,
    // we try to force subsequent reads to be block aligned.
    mReadOp.numBytes = blkOff != 0 ?
        min(mReadOp.numBytes, KFS::CHECKSUM_BLOCKSIZE - blkOff) :
        mReadOp.numBytes;
    // we got a block aligned read; make sure that we got space for it
    mReadOp.numBytes = min(mReadOp.numBytes, mReq->length - mReq->numDone);

    mReadOp.ReleaseContentBuf();
    mReadOp.AttachContentBuf((char *) ptr, mReadOp.numBytes);
    
    SET_HANDLER(this, &AsyncReadWorker::ReadResponseHeader);
    mConn->SetMaxReadAhead(MAX_READOP_RPC_HDR_LEN);

    IOBuffer::OStream os;
    mReadOp.Request(os);
    mConn->Write(&os);

    /*
    KFS_LOG_VA_INFO("Async read: seq = %d fd = %d (%s->%s), wrote out a request for chunk=%ld, offset=%ld, nbytes=%d",
                    mReadOp.seq, mReq->fd, mReq->mOurName.c_str(), mReq->mServerName.c_str(), mReq->chunkId,
                    mReadOp.offset, (int) mReadOp.numBytes);
    */
}

int
AsyncReadWorker::ReadResponseHeader(int code, void *data)
{
    IOBuffer *buffer = (IOBuffer *) data;
    int headerLen;

    switch (code) {
        case EVENT_NET_WROTE:
            KFS_LOG_VA_DEBUG("Async read: seq = %d fd = %d (%s->%s), writing out done on request for chunk=%ld, offset=%ld, nbytes=%d",
                             mReadOp.seq, mReq->fd, mReq->mOurName.c_str(), mReq->mServerName.c_str(), mReq->chunkId, mReadOp.offset, (int) mReadOp.numBytes);
            break;
        case EVENT_NET_READ:
            if (GotHeader(buffer, &headerLen, &mReadOp)) {
                if (mReadOp.status < 0) {
                    // got a header that we can't parse out; we are done
                    KFS_LOG_VA_INFO("Async header read failure on fd = %d (%s->%s) chunk=%ld, seq=%d, status = %d", 
                        mReq->fd, mReq->mOurName.c_str(), mReq->mServerName.c_str(), 
                        mReq->chunkId, mReadOp.seq, mReadOp.status);
                    Done();
                    break;
                }
                int ntodo = SetupBufferForRead(buffer, headerLen);
                if (ntodo > 0) {
                    mConn->SetMaxReadAhead(ntodo);
                    SET_HANDLER(this, &AsyncReadWorker::ReadData);                
                } else {
                    IssueRequest();
                }
            }
            break;

        case EVENT_NET_ERROR:
        default:
            KFS_LOG_VA_INFO("Async worker for fd = %d (%s->%s) chunk=%ld, seq=%d terminating due to error (code = %d)", 
                mReq->fd, mReq->mOurName.c_str(), mReq->mServerName.c_str(), 
                mReq->chunkId, mReadOp.seq, code);
            mReadOp.status = -1;
            Done();
            break;
    }
    return 0;
}

int
AsyncReadWorker::ReadData(int code, void *data)
{
    IOBuffer *buffer = (IOBuffer *) data;

    switch (code) {
        case EVENT_NET_WROTE:
            break;
        case EVENT_NET_READ:
            // if we got all the data, we are done
            if (buffer->IsLastFull()) {
                KFS_LOG_VA_DEBUG("Async read finished for seq = %d fd = %d (%s->%s), chunk=%ld, offset=%ld, issued=%d, server sent back= %d",
                                 mReadOp.seq, mReq->fd, mReq->mOurName.c_str(), mReq->mServerName.c_str(), mReq->chunkId, mReq->offset, 
                                 (int) mReadOp.numBytes, mReadOp.contentLength);

                mReq->numDone += buffer->BytesConsumable();
                buffer->Clear();
                // Verify checksum 
                if (!VerifyChecksum(mReadOp)) {
                    mReadOp.status = -KFS::EBADCKSUM;
                    Done();
                    break;
                }
                IssueRequest();
            } else {
                // dial down read ahead since we got some data back
                mConn->SetMaxReadAhead(buffer->SpaceAvailableLast());
            }
            break;
        case EVENT_NET_ERROR:
        default:
            KFS_LOG_VA_INFO("Async worker for fd = %d (%s->%s) terminating due to error, data asked = %d, data got = %d", 
                            mReq->fd, mReq->mOurName.c_str(), mReq->mServerName.c_str(), mReadOp.numBytes, 
                            mReq->numDone);
            mConn->DiscardRead();
            mReadOp.status = -1;
            Done();
            break;
    }
    return 0;
}

int
AsyncReadWorker::SetupBufferForRead(IOBuffer *buffer, int hdrLen)
{
    buffer->Consume(hdrLen);
    
    int navail = buffer->BytesConsumable();
    buffer->CopyOut(mReadOp.contentBuf, navail);
    buffer->Clear();
    mReq->numDone += navail;

    int ntodo = mReadOp.contentLength - navail;

    if (ntodo == 0)
        // we got everything we were supposed to get
        return 0;

    assert (ntodo <= (int)mReq->length);
    KFS_LOG_VA_DEBUG("Async read for seq = %d fd = %d (%s->%s), chunk=%ld, offset=%ld, issued=%d, server sent = %d, left=%d",
                     mReadOp.seq, mReq->fd, mReq->mOurName.c_str(), mReq->mServerName.c_str(), mReq->chunkId, mReq->offset, 
                     (int) mReadOp.numBytes, mReadOp.contentLength, ntodo);
    
    // setup the IOBuffer with backing store provided by the memory
    // allocated in the prefetch request
    mAllocator.SetBufferSize(ntodo);
    IOBufferData bdata(mReadOp.contentBuf + navail, 0, 0, mAllocator);
    buffer->Append(bdata);
    
    return ntodo;
}


// The handler for a write

AsyncWriteWorker::AsyncWriteWorker(Asyncer *p, AsyncWriteReq *r) :
    AsyncWorker(p, r), 
    mWriteIdAllocOp(r->seqNo, r->chunkId, r->chunkVersion, 
                    r->offset, r->length),
    mWritePrepareOp(r->seqNo + 1, r->chunkId, r->chunkVersion),
    mWriteSyncOp()
{
    // need the daisy chain info into the writeOp
    mWriteIdAllocOp.chunkServerLoc = r->daisyChain;
}

void
AsyncWriteWorker::DoneSelf()
{
    if (mWriteSyncOp.status < 0)
        mReq->numDone = -1;

    mWritePrepareOp.ReleaseContentBuf();
    mAsyncer->Done((AsyncWriteReq *) mReq);
}

void
AsyncWriteWorker::IssueRequest()
{
    // Get the write-id and then send the data
    if (((size_t) mReq->numDone >= mReq->length) || (mWriteSyncOp.status < 0)) {
        Done();
        return;
    }

    if (mWriteIdAllocOp.writeIdStr == "") {
        IssueWriteIdRequest();
        return;
    }
    // start a write
    IssueWritePrepareRequest();
}

void
AsyncWriteWorker::IssueWriteIdRequest()
{
    SET_HANDLER(this, &AsyncWriteWorker::HandleWriteIdResponse);

    IOBuffer::OStream os;
    mWriteIdAllocOp.Request(os);
    mConn->Write(&os);
}

int
AsyncWriteWorker::HandleWriteIdResponse(int code, void *data)
{
    IOBuffer *buffer = (IOBuffer *) data;
    int headerLen;

    switch (code) {
        case EVENT_NET_WROTE:
            break;
        case EVENT_NET_READ:
            if (GotWriteId(buffer, &headerLen)) {
                if (mWriteIdAllocOp.status < 0) {
                    // got a header that we can't parse out; we are done
                    mWriteSyncOp.status = -1;
                    Done();
                    break;
                }
                buffer->Consume(headerLen);
                IssueRequest();
            }
            break;

        case EVENT_NET_ERROR:
        default:
            KFS_LOG_VA_INFO("Async write worker for req=%d terminating due to error", mReq->fd);
            mWriteSyncOp.status = -1;
            Done();
            break;
    }
    return 0;
}

bool
AsyncWriteWorker::GotWriteId(IOBuffer *buffer, int *hdrLen)
{
    if (!GotHeader(buffer, hdrLen, &mWriteIdAllocOp))
        return false;
    if (mWriteIdAllocOp.status < 0)
        return true;
    // Get the write info
    
    vector<WriteInfo> &writeId = mWritePrepareOp.writeInfo;
    
    writeId.clear();
    writeId.reserve(mWriteIdAllocOp.chunkServerLoc.size());
    istringstream ist(mWriteIdAllocOp.writeIdStr);
    for (uint32_t i = 0; i < mWriteIdAllocOp.chunkServerLoc.size(); i++) {
        ServerLocation loc;
        int64_t id;

        ist >> loc.hostname;
        ist >> loc.port;
        ist >> id;
	writeId.push_back(WriteInfo(loc, id));
    }
    return true;
}

void
AsyncWriteWorker::IssueWritePrepareRequest()
{
    // send 1MB in each iteration
    uint32_t nOpsToSend = std::min((size_t) 1 << 20, 
                                   mReq->length - mReq->numDone) / CHECKSUM_BLOCKSIZE;
    if (nOpsToSend == 0)
        nOpsToSend++;

    mBytesSentInRound = 0;
    off_t offset = mReq->offset + mReq->numDone;
    size_t nleft = mReq->length - mReq->numDone;
    char *ptr    = (char *) mReq->buf + mReq->numDone;
    size_t nBytes = 0;

    for (uint32_t i = 0; i < nOpsToSend; i++) {
        mWritePrepareOp.offset   = offset;
        // don't change the CHECKSUM_BLOCKSIZE value here...we are
        // using this fact in how we build the checksum vector
        mWritePrepareOp.numBytes = std::min((size_t) CHECKSUM_BLOCKSIZE,
                                            nleft - mBytesSentInRound);

        mWritePrepareOp.ReleaseContentBuf();
        mWritePrepareOp.AttachContentBuf(ptr, mWritePrepareOp.numBytes);

        mWritePrepareOp.checksum = ComputeBlockChecksum(mWritePrepareOp.contentBuf,
                                                        mWritePrepareOp.numBytes);
        // we are sending over one checksum block at a time
        mWritePrepareOp.checksums.push_back(mWritePrepareOp.checksum);
        IOBuffer::OStream os;
        mWritePrepareOp.Request(os);
        mConn->Write(&os);

        mAllocator.SetBufferSize(mWritePrepareOp.numBytes);
        IOBufferData bdata(mWritePrepareOp.contentBuf, 0, 
                           mWritePrepareOp.numBytes, mAllocator);
        mConn->Write(bdata);
        mWritePrepareOp.seq++;
        mBytesSentInRound += mWritePrepareOp.numBytes;
        ptr += mWritePrepareOp.numBytes;
        offset += mWritePrepareOp.numBytes;
        nBytes += mWritePrepareOp.numBytes;
    }

    IssueWriteSyncRequest(nBytes);
}

void
AsyncWriteWorker::IssueWriteSyncRequest(size_t nBytes)
{
    SET_HANDLER(this, &AsyncWriteWorker::HandleWriteSyncResponse);

    // the seq # was bumped when we parsed out the header
    mWriteSyncOp.Init(mWritePrepareOp.seq, mWritePrepareOp.chunkId,
                      mWritePrepareOp.chunkVersion,
                      mWritePrepareOp.offset, nBytes,
                      mWritePrepareOp.checksums,
                      mWritePrepareOp.writeInfo);
    mWritePrepareOp.seq++;

    IOBuffer::OStream os;
    mWriteSyncOp.Request(os);
    mConn->Write(&os);
}

int
AsyncWriteWorker::HandleWriteSyncResponse(int code, void *data)
{
    IOBuffer *buffer = (IOBuffer *) data;
    int headerLen;

    switch (code) {
        case EVENT_NET_WROTE:
            break;
        case EVENT_NET_READ:
            if (GotHeader(buffer, &headerLen, &mWriteSyncOp)) {
                if (mWriteSyncOp.status < 0) {
                    // got a header that we can't parse out; we are done
                    Done();
                    break;
                }
                mReq->numDone += mBytesSentInRound;
                buffer->Consume(headerLen);
                IssueRequest();
            }
            break;

        case EVENT_NET_ERROR:
        default:
            KFS_LOG_VA_INFO("Async write worker for req=%d terminating due to error", mReq->fd);
            mWriteSyncOp.status = -1;
            Done();
            break;
    }
    return 0;
}

