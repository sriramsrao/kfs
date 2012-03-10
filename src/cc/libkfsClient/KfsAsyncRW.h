//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: KfsAsyncRW.h 2724 2011-07-08 23:33:47Z sriramr $
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
// \brief In the current implementation, the KFS prefetcher consists
// of a thread that handles a queue of requests.  A prefetch request
// is enqueued; the thread picks it up and multiplexes this request
// with the others it is handling.  As and when requests complete,
// they are returned into a "response" queue.  The KFS client read
// code can pull the completed responses and process them.
//----------------------------------------------------------------------------

#ifndef LIBKFSCLIENT_KFSPREFETCHER_H
#define LIBKFSCLIENT_KFSPREFETCHER_H

#include "libkfsIO/TcpSocket.h"
#include "libkfsIO/ITimeout.h"
#include "libkfsIO/KfsCallbackObj.h"
#include "libkfsIO/NetConnection.h"

#include "KfsOps.h"
#include "qcdio/qcthread.h"
#include "meta/queue.h"

namespace KFS
{
    // An "allocator" that does nothing.  We need this to construct an
    // IOBufferData blob and want to make sure that the buffer isn't
    // freed when the read is done.
    class NullAllocator : public libkfsio::IOBufferAllocator
    {
    public:
        NullAllocator() : bufsz(0) { }
        char *Allocate() {
            return NULL;
        }
        void Deallocate(char *buf) {  }
        size_t GetBufferSize() const { return bufsz; }
        void SetBufferSize(size_t s) { bufsz = s; }
    private:
        size_t bufsz;
    };

    struct AsyncReq {
        AsyncReq(int f, TcpSocketPtr s, kfsSeq_t n, kfsChunkId_t c, int64_t v,
                    off_t o, unsigned char *b, size_t l) :
            fd(f), sock(s), seqNo(n), chunkId(c), chunkVersion(v), offset(o), buf(b), length(l), 
            numDone(0), done(false), inQ(false) { }

        AsyncReq() : fd(-1) { }
        int         fd; // entry from the client filetable
        TcpSocketPtr  sock; // socket connected to chunkserver
        time_t      reqDoneTime; // when did the I/O finish
        std::string mOurName; // stash our host/port so we can print it out in log messages
        std::string mServerName; // stash the server name so we can print it out in log messages
        kfsSeq_t    seqNo;  // seq # to use in the RPCs
        kfsChunkId_t chunkId;
        int64_t  chunkVersion;
        off_t    offset; // starting at this position in the chunk, read/write nbytes
        unsigned char *buf;  // buffer into which data has to be read/written
        size_t      length;  // # of bytes to be read/written
        // result: -1 means error; otherwise, # of bytes read/written
        ssize_t     numDone;
        bool        done;
        bool        inQ;

    };

    struct AsyncGetIndexReq : public AsyncReq {
        AsyncGetIndexReq(int f, TcpSocketPtr s, kfsSeq_t n, kfsChunkId_t c) :
            AsyncReq(f, s, n, c, 0, 0, 0, 0) { }
        AsyncGetIndexReq() { }
    };

    struct AsyncReadReq : public AsyncReq {
        AsyncReadReq(int f, TcpSocketPtr s, kfsSeq_t n, kfsChunkId_t c, int64_t v,
                     off_t o, unsigned char *b, size_t l) :
            AsyncReq(f, s, n, c, v, o, b, l) { }
        AsyncReadReq() { }
    };

    struct AsyncWriteReq : public AsyncReq {
        AsyncWriteReq(int f, TcpSocketPtr s, kfsSeq_t n, kfsChunkId_t c, int64_t v,
                      off_t o, unsigned char *b, size_t l,
                      const std::vector<ServerLocation> &d) :
            AsyncReq(f, s, n, c, v, o, b, l), daisyChain(d) { }
        std::vector<ServerLocation> daisyChain;
        // the actual position in the file where we want this write to occur
        off_t filePosition;
    };


    class Asyncer : public QCRunnable, public ITimeout {
    public:
        Asyncer() : mNumRunning(0), mStopFlag(true) { }
        ~Asyncer() { Stop(); }
        void Start();
        void Stop();
        virtual void Run();

        void Enqueue(AsyncGetIndexReq *req);
        void Dequeue(AsyncGetIndexReq **req);
        void Done(AsyncGetIndexReq *req);

        void Enqueue(AsyncReadReq *req);
        void Dequeue(AsyncReadReq **req);
        void Done(AsyncReadReq *req);
        bool IsReadPending() const;

        void Enqueue(AsyncWriteReq *req);
        void Dequeue(AsyncWriteReq **req);
        void Done(AsyncWriteReq *req);

        void Timeout();
    private:
        QCThread			mThread;
        MetaQueue<AsyncReadReq>		mReadRequest;
        MetaQueue<AsyncReadReq>		mReadResponse;
        MetaQueue<AsyncWriteReq>	mWriteRequest;
        MetaQueue<AsyncWriteReq>	mWriteResponse;

        MetaQueue<AsyncGetIndexReq>	mGetIndexRequest;
        MetaQueue<AsyncGetIndexReq>	mGetIndexResponse;

        // to limit the # of concurrent prefetches, track how many are outstanding
        int				mNumRunning;
        bool				mStopFlag;
        void ProcessRequest(AsyncReq *req);
    };

    class AsyncWorker : public KfsCallbackObj {
    public:
        AsyncWorker(Asyncer *p, AsyncReq *r);
        virtual ~AsyncWorker();
        virtual void IssueRequest() = 0;
    protected:
        Asyncer *mAsyncer;
        AsyncReq *mReq;
        NetConnectionPtr mConn;
        NullAllocator mAllocator;
        time_t mStartTime;

        void Done();        
        bool GotHeader(IOBuffer *buffer, int *hdrLen, KfsOp *op);

        virtual void DoneSelf() = 0;
    };

    class AsyncGetIndexWorker : public AsyncWorker {
    public:
        AsyncGetIndexWorker(Asyncer *p, AsyncGetIndexReq *r);
        virtual ~AsyncGetIndexWorker() { }
        void IssueRequest();

        // Event handlers
        int ReadResponseHeader(int code, void *data);
        int ReadData(int code, void *data);

    private:
        GetChunkIndexOp mGetIndexOp;
        int SetupBufferForRead(IOBuffer *buffer, int hdrLen);
        void DoneSelf();
    };

    class AsyncReadWorker : public AsyncWorker {
    public:
        AsyncReadWorker(Asyncer *p, AsyncReadReq *r);
        virtual ~AsyncReadWorker() { }
        void IssueRequest();

        // Event handlers
        int ReadResponseHeader(int code, void *data);
        int ReadData(int code, void *data);

    private:
        ReadOp mReadOp;
        int SetupBufferForRead(IOBuffer *buffer, int hdrLen);
        void DoneSelf();
    };

    class AsyncWriteWorker : public AsyncWorker {
    public:
        AsyncWriteWorker(Asyncer *p, AsyncWriteReq *r);
        virtual ~AsyncWriteWorker() { }
        void IssueRequest();

        // Event handlers
        int HandleWriteIdResponse(int code, void *data);
        int HandleWriteSyncResponse(int code, void *data);

    private:
        WriteIdAllocOp mWriteIdAllocOp;
        WritePrepareOp mWritePrepareOp;
        WriteSyncOp mWriteSyncOp;
        // Given N bytes to send, we push in rounds of 1MB a piece.
        // In each round, we send 1MB, get an ack, and then send the
        // next round.  This variable tracks how many bytes were sent
        // in the last round.
        int mBytesSentInRound;

        void DoneSelf();
        void IssueWriteIdRequest();
        void IssueWritePrepareRequest();
        void IssueWriteSyncRequest(size_t nBytes);

        bool GotWriteId(IOBuffer *buffer, int *hdrLen);
    };


}

#endif // LIBKFSCLIENT_KFSPREFETCHER_H
