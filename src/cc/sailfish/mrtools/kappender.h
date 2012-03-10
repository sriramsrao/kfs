//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kappender.h 3603 2012-01-23 05:18:38Z sriramr $
//
// Created 2010/11/08
//
// Copyright 2010 Yahoo Corporation.  All rights reserved.
// This file is part of the Sailfish project.
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
// \brief kappender is a tool that can be used for atomic-appends into
// KFS files.  Files that are mutated via atomic-append are called
// K-files. This tool reads input from stdin and then atomic-appends
// to the desired K-file.  There are two classes of interest: (1) KWriter class that
// accepts the writes for a K-file and uses the WriteAppender class in libkfsClient
// to do the appends, and (2) Client class that creates one KWriter
// per K-file and is the callback whenever data is read from stdin.
//----------------------------------------------------------------------------

#ifndef KAPPENDER_KAPPENDER_H
#define KAPPENDER_KAPPENDER_H

#include "libkfsIO/NetConnection.h"
#include "libkfsIO/NetManager.h"
#include "libkfsClient/WriteAppender.h"
#include "ifile_base.h"
#include "util.h"
#include <boost/shared_ptr.hpp>

#ifdef USE_INTEL_IPP
#include <ipp.h>
#else
#include <lzo/lzo1x.h>
#include <lzo/lzoconf.h>
#endif

#include <set>

namespace KFS
{
    // Convenienance
    typedef WriteAppender::MetaServer MetaServer;

    typedef HttpGetOp ChunksAppendedNotifyOp;

    class InputProcessor;

    // The WriteAppender has a set of methods for doing
    // atomic-appends.  Turn them all private---the user of this
    // class doesn't need to know; whatever methods we need to expose,
    // we will use the same signature as in WriteAppender.
    // Order matters: want the Completion c'tor to be called before we
    // instantiate WriteAppender
    class Appender : private WriteAppender::Completion, private WriteAppender {
    public:
        Appender(MetaServer &inMetaServer, const char *logPrefixPtr,
            InputProcessor *ip, uint32_t partition, uint64_t mapperAttempt, int rackId);
        ~Appender() { }

        void SetBufferLimit(int l) {
            mBufferLimit = l;
        }
        void SetCompressionFlag(bool v) {
            mIsCompressionEnabled = v;
        }
        int GetPartition() const {
            return mPartition;
        }
        // open an I-file for writing
        int Open(const char *inFileNamePtr, int numReplicas = 2, bool inMakeDirsFlag = false);
        int Close();
        void Shutdown();

        // Called when end-of-input is received from the map task.  At this point, we
        // should compress (if needed) and flush any records that we have buffered
        int EndOfInput();

        // Check if the underlying WriteAppender is still actively working
        bool IsActive() const;

        //Resize the compression buffers to a larger size
        int ResizeBuffers(int newUncompSize);

        int Append(IOBuffer &inBuffer, int inKeyLength, int inDataLength);

        std::set<kfsChunkId_t> GetChunksAppended();
        // Whenever a write append is complete, the WriteAppender
        // class will call the associated completion handler.  Might
        // as well put the completion handler here.  This means that
        // this class will need to implement the abstract method from
        // Completion.
        virtual void Done(WriteAppender &inAppender, int inStatusCode);
    private:
        InputProcessor *mOwner;
        bool mIsCompressionEnabled;
        uint32_t mPartition;
        int mPendingBytes;
        char* mUncompBuffer;
        int mUncompBufferSize;
        char* mCompBuffer;
        int mCompBufferSize;
#ifdef USE_INTEL_IPP
        IppLZOState_8u *mWrkMem;
#else
        lzo_voidp mWrkMem;
#endif
        uint32_t mSequenceNumber;
        int mBytesBuffered;
        int mKeyLengthBuffered;
        int mBufferLimit;
        long mMapperAttempt;
        IOBuffer mBuffer;
        // for key-based appends, stash the key here
        IOBuffer mKeyBuffer;
        int GetPendingBytes();
        int AppendRecord(IOBuffer &inBuffer, int inKeyLength, int inDataLength);
        void CompressRecords();
        // if there is any buffered, flush it down to the I-file
        int Flush();

    };

    typedef boost::shared_ptr<Appender> AppenderPtr;

    // This class is the callback for all the input that is read from stdin.
    class InputProcessor : public KfsCallbackObj {
    public:
        InputProcessor(const std::string &metaHost, int metaPort,
            const std::string &basedir, int numPartitions,
            int mapperId, int attemptNum);
        // Typically, this call does not return; it calls the netManager to start
        // looking for data on stdin.  Any return from this method
        // should be treated as fatal/end-of-input.
        void Start(int inputFd, int outputFd, int bufferLimit,
            bool enableCompression);

        // When we get close sequence on the input, we notify each
        // appender to Close().  Once the appender is done flushing
        // data and closes, we shutdown and exit.
        void Close();
        void Shutdown();

        // This is the event handler that will get invoked whenever
        // data is read in.
        int HandleEvent(int code, void *data);

        void AppendDone(Appender *appender, int inStatusCode, int nbytesDone);
        // XXX: We will need to add some methods that decide when to
        // turn off input/turn back input; if we get too far
        // backlogged, we will need to shut input down.  Otherwise, we
        // will run of memory and start swapping...bad

        // Notify the workbuilder about the set of chunks to which we
        // appended data to.  This is required to handle losses of
        // individual chunks---we can work back to identify the set of
        // map tasks that wrote to a chunk and then re-run those tasks.
        void NotifyChunksToWorkbuilder(const std::string &jobId, ServerLocation &workbuilderLoc);

    private:
        NetManager mNetManager;
        MetaServer mMetaServer;
        bool mEndOfInputRecd;
        bool mIsInputDisabled;
        int32_t mActiveAppenders;
        uint32_t mNumPartitions;
        uint32_t mMapperId;
        uint32_t mAttempt;
        int     mNextRecordSize;
        // # of bytes in the input buffer that we have read
        // but not yet consumed
        int     mPendingInputBytes;
        uint32_t mNumRecordsAppended;
        // # of bytes that we have sent to the appender
        int64_t mPendingBytesCount;
        int64_t mMaxPendingBytesCount;
        int64_t mMinPendingBytesCount;
        int64_t mTotalBytesAppended;
        struct timeval mInputDisableStartTime;
        float   mInputDisabledSec;
        std::string mBaseDirForIFiles;
        TcpSocket mInputSock;
        TcpSocket mOutputSock;
        NetConnectionPtr mInputConnection;
        NetConnectionPtr mOutputConnection;
        // Have one k-writer per k-file
        std::vector<AppenderPtr> mAppenders;

        std::set<kfsChunkId_t> mChunksAppended;

        bool IsMsgAvail(IOBuffer *inBuffer);
        bool HandleCmd(IOBuffer *inBuffer);

        // use this to enable/disable reading input
        void SetReadAhead();
        NetConnectionPtr MakeNetConnection(int fd, TcpSocket *so);
    };

    // Wire protocol for data input.
    struct ProtoHdr_t {
        int32_t partition;
        uint32_t keyLength;
        uint32_t dataLength;
        void ConvertToHostByteOrder() {
            partition = htonl(partition);
            keyLength = htonl(keyLength);
            dataLength = htonl(dataLength);
        }
    };
}

#endif // KAPPENDER_KAPPENDER_H
