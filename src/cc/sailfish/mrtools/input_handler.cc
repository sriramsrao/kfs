//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: input_handler.cc 3603 2012-01-23 05:18:38Z sriramr $
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
// \brief Code related to input handling: read a write request and
// dispatch it to the appropriate kappender.
//----------------------------------------------------------------------------

#include <algorithm>
#include <cerrno>
#include <fcntl.h>
#include <unistd.h>
#include <sstream>
#include "common/log.h"
#include "qcdio/qcutils.h"
#include "kappender.h"
#include "libkfsClient/KfsClientInt.h"
#include "libkfsClient/Utils.h"

using namespace KFS;
using std::string;
using std::endl;
using std::ostringstream;

static float ComputeTimeDiff(const struct timeval &startTime, const struct timeval &endTime)
{
    float timeSpent;

    timeSpent = (endTime.tv_sec * 1e6 + endTime.tv_usec) -
        (startTime.tv_sec * 1e6 + startTime.tv_usec);
    return timeSpent / 1e6;
}


InputProcessor::InputProcessor(const string &metaHost, int metaPort,
    const string &basedir, int numPartitions, int mapperId, int attemptNum) :
    mNetManager(),
    mMetaServer(mNetManager, metaHost, metaPort),
    mEndOfInputRecd(false),
    mIsInputDisabled(false),
    mActiveAppenders(0),
    mNumPartitions(numPartitions),
    mMapperId(mapperId),
    mAttempt(attemptNum),
    mNextRecordSize(0),
    mPendingInputBytes(0),
    mNumRecordsAppended(0),
    mPendingBytesCount(0),
    // set the max at 64M; min at 50M---if we are ever backlogged,
    // we really want the backlog to clear
    // mMaxPendingBytesCount(64 << 20),
    // mMinPendingBytesCount(48 << 20),
    mMaxPendingBytesCount(256 << 20),
    mMinPendingBytesCount(192 << 20),
    mTotalBytesAppended(0),
    mInputDisabledSec(0.0),
    mBaseDirForIFiles(basedir)
{
    // Retry the connect a few times
    mMetaServer.SetMaxRetryCount(6);
    SET_HANDLER(this, &InputProcessor::HandleEvent);
}

void
InputProcessor::Close()
{
    // flush the appenders
    mActiveAppenders = 0;
    // if we increment in the loop below, we run the risk of the
    // appender finishing up (i.e., Close() figures there is nothing
    // left to do for that appender) and calling Shutdown().
    for (uint32_t i = 0; i < mAppenders.size(); i++) {
        if (mAppenders[i]->IsActive())
            mActiveAppenders++;
    }

    for (uint32_t i = 0; i < mAppenders.size(); i++) {
        // Flush whatever is buffered at the appender and update count
        mPendingBytesCount += mAppenders[i]->EndOfInput();
        mAppenders[i]->Close();
    }
}

void
InputProcessor::Shutdown()
{
    for (uint32_t i = 0; i < mAppenders.size(); i++) {
        mAppenders[i]->Shutdown();
    }
    if (mInputConnection) {
        mInputConnection->Close();
        mInputConnection.reset();
    }
    for (uint32_t i = 0; i < mAppenders.size(); i++) {
        std::set<kfsChunkId_t> c = mAppenders[i]->GetChunksAppended();
        mChunksAppended.insert(c.begin(), c.end());
    }
    mNetManager.Shutdown();
    KFS_LOG_STREAM_INFO << "Total time for which input was disabled: "
        << mInputDisabledSec << KFS_LOG_EOM;
}

void
InputProcessor::NotifyChunksToWorkbuilder(const string &jobId, ServerLocation &workbuilderLocation)
{
    // send the chunks appended to the workbuilder
    int retryCount = 0;

    while (retryCount < 6) {
        TcpSocket sock;

        if (sock.Connect(workbuilderLocation) < 0) {
            KFS_LOG_STREAM_INFO << "Unable to connect to: "
                << workbuilderLocation.ToString()
                << " retrying..." << KFS_LOG_EOM;
            retryCount++;
            KFS::Sleep(30);
            continue;
        }

        std::ostringstream os;
        os << "/chunksappended?jobid=" << jobId << "&mapperid=" << mMapperId
            << "&numRecords=" << mNumRecordsAppended
            << "&numBytes=" << mTotalBytesAppended;

        ChunksAppendedNotifyOp notifyOp(os.str());

        std::ostringstream chunkIds;
        // send a "," separated list of chunk id's
        bool needComma = false;
        for (std::set<kfsChunkId_t>::iterator it = mChunksAppended.begin();
             it != mChunksAppended.end(); it++) {
            if (needComma)
                chunkIds << ",";
            chunkIds << (*it);
            needComma = true;
        }

        notifyOp.AttachContent(chunkIds.str().c_str(), chunkIds.str().length(),
            chunkIds.str().length());

        int res = DoOpCommon(&notifyOp, &sock);

        if (res < 0) {
            KFS_LOG_STREAM_INFO << "Unable to notify workbuilder about chunks appended: "
                << workbuilderLocation.ToString()
                << " error: " << res
                << " retrying..." <<  KFS_LOG_EOM;
            retryCount++;
            KFS::Sleep(10);
            continue;
        }

        KFS_LOG_STREAM_INFO << "Notification of chunks appended to workbuilder successful"
            << KFS_LOG_EOM;
        return;
    }
    std::cerr << "Giving up...unable to notify workbuilder of the chunks appended"
        << std::endl;
    exit(-1);
}

NetConnectionPtr
InputProcessor::MakeNetConnection(int fd, TcpSocket *so)
{
    TcpSocket s(fd);
    NetConnectionPtr n;

    *so = s;
    s = TcpSocket();

    // don't own the socket ptr
    n.reset(new NetConnection(so, this, false, false));
    mNetManager.AddConnection(n);
    return n;
}

void
InputProcessor::Start(int inputFd, int outputFd, int bufferLimit,
    bool enableCompression)
{

    // make sure that the inputFd can do non-blocking I/O
    if (fcntl(inputFd, F_SETFL, O_NONBLOCK) < 0) {
        KFS_LOG_STREAM_FATAL << "Unable make input fd non-blocking"
                             << errno << KFS_LOG_EOM;
        return;
    }
    if (fcntl(outputFd, F_SETFL, O_NONBLOCK) < 0) {
        KFS_LOG_STREAM_FATAL << "Unable make output fd non-blocking"
                             << errno << KFS_LOG_EOM;
        return;
    }

    // XXX: Should we set an inactivity timeout on the input?  This'll help
    // with flushing any data that we got buffered for a while
    mInputConnection = MakeNetConnection(inputFd, &mInputSock);

    string myS26 = NetManager::GetMyS26Address();
    // HACK alert here...
    int myRackId = -1;
    // Rack id setup for aragog cluster
    if (myS26 == "10.128.16.192")
        myRackId = 2;
    else if (myS26 == "10.128.17.0")
        myRackId = 3;
    else if (myS26 == "10.128.17.64")
        myRackId = 4;
    else if (myS26 == "10.128.17.128")
        myRackId = 5;
    else if (myS26 == "10.128.17.192")
        myRackId = 6;

    KFS_LOG_STREAM_INFO << "My rack id is: " << myRackId << KFS_LOG_EOM;

    // If I add this connection to the list and close it, on the mac, the machine reboots.
    // Sigh...
    // mOutputConnection = MakeNetConnection(outputFd, &mOutputSock);

    if (!mMetaServer.Start()) {
        KFS_LOG_STREAM_FATAL << "Unable to start metaserver!" << KFS_LOG_EOM;
        return;
    }

    // Create the Appenders
    AppenderPtr ap;
    uint64_t mapperAttempt = (((uint64_t) mMapperId) << 32) | (mAttempt);
    for (uint32_t i = 0; i < mNumPartitions; i++) {
        // each kwriter will write to: "/jobs/<job id>/<file #>/<file>
        bool kMkdirs = true;
        ostringstream kfn;
        ostringstream logPrefix;
        int errCode;
        // make it easy to grep in the logs
        logPrefix << "[p " << i << "]";
        ap.reset(new Appender(mMetaServer, logPrefix.str().c_str(), this, i, mapperAttempt, myRackId));
        kfn << mBaseDirForIFiles << "/" << i << "/file." << i;
        // use 2 replicas rather than 3 for I-files
        // errCode = ap->Open(kfn.str().c_str(), 2, kMkdirs);
        // use 1 replica to avoid 2x network traffic
        errCode = ap->Open(kfn.str().c_str(), 1, kMkdirs);
        if (errCode < 0) {
            KFS_LOG_STREAM_FATAL << "Unable to create I-file "
                << kfn.str() << " : error = " << errCode
                << KFS_LOG_EOM;
            return;
        }
        ap->SetBufferLimit(bufferLimit);
        ap->SetCompressionFlag(enableCompression);
        mAppenders.push_back(ap);
    }
    mNetManager.MainLoop();
}

void
InputProcessor::SetReadAhead()
{
    if ((!mIsInputDisabled) &&
        (mPendingBytesCount < mMaxPendingBytesCount / 2)) {
        // Lot of room...so open it up
        mInputConnection->SetMaxReadAhead(mEndOfInputRecd ? 0 : -1);
        return;
    }

    // few things missing here: we know the # of bytes that have been
    // read but not yet consumed---mPendingInputBytes.  we ought to
    // use that to figure out how much more we need to read to get a
    // full record.  if we are low on buffer space, we need to call
    // flush on the appender.  that'll also help open up buffer space.
    int readAhead;

    if (mPendingBytesCount > mMaxPendingBytesCount) {
        if (mIsInputDisabled)
            return;
        gettimeofday(&mInputDisableStartTime, NULL);
        mIsInputDisabled = true;
        readAhead = 0;
        KFS_LOG_STREAM_DEBUG << "Disabling input since buffered = " <<
            mPendingBytesCount << " max: " << mMaxPendingBytesCount <<
            KFS_LOG_EOM;
    } else {
        if ((mPendingBytesCount > mMinPendingBytesCount) &&
            (mIsInputDisabled))
            // we have drained a bit; need to drain further before
            // connection can be opened up
            return;

        int64_t slack = mMaxPendingBytesCount - mPendingBytesCount;
        if (slack < mNextRecordSize)
            // no room; so, don't open up the connection yet
            return;
        // if the slack is < 1M, that is how much we open up;
        // if there is more room, open it up
        readAhead = (slack < (int64_t) (1 << 20)) ? slack : -1;

        if (mIsInputDisabled) {
            KFS_LOG_STREAM_DEBUG << "Re-enabling input: "
                << " bytes buffered: " << mPendingBytesCount
                << " setting readahead to: " << readAhead
                << KFS_LOG_EOM;
            mIsInputDisabled = false;
            struct timeval endTime;
            gettimeofday(&endTime, NULL);
            mInputDisabledSec += ComputeTimeDiff(mInputDisableStartTime, endTime);
        }
    }
    mInputConnection->SetMaxReadAhead(mEndOfInputRecd ? 0 : readAhead);
}

void
InputProcessor::AppendDone(Appender *appender, int inStatusCode, int nbytesDone)
{
    if (inStatusCode != 0) {
        KFS_LOG_STREAM_FATAL << "Append for partition: " << appender->GetPartition()
            << " failed with error code: " << inStatusCode
            << " " << QCUtils::SysError(abs(inStatusCode)) << KFS_LOG_EOM;
        QCUtils::FatalError("", inStatusCode);
    }

    mPendingBytesCount -= nbytesDone;

    assert(mPendingBytesCount >= 0);

    SetReadAhead();

    if (mEndOfInputRecd && (!appender->IsActive())) {
        KFS_LOG_STREAM_INFO << "Appender for partition: " << appender->GetPartition()
            << " is done" << KFS_LOG_EOM;
        mActiveAppenders--;
        if (mActiveAppenders <= 0)
            Shutdown();
    }
}

int
InputProcessor::HandleEvent(int code, void *data)
{
    IOBuffer *iobuf;
    bool gotCmd = false;

    switch(code) {
        case EVENT_NET_READ:
            iobuf = (IOBuffer *) data;
            while ((!mIsInputDisabled) && IsMsgAvail(iobuf) &&
                   (gotCmd = HandleCmd(iobuf))) {
                gotCmd = false;
            }
            mPendingInputBytes = iobuf->BytesConsumable();
            SetReadAhead();
            break;

        case EVENT_NET_WROTE:
            break;

        case EVENT_INACTIVITY_TIMEOUT:
            // can get here if the input got turned off; need to
            // re-enable reading if memory usage is under control
            break;
        case EVENT_NET_ERROR:
        default:
            KFS_LOG_STREAM_INFO << "Looks like input was closed; shutting down" << KFS_LOG_EOM;
            Shutdown();
            break;
    }
    return 0;
}

// define a wire protocol and validate that we got a header
// and data that goes with it
bool
InputProcessor::IsMsgAvail(IOBuffer *inBuffer)
{
    return inBuffer->BytesConsumable() >= (int) sizeof(ProtoHdr_t);
}

// Got a cmd; validate it and run the append
bool
InputProcessor::HandleCmd(IOBuffer *inBuffer)
{
    ProtoHdr_t header;

    inBuffer->CopyOut((char *) &header, sizeof(ProtoHdr_t));
    header.ConvertToHostByteOrder();

    mNextRecordSize = header.dataLength + header.keyLength;

    if ((header.partition < 0) && (header.dataLength == 4)) {
        inBuffer->Consume(sizeof(ProtoHdr_t));
        KFS_LOG_STREAM_INFO << "Got a 0-byte record; so we are done!" << KFS_LOG_EOM;
        mEndOfInputRecd = true;
        Close();
        if (mActiveAppenders <= 0)
            Shutdown();
        return true;
    }
    if (inBuffer->BytesConsumable() <
        (int) (sizeof(ProtoHdr_t) + header.keyLength + header.dataLength)) {
        // don't got the data yet...so, wait
        return false;
    }

    if (header.partition >= (int) mAppenders.size()) {
        KFS_LOG_STREAM_FATAL << "Got a bogus partition: " << header.partition
            << " datalength: " << header.dataLength
            << " # of bytes consumable: " << inBuffer->BytesConsumable()
            << " ...aborting" << KFS_LOG_EOM;
        Close();
        QCUtils::FatalError("", -1);
    }

    inBuffer->Consume(sizeof(ProtoHdr_t));

    assert(inBuffer->BytesConsumable() >= (int) (header.keyLength + header.dataLength));

    mNumRecordsAppended++;

    mTotalBytesAppended += header.dataLength;

    int bytesSent = mAppenders[header.partition]->Append(*inBuffer, header.keyLength,
        header.dataLength);
    mPendingBytesCount += bytesSent;

    return true;
}
