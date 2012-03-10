//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: indexreader.cc 3498 2011-12-15 03:27:27Z sriramr $
//
// Created 2011/01/06
//
// Copyright 2011 Yahoo Corporation.  All rights reserved.
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
//
//----------------------------------------------------------------------------

#include "util.h"
#include "workbuilder.h"
#include "common/log.h"
#include "common/kfstypes.h"
#include "libkfsIO/Globals.h"
#include <fstream>
#include <sys/types.h>
#include <dirent.h>
#include <sys/stat.h>

#include <boost/lexical_cast.hpp>

extern "C" {
#include <openssl/rand.h>
}
using namespace KFS;
using namespace workbuilder;

const int MAX_RPC_HDR_LEN = 4096;
const int MAX_INDEXES_TO_READ_PER_IFILE = 16384; // XXX!

// Whenever get-chunk-layout RPC to the metaserver returns -EAGAIN,
// we retry N times, before giving up and moving to next chunk.
const int MAX_GET_ALLOC_RETRIES = 6;

using std::ofstream;
using std::endl;
using std::string;

string workbuilder::gMetaserver;
int workbuilder::gMetaport;
bool gIsIndexReadDisabled = false;

void
workbuilder::initIndexReader(const string &metahost, int metaport,
    bool isIndexReadDisabled)
{
    KFS_LOG_STREAM_DEBUG << "metashost "<< metahost << ", metaport " << metaport << KFS_LOG_EOM;
    gMetaserver = metahost;
    gMetaport = metaport;
    gIsIndexReadDisabled = isIndexReadDisabled;
}

bool
workbuilder::isIndexReadDisabled()
{
    return gIsIndexReadDisabled;
}

IndexReader::IndexReader(Job *job, ofstream &l) :
    ITimeout(),
    mJob(job),
    mNextChunkOffset(0), mFilesize(0),
    mNumChunks(0), mNumIndexesRead(0),
    mGetAllocRetryCount(0),
    mJobLog(l), mIsMetaOp(true), mIsIdle(true),
    mIsAllDone(false), mReducerStarted(false),
    mIsRecoveringLostChunk(false),
    mForcedAllChunksStable(false),
    mCurrentOpPtr(NULL),
    mLookupPathOp(0, 2, NULL),
    mLeaseAcquireOp(0, 0, NULL),
    mGetAllocOp(0, 0, 0),
    mGetChunkMetadataOp(0, 0),
    mGetChunkIndexOp(0, 0),
    mForceChunksStableOp(0, 0, NULL)
{
    libkfsio::globalNetManager().RegisterTimeoutHandler(this);
    // Disable the timer to begin with; it'll get enabled whenever we need to get read lease
    Disable();

    mSeqno = 1;
    RAND_pseudo_bytes(reinterpret_cast<unsigned char *>(&mSeqno), int(sizeof(mSeqno)));
    mSeqno = (mSeqno < 0 ? -mSeqno : mSeqno) >> 1;
    SET_HANDLER(this, &IndexReader::HandleEvent);
}

IndexReader::~IndexReader()
{
    Disable();
    libkfsio::globalNetManager().UnRegisterTimeoutHandler(this);

    if (mMetaserverConn)
        mMetaserverConn->Close();
    if (mChunkserverConn)
        mChunkserverConn->Close();
}

void IndexReader::init(int iFileId, int rackId, const string &iFilePath)
{
    mIFileId = iFileId;
    mRackId = rackId;
    mIFilePath = iFilePath;
    // if this index reader fails, we'll need a way to crash the reducer...
    // that said...get the animation going...
    mNextChunkOffset = 0;
    mNumChunks = 0;
    mIsIdle = false;
    start();
}

void
IndexReader::loadIndexFromDir(const string &indexDir)
{
    Disable();
    if (mMetaserverConn)
        mMetaserverConn->Close();
    if (mChunkserverConn)
        mChunkserverConn->Close();

    struct dirent **entries;
    int count;

    count = scandir(indexDir.c_str(), &entries, 0, alphasort);
    if (count < 0) {
        KFS_LOG_STREAM_INFO << "unable to open " << indexDir << KFS_LOG_EOM;
        return;
    }

    mNumChunks = (count - 2);
    mNextChunkOffset = ((off_t) mNumChunks) * KFS::CHUNKSIZE;

    for (int i = 0; (i < count) && (mNumIndexesRead < MAX_INDEXES_TO_READ_PER_IFILE); i++) {
        string fn = entries[i]->d_name;
        if ((fn == ".") || (fn == ".."))
            continue;
        string s = indexDir + "/" + entries[i]->d_name;
        vector<string> parts;

        split(fn, parts, '.');
        off_t offset = boost::lexical_cast<off_t>(parts[0]);
        readIndexFromFile(s, offset);
        mNumIndexesRead++;
    }
    free(entries);
}

void
IndexReader::readIndexFromFile(const string &filename,
    off_t chunkOffset)
{
    struct stat statRes;
    int res;

    res = stat(filename.c_str(), &statRes);
    if (res < 0) {
        KFS_LOG_STREAM_INFO << "Unable to read index from file: " <<
            filename << KFS_LOG_EOM;
        return;
    }
    int fd = open(filename.c_str(), O_RDONLY);
    char *buffer = new char[statRes.st_size];
    int nread;

    nread = read(fd, buffer, statRes.st_size);
    if (nread < 0) {
        KFS_LOG_STREAM_INFO << "Unable to read index from file: " <<
            filename << KFS_LOG_EOM;
        return;
    }
    KFS_LOG_STREAM_INFO << "Read of index from file: " <<
        filename << " successful" << KFS_LOG_EOM;

    // add a new index
    mJob->addNewChunkIndex(mIFileId, 0, chunkOffset, nread, buffer);
    delete [] buffer;
    close(fd);
}

void
IndexReader::ensureMetaserverConnection()
{
    KFS_LOG_STREAM_DEBUG << "metaserver "<< gMetaserver << ", metaport " << gMetaport << KFS_LOG_EOM;
    if (mMetaserverConn && mMetaserverConn->IsGood())
        return;
    // we have a netconnection object, but it is bad
    if (mMetaserverConn)
        mMetaserverConn->Close();

    TcpSocket *sock = new TcpSocket;
    ServerLocation  metaserverLoc(gMetaserver, gMetaport);

    int res = sock->Connect(metaserverLoc);
    if ((res == 0) || (res == -EINPROGRESS)) {
        mMetaserverConn.reset(new NetConnection(sock, this));
        mMetaserverConn->SetMaxReadAhead(-1);

        if (res == -EINPROGRESS) {
            mMetaserverConn->SetDoingNonblockingConnect();
        }
        libkfsio::globalNetManager().AddConnection(mMetaserverConn);
    } else {
        delete sock;
    }
}

//
// XXX: We don't handle the case where a chunk is lost in the map
// phase. If the index reader can't read a chunk index, it should move
// on.  We can handle it thru reducer side failure recovery...
//
void IndexReader::lostChunkRecoveryComplete()
{
    mIsRecoveringLostChunk = false;
    mIsAllDone = false;
    start();
}

void IndexReader::start()
{
    mCurrentOpPtr = NULL;

    if (mIsAllDone)
        return;

    mJobLog << now() << " : Starting lookup on ifile = " << mIFileId
        << " ; rack = " << mRackId
        << " ; offset = " << mNextChunkOffset << endl;

    ensureMetaserverConnection();

    if ((! mMetaserverConn) || (! mMetaserverConn->IsGood())) {
        // connect failure
        // need to abandon...
        KFS_LOG_STREAM_INFO << "Unable to connect to metaserver...exiting!" << KFS_LOG_EOM;
        exit(-1);
        return;
    }

    if (mReducerStarted && !mForcedAllChunksStable) {
        forceAllChunksStable();
    } else {
        lookupPath();
    }
}

void IndexReader::submitOp()
{
    mCurrentOpPtr->seq = mSeqno++;
    KFS_LOG_VA_DEBUG("Submitting %s op: %s", mIsMetaOp ? "meta" : "chunk",
        mCurrentOpPtr->Show().c_str());
    // put it in the queue
    mCurrentOpPtr->Request(mOutBuffer);
    if (mIsMetaOp) {
        mMetaserverConn->Write(&mOutBuffer);
        mMetaserverConn->SetMaxReadAhead(-1);
        if (!mMetaserverConn->IsGood()) {
            HandleEvent(EVENT_NET_ERROR, NULL);
            return;
        }
    } else {
        mChunkserverConn->Write(&mOutBuffer);
        if (!mChunkserverConn->IsGood()) {
            HandleEvent(EVENT_NET_ERROR, NULL);
            return;
        }
    }
}

void IndexReader::dispatch(IOBuffer *buffer)
{
    if (mCurrentOpPtr->status < 0) {
        KFS_LOG_VA_INFO("op: %s finished; seq = %ld, status = %d",
            mCurrentOpPtr->Show().c_str(), mCurrentOpPtr->seq,
            mCurrentOpPtr->status);
    }

    if (mCurrentOpPtr == &mLookupPathOp)
        done(mLookupPathOp, buffer);
    else if (mCurrentOpPtr == &mGetAllocOp)
        done(mGetAllocOp, buffer);
    else if (mCurrentOpPtr == &mLeaseAcquireOp)
        done(mLeaseAcquireOp, buffer);
    else if (mCurrentOpPtr == &mGetChunkMetadataOp)
        done(mGetChunkMetadataOp, buffer);
    else if (mCurrentOpPtr == &mGetChunkIndexOp)
        done(mGetChunkIndexOp, buffer);
    else if (mCurrentOpPtr == &mForceChunksStableOp)
        done(mForceChunksStableOp, buffer);
}

void IndexReader::parseResponse(IOBuffer *buffer)
{
    const int idx = buffer->IndexOf(0, "\r\n\r\n");
    if (idx < 0) {
        if (buffer->BytesConsumable() >= MAX_RPC_HDR_LEN) {
            // this is all the data we are going to read; if the response isn't valid terminate
            KFS_LOG_VA_INFO("Got an invalid response of len: %d",
                            buffer->BytesConsumable());
            mCurrentOpPtr->status = -1;
            dispatch(buffer);
            return;
        }
        // we don't have all the data yet; hold on
        return;
    }
    // +4 to get the length

    int hdrLen = idx + 4;
    IOBuffer::IStream is(*buffer, hdrLen);
    Properties prop;
    const char kSeparator = ':';
    kfsSeq_t seq;

    prop.loadProperties(is, kSeparator, false);
    seq = prop.getValue("Cseq", (kfsSeq_t) -1);
    if (seq != mCurrentOpPtr->seq) {
        KFS_LOG_STREAM_INFO << "Seq # mismatch: expect = " << mCurrentOpPtr->seq
            << " got = " << seq << KFS_LOG_EOM;
        mCurrentOpPtr->status = -1;
        // reset connection and retry
        if (mIsMetaOp) {
            if (mMetaserverConn) {
                mMetaserverConn->Close();
            }
        } else if (mChunkserverConn)
            mChunkserverConn->Close();

        dispatch(buffer);
        return;
    }
    mCurrentOpPtr->ParseResponseHeader(prop);
    if (mCurrentOpPtr->contentLength == 0) {
        buffer->Consume(hdrLen);
        dispatch(buffer);
        return;
    }
    int navail = buffer->BytesConsumable() - hdrLen;
    int ntodo = mCurrentOpPtr->contentLength - navail;
    if (ntodo == 0) {
        buffer->Consume(hdrLen);
        dispatch(buffer);
        return;
    }
    if (mIsMetaOp)
        mMetaserverConn->SetMaxReadAhead(ntodo);
    else
        mChunkserverConn->SetMaxReadAhead(ntodo);
}

int IndexReader::HandleEvent(int code, void *data)
{
    IOBuffer *iobuf = (IOBuffer *) data;
    switch(code) {
        case EVENT_NET_READ:
            if (mCurrentOpPtr == NULL) {
                // got a response, but we don't know for what;
                // so, empty buffer and reset metaserver connection.
                // the state machine will fix itself up.
                iobuf->Consume(iobuf->BytesConsumable());
                mMetaserverConn->Close();
                break;
            }
            // parse out the response
            parseResponse(iobuf);
            break;
        case EVENT_NET_WROTE:
            // data went out the network...move along
            break;
        case EVENT_INACTIVITY_TIMEOUT:
        case EVENT_NET_ERROR:
            if (mMetaserverConn)
                mMetaserverConn->Close();
            if (mCurrentOpPtr != NULL) {
                mCurrentOpPtr->status = -ETIMEDOUT;
                dispatch(NULL);
            }
            break;
        default:
            assert(!"Unknown event");
            break;
    }
    return 0;
}

void IndexReader::Timeout()
{
    // Disable the timer
    Disable();

    if ((mIsIdle) && (!mIsAllDone)) {
        mIsIdle = false;
        start();
        return;
    }

    if (mCurrentOpPtr == &mLeaseAcquireOp) {
        mJobLog << now() << ": Retrying to get lease for " << mLookupPathOp.filename << endl;
        getReadLease();
        return;
    }
}

void
IndexReader::resetForRetry()
{
    const int kOneMinute = 60000;
    const int kFiveSecs = 5000;
    // reset and retry
    mIsMetaOp = false;
    mCurrentOpPtr = NULL;
    mIsIdle = true;
    if (mForcedAllChunksStable)
        SetTimeoutInterval(kFiveSecs, true);
    else
        SetTimeoutInterval(kOneMinute, true);
}

void IndexReader::forceAllChunksStable()
{
    mForcedAllChunksStable = true;

    if (mNextChunkOffset > (off_t) 10 * (off_t) KFS::CHUNKSIZE) {
        mJobLog << now() << " : lots of chunks; skipping force-chunk-stable for: "
            << mIFilePath << endl;

        start();
        return;
    }

    if (mLookupPathOp.fattr.fileId < 0) {
        mForcedAllChunksStable = false;
        lookupPath();
        return;
    }
    
    mIsMetaOp = true;
    mCurrentOpPtr = &mForceChunksStableOp;

    mForceChunksStableOp.fid = mLookupPathOp.fattr.fileId;
    mForceChunksStableOp.pathname = mIFilePath.c_str();

    mJobLog << now() << " : forcing chunks to be made stable for: "
        << mIFilePath << endl;

    submitOp();
}

void IndexReader::done(MakeChunksStableOp &forceChunksStableOp, IOBuffer *inBuffer)
{
    // fire and forget deal...so, move along independent of the response
    mJobLog << now() << " : force chunk stable for path: "
        << mIFilePath << " returned: "
        << mForceChunksStableOp.status << endl;

    start();
}

void IndexReader::lookupPath()
{
    mIsMetaOp = true;
    mCurrentOpPtr = &mLookupPathOp;

    mLookupPathOp.filename = mIFilePath.c_str();

    submitOp();
}

void IndexReader::done(LookupPathOp &lookupPathOp, IOBuffer *inBuffer)
{
    if (mLookupPathOp.status == -ETIMEDOUT) {
        mMetaserverConn->Close();
        mJobLog << now() << ": Unable to lookup path for " << mLookupPathOp.filename << endl;
        resetForRetry();
        return;
    }

    if (mReducerStarted && !mForcedAllChunksStable) {
        // we got a valid view of the world...reset
        start();
        return;
    }

    if (((int) mLookupPathOp.fattr.chunkCount == mNumChunks) &&
        (mNextChunkOffset == ((off_t) mNumChunks) * (off_t) KFS::CHUNKSIZE)) {

        if (mReducerStarted) {
            Disable();
            // if reducer started and we have read all the per-chunk
            // indexes, notify Job to get the planning going if
            // possible.
            mJobLog << now() << " : index reading is done for: " << mIFileId
                << " ; rack = " << mRackId
                << endl;
            mIsAllDone = true;
            mIsIdle = true;
            mJob->indexReaderDone();
            if (mMetaserverConn)
                mMetaserverConn->Close();
            if (mChunkserverConn)
                mChunkserverConn->Close();
            return;
        }
        if (mIsAllDone) {
            mJobLog << now() << " : all done with index reading for: " << mIFileId
                << " ; rack = " << mRackId
                << endl;
            return;
        }
        const int kOneMinute = 60000;
        SetTimeoutInterval(kOneMinute, true);
        mIsIdle = true;
        return;
    }
    mNumChunks = mLookupPathOp.fattr.chunkCount;
    getChunkLayout();
}

void IndexReader::getChunkLayout()
{
    mCurrentOpPtr = &mGetAllocOp;
    mGetAllocOp.fid = mLookupPathOp.fattr.fileId;
    mGetAllocOp.fileOffset = mNextChunkOffset;
    mGetAllocOp.filename = mIFilePath;
    mGetAllocOp.chunkServers.clear();
    submitOp();
}

void IndexReader::done(GetAllocOp &allocOp, IOBuffer *inBuffer)
{
    if (mGetAllocOp.status < 0) {
        if (mGetAllocOp.status == -ETIMEDOUT) {
            mMetaserverConn->Close();
        }
        mJobLog << now() << ": Unable to get chunk layout for " << mLookupPathOp.filename
            << " offset: " << mNextChunkOffset
            << " error code: " << mGetAllocOp.status << endl;

        if (mGetAllocOp.status == -EAGAIN) {
            // We get this error code from the metaserver whenever the
            // chunk is lost.
            mGetAllocRetryCount++;
            if (mGetAllocRetryCount >= MAX_GET_ALLOC_RETRIES) {
                mJobLog << now() << "Too many retries for getting layout for "
                    << mLookupPathOp.filename
                    << " offset: " << mNextChunkOffset
                    << "  giving up and moving on... " << endl;
                mGetAllocRetryCount = 0;
                mNextChunkOffset += KFS::CHUNKSIZE;
                // march on...
                start();
            }
        }
        resetForRetry();
        return;
    }

    // reset
    mGetAllocRetryCount = 0;

    mLeaseRetryCount = 0;
    getReadLease();
}

void IndexReader::getReadLease()
{
    mLeaseRetryCount++;
    mCurrentOpPtr = &mLeaseAcquireOp;
    mLeaseAcquireOp.status = -1;
    mLeaseAcquireOp.pathname = mLookupPathOp.filename;
    mLeaseAcquireOp.chunkId = mGetAllocOp.chunkId;
    submitOp();
}

void IndexReader::done(LeaseAcquireOp &leaseAcquireOp, IOBuffer *inBuffer)
{
    // clear pending timers if any---this covers the case where we had
    // set a timer to get a reade lease and we got a reply before the
    // timer fired; alternately, we are here because the timer we had
    // set had fired and we many need to start a new one
    Disable();
    const int kOneMinute = 60000;
    const int kFiveSecs = 5000;

    // if we didn't get the lease, need to schedule a retry
    if (mLeaseAcquireOp.status == -EBUSY) {
        mJobLog << now() << ": Got EBUSY when trying to get lease for "
            << mLookupPathOp.filename
            << " offset: " << mNextChunkOffset << " retry count: " << mLeaseRetryCount
            << endl;
        if (mForcedAllChunksStable)
            SetTimeoutInterval(kFiveSecs, true);
        else
            SetTimeoutInterval(kOneMinute, true);
        return;
    }
    if (mLeaseAcquireOp.status == 0) {
        // got the lease...figure out if the index is ready
        mReadIndexFromReplica = 0;
        readChunkMetadata();
        return;
    }

    // reset and retry
    mCurrentOpPtr = NULL;
    mIsIdle = true;

    mMetaserverConn->Close();
    mJobLog << now() << ": Unable to get acquire lease for " << mLookupPathOp.filename
        << " offset: " << mNextChunkOffset
        << " error code: " << mLeaseAcquireOp.status << endl;
    SetTimeoutInterval(kOneMinute, true);
    return;
}

bool IndexReader::areSufficientIndexesRead() {
    return (mNumIndexesRead > MAX_INDEXES_TO_READ_PER_IFILE);
}

void IndexReader::readChunkMetadata()
{
    mIsMetaOp = false;

    // connect to a replica in the mGetAllocOp
    TcpSocket *sock = new TcpSocket;

    int res = sock->Connect(mGetAllocOp.chunkServers[mReadIndexFromReplica], true);

    if ((res == 0) || (res == -EINPROGRESS)) {
        mChunkserverConn.reset(new NetConnection(sock, this));
        mChunkserverConn->SetMaxReadAhead(MAX_RPC_HDR_LEN);

        if (res == -EINPROGRESS) {
            mChunkserverConn->SetDoingNonblockingConnect();
        }
        libkfsio::globalNetManager().AddConnection(mChunkserverConn);

        getChunkMetadata();
        return;
    }
    delete sock;
    // didn't work..retry
    mJobLog << now() << ": Unable to connect to: " << mGetAllocOp.chunkServers[0].ToString()
            << ' ' <<  mLookupPathOp.filename << endl;
    // retry
    mCurrentOpPtr = NULL;
    mIsIdle = true;

    const int kOneMinute = 60000;
    SetTimeoutInterval(kOneMinute, true);
}

void IndexReader::getChunkMetadata()
{
    mJobLog << now() << ": Starting get chunk metadata for: " << mIFileId << ","
        << ",rack = " << mRackId  << ","
        << mNextChunkOffset << endl;
    mCurrentOpPtr = &mGetChunkMetadataOp;

    mGetChunkMetadataOp.Clear();
    mGetChunkMetadataOp.chunkId = mGetAllocOp.chunkId;
    submitOp();
}

void IndexReader::done(GetChunkMetadataOp &getChunkMetadataOp, IOBuffer *inBuffer)
{
    if (mGetChunkMetadataOp.status < 0) {
        mChunkserverConn->Close();
        mJobLog << now() << ": Unable to get metadata for " << mLookupPathOp.filename
            << " offset: " << mNextChunkOffset
            << " from: " << mGetAllocOp.chunkServers[0].ToString()
            << " error code: " << mGetChunkMetadataOp.status << endl;
        // reset and retry;
        // wait before retrying...otherwise, we can exhaust all the ports
        // by going into a loop
        mCurrentOpPtr = NULL;
        mIsIdle = true;

        const int kOneMinute = 60000;
        SetTimeoutInterval(kOneMinute, true);

        return;
    }

    // if there are any checksums, discard them
    if (mGetChunkMetadataOp.contentLength > 0)
        inBuffer->Consume(mGetChunkMetadataOp.contentLength);

    if (mGetChunkMetadataOp.chunkSize == 0) {
        mJobLog << now() << ": in file: " << mLookupPathOp.filename
            << " chunk offset: " << mNextChunkOffset
            << " has a 0 size...mmmm..." << endl;
    }

    if ((mGetChunkMetadataOp.chunkSize > 0) &&
        (mGetChunkMetadataOp.indexSize <= 0)) {
        mJobLog << now() << ": index for offset: " << mLookupPathOp.filename
            << " offset: " << mNextChunkOffset
            << " isn't ready yet...waiting" << endl;
        mChunkserverConn->Close();
        // means that the index isn't ready yet...
        resetForRetry();
        return;
    }
    // validate that the index is built on all the replicas
    mReadIndexFromReplica++;
    getIndex();
}

void IndexReader::getIndex()
{
    if (mReadIndexFromReplica < mGetAllocOp.chunkServers.size()) {
        mJobLog << now() << " :Moving to next replica; starting to get chunk metadata for: "
            << mIFileId << "," << mRackId << "," << mNextChunkOffset << endl;
        mChunkserverConn->Close();
        readChunkMetadata();
        return;
    }

    // By the time we get here, we know that the index has been built
    if (gIsIndexReadDisabled || areSufficientIndexesRead() ||
        (mGetChunkMetadataOp.chunkSize == 0)) {
        // index reading is disabled or no longer needed or chunk has no index;
        // move on...
        mNextChunkOffset += KFS::CHUNKSIZE;
        mChunkserverConn->Close();
        // continue on...
        start();
        return;
    }

    mJobLog << now() << ": Starting index read for: " << mIFileId << ","
        << " ; rack = " << mRackId
        << ", " << mNextChunkOffset << endl;
    mCurrentOpPtr = &mGetChunkIndexOp;
    mGetChunkIndexOp.Clear();
    mGetChunkIndexOp.chunkId = mGetAllocOp.chunkId;
    submitOp();
}

void IndexReader::done(GetChunkIndexOp &getIndexOp, IOBuffer *inBuffer)
{
    if (mGetChunkIndexOp.status < 0) {
        mChunkserverConn->Close();
        mJobLog << now() << ": Unable to get index for " << mLookupPathOp.filename
            << " offset: " << mNextChunkOffset
            << " from: " << mGetAllocOp.chunkServers[0].ToString()
            << " error code: " << mGetChunkIndexOp.status << endl;
        // reset and retry;
        // wait before retrying...otherwise, we can exhaust all the ports
        // by going into a loop
        mCurrentOpPtr = NULL;
        mIsIdle = true;

        const int kOneMinute = 60000;
        SetTimeoutInterval(kOneMinute, true);

        return;
    }

    if (mGetChunkIndexOp.indexSize == 0) {
        mJobLog << now() << ": index for offset: " << mLookupPathOp.filename
            << " offset: " << mNextChunkOffset
            << " isn't ready yet...waiting" << endl;
        // means that the index isn't ready yet...
        mChunkserverConn->Close();
        resetForRetry();
        return;
    }

    mGetChunkIndexOp.contentBufLen = mGetChunkIndexOp.indexSize;
    mGetChunkIndexOp.contentBuf = new char[mGetChunkIndexOp.indexSize];

    int nbytesCopied = inBuffer->CopyOut(mGetChunkIndexOp.contentBuf,
        mGetChunkIndexOp.indexSize);

    if (nbytesCopied != (int) mGetChunkIndexOp.indexSize) {
        mJobLog << now() << ": got a partial index for offset: "
            << mNextChunkOffset
            << " promised: " << mGetChunkIndexOp.indexSize
            << " but got" << nbytesCopied << endl;
        mChunkserverConn->Close();
        resetForRetry();
        return;
    }

    mJobLog << now() << ": index read for: " << mIFileId
        << ", rack = " << mRackId
        << "," << mNextChunkOffset
        << " is done; starting processing..." << endl;

    mNumIndexesRead++;

    mFilesize += mGetChunkMetadataOp.chunkSize;

    mJob->addNewChunkIndex(mIFileId, mGetChunkIndexOp.chunkId, mNextChunkOffset,
        mGetChunkIndexOp.indexSize,
        mGetChunkIndexOp.contentBuf);

    mJobLog << now() << ": index read for: " << mIFileId
        << ", rack = " << mRackId
        << "," << mNextChunkOffset
        << " processing is done" << endl;

    mGetChunkIndexOp.Clear();
    mChunkserverConn->Close();

    mNextChunkOffset += KFS::CHUNKSIZE;

    // continue on...
    start();
}

