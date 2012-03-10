//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: appender.cc 3365 2011-11-23 22:40:49Z sriramr $
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
// \brief Class that handles atomic-appends for an I-file.
//----------------------------------------------------------------------------


#ifdef USE_INTEL_IPP
#include <ipp.h>
#else
#include <lzo/lzo1x.h>
#include <lzo/lzoconf.h>
#endif

#include "kappender.h"
#include "common/log.h"
#include "util.h"

using namespace KFS;

#ifndef IN_LEN
#define IN_LEN      (128*1024L)
#endif
#define OUT_LEN     (IN_LEN + IN_LEN / 16 + 64 + 3)

Appender::Appender(MetaServer& inMetaServer, const char *logPrefixPtr,
    InputProcessor *ip, uint32_t partition,
    uint64_t mapperAttempt, int rackId) :
    WriteAppender::Completion(),
    WriteAppender(inMetaServer, this, logPrefixPtr, rackId),
    mOwner(ip),
    mIsCompressionEnabled(false),
    mPartition(partition),
    mPendingBytes(0),
    mUncompBuffer(0),
    mUncompBufferSize(0),
    mCompBuffer(0),
    mCompBufferSize(0),
    mSequenceNumber(0),
    mBytesBuffered(0),
    mKeyLengthBuffered(0),
    mBufferLimit(64*1024),
    mMapperAttempt(mapperAttempt)
{
    mWrkMem = NULL;
#ifdef USE_INTEL_IPP
    Ipp32u lzoSize;
    ippsEncodeLZOGetSize(IppLZO1XST, 0, &lzoSize);
    MemAlignedMalloc(&mWrkMem, sizeof(IppLZOState_8u*), lzoSize);
    ippsEncodeLZOInit_8u(IppLZO1XST, 0, mWrkMem);
#else
    MemAlignedMalloc(&mWrkMem, sizeof(lzo_align_t), LZO1X_1_MEM_COMPRESS);
    assert(mWrkMem != NULL);
#endif
}

int
Appender::Open(const char *inFileNamePtr,
        int numReplicas,
        bool inMakeDirsFlag)
{
    return WriteAppender::Open(inFileNamePtr, numReplicas, inMakeDirsFlag);
}

bool
Appender::IsActive() const
{
    return WriteAppender::IsActive();
}

    void
Appender::Shutdown()
{
    return WriteAppender::Shutdown();
}

    int
Appender::Close()
{
    return WriteAppender::Close();
}

std::set<kfsChunkId_t>
Appender::GetChunksAppended()
{
    return WriteAppender::GetChunksAppended();
}

int
Appender::EndOfInput()
{
    if (mBytesBuffered == 0)
        return 0;
    if (mIsCompressionEnabled) {
        CompressRecords();
        mBytesBuffered = mBuffer.BytesConsumable();
    }
    return Flush();
}

int
Appender::Flush()
{
    KFS_LOG_STREAM_DEBUG << "mBytesBuffered " << mBytesBuffered
        << ", doing flush" << KFS_LOG_EOM;

    // mBytesBuffered is what we are flushing down; so, update what is pending
    mPendingBytes += mBytesBuffered;

    //int retCode = WriteAppender::Append(mBuffer, mBytesBuffered);
    int retCode = WriteAppender::Append(mBuffer, mBytesBuffered,
        &mKeyBuffer, mKeyLengthBuffered);
    if (retCode > 0) {
        // The write-appender either takes everything or nothing
        mBytesBuffered = 0;
        mKeyLengthBuffered = 0;
    }
    assert(mBuffer.BytesConsumable() == 0);
    return retCode;
}

int
Appender::Append(IOBuffer &inBuffer, int inKeyLength, int inDataLength)
{
    int buffered = AppendRecord(inBuffer, inKeyLength, inDataLength);
    mBytesBuffered += buffered;
    assert(mBytesBuffered == mBuffer.BytesConsumable());
    if (inKeyLength > 0) {
        // when we copy a key, we also needs its length and the length of data
        mKeyLengthBuffered += inKeyLength + sizeof(int) + sizeof(int);
    }
    KFS_LOG_STREAM_DEBUG << "mBytesBuffered: " << mBytesBuffered << KFS_LOG_EOM;
    //TODO: decide if we should add mBytesBuffered and mKeyLengthBuffered for
    //this
    if (mBytesBuffered > mBufferLimit)
    {
        KFS_LOG_STREAM_DEBUG << "mBytesBuffered exceeds limit " << mBufferLimit
            << ", flushing" << KFS_LOG_EOM;
        if (mIsCompressionEnabled) {
            CompressRecords();
            mBytesBuffered = mBuffer.BytesConsumable();
        }
        return Flush();
    }
    else {
        KFS_LOG_STREAM_DEBUG << "mBytesBuffered does not exceed limit " << mBufferLimit
            << KFS_LOG_EOM;
        // return buffered;
        return 0;
    }
}

int
Appender::AppendRecord(IOBuffer &inBuffer, int inKeyLength, int inDataLength)
{
    // Store the record into the buffer
    //create packet header
    IFilePktHdr_t pktHdr(mMapperAttempt, mPartition, inDataLength + 1, inDataLength);
    int totalBytes = sizeof(IFilePktHdr_t);
    pktHdr.codec=0x0;
    pktHdr.sequenceNumber = mSequenceNumber;
    mSequenceNumber++;
    totalBytes += inDataLength;
    // This call can allocate a buffer of the default size
    mBuffer.CopyIn((const char *) &pktHdr, sizeof(IFilePktHdr_t));
    if (inKeyLength > 0) {
        // for each record, also include in the per-pkt header
        // this per-pkt header is stripped out in groupby
        int totalDataLen = inDataLength + sizeof(IFilePktHdr_t);
        mKeyBuffer.CopyIn((const char *) &inKeyLength, sizeof(int));
        mKeyBuffer.CopyIn((const char *) &totalDataLen, sizeof(int));
        mKeyBuffer.Copy(&inBuffer, inKeyLength);
        inBuffer.Consume(inKeyLength);
    }
    if (inDataLength < IOBufferData::GetDefaultBufferSize() * 2) {
        // If the record is small, just copy; otherwise, we waste the
        // buffer we allocated above.
        mBuffer.ReplaceKeepBuffersFull(&inBuffer,
            mBuffer.BytesConsumable(), inDataLength);
    } else {
        mBuffer.Move(&inBuffer, inDataLength);
    }

    KFS_LOG_STREAM_DEBUG << "[p = " << mPartition << "]: "
        << inKeyLength << '/' << inDataLength << KFS_LOG_EOM;
    return totalBytes;
}

void
Appender::CompressRecords()
{
    if (mBuffer.BytesConsumable() > mUncompBufferSize) {
        mUncompBufferSize = mBuffer.BytesConsumable();
#ifdef USE_INTEL_IPP
        size_t alignment = sizeof(Ipp8u *);
#else
        size_t alignment = sizeof(lzo_voidp);
#endif
        mUncompBuffer = new char[mUncompBufferSize];
        /*
        MemAlignedMalloc(&mUncompBuffer, alignment,
            (size_t) mUncompBufferSize);
        */
        assert(mUncompBuffer != NULL);
    }
    int ulen = mBuffer.BytesConsumable();

    mBuffer.CopyOut(mUncompBuffer, ulen);
    // clear the buffer
    mBuffer.Consume(ulen);
    // make sure we got memory for the compressed buffer
#ifdef USE_INTEL_IPP
    Ipp32u clen = 2 * ulen;
    if (clen > (Ipp32u) mCompBufferSize) {
        mCompBufferSize = clen;
        /*
        MemAlignedMalloc(&mCompBuffer, sizeof(Ipp8u *),
            (size_t) mCompBufferSize);
        */
        mCompBuffer = new char[mCompBufferSize];

        assert(mCompBuffer != NULL);
    }
#else
    lzo_uint clen = 2 * ulen;
    if (clen > (lzo_uint) mCompBufferSize) {
        mCompBufferSize = clen;
        mCompBuffer = new char[mCompBufferSize];
        /*
        MemAlignedMalloc(&mCompBuffer, sizeof(lzo_voidp),
            (size_t) mCompBufferSize);
        */
        assert(mCompBuffer != NULL);
    }
#endif

    //create header for the compressed block
    IFilePktHdr_t pktHdr(mMapperAttempt, mPartition, 0, ulen);
    pktHdr.codec = LZO_CODEC;

    bool isCompressionOk = false;
#ifdef USE_INTEL_IPP
    IppStatus retCode = ippsEncodeLZO_8u(
        (const Ipp8u*)mUncompBuffer, (Ipp32u) ulen,
        (Ipp8u*)mCompBuffer,&clen,mWrkMem);
    // isCompressionOk = (retCode == ippStsNoErr);
    if (retCode) {
        assert(!"Not possible");
        isCompressionOk = false;
    }
    else {
        isCompressionOk = true;
    }
#else
    int retCode = lzo1x_1_compress(
        (const unsigned char*)mUncompBuffer, ulen,
        (unsigned char*)mCompBuffer,&clen,mWrkMem);
    isCompressionOk = (retCode == LZO_E_OK);
#endif

    if (!isCompressionOk) {
        KFS_LOG_STREAM_INFO << "Compression failed...sending uncompressed data: err = "
            << retCode << KFS_LOG_EOM;
        // compression didn't work...sigh!
        // No need to add the "compression block" header
        mBuffer.CopyIn((const char *) mUncompBuffer, ulen);
    } else {
        pktHdr.compressedRecLen = clen;
        mBuffer.CopyIn((const char *) &pktHdr, sizeof(IFilePktHdr_t));
        mBuffer.CopyIn((const char *) mCompBuffer, clen);
    }
    delete [] mUncompBuffer;
    delete [] mCompBuffer;
    mUncompBufferSize = mCompBufferSize = 0;
    mUncompBuffer = mCompBuffer = NULL;
}

int
Appender::GetPendingBytes()
{
    // return how much data is in pending at the underlying appender
    return WriteAppender::GetPendingSize();
}

void
Appender::Done(WriteAppender &inAppender, int inStatusCode)
{
    int nDone = mPendingBytes - GetPendingBytes();

    assert(nDone >= 0);
    mOwner->AppendDone(this, inStatusCode, nDone);
    mPendingBytes -= nDone;

    assert(mPendingBytes >= 0);
}
