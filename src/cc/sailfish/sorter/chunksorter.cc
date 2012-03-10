//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: chunksorter.cc $
//
// Created 2011/02/08
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

#include "chunksorter.h"
#include "common/properties.h"

// leverage all the code from chunkserver
#include "chunk/KfsOps.h"
#include "chunk/Utils.h"
#include "util.h"

#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>

#include <sys/stat.h>
#include <cerrno>
#include <arpa/inet.h>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <vector>
#include <boost/scoped_array.hpp>

using namespace KFS;
using std::string;
using std::ostringstream;
using std::sort;
using std::vector;
using std::find_if;

SortWorkerManager::SortWorkerManager()
{
#ifdef USE_INTEL_IPP
    IppStatus retCode = ippStaticInit();
    assert(retCode == ippStsNoErr);
#else
    int retCode = lzo_init();
    assert(retCode == LZO_E_OK);
#endif
    // init the RPC parse handlers from chunkserver code
    InitParseHandlers();

    SET_HANDLER(this, &SortWorkerManager::HandleEvent);
}

SortWorkerManager::~SortWorkerManager()
{
    if (mSock.GetFd() == 1)
        mSock.Clear();
}

int
SortWorkerManager::HandleEvent(int code, void *data)
{
    IOBuffer *iobuf;
    int msgLen;
    bool gotCmd = true;

    switch(code) {
        case EVENT_NET_READ:
            iobuf = (IOBuffer *) data;
            while (IsMsgAvail(iobuf, &msgLen) && gotCmd) {
                gotCmd = HandleCmd(iobuf, msgLen);
            }
            break;

        case EVENT_NET_WROTE:
            break;

        case EVENT_INACTIVITY_TIMEOUT:
            break;
        case EVENT_NET_ERROR:
        default:
            KFS_LOG_STREAM_INFO << "Looks like connection broke; shutting down" << KFS_LOG_EOM;
            mNetManager.Shutdown();
            break;
    }
    return 0;
}

bool
SortWorkerManager::HandleCmd(IOBuffer *iobuf, int msgLen)
{
    IOBuffer::IStream is(*iobuf, msgLen);
    KfsOp *op = NULL;
    AllocChunkOp *ac;
    RecordAppendOp *ra;
    SorterFlushOp *shf;
    SortFileOp *sf;

    if (ParseCommand(is, &op) != 0) {
        assert(! op);
        is.Rewind(msgLen);
        string line;
        int    maxLines = 64;
        while (--maxLines >= 0 && getline(is, line)) {
            KFS_LOG_STREAM_ERROR <<
                "invalid request: " << line <<
                KFS_LOG_EOM;
        }
        iobuf->Consume(msgLen);
        // got a bogus command
        return true;
    }

    if (op->op == CMD_RECORD_APPEND) {
        ra = static_cast<RecordAppendOp*>(op);
        if ((int) ra->numBytes > (int) iobuf->BytesConsumable() - msgLen) {
            // wait until we got all the data
            delete op;
            return false;
        }
    }

    iobuf->Consume(msgLen);

    switch(op->op) {
        case CMD_ALLOC_CHUNK:
            ac = static_cast<AllocChunkOp *>(op);
            HandleAllocChunk(ac);
            break;

        case CMD_RECORD_APPEND:
            ra = static_cast<RecordAppendOp*>(op);
            ra->dataBuf.Move(iobuf, ra->numBytes);
            HandleRecordAppend(ra);
            break;
        case CMD_SORTER_FLUSH:
            shf = static_cast<SorterFlushOp*>(op);
            HandleFlush(shf);
            break;
        case CMD_SORT_FILE:
            sf = static_cast<SortFileOp*>(op);
            HandleSortFile(sf);
            break;
        default:
            KFS_LOG_STREAM_INFO << "Got unknown cmd: " <<
                op->op << KFS_LOG_EOM;
            break;
    }
    delete op;
    return true;
}

void
SortWorkerManager::HandleAllocChunk(AllocChunkOp *op)
{
    SortWorkerPtr sw;

    sw.reset(new SortWorker(op->fileId, op->chunkId, op->chunkVersion));

    mWorkers.push_back(sw);

    KFS_LOG_STREAM_INFO << "Created a new sort worker for chunk: "
        << op->chunkId << KFS_LOG_EOM;

    KFS_LOG_STREAM_INFO << "# of active sorters: " << mWorkers.size()
        << KFS_LOG_EOM;

}

class WorkerMatcher {
    kfsChunkId_t chunkId;
public:
    WorkerMatcher(kfsChunkId_t c) : chunkId(c) { }
    bool operator() (const SortWorkerPtr &w) {
        return w->getChunkId() == chunkId;
    }
};

void
SortWorkerManager::HandleRecordAppend(RecordAppendOp *op)
{
    vector<SortWorkerPtr>::iterator entry;

    entry = find_if(mWorkers.begin(), mWorkers.end(), WorkerMatcher(op->chunkId));
    if (entry == mWorkers.end()) {
        KFS_LOG_STREAM_INFO << "Got an append request for a non-existent chunk: "
            << op->chunkId << KFS_LOG_EOM;
        return;
    }
    (*entry)->append(op);
}

void
SortWorkerManager::HandleFlush(SorterFlushOp *op)
{
    vector<SortWorkerPtr>::iterator entry;
    IOBuffer::OStream os;

    entry = find_if(mWorkers.begin(), mWorkers.end(), WorkerMatcher(op->chunkId));
    if (entry == mWorkers.end()) {
        KFS_LOG_STREAM_INFO << "Got a flush request for a non-existent chunk: "
            << op->chunkId << KFS_LOG_EOM;
        op->status = -1;
    } else {
        (*entry)->flush(op);
        KFS_LOG_STREAM_INFO << "Result of flushing: " << op->outputFn << " : "
            << op->status << KFS_LOG_EOM;
        mWorkers.erase(entry);
        KFS_LOG_STREAM_INFO << "# of active sorters: " << mWorkers.size()
            << KFS_LOG_EOM;
    }
    op->Response(os);
    mNetConnection->Write(&os);
}

void
SortWorkerManager::HandleSortFile(SortFileOp *op)
{
    SortWorker sw(op->fid, op->chunkId, op->chunkVersion);
    IOBuffer::OStream os;

    sw.sortFile(op);
    op->Response(os);
    mNetConnection->Write(&os);
}

int
SortWorkerManager::mainLoop(int chunkserverPort)
{
    ServerLocation location("localhost", chunkserverPort);
    int retryCount = 0;

    if (chunkserverPort == 0) {
        fcntl(chunkserverPort, F_SETFL, O_NONBLOCK);
        TcpSocket s(chunkserverPort);

        mSock = s;
        s = TcpSocket();

    } else {
        while (retryCount < 5) {
            if (mSock.Connect(location) < 0) {
                KFS_LOG_STREAM_INFO << "Unable to connect to chunkserver at port: " <<
                    chunkserverPort << "sleeping..." << KFS_LOG_EOM;
                sleep(60);
                retryCount++;
            }
            else {
                retryCount = 0;
                break;
            }
        }

        if (retryCount > 0) {
            KFS_LOG_STREAM_INFO << "Unable to connect to chunkserver; giving up!" << KFS_LOG_EOM;
            return -1;
        }
    }

    // Don't let the netconnection call delete on mSock
    mNetConnection.reset(new NetConnection(&mSock, this, false, false));

    mNetManager.AddConnection(mNetConnection);

    mNetManager.MainLoop();

    return 0;
}

SortWorker::SortWorker(kfsFileId_t f, kfsChunkId_t c, int64_t v) :
    mFileId(f), mChunkId(c), mChunkVersion(v),
    mChunkDataLen(0),
    mUncompBuffer(0), mUncompBufferSz(0)
{
    const long kPageSz = sysconf(_SC_PAGESIZE);
    mChunkBuffer = NULL;
    MemAlignedMalloc(&mChunkBuffer, kPageSz, KFS_CHUNK_HEADER_SIZE + KFS::CHUNKSIZE);
    mChunkData = mChunkBuffer + KFS_CHUNK_HEADER_SIZE;
    mIndexBuffer = NULL;
    MemAlignedMalloc(&mIndexBuffer, kPageSz, INDEX_BUFFER_SIZE);
    mDci = NULL;
    MemAlignedMalloc(&mDci, kPageSz, KFS_CHUNK_HEADER_SIZE);
#ifdef USE_INTEL_IPP
    Ipp32u lzoSize;
    ippsEncodeLZOGetSize(IppLZO1XST, 0, &lzoSize);
    mWrkMem = (IppLZOState_8u*)ippsMalloc_8u(lzoSize);
    ippsEncodeLZOInit_8u(IppLZO1XST, 0, mWrkMem);
#else
    mWrkMem = 0;
    MemAlignedMalloc(&mWrkMem, sizeof(lzo_align_t), LZO1X_1_MEM_COMPRESS);
#endif
}

SortWorker::~SortWorker()
{
    free(mChunkBuffer);
    free(mIndexBuffer);
    free(mDci);
#ifdef USE_INTEL_IPP
    ippsFree(mWrkMem);
#else
    // These are allocated via posix_memalign
    free(mWrkMem);
#endif
    free(mUncompBuffer);
    mRecords.clear();
}

void
SortWorker::append(RecordAppendOp *op)
{
    size_t nRead = 0;
    IOBuffer::IStream is(op->dataBuf, op->numBytes);
    const int kRecOverhead = sizeof(IFilePktHdr_t) + 2 * sizeof(uint32_t);

    while (nRead < op->numBytes) {
        IFileRecord_t record;

        // Format: <pkt hdr><4 byte key-len><key><4-byte val-len><value>
        is.read((char *) &record.pktHdr, sizeof(IFilePktHdr_t));
        is.read((char *) &record.keyLen, sizeof(uint32_t));
        record.keyLen = ntohl(record.keyLen);
        record.key.reset(new unsigned char[record.keyLen]);
        is.read((char *) record.key.get(), record.keyLen);
        is.read((char *) &record.valLen, sizeof(uint32_t));
        record.valLen = ntohl(record.valLen);
        record.value.reset(new unsigned char[record.valLen]);
        is.read((char *) record.value.get(), record.valLen);
        record.recLen = record.keyLen + record.valLen;
        mRecords.push_back(record);
        nRead += record.recLen + kRecOverhead;
    }
    assert(nRead == op->numBytes);
    op->dataBuf.Consume(op->numBytes);
}

void
SortWorker::sortFile(SortFileOp *op)
{
    op->status = readFromChunk(op->inputFn);
    if (op->status < 0)
        return;

    KFS_LOG_STREAM_INFO << "Starting sort on chunk: " << mChunkId
        << " # of records = " << mRecords.size() << KFS_LOG_EOM;
    sort(mRecords.begin(), mRecords.end());
    KFS_LOG_STREAM_INFO << "Done with sort" << KFS_LOG_EOM;

    op->status = writeToChunk(op->outputFn, mRecords, op->indexSize);
}

void
SortWorker::flush(SorterFlushOp *op)
{
    KFS_LOG_STREAM_INFO << "Starting sort on chunk: " << mChunkId
        << " # of records = " << mRecords.size() << KFS_LOG_EOM;
    sort(mRecords.begin(), mRecords.end());
    KFS_LOG_STREAM_INFO << "Done with sort" << KFS_LOG_EOM;

    op->status = writeToChunk(op->outputFn, mRecords, op->indexSize);
}

/*
 * Use lzo to compress the data in the buffer.  The data is compressed
 * to the compressedBuf (in/out param).  After compression, the
 * compressedBuf pointer is adjusted to point to the next location
 * where data can be compressed.
 */
int
SortWorker::compressRecords(const unsigned char *ubufStart, int ulen,
    unsigned char **compressedBuf)
{
#ifdef USE_INTEL_IPP
    if (ulen == 0)
        return ippStsNoErr;
#else
    if (ulen == 0)
        return LZO_E_OK;
#endif

    unsigned char *curP = *compressedBuf;

    // Stick in a blob header that says what the length of the
    // compressed/uncompressed data is.
    IFilePktHdr_t pktHdr(0, 0, 0, ulen);

    pktHdr.codec = LZO_CODEC;
    curP += sizeof(IFilePktHdr_t);
#ifdef USE_INTEL_IPP
    Ipp32u clen;
    IppStatus retCode = ippsEncodeLZO_8u(
        (const Ipp8u *) ubufStart, (Ipp32u) ulen,
        (Ipp8u *) curP, &clen, mWrkMem);
    assert(retCode == ippStsNoErr);
#else
    lzo_uint clen;
    int retCode = lzo1x_1_compress(ubufStart, ulen, curP, &clen, mWrkMem);
    assert(retCode == LZO_E_OK);
#endif
    pktHdr.compressedRecLen = clen;
    curP = *compressedBuf;
    memcpy(curP, &pktHdr, sizeof(IFilePktHdr_t));
    *compressedBuf = curP + sizeof(IFilePktHdr_t) + clen;
    return retCode;
}

int
SortWorker::writeToChunk(const string &outputFn, vector<IFileRecord_t> &records,
    int &indexLen)
{
    int fd;

    fd = open(outputFn.c_str(), O_WRONLY|O_CREAT, S_IRUSR|S_IWUSR);
    if (fd < 0) {
        KFS_LOG_STREAM_INFO << "Unable to open: " << outputFn
            << " bailing..." << KFS_LOG_EOM;
        return -1;
    }

#if 0
// XXX: fallocate code path needs more testing
#ifdef KFS_USE_FALLOCATE
    int fallocStatus = posix_fallocate(fd, 0, KFS_CHUNK_HEADER_SIZE + KFS::CHUNKSIZE);
    if (fallocStatus < 0) {
        KFS_LOG_STREAM_INFO << "For file: " << outputFn
            << " fallocate failed with error: " << fallocStatus
            << KFS_LOG_EOM;
    }
#endif
#endif
    lseek(fd, KFS_CHUNK_HEADER_SIZE, SEEK_SET);

    indexLen = 0;

    // 256K
    const int kCompressBlobLen = 1 << 18;
    boost::scoped_array<unsigned char> uncompressedBuffer;
    uncompressedBuffer.reset(new unsigned char[2 * kCompressBlobLen]);

    unsigned char *chunkDataP = uncompressedBuffer.get();
    unsigned char *compressedBufP = mChunkData;
    char *indexBufferP = mIndexBuffer;
    int totalDataLen = 0;
    int recordStartPos = 0;
    int recLen, indexEntryLen;
    int recordNumber = 0;
    int indexEntryCount = 0, lastEntryPos = 0;
    int nextIndexEntryPos = 0;
    // get the data written out
    for (uint32_t i = 0; i < records.size(); i++) {
        // each index entry corresponds to an LZO-compression block
        // also, write one for the first record
        // and one for the last record.
        if ((lastEntryPos == 0) || (i == records.size() - 1) ||
            ((recordStartPos - lastEntryPos) + records[i].serializedLen() >=
                kCompressBlobLen)) {

            if (lastEntryPos != 0) {
                compressRecords(uncompressedBuffer.get(), totalDataLen,
                    &compressedBufP);

                // stash the offset in the chunk where the record that
                // corresponds to the start of the next compressed blob is
                // going to be.
                nextIndexEntryPos = compressedBufP - mChunkData;

                // reset
                chunkDataP = uncompressedBuffer.get();
                totalDataLen = 0;
            }

            // beginning of a new compression block...so, record the
            // index entry that corresponds to the first record in the block.
            indexEntryLen = records[i].serializeIndexEntry(indexBufferP, nextIndexEntryPos,
                recordNumber);
            indexEntryCount++;
            indexLen += indexEntryLen;
            indexBufferP += indexEntryLen;
            lastEntryPos = recordStartPos;

            assert(indexLen < (int) INDEX_BUFFER_SIZE);
        }
        assert(records[i].serializedLen() < kCompressBlobLen);
        recLen = records[i].serialize((char *) chunkDataP);
        totalDataLen += recLen;
        chunkDataP += recLen;

        recordStartPos += recLen;
        recordNumber++;
    }
    // compress whatever is left
    compressRecords(uncompressedBuffer.get(), totalDataLen,
        &compressedBufP);

    totalDataLen = compressedBufP - mChunkData;

    assert(totalDataLen < KFS::CHUNKSIZE);

    chunkDataP = mChunkData;
    write(fd, mChunkData, totalDataLen);

#if 0
#ifdef KFS_USE_FALLOCATE
    if (fallocStatus == 0) {
        // release any of the unused reserved space
        off_t currPos = lseek(fd, 0, SEEK_CUR);

        ftruncate(fd, currPos);
    }
#endif
#endif

    // get the index written out
    lseek(fd, KFS_CHUNK_HEADER_SIZE + KFS::CHUNKSIZE, SEEK_SET);
    // stick an 4-byte entry that says how many entries are in the index
    write(fd, &indexEntryCount, sizeof(int));
    indexLen += sizeof(int);

    assert(indexLen < (int) INDEX_BUFFER_SIZE);

    write(fd, mIndexBuffer, indexLen);

    mDci->chunkSize = totalDataLen;
    mDci->chunkIndexSize = indexLen;

    if (totalDataLen % KFS::CHECKSUM_BLOCKSIZE) {
        int padding = KFS::CHECKSUM_BLOCKSIZE - (totalDataLen % KFS::CHECKSUM_BLOCKSIZE);

        assert(totalDataLen + padding <= (int) KFS::CHUNKSIZE);

        memset(chunkDataP + totalDataLen, 0, padding);
        totalDataLen += padding;

    }
    // now compute the checksums on the data
    for (uint32_t i = 0; i < OffsetToChecksumBlockNum(totalDataLen); i++) {
        mDci->chunkBlockChecksum[i] = ComputeBlockChecksum((const char *) chunkDataP,
            KFS::CHECKSUM_BLOCKSIZE);
        chunkDataP += KFS::CHECKSUM_BLOCKSIZE;
    }

    lseek(fd, 0, SEEK_SET);
    write(fd, mDci, sizeof(DiskChunkInfo_t));


    close(fd);

    KFS_LOG_STREAM_INFO << "Done with writing to output file: " << outputFn
        << " filesz: " << mDci->chunkSize << " indexSz: " << indexLen << KFS_LOG_EOM;
    return 0;
}

//
// Parse out one record (in uncompressed format) from buffer
//
int
SortWorker::parseOneRecord(const char *buffer)
{
    const char *curP = buffer;

    IFileRecord_t record;

    // Format: <pkt hdr><4 byte key-len><key><4-byte val-len><value>
    memcpy(&record.pktHdr, curP, sizeof(IFilePktHdr_t));
    curP += sizeof(IFilePktHdr_t);

    assert(record.pktHdr.codec == 0x0);
    assert(record.pktHdr.uncompressedRecLen != 0);

    memcpy(&record.keyLen, curP, sizeof(uint32_t));
    curP += sizeof(uint32_t);
    record.keyLen = ntohl(record.keyLen);
    record.key.reset(new unsigned char[record.keyLen]);
    memcpy(record.key.get(), curP, record.keyLen);
    curP += record.keyLen;
    memcpy(&record.valLen, curP, sizeof(uint32_t));
    curP += sizeof(uint32_t);
    record.valLen = ntohl(record.valLen);
    record.value.reset(new unsigned char[record.valLen]);
    memcpy(record.value.get(), curP, record.valLen);
    curP += record.valLen;
    record.recLen = record.keyLen + record.valLen;
    mRecords.push_back(record);

    return curP - buffer;
}

void
SortWorker::parseChunk()
{
    const char *curP = (const char *) mChunkData;
    const char *endP = curP + mChunkDataLen;

    while (curP < endP) {
        // handle compression
        IFilePktHdr_t blobHdr;
        const char *bufStart, *bufEnd;
        memcpy(&blobHdr, curP, sizeof(IFilePktHdr_t));
        if (blobHdr.codec == LZO_CODEC) {
            curP += sizeof(IFilePktHdr_t);
            if ((uint32_t) mUncompBufferSz < blobHdr.uncompressedRecLen) {
#ifdef USE_INTEL_IPP
                size_t alignment = sizeof(Ipp8u *);
#else
                size_t alignment = sizeof(lzo_voidp);
#endif
                MemAlignedMalloc(&mUncompBuffer, alignment,
                    (size_t) blobHdr.uncompressedRecLen);
                assert(mUncompBuffer != NULL);
                mUncompBufferSz = blobHdr.uncompressedRecLen;
            }
#ifdef USE_INTEL_IPP
            Ipp32u ulen = blobHdr.uncompressedRecLen;
            IppStatus retCode = ippsDecodeLZO_8u(
                (Ipp8u*) curP,
                (Ipp32u) blobHdr.compressedRecLen,
                (Ipp8u*) mUncompBuffer,
                &ulen);
            assert(retCode == ippStsNoErr);
#else
            lzo_uint ulen = blobHdr.uncompressedRecLen;
            int retCode = lzo1x_decompress(
                (const unsigned char *) curP,
                blobHdr.compressedRecLen,
                (unsigned char*) mUncompBuffer,
                &ulen, mWrkMem);
            assert(retCode == LZO_E_OK);
#endif
            assert(ulen == blobHdr.uncompressedRecLen);
            bufStart = (const char *) mUncompBuffer;
            bufEnd = bufStart + ulen;
            // move to next compression block
            curP += blobHdr.compressedRecLen;
        } else {
            bufStart = curP;
            bufEnd = bufStart + sizeof(IFilePktHdr_t) +
                blobHdr.uncompressedRecLen;
            // move to next record
            curP = bufEnd;
        }

        while (bufStart < bufEnd) {
            int len;

            len = parseOneRecord(bufStart);
            bufStart += len;
        }
    }
}

int
SortWorker::readFromChunk(const string &inputFn)
{
    struct stat statRes;
    int fd, res, nread;

    KFS_LOG_STREAM_INFO << "Starting read on chunk: " << mChunkId
        << KFS_LOG_EOM;

    res = stat(inputFn.c_str(), &statRes);
    if (res < 0) {
        KFS_LOG_STREAM_INFO << "Unable to stat: " << inputFn
            << " bailing..." << KFS_LOG_EOM;
        return res;
    }
#if 0
    // enable O_DIRECT code path after more testing
    fd = open(inputFn.c_str(), O_RDONLY
#ifdef O_DIRECT
        |O_DIRECT
#endif
        );
#endif
    fd = open(inputFn.c_str(), O_RDONLY);
    if (fd < 0) {
        KFS_LOG_STREAM_INFO << "Unable to open: " << inputFn
            << " bailing..." << KFS_LOG_EOM;
        return res;
    }
    // don't worry about any index that exists in the file
    if (statRes.st_size > (int) (KFS_CHUNK_HEADER_SIZE + KFS::CHUNKSIZE))
        statRes.st_size = KFS_CHUNK_HEADER_SIZE + KFS::CHUNKSIZE;

    size_t nBytesToRead = statRes.st_size;
    const long kPageSz = sysconf(_SC_PAGESIZE);
    if ((nBytesToRead % kPageSz) != 0)
        // round up
        nBytesToRead = ((nBytesToRead / kPageSz) * kPageSz) + kPageSz;

    nread = read(fd, mChunkBuffer, nBytesToRead);
    close(fd);

    memcpy(mDci, mChunkBuffer, sizeof(DiskChunkInfo_t));
    mChunkDataLen = mDci->chunkSize;

    if ((int) (nread - KFS_CHUNK_HEADER_SIZE) < (int) mChunkDataLen) {
        KFS_LOG_STREAM_INFO << "Short read: expect = " << mChunkDataLen
            << " ; however, read = " << nread - KFS_CHUNK_HEADER_SIZE
            << " err = " << errno << KFS_LOG_EOM;
        return -1;
    }

    KFS_LOG_STREAM_INFO << "Done with reading from chunk: " << mChunkId
        << " ...now parsing..." << KFS_LOG_EOM;

    parseChunk();

    KFS_LOG_STREAM_INFO << "Done with parsing from chunk: " << mChunkId
        << KFS_LOG_EOM;

    return 0;
}
