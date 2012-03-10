//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: imerger.cc 3832 2012-03-05 22:57:51Z sriramr $
//
// Created 2010/11/14
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
//
//----------------------------------------------------------------------------

#include <iostream>
#include <sstream>
#include <algorithm>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <boost/lexical_cast.hpp>
#include <boost/scoped_ptr.hpp>

#include "common/log.h"
#include "imerger.h"
#include "kappender.h"
#include "util.h"

using namespace KFS;
using std::string;
using std::vector;
using std::set;
using std::pair;
using std::ostringstream;
using std::max;


SortedRun_t::SortedRun_t() :
    fd(-1), buffer(0), bufsz(0),
    bufDataLen(0),
    uncompressedBufPtr(0),
    nextRecordPtr(0),
    uncompressedBufDataSz(0),
    uncompressedBufLen(0),
    curPtr(0), isPrefetchPending(false),
    isAllDone(false)
{
#ifndef USE_INTEL_IPP
    wrkMem = 0;
    MemAlignedMalloc(&wrkMem, sizeof(lzo_align_t), LZO1X_1_MEM_COMPRESS);
    assert(wrkMem != 0);
#endif
}

bool IMergerEntry_t::operator< (const IMergerEntry_t &other) const {
    // return key < other.key;
    // Since we use a priority queue for merging the records,
    // the priority queue sorts in reverse order:
    // largest->smallest.  We'd like to go the other way.
    // Hence, reverse the comparison order.

    // There is a very subtle issue if keys are equal: we want to
    // drain all the sorted runs at the same rate; otherwise, if all
    // keys are equal, we may drain runs one by one, causing stalls
    // when reading from KFS.
    if (other.key == key) {
        // whichever run has the most keys available, prefer it
        return iMerger->getBytesAvailForSortedRun(sortedRunIdx) <
            iMerger->getBytesAvailForSortedRun(other.sortedRunIdx);
    }

    return other.key < key;
}

IMerger::IMerger(KfsClientPtr &clnt, int partition,
    const string &jobId, const string &workbuilder, const string &basedir) :
    mKfsClnt(clnt),
    mWorkgetter(workbuilder, jobId, partition, this),
    mPartition(partition), mRecordCount(0),
    mWbUpdateIntervalSecs(60),
    mBasedir(basedir),
    mAssignedKeys(true)
{
#ifdef USE_INTEL_IPP
    IppStatus retCode = ippStaticInit();
    assert(retCode == ippStsNoErr);
#else
    int retCode = lzo_init();
    assert(retCode == LZO_E_OK);
#endif
}

//
// Get the id of the dead mappers; records from them have to be
// filtered out.
//
void
IMerger::GetDeadMappers(const string &deadMappersDir)
{
    vector<string> entries;
    uint64_t id;

    mKfsClnt->Readdir(deadMappersDir.c_str(), entries);
    for (uint32_t i = 0; i < entries.size(); i++) {
        if ((entries[i].compare(".") == 0) ||
            (entries[i].compare("..") == 0))
            continue;
        id = boost::lexical_cast<uint64_t>(entries[i]);
        KFS_LOG_STREAM_DEBUG << "Found dead mapper: " << id << KFS_LOG_EOM;
        mDeadMappers.insert(id);
    }
}

void
IMerger::parseAssignment(const char* assignment, int size)
{
    const char *c = assignment;

    c += 6;
    memcpy(&mPartition, c, sizeof(int));
    c += sizeof(int);
    KFS_LOG_STREAM_INFO << "Assigned ifile (aka partition) is: " << mPartition << KFS_LOG_EOM;

    ostringstream iFilename;
    iFilename << mBasedir << "/" << mPartition << "/file." << mPartition;
    
    mFilenames.push_back(iFilename.str());

    if (memcmp(assignment, "CHUNKS", 6) == 0)
        parseAssignmentByChunks(c, assignment + size);
    else if (memcmp(assignment, "RANGES", 6) == 0) {
        parseAssignmentByKeys(c, assignment + size);
    }
}

void
IMerger::parseAssignmentByChunks(const char* startPtr, const char *endPtr)
{
    assert(!"No longer supported!");
    exit(-1);
    /*
    int len;

    mAssignedKeys = false;
    memcpy(&mStartChunkOffset, startPtr, sizeof(off_t));
    startPtr += sizeof(off_t);
    memcpy(&len, startPtr, sizeof(int));
    mEndChunkOffset = mStartChunkOffset + len;
    KFS_LOG_STREAM_INFO << "Assigned range: " << mStartChunkOffset << ","
        << mEndChunkOffset << KFS_LOG_EOM;
    */
}

void
IMerger::parseAssignmentByKeys(const char* startPtr, const char *endPtr)
{
    const char *c = startPtr;

    mAssignedKeys = true;

    memcpy(&mLastOffsetInFile, c, sizeof(off_t));
    c += sizeof(off_t);
    memcpy(&mKeyRange.lowerKeyLen, c, sizeof(uint32_t));
    c += sizeof(uint32_t);
    mKeyRange.lowerKey.reset(new unsigned char[mKeyRange.lowerKeyLen + 1]);
    mKeyRange.lowerKey[mKeyRange.lowerKeyLen] = '\0';
    memcpy(mKeyRange.lowerKey.get(), c, mKeyRange.lowerKeyLen);
    c += mKeyRange.lowerKeyLen;

    memcpy(&mKeyRange.upperKeyLen, c, sizeof(uint32_t));
    c += sizeof(uint32_t);
    mKeyRange.upperKey.reset(new unsigned char[mKeyRange.upperKeyLen + 1]);
    mKeyRange.upperKey[mKeyRange.upperKeyLen] = '\0';
    memcpy(mKeyRange.upperKey.get(), c, mKeyRange.upperKeyLen);
    c += mKeyRange.upperKeyLen;

    KFS_LOG_STREAM_INFO << "Assigned key range: " << mKeyRange.lowerKey.get()
        << "->" << mKeyRange.upperKey.get() << KFS_LOG_EOM;
}

//
// Index read failed; we need to notify the workbuilder of a lost chunk
// we also need to drain out any pending async index read requests
// before we can retry.
//
void
IMerger::DrainIndexReads(uint32_t sortedRunStart)
{
    for (uint32_t i = sortedRunStart; i < mSortedRuns.size(); i++) {
        char *indexBuffer;
        int indexSize, nRead;

        if (mSortedRuns[i].fd < 0) {
            continue;
        }

        nRead = mKfsClnt->GetChunkIndex(mSortedRuns[i].fd, &indexBuffer, indexSize);
        if (nRead < 0)
            continue;
        delete [] indexBuffer;
    }
}

// Go chunk by chunk and get the index; in each chunk, use the async API to read the range
bool
IMerger::GetKeyRanges(off_t fileSz)
{
    // open up each chunk and pull data in parallel
    int fd, res = 0;

    // mStartChunkOffset = 0;
    // mEndChunkOffset = fileSz;

    // prevent memory allocation on this...
    int numSortedRuns = (fileSz / KFS::CHUNKSIZE) + 1;
    mSortedRuns.reserve(numSortedRuns);

    // Issue the prefetch on the index
    for (off_t currChunkOffset = 0; currChunkOffset < fileSz;
         currChunkOffset += KFS::CHUNKSIZE) {
        int fd;

        fd = mKfsClnt->Open(mFilename.c_str(), O_RDWR);

        if (fd < 0) {
            KFS_LOG_STREAM_INFO << "Unable to open: " << mFilename <<
                " error is: " << res << KFS_LOG_EOM;
            exit(-1);
        }
        mKfsClnt->SkipHolesInFile(fd);
        mKfsClnt->Seek(fd, currChunkOffset);
        mKfsClnt->SetEOFMark(fd, currChunkOffset + KFS::CHUNKSIZE);

        res = mKfsClnt->GetChunkIndexPrefetch(fd);
        if (res < 0) {
            KFS_LOG_STREAM_INFO << "Unable to read index for offset: " << currChunkOffset
                << " error: " << res << KFS_LOG_EOM;
            res = mWorkgetter.notifyLostChunk(mKfsClnt->GetCurrChunkId(fd));
            if (res == 201) {
                mKfsClnt->Close(fd);
                DrainIndexReads(0);
                // need to wait
                return false;
            }
            assert(res == 200);
            // ignore this chunk and move on
            mKfsClnt->Close(fd);
            // stick the entry in sortedRuns so that all the math
            // about looping using currChunkOffset works
            fd = -1;
        }
        // XXX: This is a hack; should be a shared pointer
        SortedRun_t sr;
        sr.fd = fd;
        mSortedRuns.push_back(sr);
        sr.clear();
    }

    // make sure each run gets at least a 256K buffer; if we have
    // sufficient memory, leave 512K to deal with
    // compression/decompression buffer space.
    uint64_t perRunBufferSize = max((mMergeHeapSize / mSortedRuns.size()) - (512 * 1024), (uint64_t) (256 * 1024));
    KFS_LOG_STREAM_INFO << "Per sorted run merge buffer size: " <<
        perRunBufferSize << KFS_LOG_EOM;

    // get the index and issue the read prefetch
    for (uint32_t i = 0; i < mSortedRuns.size(); i++) {
        char *indexBuffer;
        int indexSize;
        int nRead;
        SortedRun_t &sr = mSortedRuns[i];
        off_t currChunkOffset = ((off_t) i) * KFS::CHUNKSIZE;

        fd = sr.fd;
        if (fd < 0) {
            sr.bufsz = 0;
            continue;
        }

        nRead = mKfsClnt->GetChunkIndex(fd, &indexBuffer, indexSize);
        if (nRead < 0) {
            KFS_LOG_STREAM_INFO << "Unable to read index for offset: " << currChunkOffset
                << KFS_LOG_EOM;
            res = mWorkgetter.notifyLostChunk(mKfsClnt->GetCurrChunkId(fd));
            if (res == 201) {
                mKfsClnt->Close(sr.fd);
                sr.fd = -1;
                DrainIndexReads(i + 1);
                // need to wait
                return false;
            }
            assert(res == 200);
            // chunk was lost and the data has been re-generated; so,
            // treat this chunk as having 0-bytes and move on
            sr.bufsz = 0;
            continue;
        }

        // The data in the chunk is sorted; so, we just need to pull
        // out our byte range within the chunk: start/end are within
        // the chunk.
        GetRangeToReadFromChunkIndex(indexBuffer, indexSize, sr.startPos, sr.endPos);
        sr.fileEndPos = currChunkOffset + sr.endPos;

        if (sr.fileEndPos > fileSz)
            sr.fileEndPos = fileSz;

        delete [] indexBuffer;

        if (sr.startPos < 0) {
            sr.bufsz = 0;
            KFS_LOG_STREAM_INFO << "No data to read from fd: "
                << fd << " which corresponds to chunk: " << currChunkOffset
                << KFS_LOG_EOM;
            continue;
        }

        KFS_LOG_STREAM_DEBUG << "From fd: " << fd << " Reading range: "
            << sr.startPos << "->" << sr.endPos << KFS_LOG_EOM;

        // get to the beginning of our range within the chunk; in the
        // loop above, fd is set to the beginning of the chunk
        mKfsClnt->Seek(fd, sr.startPos, SEEK_CUR);

        sr.bufsz = perRunBufferSize;
        sr.buffer = new unsigned char[sr.bufsz];

        res = mKfsClnt->ReadPrefetch(fd, (char *) sr.buffer, sr.bufsz);
        if (res == -EAGAIN)
            // there is a catch-all code below that fixes this up.
            continue;
        if (res < 0) {
            KFS_LOG_STREAM_INFO << "Unable to prefetch: " << mFilename
                << " offset: " << currChunkOffset
                << " error is: " << res << KFS_LOG_EOM;
            exit(-1);
        }
    }
    // Make the read event-driven; whatever is available, we'll parse
    uint32_t numRead = 0;
    off_t totalBytesRead = 0;
    while (numRead < mSortedRuns.size()) {
        int fdToRead;
        uint32_t sortedRunIdx = 0;

        fdToRead = mKfsClnt->WaitForPrefetch();
        if (fdToRead < 0)
            break;
        numRead++;
        // find the fd for which data is ready
        for (uint32_t i = 0; i < mSortedRuns.size(); i++) {
            if (mSortedRuns[i].fd == fdToRead) {
                sortedRunIdx = i;
                break;
            }
        }
        SortedRun_t &sortedRun = mSortedRuns[sortedRunIdx];
        if (sortedRun.bufDataLen > 0) {
            // very subtle issue: process of loading a record in this
            // phase, can cause a prefetch on the run.  If the
            // prefetch returns within this loop, we need to adjust
            // our view of the world.
            assert(sortedRun.isPrefetchPending);
            res = mKfsClnt->Read(sortedRun.fd, (char *) sortedRun.prefetchPtr,
                sortedRun.prefetchLen);
            sortedRun.isPrefetchPending = false;
            // this is the # of bytes read
            if (res > 0)
                sortedRun.bufDataLen += res;

        } else {
            res = mKfsClnt->Read(sortedRun.fd, (char *) sortedRun.buffer, sortedRun.bufsz);
            // this is the # of bytes read
            if (res > 0)
                sortedRun.bufDataLen = res;
        }
        if (res < 0) {
            KFS_LOG_STREAM_INFO << "read failed on: " << mFilename
                << " offset: " << (sortedRunIdx * KFS::CHUNKSIZE)
                << " error is: " << res << KFS_LOG_EOM;
            // << " offset: " << mStartChunkOffset + (sortedRunIdx * KFS::CHUNKSIZE)
            assert(!"Not possible");
        }

        KFS_LOG_STREAM_DEBUG << "read returned: " << res << KFS_LOG_EOM;

        sortedRun.curPtr = sortedRun.buffer;

        totalBytesRead += res;
        // load one record into the heap
        LoadNextRecord(sortedRunIdx);
    }

    if (numRead != mSortedRuns.size()) {
        KFS_LOG_STREAM_INFO << "Looks like we didn't get data for all chunks: "
            << " expect: " << mSortedRuns.size() << " ; got: " << numRead << KFS_LOG_EOM;
    }

    // safety/cleanup
    // collect all the results back
    for (uint32_t i = 0; i < mSortedRuns.size(); i++) {
        if (mSortedRuns[i].bufsz == 0) {
            mSortedRuns[i].isAllDone = true;
            mKfsClnt->Close(mSortedRuns[i].fd);
            continue;
        }
        SortedRun_t &sortedRun = mSortedRuns[i];
        if (sortedRun.bufDataLen > 0)
            continue;

        res = mKfsClnt->Read(sortedRun.fd, (char *) sortedRun.buffer, sortedRun.bufsz);
        if (res < 0) {
            KFS_LOG_STREAM_INFO << "read failed on: " << mFilename
                << " offset: " << (i * KFS::CHUNKSIZE)
                << " error is: " << res << KFS_LOG_EOM;
            assert(!"Not possible");
        }
        totalBytesRead += res;
        sortedRun.curPtr = sortedRun.buffer;
        // this is the # of bytes read
        sortedRun.bufDataLen = res;
        // load one record into the heap
        LoadNextRecord(i);
    }
    KFS_LOG_STREAM_INFO << "Total bytes read: " << totalBytesRead << KFS_LOG_EOM;
    return true;
}


//
// Read bytes from a chunk.
//
int
IMerger::GetDataFromChunk(int kfsFd, char *buffer, int nbytesToRead)
{
    int nRead = 0;
    while (1) {
        nRead = mKfsClnt->Read(kfsFd, (char *) buffer, nbytesToRead);
        if (nRead >= 0)
            break;
        if ((nRead < 0) && (nRead == -EBUSY))
            continue;
        else
            break;
    }
    return nRead;
}

// For LB case, we need < comparison, so that every entry
// that is < key needs to be skipped; the LB is the first
// entry such that entry >= key.
// When keys are equal, use the logic from IFileKey_t < comparison.
static bool
operator < (const IFileIndexEntry_t &entry, const IFileKey_t &key)
{
    uint32_t cLen = std::min(key.keyLen, entry.keyLen);
    int cmpVal = memcmp(entry.key, key.key.get(), cLen);
    if (entry.keyLen == key.keyLen)
        return cmpVal < 0;
    if (cmpVal == 0)
        return entry.keyLen < key.keyLen;
    return cmpVal < 0;
}

// This is the converse: UB case
static bool
operator < (const IFileKey_t &key, const IFileIndexEntry_t &entry)
{
    uint32_t cLen = std::min(key.keyLen, entry.keyLen);
    int cmpVal = memcmp(key.key.get(), entry.key, cLen);
    if (key.keyLen == entry.keyLen)
        return cmpVal < 0;
    if (cmpVal == 0)
        return key.keyLen < entry.keyLen;
    return cmpVal < 0;
}

// For LB case, we need < comparison, so that every entry
// that is < key needs to be skipped; the LB is the first
// entry such that entry >= key
static bool
IFileKeyLBComparable(const IFileIndexEntry_t &entry, const IFileKey_t &key)
{
    return entry < key;
    /*
    uint32_t cLen = std::min(key.keyLen, entry.keyLen);
    int cmpVal = memcmp(entry.key, key.key.get(), cLen);
    return cmpVal < 0;
    */
}

// For UB case, we need <= comparison, so that every entry
// that is < key is included; the UB is the first entry
// such that entry >= key---> upper bound is exclusive
static bool
IFileKeyUBComparable(const IFileKey_t &key, const IFileIndexEntry_t &entry)
{
    return key < entry;
    /*
    uint32_t cLen = std::min(key.keyLen, entry.keyLen);
    int cmpVal = memcmp(key.key.get(), entry.key, cLen);
    return cmpVal <= 0;
    */
}

void
IMerger::GetRangeToReadFromChunkIndex(const char *indexBuffer, int indexSize,
    int &startPos, int &endPos)
{
    vector<IFileIndexEntry_t> indexEntries;
    int nIndexEntries;
    const char *curBufPtr = indexBuffer;

    if (indexSize == 0) {
        startPos = -1;
        return;
    }
    memcpy(&nIndexEntries, curBufPtr, sizeof(int));
    curBufPtr += sizeof(int);
    indexEntries.resize(nIndexEntries);
    for (int i = 0; i < nIndexEntries; i++) {
        memcpy(&indexEntries[i].keyLen, curBufPtr, sizeof(uint32_t));
        curBufPtr += sizeof(uint32_t);
        memcpy(&indexEntries[i].recLen, curBufPtr, sizeof(uint32_t));
        curBufPtr += sizeof(uint32_t);
        memcpy(&indexEntries[i].recordNumber, curBufPtr, sizeof(uint32_t));
        curBufPtr += sizeof(uint32_t);
        memcpy(&indexEntries[i].offset, curBufPtr, sizeof(off_t));
        curBufPtr += sizeof(off_t);
        indexEntries[i].key = curBufPtr;
        curBufPtr += indexEntries[i].keyLen;
    }
    // index is sorted...binary search to get our range
    vector<IFileIndexEntry_t>::iterator startP, endP;

    // Minor issue: if the index is compressed (as in entries one per 64K),
    // then we'll need to adjust what we read---lower bound we'll have to one less

    // [lower key, upper key) is our range.
    // stl lower_bound returns iterator p such that p >= lower key
    // find the lower-bound of the range; that is where we start from
    IFileKey_t key;
    key.key = mKeyRange.lowerKey;
    key.keyLen = mKeyRange.lowerKeyLen;
    startP = lower_bound(indexEntries.begin(), indexEntries.end(), key, IFileKeyLBComparable);

    if (startP == indexEntries.end()) {
        startPos = -1;
        return;
    }

    startPos = startP->offset;

    // move the iterator backwards until we find the first key that is
    // strictly < than the lower range; we can then read from there
    // and discard what isn't ours.

    while (1) {
        // STL lower bound guarantees that: startP >= key; if startP
        // == key, we'll need to move backwards until we find the
        // entry such that startP < key
        if (*startP < key) {
            startPos = startP->offset;
            break;
        }
        if (startP == indexEntries.begin()) {
            startPos = 0;
            break;
        }
        startP--;
        KFS_LOG_STREAM_DEBUG << "Moving iterator backwards..." << KFS_LOG_EOM;
    }

    key.key = mKeyRange.upperKey;
    key.keyLen = mKeyRange.upperKeyLen;

    // find the upper bound of the range; that gives us the first key past our range
    // the offset where that key begins is the end point to which we need to read
    endP = upper_bound(indexEntries.begin(), indexEntries.end(), key, IFileKeyUBComparable);

    while (1) {
        if (endP == indexEntries.end()) {
            endP = indexEntries.end() - 1;
            endPos = KFS::CHUNKSIZE;
            break;
        }
        // STL upper bound guarantees that: key >= endP; if endP
        // == key, we'll need to move forward until we find the
        // entry such that key < endP
        if (key < *endP) {
            endPos = endP->offset;
            break;
        }
        endP++;
    }

    // record # is 0-based; this value of mRecordCount is an estimate
    mRecordCount += ((endP->recordNumber - startP->recordNumber) + 1);
    KFS_LOG_STREAM_DEBUG << "Start record #: " << startP->recordNumber
        << " ; ending record #: " << endP->recordNumber << KFS_LOG_EOM;
}

//
// Read the chunk index and then selectively read data from the chunk.
//
int
IMerger::GetKeyRangeFromChunk(int kfsFd, char *buffer, int nbytesToRead)
{
    int nRead = 0;
    char *indexBuffer;
    int indexSize;
    int startPos, endPos;

    nRead = mKfsClnt->GetChunkIndex(kfsFd, &indexBuffer, indexSize);
    if (nRead < 0) {
        KFS_LOG_STREAM_INFO << "Index read failed...reading the full chunk"
            << KFS_LOG_EOM;
        // return GetDataFromChunk(kfsFd, buffer, nbytesToRead);
        assert(!"Unable to read index; aborting");
    }

    // The data in the chunk is sorted; so, we just need to pull out our byte range
    GetRangeToReadFromChunkIndex(indexBuffer, indexSize, startPos, endPos);

    delete [] indexBuffer;

    if (startPos < 0)
        return 0;

    KFS_LOG_STREAM_INFO << "From fd = " << kfsFd << " reading range: "
        << startPos << "->" << endPos << KFS_LOG_EOM;

    // off_t currPos = mKfsClnt->Tell(kfsFd);
    // seek relative to the current position within the chunk to read what we need
    mKfsClnt->Seek(kfsFd, startPos, SEEK_CUR);
    nRead = mKfsClnt->Read(kfsFd, buffer, endPos - startPos);
    if (nRead < 0) {
        KFS_LOG_STREAM_INFO << "Unable to read key range from chunk!"
                << KFS_LOG_EOM;
        assert(!"Unable to read index; aborting");
    }
    return nRead;
}

void
IMerger::IssuePrefetch(int sortedRunIdx)
{
    SortedRun_t& sortedRun = mSortedRuns[sortedRunIdx];
    int res;
    int len = sortedRun.bytesAvail();

    if (sortedRun.isPrefetchPending)
        return;
    sortedRun.isPrefetchPending = true;
    // shift to the beginning and then backfill
    memmove(sortedRun.buffer, sortedRun.curPtr, len);
    sortedRun.curPtr = sortedRun.buffer;
    sortedRun.bufDataLen = len;
    sortedRun.prefetchPtr = sortedRun.curPtr + len;
    sortedRun.prefetchLen = sortedRun.bufsz - len;

    res = mKfsClnt->ReadPrefetch(sortedRun.fd, (char *) sortedRun.prefetchPtr, sortedRun.prefetchLen);

    KFS_LOG_STREAM_INFO << "Issuing prefetch for sorted run: "
        << sortedRunIdx << " of len: "
        << sortedRun.prefetchLen
        << KFS_LOG_EOM;
}

void
IMerger::GetMoreData(int sortedRunIdx)
{
    SortedRun_t& sortedRun = mSortedRuns[sortedRunIdx];
    int nRead;

    if (!sortedRun.isPrefetchPending) {
        KFS_LOG_STREAM_INFO << "Doing a sync read on sorted run: "
            << sortedRunIdx << KFS_LOG_EOM;
        // Get everything setup
        IssuePrefetch(sortedRunIdx);
    }

    sortedRun.isPrefetchPending = false;
    nRead = mKfsClnt->Read(sortedRun.fd, (char *) sortedRun.prefetchPtr, sortedRun.prefetchLen);
    if (nRead < 0) {
        KFS_LOG_STREAM_INFO << "Failed to get data? error = "
            << nRead << KFS_LOG_EOM;
    } else {
        sortedRun.bufDataLen += nRead;
    }
}

void
IMerger::DecompressNextBlob(int sortedRunIdx)
{
    SortedRun_t& sortedRun = mSortedRuns[sortedRunIdx];
    if ((sortedRun.bytesAvail() == 0) &&
        (mKfsClnt->Tell(sortedRun.fd) >= sortedRun.fileEndPos)) {
        if (sortedRun.isPrefetchPending)
            // clear out any prefetches
            GetMoreData(sortedRunIdx);
        assert(!sortedRun.isPrefetchPending);
        KFS_LOG_STREAM_INFO << "All done with sorted run: " << sortedRunIdx << KFS_LOG_EOM;
        mKfsClnt->Close(sortedRun.fd);
        sortedRun.isAllDone = true;
        return;
    }
    // if the data in the buffer is less than a packet header, we
    // got read in the header

    // read the blob header; then get the records
    bool needMoreData = sortedRun.bytesAvail() < sizeof(IFilePktHdr_t);

    IFilePktHdr_t pktHdr;

    if (!needMoreData) {
        int minBytesNeeded;
        memcpy(&pktHdr, sortedRun.curPtr, sizeof(IFilePktHdr_t));
        minBytesNeeded = (pktHdr.codec == 0x0) ?
            pktHdr.uncompressedRecLen : pktHdr.compressedRecLen;
        needMoreData = sortedRun.bytesAvail() <
            minBytesNeeded + sizeof(IFilePktHdr_t);
    }
    if (needMoreData) {
        for (int retryCount = 0; retryCount < 2; retryCount++) {
            GetMoreData(sortedRunIdx);
            if (sortedRun.bytesAvail() >= sizeof(IFilePktHdr_t))
                break;
        }
        // Wonky case: we don't have any data in the buffer and we hit
        // EOF.  However, the fileEndPos as far as the sortedRun is
        // concerned is past EOF.  Happens when there is a size
        // mismatch between what the metaserver thinks and what is
        // actually there on the chunkserver
        if ((sortedRun.bytesAvail() == 0) &&
            ((mKfsClnt->Tell(sortedRun.fd) >= sortedRun.fileEndPos) ||
                (mKfsClnt->IsEof(sortedRun.fd)))) {
            assert(!sortedRun.isPrefetchPending);
            KFS_LOG_STREAM_INFO << "All done with sorted run: " << sortedRunIdx << KFS_LOG_EOM;
            mKfsClnt->Close(sortedRun.fd);
            sortedRun.isAllDone = true;
            return;
        }
        assert(sortedRun.bytesAvail() >= sizeof(IFilePktHdr_t));
    }
    memcpy(&pktHdr, sortedRun.curPtr, sizeof(IFilePktHdr_t));
    if (pktHdr.codec == 0x0) {

        assert(sortedRun.bytesAvail() >=
            sizeof(IFilePktHdr_t) + pktHdr.uncompressedRecLen);

        if (sortedRun.uncompressedBufLen != 0) {
            free(sortedRun.uncompressedBufPtr);
            sortedRun.uncompressedBufLen = 0;
        }
        sortedRun.uncompressedBufPtr = sortedRun.curPtr;
        sortedRun.uncompressedBufDataSz = sizeof(IFilePktHdr_t) +
            pktHdr.uncompressedRecLen;
        sortedRun.nextRecordPtr = sortedRun.curPtr;
        sortedRun.curPtr += sizeof(IFilePktHdr_t) +
            pktHdr.uncompressedRecLen;
        return;
    }
    assert(pktHdr.codec == LZO_CODEC);

    sortedRun.curPtr += sizeof(IFilePktHdr_t);

    // if we don't have the record, retry to get it...
    needMoreData = (sortedRun.bytesAvail() < pktHdr.compressedRecLen);
    if (needMoreData) {
        for (int retryCount = 0; retryCount < 4; retryCount++) {
            GetMoreData(sortedRunIdx);
            if (sortedRun.bytesAvail() >= pktHdr.compressedRecLen)
                break;
            sleep(30);
        }
    }

    assert(sortedRun.bytesAvail() >= pktHdr.compressedRecLen);

    if (sortedRun.uncompressedBufLen == 0)
        sortedRun.uncompressedBufPtr = NULL;

    if (sortedRun.uncompressedBufLen < pktHdr.uncompressedRecLen) {
#ifdef USE_INTEL_IPP
        size_t alignment = sizeof(Ipp8u *);
#else
        size_t alignment = sizeof(lzo_voidp);
#endif
        MemAlignedMalloc(&sortedRun.uncompressedBufPtr, alignment,
            (size_t) pktHdr.uncompressedRecLen);
        sortedRun.uncompressedBufLen = pktHdr.uncompressedRecLen;
    }

#ifdef USE_INTEL_IPP
    IppStatus retCode = ippsDecodeLZO_8u(
        (Ipp8u *)sortedRun.curPtr, (Ipp32u) pktHdr.compressedRecLen,
        (Ipp8u *) sortedRun.uncompressedBufPtr, &sortedRun.uncompressedBufDataSz);
    assert(retCode == ippStsNoErr);
#else
    int retCode = lzo1x_decompress(sortedRun.curPtr, pktHdr.compressedRecLen,
        sortedRun.uncompressedBufPtr, &sortedRun.uncompressedBufDataSz,
        sortedRun.wrkMem);
    assert(retCode == LZO_E_OK);
#endif
    sortedRun.nextRecordPtr = sortedRun.uncompressedBufPtr;
    sortedRun.curPtr += pktHdr.compressedRecLen;
}

void
IMerger::LoadNextRecord(int sortedRunIdx)
{
    SortedRun_t& sortedRun = mSortedRuns[sortedRunIdx];
    IFileKey_t lowerRangeKey, upperRangeKey;

    assert(mAssignedKeys);

    lowerRangeKey.key = mKeyRange.lowerKey;
    lowerRangeKey.keyLen = mKeyRange.lowerKeyLen;

    upperRangeKey.key = mKeyRange.upperKey;
    upperRangeKey.keyLen = mKeyRange.upperKeyLen;

    while (1) {
        // walk the buffer until we get a valid record
        if (!sortedRun.isUncompressedRecordAvail()) {
            DecompressNextBlob(sortedRunIdx);
            if (sortedRun.isAllDone)
                return;
        }

        assert(sortedRun.isUncompressedRecordAvail());

        // read the packet header; then get the records
        IFilePktHdr_t pktHdr;

        memcpy(&pktHdr, sortedRun.nextRecordPtr, sizeof(IFilePktHdr_t));
        sortedRun.nextRecordPtr += sizeof(IFilePktHdr_t);
        uint32_t recUncompressedLength = pktHdr.uncompressedRecLen;

        if (mDeadMappers.find(pktHdr.mapperAttempt) != mDeadMappers.end()) {
            // Got a record from a dead mapper; move on
            sortedRun.nextRecordPtr += recUncompressedLength;
            continue;
        }

        // more than half the buffer is available or at least 1M is available
        // and we are really out of data, then prefetch; otherwise, we'll cause seeks
        if ((sortedRun.bytesAvail() < (uint32_t) 1048576)  &&
            (sortedRun.spaceAvail() >
                std::min(sortedRun.bufsz / 2, (uint32_t) 1048576))) {
            IssuePrefetch(sortedRunIdx);
        }

        unsigned char *recBuffer = sortedRun.nextRecordPtr;

        sortedRun.nextRecordPtr += recUncompressedLength;

        IMergerEntry_t record;
        record.mapperAttempt = pktHdr.mapperAttempt;
        record.sequenceNumber = pktHdr.sequenceNumber;

        record.sortedRunIdx = sortedRunIdx;
        memcpy(&record.key.keyLen, recBuffer, sizeof(uint32_t));
        recBuffer += sizeof(uint32_t);
        // freakin' java...get in network-byte-order
        record.key.keyLen = ntohl(record.key.keyLen);
        record.key.key.reset(new unsigned char[record.key.keyLen]);
        memcpy(record.key.key.get(), recBuffer, record.key.keyLen);
        recBuffer += record.key.keyLen;
        memcpy(&record.valLen, recBuffer, sizeof(uint32_t));
        recBuffer += sizeof(uint32_t);
        // freakin' java...get in network-byte-order
        record.valLen = ntohl(record.valLen);
        record.value = recBuffer;

        // put this back
        KFS_LOG_STREAM_DEBUG << "building IFileKey_t from " << record.key.key
            << ", " << record.key.keyLen << KFS_LOG_EOM;
        IFileKey_t key(record.key.key, record.key.keyLen);

        if (!IsInMyKeyRange(key, lowerRangeKey, upperRangeKey)) {
            // key is below our smallest key...keep going
            if (key < lowerRangeKey)
                continue;

            if (mKfsClnt->Tell(sortedRun.fd) >= sortedRun.fileEndPos) {
                if (sortedRun.isPrefetchPending) {
                    // clear out any prefetches
                    GetMoreData(sortedRunIdx);
                }
                KFS_LOG_STREAM_DEBUG << " For fd: " << sortedRun.fd
                    << "Hit a key that is outside our range..."
                    << " our end: " << sortedRun.fileEndPos
                    << " file position: " << mKfsClnt->Tell(sortedRun.fd)
                    << KFS_LOG_EOM;
                sortedRun.isAllDone = true;
                // we are done at this point...we got a key that
                // doesn't belong to us and we have read everything we
                // were supposed to
                return;
            }
            continue;
        }

        if (mAssignedKeys) {
            assert((lowerRangeKey < key) || (lowerRangeKey == key));
            assert(key < upperRangeKey);
        }
        KFS_LOG_STREAM_DEBUG << "Got key for us: " << record.key.key << " with length: " << record.key.keyLen
            << KFS_LOG_EOM;

        record.iMerger = this;

        mRecords.push(record);

        break;

    }
}

bool
IMerger::IsInMyKeyRange(const IFileKey_t &key, const IFileKey_t &lowerRangeKey,
    const IFileKey_t &upperRangeKey)
{
    if (!mAssignedKeys)
        return true;

    // key >= upperRangeKey
    if ((upperRangeKey < key) || (key == upperRangeKey))
        return false;

    if (key < lowerRangeKey)
        return false;

    return true;
}

void
IMerger::LoadData(int outFd, string deadMappersDir)
{
    int recordCount, rv;
    // int ifd, rv;
    struct stat statRes;

    mKfsClnt->EnableAsyncRW();

    while (1) {
        while (!mWorkgetter.haveWork()) {
            mWorkgetter.get();
            if (!mWorkgetter.haveWork()) {
                KFS_LOG_STREAM_INFO << "Got a busy response from workbuilder...waiting..."
                    << KFS_LOG_EOM;
                //sleep and try again
                sleep(5);
            }
        }

        GetDeadMappers(deadMappersDir);

        assert(mAssignedKeys);

        bool allGood = true;

        for (uint32_t i = 0; i < mFilenames.size(); i++) {
            mFilename = mFilenames[i];

            if (mKfsClnt->Stat(mFilename.c_str(), statRes) < 0) {
                std::cerr << "Unable to stat: " << mFilename << std::endl;
                exit(-1);
            }

            std::cerr << "Size of file: " << mFilename << " is: "
                << statRes.st_size << std::endl;

            if (statRes.st_size > mLastOffsetInFile) {
                std::cerr << "Trimming our filesz to: "
                    << mLastOffsetInFile << " which is the"
                    << " value specified by the workbuilder" << std::endl;
                statRes.st_size = mLastOffsetInFile;
            }

            // this loads data concurrently
            if (GetKeyRanges(statRes.st_size))
                continue;
            allGood = false;
            // else we hit a failure when reading the range...retry
            for (uint32_t j = 0; j < mSortedRuns.size(); j++) {
                if (mSortedRuns[j].fd > 0)
                    mKfsClnt->Close(mSortedRuns[j].fd);
            }
            mSortedRuns.clear();
            mWorkgetter.clearWorkReceived();
            mFilenames.clear();
            break;
        }
        if (allGood)
            break;
        // wait a bit before retrying...wait for mappers to regenerate lost data
        sleep(60);
    }

    std::cerr << "Estimated number of records for partition: " << mPartition <<
        " is = " << mRecordCount << " records" << std::endl;
    recordCount = htonl(mRecordCount);
    rv = write(outFd, &recordCount, sizeof(int));
    if (rv < 0) {
        KFS_LOG_STREAM_FATAL << "Unable to send record count...Exiting"
            << KFS_LOG_EOM;
        exit(-1);
    }
}


static double ComputeTimeDiff(const struct timeval &startTime, const struct timeval &endTime)
{
    double timeSpent;

    timeSpent = (endTime.tv_sec * 1e6 + endTime.tv_usec) -
        (startTime.tv_sec * 1e6 + startTime.tv_usec);
    return timeSpent / 1e6;
}

void
IMerger::Start(int fd, string deadMappersDir, uint64_t mergeHeapSize)
{
    uint32_t recordCount = 0;
    uint32_t dupRecords = 0;
    int retVal;
    int expectedRecordCount;
    IFileKey_t prevKey((unsigned char *)"", 0);
    // for debugging, which run did the previous record come from
    int prevIndex;
    std::vector<uint32_t> reduceGroupHistogram;
    uint32_t reduceGroupSize = 0;
    struct timeval lapStart, lapEnd;
    double timeBlockedKFS = 0.0, timeBlockedJava = 0.0;
    uint32_t numRecsProcessedAtPreviousHeartbeat = 0;
    // in bytes
    mMergeHeapSize = mergeHeapSize;

    // disable any read-ahead in the KFS client; all the read-ahead's
    // are triggered by the merger using the Async API.
    mKfsClnt->SetDefaultReadAheadSize(0);
    // All the reads are done in buffers we pass down to KFS.  Limit
    // the buffer allocation to 64K---which is the KFS checksum block size.
    mKfsClnt->SetDefaultIoBufferSize(65536);

    // this is the setup which gets the per-chunk index and gets us going;
    // we'll page in the data as we go along
    LoadData(fd, deadMappersDir);

    KFS_LOG_STREAM_INFO << "Loading of heap is done..." << KFS_LOG_EOM;

    bool shouldTerminateEarly = false;
    struct timeval lastUpdateTime;
    gettimeofday(&lastUpdateTime, NULL);

    while (1) {
        if (mRecords.empty()) {
            for (uint32_t i = 0; i < mSortedRuns.size(); i++) {
                if (mSortedRuns[i].isAllDone)
                    continue;
                LoadNextRecord(i);
            }
            if (mRecords.empty()) {
                KFS_LOG_STREAM_INFO << "Heap is truly empty...all done record count "
                    << " expected: " << mRecordCount
                    << " actual: " << recordCount
                    << KFS_LOG_EOM;
                break;
            }
        }

        const IMergerEntry_t &v = mRecords.top();
        int sortedRunIdx = v.sortedRunIdx;

        // check if it is a duplicate
        SeenRecordsMapIter srmi = mSeenRecords.find(v.mapperAttempt);
        bool isDup = false;
        if (srmi == mSeenRecords.end()) {
            std::set<uint32_t> rsnSet;

            rsnSet.insert(v.sequenceNumber);
            mSeenRecords[v.mapperAttempt] = rsnSet;
        } else {
            std::set<uint32_t> &seenRecords = srmi->second;
            if (seenRecords.find(v.sequenceNumber) != seenRecords.end())
                isDup = true;
            else {
                seenRecords.insert(v.sequenceNumber);
            }
        }

        if (isDup) {
            dupRecords++;
        } else {
            gettimeofday(&lapStart, NULL);
            int delta = ComputeTimeDiff(lastUpdateTime, lapStart);
            if (delta >= mWbUpdateIntervalSecs) {
                
                KFS_LOG_STREAM_INFO << "**PAUSING: " 
                    << " Delta: " << delta 
                    << " update interval: " << mWbUpdateIntervalSecs
                    << " Updating...next key is "
                    << v.key.key.get() << KFS_LOG_EOM;
                //report the next key to be processed and the
                //result tells us if we need to early terminate
                shouldTerminateEarly = 
                    mWorkgetter.shouldTerminateEarly(v.key.key.get(), v.key.keyLen,
                        recordCount - numRecsProcessedAtPreviousHeartbeat,
                        recordCount);
                numRecsProcessedAtPreviousHeartbeat = recordCount;
                if(shouldTerminateEarly) {
                    KFS_LOG_STREAM_INFO << "**PAUSING:  asked to WIND UP!!" << KFS_LOG_EOM;
                    //wind up the reducer
                    break; 
                }
                gettimeofday(&lastUpdateTime, NULL);                
            }

            int len = htonl(v.key.keyLen);
            gettimeofday(&lapStart, NULL);
            retVal = write(fd, &len, sizeof(int));
            retVal = write(fd, v.key.key.get(), v.key.keyLen);
            len = htonl(v.valLen);
            retVal = write(fd, &len, sizeof(int));
            retVal = write(fd, v.value, v.valLen);
            gettimeofday(&lapEnd, NULL);
            timeBlockedJava += ComputeTimeDiff(lapStart, lapEnd);
            if (retVal < 0) {
                KFS_LOG_STREAM_FATAL << "Unable to send a record...Exiting"
                    << KFS_LOG_EOM;
                exit(-1);
            }
            if (recordCount > 0) {
                assert((prevKey == v.key) || (prevKey < v.key));
            }
            if (prevKey == v.key)
                reduceGroupSize++;
            else {
                if (reduceGroupSize > 0) {
                    reduceGroupHistogram.push_back(reduceGroupSize);
                }
                reduceGroupSize = 1;
                prevKey = v.key;
                prevIndex = sortedRunIdx;
                // we have a new reduce group now.  since the
                // dups are only within a single reduce group, as an
                // optimization, reset.
                mSeenRecords.clear();
                // put the fact that we have seen and consumed this new record back in
                {
                    std::set<uint32_t> rsnSet;

                    rsnSet.insert(v.sequenceNumber);
                    mSeenRecords[v.mapperAttempt] = rsnSet;
                }
            }
            recordCount++;
        }
        mRecords.pop();
        double delta;
        gettimeofday(&lapStart, NULL);
        // add an entry from the file we pulled this record from
        LoadNextRecord(sortedRunIdx);
        gettimeofday(&lapEnd, NULL);
        delta = ComputeTimeDiff(lapStart, lapEnd);
        if (delta > 0.1) {
            KFS_LOG_STREAM_INFO << "Read from run: " << sortedRunIdx <<
                " took: " << delta << " secs" << KFS_LOG_EOM;
        }
        timeBlockedKFS += delta;
    }

    if (reduceGroupSize > 0)
        reduceGroupHistogram.push_back(reduceGroupSize);

    mKfsClnt->DisableAsyncRW();

    KFS_LOG_STREAM_INFO << "Time blocked on KFS: "
        << timeBlockedKFS
        << " ; time blocked on reducer: "
        << timeBlockedJava
        << KFS_LOG_EOM;

    std::cerr << "Done with writing out: " << recordCount
        << " records; expected records was: " << mRecordCount
        << " records; dup records was: " << dupRecords
        << std::endl;

    // dump out the histogram
    /*
    for (uint32_t i = 0; i < reduceGroupHistogram.size(); i++) {
        std::cerr << i << " = " << reduceGroupHistogram[i] << std::endl;
    }
    */

    expectedRecordCount = shouldTerminateEarly ? recordCount : mRecordCount;

    // Actual record sent
    mRecordCount = recordCount;
    const int zero = 0;
    const int magic = htonl(0xDEADDEAD);
    retVal = write(fd, &zero, sizeof(int));
    retVal = write(fd, &magic, sizeof(int));
    recordCount = htonl(recordCount);
    retVal = write(fd, &recordCount, sizeof(int));

    mWorkgetter.postNumRecords(mPartition, mRecordCount, expectedRecordCount);
}

