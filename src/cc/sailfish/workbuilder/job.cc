//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: job.cc 3830 2012-03-05 19:54:45Z sriramr $
//
// Created 2011/01/04
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

#include "workbuilder.h"
#include "common/log.h"
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include <map>
#include <set>
#include <cmath>
#include <iostream>
#include <sstream>
#include <algorithm> /* ELASTICITY */

using namespace std;
using namespace workbuilder;
using namespace KFS;

Job::Job(Workbuilder *workbuilder, const std::string &iFileBasedir, const std::string &logdir,
    const std::string &jobtracker, const std::string &jobid,
    const std::string &indexReaderJobId,
    int nMaps, int nIFiles) :
    mWorkbuilder(workbuilder),
    mJobTracker(jobtracker), mJobId(jobid),
    mIFileBasedir(iFileBasedir),
    mNumMappers(nMaps),
    mNumIFiles(nIFiles),
    mNumReducers(-1),
    mNumReducersToPreempt(0),
    mNumIndexesProcessed(0),
    mReducerStarted(false),
    mTestMode(false),
    mStatusChecker(jobtracker, jobid),
    mJTNotifier(jobtracker, jobid),
    mIsPlanInProgress(false),
    mIsPlanDone(false),
    mIsStoringCompressedIndexes(false)
{
    string jobLogFn = logdir + "/" + jobid + ".log";

    mWrkMem = 0;

#ifdef USE_INTEL_IPP
    Ipp32u lzoSize;
    ippsEncodeLZOGetSize(IppLZO1XST, 0, &lzoSize);
    MemAlignedMalloc(&mWrkMem, sizeof(IppLZOState_8u*), lzoSize);
    ippsEncodeLZOInit_8u(IppLZO1XST, 0, mWrkMem);
#else
    MemAlignedMalloc(&mWrkMem, sizeof(lzo_align_t), LZO1X_1_MEM_COMPRESS);
    assert(mWrkMem != NULL);
#endif

    mJobLog.open(jobLogFn.c_str(), ios_base::ate);
    if (nIFiles == 0)
        return;
    mIFileKeys.resize(nIFiles);
    mRecordsPerIFile.resize(nIFiles);
    mCompressedIndex.resize(nIFiles);
    for (int i = 0; i < nIFiles; i++) {
        mRecordsPerIFile[i] = 0;
        setupIndexReader(i, indexReaderJobId);
    }
}

Job::~Job()
{
    for (uint32_t i = 0; i < mIndexReaders.size(); i++) {
        delete mIndexReaders[i];
    }
    mIndexReaders.clear();
    mCompressedIndex.clear();
    free(mWrkMem);
}

void
Job::setupIndexReader(int ifileId, const string &indexReaderJobId)
{
    IndexReader *ir = new IndexReader(this, mJobLog);
    string iFilename = mIFileBasedir + "/" + indexReaderJobId + "/" +
        boost::lexical_cast<string>(ifileId) + "/file." + boost::lexical_cast<string>(ifileId);

    mIndexReaders.push_back(ir);
    ir->init(ifileId, -1, iFilename);
}

void
Job::setTestMode(int ifileId)
{
    // in test mode, we don't check job status
    // mainly used for debugging
    mTestMode = true;
    mNumIFiles = ifileId + 1;
    mIFileKeys.resize(mNumIFiles);
    mRecordsPerIFile.resize(mNumIFiles);
    mCompressedIndex.resize(mNumIFiles);
    for (int i = 0; i < ifileId; i++) {
        mRecordsPerIFile[i] = 0;
        mIndexReaders.push_back(new IndexReader(this, mJobLog));
        mIndexReaders[i]->setAllDone();
    }

    setupIndexReader(ifileId, mJobId);
}

void
Job::loadIndexFromDir(const string &indexDir)
{
    // you are in test mode
    // # of I-files is 1; # of index readers is 1
    // tell the index reader to stop its thing and use this...
    if (!mTestMode) {
        KFS_LOG_STREAM_INFO << "Can't see the index dir: "
            << indexDir << KFS_LOG_EOM;
        exit(-1);
    }
    mIndexReaders[0]->loadIndexFromDir(indexDir);
    mReducerStarted = true;
    KFS_LOG_STREAM_INFO << "Dummy calling assignReduceTaskWork" << KFS_LOG_EOM;
    assignReduceTaskWork();
}

void
Job::getNumReducersToPreempt()
{
    if (mTestMode || !mReducerStarted)
        return;

    if (mNumReducersToPreempt > 0)
        return;

    JobStatus js;

    js = mJTNotifier.getNumReducersToPreempt(mNumReducersToPreempt);
    if ((js == kSucceeded) && (mNumReducersToPreempt > 0))
        mJobLog << now() << ": Asked to preempt: " << mNumReducersToPreempt
            << endl;
}

JobStatus
Job::checkStatus(bool force)
{
    if (mTestMode)
        return kRunning;

    return mStatusChecker.getStatus(force);
}

void
Job::reduceTaskHeartbeat(int reducerId, uint32_t recordCountDelta, uint32_t recordCount)
{
    mJobLog << now() << " reducer = " << reducerId << " progress = " 
        << recordCountDelta << endl;

    if (shouldPreemptReduceTask())
        mJobLog << now() << " reducer = " << reducerId << " aborted at = " 
            << - (int32_t) recordCount << endl;
}

void
Job::reducerStart()
{
    if (mReducerStarted)
        return;
    mReducerStarted = true;
    for (uint32_t i = 0; i < mIndexReaders.size(); i++) {
        mIndexReaders[i]->setReducerStart();
    }
}

void
Job::storeReduceTaskRecordCount(int reducerId, int partition,
    int actual, int expected)
{
    mJobLog << now() << ": Counter: task = " << reducerId <<
        ' ' << partition << ' ' << actual << ' ' << expected << endl;
}

void
Job::indexReaderDone()
{
    bool allDone = true;

    for (uint32_t i = 0; i < mIndexReaders.size(); i++) {
        if (!mIndexReaders[i]->isAllDone()) {
            KFS_LOG_STREAM_INFO << "Index reader for file: " << i
                << " isn't done yet; waiting..." << KFS_LOG_EOM;
            allDone = false;
            return;
        }
    }

    if (isPlanDone()) {
        KFS_LOG_STREAM_INFO << "Recovery for all index readers is done"
            << KFS_LOG_EOM;
        return;
    }

    KFS_LOG_STREAM_INFO << "All index readers are done...moving to building plan"
        << KFS_LOG_EOM;
    mWorkbuilder->buildPlan();
}

void
Job::addChunkAppender(int mapperId, kfsChunkId_t chunkId)
{
    ChunksToAppendersMapIter c = mChunksToAppenders.find(chunkId);
    if (c == mChunksToAppenders.end()) {
        std::vector<int> tasks;
        tasks.push_back(mapperId);
        mChunksToAppenders[chunkId] = tasks;
    } else {
        std::vector<int> &tasks = c->second;
        tasks.push_back(mapperId);
    }
}

int
Job::handleLostChunk(kfsFileId_t iFileId, kfsChunkId_t lostChunkId)
{
    ChunksToAppendersMapIter c = mChunksToAppenders.find(lostChunkId);
    if (c == mChunksToAppenders.end()) {
        return -1;
    }
    mJobLog << now() << "lost chunk in: ifile = " << iFileId <<
        " chunkId = " << lostChunkId << endl;
    mJobLog << now() << " to recover chunkId = " << lostChunkId <<
        " need to re-run: ";
    std::vector<int> &tasks = c->second;
    // if a map task failed and was re-run, and if both tasks happened
    // to write to the same chunk, we want to re-run the task only once.
    sort(tasks.begin(), tasks.end());
    tasks.erase(unique(tasks.begin(), tasks.end()), tasks.end());
    for (uint32_t i = 0; i < tasks.size(); i++) {
        mJobLog << tasks[i] << ",";
        // XXX: What if the notification fails???
        mJTNotifier.reRunTask(tasks[i]);
    }
    mJobLog << endl;
    // Record the fact that recovery for I-file is initiated
    mRecoveringChunks[lostChunkId] = false;
    mIndexReaders[iFileId]->setRecoveringLostChunk();
    return 0;
}

void
Job::storeCompressedChunkIndex(uint64_t ifileid, uint32_t indexSize, char *indexBuffer)
{
    CompressedIndex_t ci;
    char *compressBuf = new char[2 * indexSize];
    bool isCompressionOk;

    ci.uLen = indexSize;
    ci.cLen = 2 * indexSize;
#ifdef USE_INTEL_IPP
    IppStatus retCode = ippsEncodeLZO_8u(
        (const Ipp8u*)indexBuffer, (Ipp32u) indexSize,
        (Ipp8u*)compressBuf,&ci.cLen,mWrkMem);
    isCompressionOk = (retCode == ippStsNoErr);
#else
    lzo_uint cLen = 2 * indexSize;
    int retCode = lzo1x_1_compress(
        (const unsigned char*) indexBuffer, indexSize,
        (unsigned char*) compressBuf, &cLen, mWrkMem);
    isCompressionOk = (retCode == LZO_E_OK);
    ci.cLen = cLen;
#endif
    assert(isCompressionOk);

    ci.compressedRecords.reset(new char[ci.cLen]);
    memcpy(ci.compressedRecords.get(), compressBuf, ci.cLen);
    mCompressedIndex[ifileid].push_back(ci);
    delete [] compressBuf;

    mJobLog << now() << " Compressed index for ifile: " << ifileid
        << " o = " << indexSize << " c = " << ci.cLen << endl;

}

void
Job::addNewChunkIndex(uint64_t ifileid, uint64_t chunkid, off_t chunkOffset, 
    int indexSize, char * indexBuffer)
{
    const off_t kThreeGig = ((off_t) 1) << 33;
    if (mIsStoringCompressedIndexes) {
        if ((mIndexReaders[ifileid]->getNumIndexesRead() > 50) &&
            (chunkOffset % kThreeGig) != 0) {
            // we got a lot of data; start down-sampling
            return;
        }
        storeCompressedChunkIndex(ifileid, indexSize, indexBuffer);
    } else {
        processChunkIndex(ifileid, chunkid, chunkOffset, indexSize, indexBuffer);
    }
}

void
Job::processChunkIndex(uint64_t ifileid, uint64_t chunkid, off_t chunkOffset, 
    int indexSize, char * indexBuffer)
{
    int bytesRead = 0;
    uint32_t keyLength;
    uint32_t numBytes;
    off_t offset;
    char *curPtr = indexBuffer;
    uint32_t numIndexEntries, nentriesRead = 0;
    uint32_t recordNumber = 0;
    bool isSkipping = false;

    // if planning is all done, the only reason we are here is because
    // we recovered from a lost chunk---some maps were re-run.
    // Nothing to do...move on
    if (isPlanDone())
        return;

    const off_t kOneGig = (off_t) (1 << 30);
    // if ((mIndexReaders[ifileid]->getNumIndexesRead() > 50) &&
    if ((mNumIndexesProcessed > 50) &&
        (chunkOffset % kOneGig) != 0) {
        // after we have read 50 indexes, we got quite a bit of data;
        // after this point, downsample: take every 8th index---128M chunksize.
        // otherwise, the processing overheads in
        // assignReduceTaskWork() become very significant.  This
        // causes the "lag" between sort finish, and then work assign,
        // to be on the on order of an hour.

        // for reduce planning we need to see the entire range of
        // keys.  This means that given a chunk index, we should at
        // least take the first and last entry.
        isSkipping = true;
        KFS_LOG_STREAM_INFO << "Skipping processing of new chunk index: ifile = "
            << ifileid << " offset = " << chunkOffset
            << KFS_LOG_EOM;
    }

    memcpy(&numIndexEntries, curPtr, sizeof(uint32_t));
    curPtr += sizeof(uint32_t);

    bytesRead += sizeof(uint32_t);

    while (nentriesRead < numIndexEntries) {
        char *startP = curPtr;
        // Maybe move this deserialization code into IFileRecord_t or IFileIndexEntry_t ?
        memcpy(&keyLength, curPtr, sizeof(uint32_t));
        curPtr += sizeof(uint32_t);
        memcpy(&numBytes, curPtr, sizeof(uint32_t));
        curPtr += sizeof(uint32_t);
        memcpy(&recordNumber, curPtr, sizeof(uint32_t));
        // this is is 0-based...
        recordNumber++;
        curPtr += sizeof(uint32_t);
        memcpy(&offset, curPtr, sizeof(off_t));
        curPtr += sizeof(off_t);

        // either we are not skipping or if we are, then we only take
        // the first and last entry.
        if ((!isSkipping) || (nentriesRead == 0) || ((nentriesRead == numIndexEntries - 1))) {
            //construct key holder
            IFileKey_t k((unsigned char*) curPtr,(uint32_t)keyLength);
            mIFileKeys[ifileid].push_back(k);
        }

        curPtr += keyLength;

        nentriesRead++;

        assert(nentriesRead <= numIndexEntries);
        //update total size
        mTotalSize += numBytes;
        KFS_LOG_VA_DEBUG("finished reading in index entry, key with keylen = %d,len = %d",
            keyLength, numBytes);

        assert (bytesRead + (curPtr - startP) <= indexSize);
        bytesRead += (curPtr - startP);
    }
    if (!isSkipping)
        mNumIndexesProcessed++;

    mRecordsPerIFile[ifileid] += recordNumber;

    KFS_LOG_STREAM_INFO << "Adding new chunk index: ifile = "
        << ifileid << " offset = " << chunkOffset
        << " numIndexEntries = " << numIndexEntries
        << " last record # = " << recordNumber << KFS_LOG_EOM;
}


//Function assigns a roughly equal number of key/value pairs to each reduce
//task.  This roughly assumes we pay a seek per pair.  If we instead pay a seek
//per unique key or per total size of assigned records, we should change the assignment policy.
int
Job::assignReduceTaskWork()
{
    if (mIsPlanDone) {
        return -1;
    }

    mNumReducers = mNumIFiles;

    //different policies divide work in different ways
    //int recordsPerTask = mJob->mTotalRecords/numReduceTasks;
    // mSizePerTask = 1 << 30;
    KFS_LOG_STREAM_INFO << "number of bytes per task: " <<
        mTaskWorkLimitBytes << KFS_LOG_EOM;

    if (isIndexReadDisabled()) {
        assignReduceTaskWorkByChunks(mTaskWorkLimitBytes);
    } else {
        assignReduceTaskWorkByKeyRanges(mTaskWorkLimitBytes);
    }
    return mNumReducers;
}

void
Job::assignReduceTaskWorkByChunks(uint64_t sizePerTask)
{
    int currTask = 0;
    for (int32_t iFileId = 0; iFileId < mNumIFiles; iFileId++) {
        off_t iFileSz = mIndexReaders[iFileId]->getFilesize();
        int nTasks = iFileSz / sizePerTask;
        if (iFileSz % sizePerTask)
            nTasks++;
        off_t nextChunkOffset = 0;
        for (int i = 0; i < nTasks; i++) {
            KFS_LOG_STREAM_INFO << "Assigning to task: " << currTask + i
                << " iFile: " << iFileId << " <" << nextChunkOffset
                << " , " << nextChunkOffset + sizePerTask
                << ">" << KFS_LOG_EOM;

            mReduceTaskToChunks[currTask + i] = pair<int, off_t>(iFileId, nextChunkOffset);
            nextChunkOffset += sizePerTask;
        }
        currTask += nTasks;
    }
    if (mNumReducers < currTask)
        mNumReducers = currTask;
}

void
Job::decompressChunkIndex(char *decompressedBuf, CompressedIndex_t &entry)
{
    uint32_t uLen = entry.uLen;

#ifdef USE_INTEL_IPP
    IppStatus retCode = ippsDecodeLZO_8u(
        (Ipp8u *)entry.compressedRecords.get(), (Ipp32u) entry.cLen,
        (Ipp8u *) decompressedBuf, &entry.uLen);
    assert(retCode == ippStsNoErr);
#else
    lzo_uint uncompressLen = entry.uLen;
    int retCode = lzo1x_decompress((unsigned char *) entry.compressedRecords.get(), entry.cLen,
        (unsigned char *) decompressedBuf, &uncompressLen, mWrkMem);
    assert(retCode == LZO_E_OK);
    uLen = uncompressLen;
#endif
    assert(entry.uLen == uLen);
}

void
Job::assignReduceTaskWorkByKeyRanges(uint64_t sizePerTask)
{
    // go file by file and identify split points
    int currTask = 0;
    for (int32_t iFileId = 0; iFileId < mNumIFiles; iFileId++) {
        off_t iFileSz = getIFilesize(iFileId);
        int nTasks = iFileSz / sizePerTask;
        if (iFileSz % sizePerTask)
            nTasks++;

        if (mIsStoringCompressedIndexes) {
            vector<CompressedIndex_t> &perFileIndex = mCompressedIndex[iFileId];
            for (uint32_t cid = 0; cid < perFileIndex.size(); cid++) {
                uint32_t uLen = perFileIndex[cid].uLen;
                char *decompressedBuf = new char[uLen];

                decompressChunkIndex(decompressedBuf, perFileIndex[cid]);
                processChunkIndex(iFileId, 0, ((off_t) cid) * KFS::CHUNKSIZE,
                    uLen, decompressedBuf);
                delete [] decompressedBuf;
            }
            mCompressedIndex[iFileId].clear();
        }

        KFS_LOG_STREAM_INFO << "Ifile: " << iFileId <<
            " has records = " << mRecordsPerIFile[iFileId] << KFS_LOG_EOM;

        vector<IFileKey_t> &keys = mIFileKeys[iFileId];

        if ((nTasks == 0) || (keys.size() == 0)) {
            KFS_LOG_STREAM_INFO << "Ifile: " << iFileId << " is empty...hmmm" << KFS_LOG_EOM;
            continue;
        }

        sort(keys.begin(), keys.end());
        keys.erase(unique(keys.begin(), keys.end()), keys.end());

        assert(keys.size() >= 1);

        // We see the indexes from all the chunks.  So, we just need
        // to ensure that we have a key that is outside the range---so
        // that we can process everything including the last key.  So,
        // take the last key and flip one bit somewhere

        IFileKey_t lastKey(keys[keys.size() - 1]);
        // we really want a deep copy
        IFileKey_t sentinel(lastKey.key.get(), lastKey.keyLen);
        for (uint32_t i = 0; i < sentinel.keyLen; i++)
            if (sentinel.key[i] != 0x7f) {
                sentinel.key[i]++;
                break;
            }

        keys.push_back(sentinel);

        assert(keys.size() >= 2);

        uint64_t splitLength = keys.size() / nTasks;
        if ((splitLength == 0) || (keys.size() == (uint32_t) nTasks)) {
            splitLength = 1;
            // fewer keys than tasks; so distribute 1 key per task
            // we don't have enough info here to cut the range
            // so, reduce the # of tasks to the # of keys we have
            // note that the entry at keys.size() is sentinel key.
            nTasks = keys.size() - 1;
        }

        assert(nTasks <= (int) keys.size());

        KFS_LOG_STREAM_INFO << "For ifile: " << iFileId <<
            " split length is: " << splitLength <<
            " # of unique keys: " << keys.size()
            << KFS_LOG_EOM;

        mJobLog << now() << ": ifile = " << iFileId << " has: "
            << mRecordsPerIFile[iFileId]
            << " ; # of tasks assigned to the ifile: " << nTasks
            << " ; # of unique keys we saw: " << keys.size()
            << endl;

        for (int i = 0; i < nTasks; i++) {
            uint64_t endPt = std::min((uint64_t) (keys.size() - 1),
                (i+1) * splitLength);

            // for the last task for this I-file, we have to give it everything in the
            // final interval; we only specify the begin point...
            if (i == nTasks - 1)
                endPt = keys.size() - 1;

            mReduceTaskToKeys[currTask] = ReduceTaskKeyRangeWorkInfo(iFileId,
                keys[i*splitLength], keys[endPt]);

            KFS_LOG_STREAM_DEBUG << "For task: " << currTask
                << " in ifile: " << iFileId << " assigned range: "
                << keys[i*splitLength].key.get() << "->" << keys[endPt].key.get()
                << KFS_LOG_EOM;
            currTask++;
        }
    }

    mIFileKeys.clear();

    mNumReducers = currTask;
}

void
Job::addRemainderReduceTask(int reduceId, IFileKey_t startKey)
{
    ReduceTaskKeyRangeWorkInfo pausedTaskInfo = mReduceTaskToKeys[reduceId];
    
    assert(mNumReducers != -1);
    mNumReducersToPreempt--;
    if (mNumReducersToPreempt < 0)
        mNumReducersToPreempt = 0;

    mReduceTaskToKeys[mNumReducers] = ReduceTaskKeyRangeWorkInfo(pausedTaskInfo.iFileId, 
        startKey, pausedTaskInfo.upperKey);
    KFS_LOG_STREAM_INFO << "**PAUSING: " << "Remainder Task " 
        << mNumReducers << " - start:" 
        << startKey.key.get() << " end:" << pausedTaskInfo.upperKey.key.get() << KFS_LOG_EOM;
    mNumReducers++;
}

/*
 * Returns work list for task.  Format is multiple of:
 * <key_length \t key \t numChunks \t chunkid \t chunkid \t ...>
 */
int
Job::getReduceTaskWork(int taskId, char **response, int &responseLen)
{
    if (isIndexReadDisabled())
        return getReduceTaskWorkByChunks(taskId, response, responseLen);
    else
        return getReduceTaskWorkByKeyRanges(taskId, response, responseLen);
}

/*
 * When index reads are disabled, each reduce task is assigned a set
 * of chunks.  The task is responsible for all the keys in those
 * chunks.  This corresponds to the scenario where all keys are
 * unique.  Furthermore, this gives us a lower bound on performance.
 * Each chunk is accessed only once (whereas, with keys, a chunk could
 * be accessed multiple times).
 */
int
Job::getReduceTaskWorkByChunks(int taskId, char **response, int &responseLen)
{
    KFS_LOG_VA_DEBUG("in getReduceTaskWork, taskId %i",taskId);
    *response = NULL;

    map<int, ReduceTaskChunkWorkInfo>::iterator taskIter;
    taskIter = mReduceTaskToChunks.find(taskId);
    if (taskIter == mReduceTaskToChunks.end()) {
        KFS_LOG_VA_ERROR("Task %i has no work and so should not exist", taskId);
        return 404;
    }
    int bufsz = 1 << 20;
    char *buffer = new char[bufsz];
    char *curBufPtr = buffer;
    ReduceTaskChunkWorkInfo &taskWork = taskIter->second;
    int iFileId = taskWork.first;
    off_t chunkStartOffset = taskWork.second;

    memcpy(curBufPtr, "CHUNKS", 6);
    curBufPtr += 6;
    memcpy(curBufPtr, &iFileId, sizeof(int));
    curBufPtr += sizeof(int);
    memcpy(curBufPtr, &chunkStartOffset, sizeof(off_t));
    curBufPtr += sizeof(off_t);
    memcpy(curBufPtr, &mTaskWorkLimitBytes, sizeof(uint32_t));
    curBufPtr += sizeof(uint32_t);

    *response = buffer;
    responseLen = curBufPtr - buffer;
    KFS_LOG_STREAM_INFO << "For task = " << taskId
        << " response size: " << responseLen
        << KFS_LOG_EOM;
    return 200;
}

/*
 * Returns work list for task.  Format is:
 * <ifileid><key range begin><key range end>
 */
int
Job::getReduceTaskWorkByKeyRanges(int taskId, char **response, int &responseLen)
{
    KFS_LOG_VA_DEBUG("in getReduceTaskWork, taskId %i",taskId);
    *response = NULL;

    map<int,ReduceTaskKeyRangeWorkInfo>::iterator taskIter;
    taskIter = mReduceTaskToKeys.find(taskId);
    if (taskIter == mReduceTaskToKeys.end()) {
        KFS_LOG_VA_ERROR("Task %i has no work and so should not exist", taskId);
        return 404;
    }
    int bufsz = 1 << 20;
    char *buffer = new char[bufsz];
    char *curBufPtr = buffer;
    ReduceTaskKeyRangeWorkInfo iFileKeyRange = taskIter->second;

    if (mIndexReaders[iFileKeyRange.iFileId]->isRecoveringLostChunk()) {
        int numUnfinishedMaps = mJTNotifier.getNumUnfinishedMappers();
        KFS_LOG_STREAM_INFO << "# of unfinished maps for job: " << mJobId
            << " is: " << numUnfinishedMaps << KFS_LOG_EOM;
        if (numUnfinishedMaps == 0) {
            for (std::map<KFS::kfsChunkId_t, bool>::iterator it = mRecoveringChunks.begin();
                 it != mRecoveringChunks.end(); it++) {
                // for all lost chunks, recovery is complete
                it->second = true;
            }
            // Notify the index reader so that it can read the indexes
            // of the re-generated chunks and finish recovery.
            mIndexReaders[iFileKeyRange.iFileId]->lostChunkRecoveryComplete();
        }
        mJobLog << now() << " : Recovery on Ifile: " << iFileKeyRange.iFileId
            << " is going on...returning 201 to task: " << taskId << endl;
        return 201;
    }

    if (!mIndexReaders[iFileKeyRange.iFileId]->isAllDone())
        // recovery is finished; but we aren't done with reading the
        // indexes for this ifile.  so wait.
        return 201;

    memcpy(curBufPtr, "RANGES", 6);
    curBufPtr += 6;

    memcpy(curBufPtr, &iFileKeyRange.iFileId, sizeof(int));
    curBufPtr += sizeof(int);

    // tell the merger where we think the EOF is.  this ensures that
    // even if recovery is going on and new chunks get added to this
    // ifile (even tho no block of this particular ifile was lost),
    // the merger can safely ignore those blocks.
    off_t iFileSz = mIndexReaders[iFileKeyRange.iFileId]->getEOFChunkOffset();
    memcpy(curBufPtr, &iFileSz, sizeof(off_t));
    curBufPtr += sizeof(off_t);

    // now pass the key range.
    uint32_t sz = iFileKeyRange.lowerKey.keyLen;
    memcpy(curBufPtr, &sz, sizeof(uint32_t));
    curBufPtr += sizeof(uint32_t);
    memcpy(curBufPtr, iFileKeyRange.lowerKey.key.get(), sz);
    curBufPtr += sz;

    sz = iFileKeyRange.upperKey.keyLen;
    memcpy(curBufPtr, &sz, sizeof(uint32_t));
    curBufPtr += sizeof(uint32_t);
    memcpy(curBufPtr, iFileKeyRange.upperKey.key.get(), sz);
    curBufPtr += sz;
    *response = buffer;
    responseLen = curBufPtr - buffer;
    return 200;
}
