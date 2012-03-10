//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: workbuilder.h 3830 2012-03-05 19:54:45Z sriramr $
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

#ifndef WORKBUILDER_WORKBUILDER_H
#define WORKBUILDER_WORKBUILDER_H
#include <string>
#include <fstream>
#include <set>
#include <boost/shared_array.hpp>
#include <tr1/unordered_map>
#include "common/properties.h"
#include "common/kfstypes.h"
#include "common/hsieh_hash.h"
#include "libkfsIO/KfsCallbackObj.h"
#include "libkfsIO/ITimeout.h"
#include "libkfsIO/IOBuffer.h"
#include "libkfsIO/NetConnection.h"
#include "libkfsClient/KfsOps.h"
#include "statuschecker.h"
#include "setnumreducer.h"

#ifdef USE_INTEL_IPP
#include <ipp.h>
#else
#include <lzo/lzo1x.h>
#include <lzo/lzoconf.h>
#endif

namespace workbuilder
{

    class Job;

    class IndexReader : public KFS::KfsCallbackObj, private KFS::ITimeout {
    public:
        IndexReader(Job *job, std::ofstream &l);
        ~IndexReader();
        int HandleEvent(int code, void *data);
        void Timeout();

        int getNumChunks() const {
            return mNumChunks;
        }
        off_t getFilesize() const {
            return mFilesize;
        }
        // offset that corresponds to EOF
        off_t getEOFChunkOffset() const {
            return mNextChunkOffset;
        }
        void init(int iFileId, int rackId, const std::string &iFilePath);
        bool isAllDone() const {
            return mIsAllDone;
        }
        void setAllDone() {
            mIsAllDone = true;
        }

        void lostChunkRecoveryComplete();

        void setRecoveringLostChunk() {
            mIsRecoveringLostChunk = true;
        }
        bool isRecoveringLostChunk() {
            return mIsRecoveringLostChunk;
        }
        int getNumIndexesRead() const {
            return mNumIndexesRead;
        }

        bool areSufficientIndexesRead();

        void loadIndexFromDir(const std::string &indexDir);

        void setReducerStart() {
            mReducerStarted = true;
            mForcedAllChunksStable = false;
            if (mIsIdle)
                start();
        }

    private:
        // a backpointer
        Job *mJob;
        int mIFileId;
        // if we have to read an I-file that is on a rack
        int mRackId;
        int mLeaseRetryCount;
        int64_t mSeqno;
        off_t mNextChunkOffset;
        // computed as the sum of the sizes of individual chunks
        off_t mFilesize;
        int   mNumChunks;
        int  mNumIndexesRead;
        // cycle thru the replicas of a given chunk and ensure the
        // index is built there before moving to next one
        uint32_t mReadIndexFromReplica;

        // if we can't get the chunk location after N retries, we move
        // on.  We let failure recovery handle the lost chunk.
        int mGetAllocRetryCount;

        std::ofstream &mJobLog;

        std::string mIFilePath;
        // connection to the metaserver/chunkserver
        KFS::NetConnectionPtr mMetaserverConn;
        KFS::NetConnectionPtr mChunkserverConn;
        KFS::IOBuffer::OStream  mOutBuffer;

        bool mIsMetaOp;
        bool mIsIdle;
        bool mIsAllDone;
        bool mReducerStarted;
        bool mIsRecoveringLostChunk;
        bool mForcedAllChunksStable;

        KFS::KfsOp               *mCurrentOpPtr;
        KFS::LookupPathOp         mLookupPathOp;
        KFS::LeaseAcquireOp       mLeaseAcquireOp;
        KFS::GetAllocOp           mGetAllocOp;
        KFS::GetChunkMetadataOp   mGetChunkMetadataOp;
        KFS::GetChunkIndexOp      mGetChunkIndexOp;
        KFS::MakeChunksStableOp   mForceChunksStableOp;

        void resetForRetry();

        void readIndexFromFile(const std::string &filename, off_t chunkOffset);

        void start();
        void submitOp();
        void dispatch(KFS::IOBuffer *buffer);
        void parseResponse(KFS::IOBuffer *buffer);
        // handle various ops
        void forceAllChunksStable();
        void lookupPath();
        void getChunkLayout();
        void getReadLease();
        void readChunkMetadata();
        void getChunkMetadata();
        void getIndex();

        void done(KFS::MakeChunksStableOp &forceChunksStableOp, KFS::IOBuffer *inBuffer);
        void done(KFS::LookupPathOp &lookupPathOp, KFS::IOBuffer *inBuffer);
        void done(KFS::GetAllocOp &allocOp, KFS::IOBuffer *inBuffer);
        void done(KFS::LeaseAcquireOp &leaseAcquireOp, KFS::IOBuffer *inBuffer);
        void done(KFS::GetChunkMetadataOp &getChunkMetadataOp, KFS::IOBuffer *inBuffer);
        void done(KFS::GetChunkIndexOp &getIndexOp, KFS::IOBuffer *inBuffer);

        void ensureMetaserverConnection();
    };


    typedef boost::shared_array<unsigned char> KeyPtr_t;

    struct IFileKey_t {
        KeyPtr_t key;
        uint32_t keyLen;
        IFileKey_t() : keyLen(0) { }
        IFileKey_t(KeyPtr_t k, uint32_t l) :
            key(k), keyLen(l) { }
        IFileKey_t(const unsigned char *k, uint32_t l) : keyLen(l) {
            key.reset(new unsigned char[l + 1]);
            memcpy(key.get(), k, l);
            key.get()[l] = '\0';
        }
        IFileKey_t(const IFileKey_t &other) :
            key(other.key), keyLen(other.keyLen) { }
        IFileKey_t & operator = (const IFileKey_t &other) {
            key = other.key;
            keyLen = other.keyLen;
            return *this;
        }
        IFileKey_t & operator = (IFileKey_t &other) {
            key = other.key;
            keyLen = other.keyLen;
            return *this;
        }
        bool operator == (const IFileKey_t &other) const {
            return ((keyLen == other.keyLen) &&
                    (memcmp(key.get(), other.key.get(), keyLen) == 0));
        }
        bool operator < (const IFileKey_t &other) const {
            uint32_t cLen = std::min(keyLen, other.keyLen);
            int cmpVal = memcmp(key.get(), other.key.get(), cLen);
            if (keyLen == other.keyLen)
                // if keylength's are the same, then comparison value determines
                // order
                return cmpVal < 0;
            if (cmpVal == 0)
                // in case one is a prefix of the other, the one with the
                // shorter key length is smaller
                return keyLen < other.keyLen;
            // all else...
            return cmpVal < 0;
        }
    };


    struct IFileKeyCompare {
        bool operator() (const IFileKey_t &k1, const IFileKey_t &k2) const {
            return k1 < k2;
        }
        bool operator() (const IFileKey_t *k1, const IFileKey_t *k2) const {
            return (*k1) < (*k2);
        }
    };

    struct IFileKeyHash_fcn {
        KFS::Hsieh_hash_fcn h;
        std::size_t operator()(const IFileKey_t &data) const {
            return h((const char *)data.key.get(), data.keyLen);
        }
    };

    struct ReduceTaskKeyRangeWorkInfo {
        ReduceTaskKeyRangeWorkInfo() { }
        ReduceTaskKeyRangeWorkInfo(int i, const IFileKey_t &l, const IFileKey_t &u) :
            iFileId(i), lowerKey(l), upperKey(u) { }
        int iFileId;
        IFileKey_t lowerKey;
        IFileKey_t upperKey;
    };

    typedef std::pair<int, off_t > ReduceTaskChunkWorkInfo;

    typedef std::map <KFS::kfsChunkId_t, std::vector<int> > ChunksToAppendersMap;
    typedef std::map <KFS::kfsChunkId_t, std::vector<int> >::iterator ChunksToAppendersMapIter;

    class Workbuilder;

    struct CompressedIndex_t {
        CompressedIndex_t(): cLen(0), uLen(0) { }
        CompressedIndex_t(boost::shared_ptr<char> p, uint32_t c, uint32_t u):
            compressedRecords(p), cLen(c), uLen(u) { }
        CompressedIndex_t & operator = (CompressedIndex_t &other) {
            compressedRecords = other.compressedRecords;
            cLen = other.cLen;
            uLen = other.uLen;
            return *this;
        }
        CompressedIndex_t & operator = (const CompressedIndex_t &other) {
            compressedRecords = other.compressedRecords;
            cLen = other.cLen;
            uLen = other.uLen;
            return *this;
        }

        boost::shared_ptr<char> compressedRecords;
        uint32_t cLen;
        uint32_t uLen;
    };

    class Job {
    public:
        Job(Workbuilder *workbuilder, const std::string &iFileBasedir,
            const std::string &logdir,
            const std::string &jobtracker, const std::string &jobid,
            const std::string &indexReaderJobId,
            int nMaps, int nIFiles);
        ~Job();
        std::string getJobName() const {
            return mJobTracker + "-" + mJobId;
        }
        std::string getJobTracker() const {
            return mJobTracker;
        }
        std::string getJobId() const {
            return mJobId;
        }
        void loadFromIndexDir(const std::string &indexDir);
        void setTestMode(int ifileId);

        bool isInTestMode() const {
            return mTestMode;
        }

        bool isRunningJob(const std::string &jobname) {
            return getJobName() == jobname;
        }

        /* PAUSING BEGIN */
        void setNumReducers(int nReducers) {
            mNumReducers = nReducers;
        }
        int getNumReducers() {
            return mNumReducers;
        }
        void reduceTaskHeartbeat(int reducerId, uint32_t recordCountDelta,
            uint32_t recordCount);

        /* PAUSING END */

        // Check with the job-tracker if the job is still running
        JobStatus checkStatus(bool force = false);
        void reducerStart();
        void storeReduceTaskRecordCount(int reducerId, int partition,
            int actual, int expected);

        bool hasReducerStarted() const {
            return mReducerStarted;
        }
        JTNotifier getJTNotifier() const {
            return mJTNotifier;
        }

        int getNumReducers() const {
            return mReduceTaskToKeys.size();
        }
        // For testing, the chunk indexes are stored in a dir; one file
        // per chunk.  Load them and build the plan
        void loadIndexFromDir(const string &indexDir);

        void indexReaderDone();
        void addNewChunkIndex(uint64_t ifileid, uint64_t chunkid,
            off_t chunkOffset, int indexSize, char * indexBuffer);

        void addChunkAppender(int mapperId, KFS::kfsChunkId_t chunkId);
        int handleLostChunk(KFS::kfsFileId_t iFileId, KFS::kfsChunkId_t lostChunkId);
        bool isChunkLost(KFS::kfsChunkId_t lostChunkId) {
            std::map<KFS::kfsChunkId_t, bool>::iterator it = mRecoveringChunks.find(lostChunkId);
            return it != mRecoveringChunks.end();
        }
        bool isChunkRecoveryComplete(KFS::kfsChunkId_t lostChunkId) {
            std::map<KFS::kfsChunkId_t, bool>::iterator it = mRecoveringChunks.find(lostChunkId);
            if (it == mRecoveringChunks.end())
                // if we weren't recovering this chunk, recovery is complete
                return true;
            return it->second;
        }


        int assignReduceTaskWork();
        // return response code
        int getReduceTaskWork(int taskId, char **response, int &responseLen);

        /* PAUSING BEGIN */
        void addRemainderReduceTask(int reduceId, IFileKey_t startKey);
        bool shouldPreemptReduceTask() {
            return mNumReducersToPreempt > 0;
        }   
        void getNumReducersToPreempt();
        /* PAUSING END */

        void setPlanDone() {
            mIsPlanInProgress = false;
            mIsPlanDone = true;
        }
        bool isPlanDone() {
            return mIsPlanDone;
        }
        void setPlanInProgress() {
            mIsPlanInProgress = true;
        }
        bool isPlanInProgress() {
            return mIsPlanInProgress;
        }
        void setTaskWorkLimit(uint64_t taskWorkLimit) {
            mTaskWorkLimitBytes = taskWorkLimit;
        }

        std::ofstream& getLog() {
            return mJobLog;
        }

    private:
        Workbuilder *mWorkbuilder;
        std::string mJobTracker;
        std::string mJobId;
        std::string mIFileBasedir;
        int    mNumMappers;
        int    mNumIFiles;
        int    mNumReducers;
        int    mNumReducersToPreempt;
        int  mNumIndexesProcessed;
        bool mReducerStarted;
        bool mTestMode;
        std::ofstream mJobLog;
        // have one reader per I-file
        std::vector<IndexReader *> mIndexReaders;

        // store some # of keys from each i-file; we sample these
        // to work out where the split points for reduce tasks are
        std::vector< std::vector<IFileKey_t> > mIFileKeys;
        std::vector< off_t > mRecordsPerIFile;

#ifdef USE_INTEL_IPP
        IppLZOState_8u *mWrkMem;
#else
        lzo_voidp mWrkMem;
#endif
        std::vector< std::vector<CompressedIndex_t> > mCompressedIndex;

        // for each chunk, store the set of appenders (map tasks) that
        // wrote data to that chunk.  We can use this to work out what map
        // tasks to re-run whenever a chunk is lost.
        ChunksToAppendersMap mChunksToAppenders;

        // send a request to the JT to check job status
        StatusChecker mStatusChecker;
        // notify JT the desired # of reducers for the job
        JTNotifier mJTNotifier;
        uint64_t mTaskWorkLimitBytes;

        int mTaskWorkPolicy;
        bool mIsPlanInProgress;
        bool mIsPlanDone;
        bool mIsStoringCompressedIndexes;

        // Reducer id->{ifileid, set of keys in that ifile}
        std::map<int, ReduceTaskKeyRangeWorkInfo> mReduceTaskToKeys;
        // Reducer id->{ifileid, set of chunks in that ifile}
        std::map<int, ReduceTaskChunkWorkInfo> mReduceTaskToChunks;

        // Track the status of lost chunks whose content is being
        // recovered via re-execution of map tasks that wrote to them.
        std::map<KFS::kfsChunkId_t, bool> mRecoveringChunks;

        off_t mTotalRecords;
        off_t mTotalSize;
        off_t mSeenKeys;
        bool mConverted;

        void setupIndexReader(int ifileId, const std::string &indexReaderJobId);

        bool areSufficientIndexesRead(uint64_t ifileId) {
            return mIndexReaders[ifileId]->areSufficientIndexesRead();
        }

        off_t getIFilesize(int ifileId) {
            return mIndexReaders[ifileId]->getFilesize();
        }

        void processChunkIndex(uint64_t ifileid, uint64_t chunkid,
            off_t chunkOffset, int indexSize, char * indexBuffer);
        void decompressChunkIndex(char *decompressedBuf, CompressedIndex_t &entry);
        void storeCompressedChunkIndex(uint64_t ifileid, uint32_t indexSize, char * indexBuffer);

        void assignReduceTaskWorkByChunks(uint64_t sizePerTask);
        void assignReduceTaskWorkByKeyRanges(uint64_t sizePerTask);

        int getReduceTaskWorkByChunks(int taskId, char **response, int &responseLen);
        int getReduceTaskWorkByKeyRanges(int taskId, char **response, int &responseLen);
    };


    enum WorkbuilderOp_t {
        kRestart = 100,
        kMapperStart,
        kChunksAppended,
        kChunkLost, // called by the merger when it can't read a chunk
        kReducerStart, // called by individual reducers asking for work
        kReducerRecordCount, // kgroupby pushes counters to us
        kBuildPlan, // called when the first reducer starts up---signal that maps are done
        // an update from the reducer saying the # of records it has gotten so far
        kReducerUpdate,
        kHealthCheck,
        kUndefined,
        kNumOps
    };

    struct WorkbuilderOp {
        WorkbuilderOp() : opType(kUndefined), cseq(1), responseCode(404),
                          response(0), responseLen(0) { }
        ~WorkbuilderOp() {
            delete [] response;
        }
        WorkbuilderOp_t opType;
        // pass in the query string: is either an opName or a file that the user is asking for
        std::string opName;
        KFS::Properties params;
        int cseq;
        int responseCode;
        // content data is a shared reference
        int contentLen;
        char *contentData;
        char *response;
        int responseLen;
        void show(KFS::IOBuffer::OStream &os) const {
            os << "HTTP/1.1 " << responseCode;
            if (responseCode == 200)
                os << " OK\r\n";
            else
                os << " NOT FOUND\r\n";
            os << "Cseq: " << cseq << "\r\n";
            os << "Status: 0\r\n";
            os << "Http-Status-Code: " << responseCode << "\r\n";
            os << "Connection: Close\r\n";
            if (response == NULL) {
                os << "\r\n";
                return;
            }
            os << "Content-length: " << responseLen << "\r\n\r\n";
            os.write(response, responseLen);
        }
    };

    class Workbuilder : public KFS::ITimeout {
    public:
        Workbuilder() : KFS::ITimeout(), mJob(NULL), mAlwaysReplyWithRetry(false), mRetrySetNumReducers(false) { };
        void mainLoop(const std::string &indexDir,
            const std::string &iFileBasedir, const std::string &logdir,
            int clientAcceptPort);
        void handleOp(WorkbuilderOp &op);
        int dummyMapperStart(const std::string &iFileBasedir,
            const std::string &logdir, std::string job,
            int ifileId);
        void setJobParams(uint64_t taskWorkLimit);
        void setAlwaysReplyWithRetry(bool retry) {
            mAlwaysReplyWithRetry=retry;
        }
        void buildPlan();

    private:
        Job *mJob;
        bool mAlwaysReplyWithRetry;
        bool mRetrySetNumReducers;

        std::string mIFileBasedir;
        std::string mLogdir;
        uint64_t mTaskWorkLimitBytes;

        void restart();
        int mapperStart(KFS::Properties &kv);
        int chunksAppended(KFS::Properties &kv, int contentLen, const char *contentData);
        int chunkLost(KFS::Properties &kv, int &responseCode);
        int reducerStart(KFS::Properties &kv, char **response,
            int &responseLen, int &responseCode);
        int reducerRecordCount(KFS::Properties &kv);
        /* PAUSING BEGIN */
        int reducerUpdate(KFS::Properties &kv, int contentLen, const char *contentData);
        /* PAUSING END */
        int initBuildPlan(KFS::Properties &kv);
        void Timeout();
        
    };

    void initWorkbuilderOpMap();
    WorkbuilderOp_t getWorkbuilderOp(const std::string &query);

    extern void initIndexReader(const std::string &metahost, int metaport,
        bool isIndexReadDisabled);

    bool isIndexReadDisabled();

    extern Workbuilder gWorkbuilder;
    extern std::string gMetaserver;
    extern int gMetaport;
}

#endif // WORKBUILDER_WORKBUILDER_H
