//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: imerger.h 3472 2011-12-13 08:59:39Z sriramr $
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

#ifndef KAPPENDER_KGROUPBY_H
#define KAPPENDER_KGROUPBY_H

#include <algorithm>
#include <queue>
#include <string>
#include <tr1/unordered_map>
#include <set>
#include <boost/shared_array.hpp>
#include "libkfsClient/KfsClient.h"
#include "common/hsieh_hash.h"
#include "workgetter.h"
#include "ifile_base.h"

#ifdef USE_INTEL_IPP
#include <ipp.h>
#else
#include <lzo/lzo1x.h>
#include <lzo/lzoconf.h>
#endif

extern "C" {
#include <string.h>
}

namespace KFS
{
    typedef boost::shared_array<unsigned char> KeyPtr_t;

    struct IFileKeyRange_t {
        KeyPtr_t lowerKey;
        uint32_t lowerKeyLen;
        KeyPtr_t upperKey;
        uint32_t upperKeyLen;
    };

    struct IFileKey_t {
        KeyPtr_t key;
        uint32_t keyLen;
        IFileKey_t() : keyLen(0) { }
        IFileKey_t(KeyPtr_t k, uint32_t l) :
            key(k), keyLen(l) { }
        IFileKey_t(const unsigned char *k, uint32_t l) : keyLen(l) {
            key.reset(new unsigned char[l + 1]);
            memcpy(key.get(), k, l);
            key[l] = '\0';
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
        Hsieh_hash_fcn h;
        std::size_t operator()(const IFileKey_t &data) const {
            return h((const char *)data.key.get(), data.keyLen);
        }
    };


    struct SortedRun_t {
        SortedRun_t();
        ~SortedRun_t() {
            delete [] buffer;
            if (uncompressedBufLen != 0)
                free(uncompressedBufPtr);
#ifndef USE_INTEL_IPP
            free(wrkMem);
#endif
        }
        void clear() {
#ifndef USE_INTEL_IPP
            wrkMem = 0;
#endif
            uncompressedBufLen = 0;
            buffer = NULL;
        }
        uint32_t bytesAvail() const {
            unsigned char *endPtr = buffer + bufDataLen;
            assert(curPtr <= endPtr);
            return endPtr - curPtr;
        }
        uint32_t spaceAvail() {
            // everything from start to current has been consumed
            return curPtr - buffer;
        }
        bool isUncompressedRecordAvail() const {
            return nextRecordPtr !=
                uncompressedBufPtr + uncompressedBufDataSz;
        }

        // the KFS client provided fd for this run
        int fd;
        // byte range in the fd to read data from: start/end are
        // within the chunk
        int startPos;
        int endPos;
        // this is the absolute end point---endPos modified to reflect
        // the file end position.
        off_t fileEndPos;
        // The data we have read for this run
        unsigned char *buffer;
        // buffer size
        uint32_t bufsz;
        // how much data do we have the buffer: 0..end
        uint32_t bufDataLen;

        unsigned char *uncompressedBufPtr;
        unsigned char *nextRecordPtr;
#ifdef USE_INTEL_IPP
        Ipp32u uncompressedBufDataSz;
#else
        lzo_voidp wrkMem;
        lzo_uint uncompressedBufDataSz;
#endif
        uint32_t uncompressedBufLen;
        // Pointer to the next "blob" in the buffer
        unsigned char *curPtr;
        unsigned char *prefetchPtr;
        int prefetchLen;
        bool isPrefetchPending;
        bool isAllDone;
    };

    class IMerger;

    struct IMergerEntry_t {
        IMergerEntry_t() : iMerger(0), sortedRunIdx(-1), value(0), valLen(0) {}
        ~IMergerEntry_t() {
            value = NULL;
        }
        // backpointer to the imerger singleton
        IMerger *iMerger;
        int sortedRunIdx;
        // for filtering duplicates, pass in the src info and the seq #
        uint64_t mapperAttempt;
        uint32_t sequenceNumber;
        IFileKey_t key;
        // pointer into a sorted run's buffer
        unsigned char *value;
        uint32_t valLen;
        bool operator< (const IMergerEntry_t &other) const;
    };

    typedef std::priority_queue<IMergerEntry_t> RecordHeap;

    // For duplicate filtering, for a reduce group, given an
    // mapper/attempt, the sequence #'s for the records from that
    // mapper that have been sent to the reducer so far.
    typedef std::tr1::unordered_map<uint64_t, std::set<uint32_t> > SeenRecordsMap;
    typedef std::tr1::unordered_map<uint64_t, std::set<uint32_t> >::iterator SeenRecordsMapIter;

    /*
    typedef std::tr1::unordered_map<IFileKey_t, std::vector<IFileRecord_t>, IFileKeyHash_fcn > RecordMap;
    typedef std::tr1::unordered_map<IFileKey_t, std::vector<IFileRecord_t>, IFileKeyHash_fcn >::iterator
        RecordMapIter;
    */

    class IMerger {
    public:
        IMerger(KfsClientPtr &clnt, int partition,
            const std::string &jobId, const std::string &workbuilder,
            const std::string &basedir);
        void Start(int fd, std::string deadMappersDir, uint64_t mergeHeapSize);
        void parseAssignment(const char* assignment, int size);
        int getBytesAvailForSortedRun(int runIdx) const {
            return mSortedRuns[runIdx].bytesAvail();
        }
        int getPartition() const {
            return mPartition;
        }
        void setWbUpdateIntervalSecs(int secs) {
            mWbUpdateIntervalSecs = secs;
        }
    private:
        KfsClientPtr mKfsClnt;
        Workgetter mWorkgetter;
        int mPartition;
        int mRecordCount;
        // how frequently do we notify the workbuilder of progress
        int mWbUpdateIntervalSecs;
        uint64_t mMergeHeapSize; // in bytes
        std::string mBasedir;
        // if the data is in multiple files, we need to read'em all
        // XXX: this field is legacy.  we don't do this anymore.
        // Leaving the code around in case this is needed again.
        vector<std::string> mFilenames;
        // The current file we are reading from
        std::string mFilename;
        std::set<uint64_t> mDeadMappers;

        // Are we assigned key ranges or chunks
        // always keys
        bool mAssignedKeys;

        // the offset in the file until which we have to process keys
        off_t mLastOffsetInFile;

        IFileKeyRange_t mKeyRange;
        // one sorted run per chunk of the I-file.  we merge from these runs.
        std::vector<SortedRun_t> mSortedRuns;

        SeenRecordsMap mSeenRecords;

        RecordHeap mRecords;

        void DrainIndexReads(uint32_t sortedRunStart);

        void LoadData(int fd, string deadMappersDir);
        void LoadNextRecord(int sortedRunIdx);

        void IssuePrefetch(int sortedRunIdx);
        void GetMoreData(int sortedRunIdx);

        void DecompressNextBlob(int sortedRunIdx);

        void GetDeadMappers(const std::string &deadMappersDir);

        // These two methods read data concurrently: for the chunks we
        // need to read in a given range, open up the chunks and use
        // the KFS client's ASYNC API to read the data.
        void GetChunks();
        bool GetKeyRanges(off_t fileSz);

        bool IsInMyKeyRange(const IFileKey_t &key, const IFileKey_t &lowerRangeKey,
            const IFileKey_t &upperRangeKey);

        void parseAssignmentByKeys(const char* startPtr, const char *endPtr);
        void parseAssignmentByChunks(const char* startPtr, const char *endPtr);

        int GetDataFromChunk(int kfsFd, char *buffer, int nbytesToRead);
        void GetRangeToReadFromChunkIndex(const char *indexBuffer, int indexSize,
            int &startPos, int &endPos);
        int GetKeyRangeFromChunk(int kfsFd, char *buffer, int nbytesToRead);
    };
}

#endif // KAPPENDER_IMERGER_H
