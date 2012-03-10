//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kgroupby.h 2591 2011-05-24 23:08:58Z sriramr $
//
// Created 2010/11/14
//
// Copyright 2010 Yahoo Corporation.  All rights reserved.
// Yahoo PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KAPPENDER_KGROUPBY_H
#define KAPPENDER_KGROUPBY_H

#include <tr1/unordered_map>
#include <string>
#include <set>
#include <boost/shared_array.hpp>
#include "libkfsClient/KfsClient.h"
#include "common/hsieh_hash.h"
#include "workgetter.h"
#include "ifile_base.h"
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


    typedef std::tr1::unordered_map<IFileKey_t, std::vector<IFileRecord_t>, IFileKeyHash_fcn > RecordMap;
    typedef std::tr1::unordered_map<IFileKey_t, std::vector<IFileRecord_t>, IFileKeyHash_fcn >::iterator
        RecordMapIter;    

    class KGroupBy {
    public:
        KGroupBy(KfsClientPtr &clnt, int partition,
                const std::string &jobId, const std::string &workbuilder, 
            const std::string &basedir, bool usingPerRackIFiles);
        void LoadData(int fd, std::string deadMappersDir);
        void Start(int fd);
        void parseAssignment(const char* assignment, int size);
    private:
        KfsClientPtr mKfsClnt;
        Workgetter mWorkgetter;
        int mPartition;
        int mRecordCount;
        std::string mBasedir;
        // if the data is in multiple files, we need to read'em all
        vector<std::string> mFilenames;
        // another hack to minimize code...mFilename used to be there; now it'll have one
        // value from mFilenames..should fix this cleanly
        std::string mFilename;
        char* mUncompBuffer;
        uint32_t mUncompBufferSize;
        // buffer for loading the records from ifile
        unsigned char* mBuffer;
        std::set<uint64_t> mDeadMappers;

        // Are we assigned key ranges or chunks
        bool mAssignedKeys;
        // is the data in a per rack I-file
        bool mUsingPerRackIFiles;
        // if we are assigned chunks...
        off_t mStartChunkOffset;
        off_t mEndChunkOffset;
        
        IFileKeyRange_t mKeyRange;

        RecordMap mRecords;
        void GetDeadMappers(const std::string &deadMappersDir);
        void ReadParseChunks(int kfsFd, off_t fileSz);
        int ReadParseChunk(int kfsFd, int nbytesToRead, unsigned char *buffer);
        int ResizeBuffers(int newUncompSize);

        void ParseChunk(unsigned char *buffer, int bufSz);

        // These two methods read data concurrently: for the chunks we
        // need to read in a given range, open up the chunks and use
        // the KFS client's ASYNC API to read the data.
        void GetChunks();
        void GetKeyRanges(off_t fileSz);

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

#endif // KAPPENDER_KGROUPBY_H
