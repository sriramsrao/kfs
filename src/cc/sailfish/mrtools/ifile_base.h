//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: ifile_base.h $
//
// Created 2011/02/07
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
// \brief Common declarations related to I-files.
//----------------------------------------------------------------------------

#ifndef KAPPENDER_IFILE_BASE_H
#define KAPPENDER_IFILE_BASE_H
#include <string.h>
#include <arpa/inet.h>
#include <boost/shared_ptr.hpp>
#include <boost/shared_array.hpp>

namespace KFS
{

    const uint32_t LZO_CODEC = 'lzo ';
    /*
     * A packet in an I-file contains one or more records
     * <4-bytes magic> in front of each packet
     * <4-bytes codec>: 0 means no compression; otherwise 'lzo '
     * <8-bytes mapper id>: <4 bytes of task id>, <4 bytes for retry count>
     * <4-bytes partition id> --- overkill since we'll have 1 partition/file...but ok
     * <4-bytes compressed len: length of payload
     * <4-bytes uncompressed len: so we know how big of a buffer for decompressing
    */
    struct IFilePktHdr_t {
        uint32_t magic;
        uint32_t codec;
        // per-partition record seq. #.  When used with at-least-once
        // append semantics, we use this value to filter out
        // duplicates records emitted by an appender.
        uint32_t sequenceNumber;
        uint64_t mapperAttempt;
        uint32_t partition;
        uint32_t compressedRecLen;
        uint32_t uncompressedRecLen;
        IFilePktHdr_t(uint64_t ma = 0, int p = 0, int c = 0, int l = 0) :
            magic(0xEBCDEBCD),
            codec(0x0),
            sequenceNumber(0),
            mapperAttempt(ma),
            partition(p),
            compressedRecLen(c),
            uncompressedRecLen(l) { }
    };

    struct IFileSortKey_t {
        unsigned char *key;
        uint32_t keyLen;
        uint32_t recordIndex;
        IFileSortKey_t() : key(0), keyLen(0), recordIndex(0) { }
        ~IFileSortKey_t() {
            delete [] key;
        }
        void init(const char *k, uint32_t l, uint32_t r) {
            keyLen = l;
            recordIndex = r;
            key = new unsigned char[keyLen];
            memcpy(key, k, keyLen);
        }
        static inline void swap(IFileSortKey_t *a, IFileSortKey_t *b) {
            unsigned char *tempKey = a->key;
            uint32_t tempLen = a->keyLen;
            uint32_t tempIndex = a->recordIndex;

            a->key = b->key;
            a->keyLen = b->keyLen;
            a->recordIndex = b->recordIndex;

            b->key = tempKey;
            b->keyLen = tempLen;
            b->recordIndex = tempIndex;
        }
        bool operator < (const IFileSortKey_t &other) const {
            uint32_t cLen = std::min(keyLen, other.keyLen);
            int cmpVal = memcmp(key, other.key, cLen);
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

    struct IFileIndexEntry_t {
        uint32_t keyLen;
        uint32_t recLen;
        // to get a count of the # of records between a pair of index
        // entries, stick in the record # in the index.
        uint32_t recordNumber;
        off_t offset;
        const char *key;
        IFileIndexEntry_t() : keyLen(0), recLen(0), recordNumber(0),
                              offset(0), key(0) {}
    };

    /*
     * A single record in an I-file
     */
    struct IFileRecord_t {
        struct IFilePktHdr_t pktHdr;
        uint32_t recLen;
        uint32_t keyLen;
        boost::shared_array<unsigned char> key;
        uint32_t valLen;
        boost::shared_array<unsigned char> value;
        IFileRecord_t() { };
        IFileRecord_t(const IFileRecord_t &other) {
            pktHdr = other.pktHdr;
            recLen = other.recLen;
            keyLen = other.keyLen;
            key = other.key;
            valLen = other.valLen;
            value = other.value;
        };
        int serializedLen() {
            uint32_t len = sizeof(IFilePktHdr_t);
            len += sizeof(keyLen) + keyLen;
            len += sizeof(valLen) + valLen;
            return len;
        }

        int serialize(char *buffer) {
            uint32_t v = htonl(keyLen);
            char *p = buffer;

            memcpy(p, &pktHdr, sizeof(IFilePktHdr_t));
            p += sizeof(IFilePktHdr_t);
            memcpy(p, &v, sizeof(uint32_t));
            p += sizeof(uint32_t);
            memcpy(p, key.get(), keyLen);
            p += keyLen;
            v = htonl(valLen);
            memcpy(p, &v, sizeof(uint32_t));
            p += sizeof(uint32_t);
            memcpy(p, value.get(), valLen);
            p += valLen;
            return (p - buffer);
        };

        int serializeIndexEntry(char *buffer, off_t offset, uint32_t recordNumber) {
            char *p = buffer;

            memcpy(p, &keyLen, sizeof(uint32_t));
            p += sizeof(uint32_t);
            memcpy(p, &recLen, sizeof(uint32_t));
            p += sizeof(uint32_t);
            memcpy(p, &recordNumber, sizeof(uint32_t));
            p += sizeof(uint32_t);
            memcpy(p, &offset, sizeof(off_t));
            p += sizeof(off_t);
            memcpy(p, key.get(), keyLen);
            p += keyLen;
            return (p - buffer);
        };

        bool operator < (const IFileRecord_t &other) const {
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
}

#endif // KAPPENDER_IFILE_BASE_H
