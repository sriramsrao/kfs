//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: Checksum.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2006/09/12
//
// Copyright 2008 Quantcast Corp.
// Copyright 2006-2008 Kosmix Corp.
//
// This file is part of Kosmos File System (KFS).
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
// An adaptation of the 32-bit Adler checksum algorithm
//
//----------------------------------------------------------------------------

#include "Checksum.h"

#include <algorithm>
#include <vector>

#ifdef USE_INTEL_IPP
#include <ipp.h>
#else
#include <zlib.h>
#endif


using std::min;
using std::vector;
using std::list;

namespace KFS {

#ifdef USE_INTEL_IPP
static const IppStatus sIppStatus       = ippStaticInit();
static const uint32_t  kKfsNullChecksum = 1;

static inline uint32_t
KfsChecksum(uint32_t chksum, const void* buf, size_t len)
{
    if (len <= 0) {
        return kKfsNullChecksum;
    }
    Ipp32u       res     = chksum;
    const size_t kMaxLen = 0x7FFFFFFFu;
    const Ipp8u* p       = reinterpret_cast<const Ipp8u*>(buf);
    for (size_t i = len; i > 0; ) {
        const int l = (int)min(i, kMaxLen);
        ippsAdler32_8u(p, l, &res);
        i -= l;
        p += l;
    }
    return res;
}
#else
static const uint32_t kKfsNullChecksum = adler32(0, Z_NULL, 0);

static inline uint32_t
KfsChecksum(uint32_t chksum, const void* buf, size_t len)
{
    return adler32(chksum, reinterpret_cast<const Bytef*>(buf), len);
}
#endif

uint32_t
OffsetToChecksumBlockNum(off_t offset)
{
    return offset / CHECKSUM_BLOCKSIZE;
}

uint32_t
OffsetToChecksumBlockStart(off_t offset)
{
    return (offset / CHECKSUM_BLOCKSIZE) *
        CHECKSUM_BLOCKSIZE;
}

uint32_t
OffsetToChecksumBlockEnd(off_t offset)
{
    return ((offset / CHECKSUM_BLOCKSIZE) + 1) *
        CHECKSUM_BLOCKSIZE;
}

uint32_t
ComputeBlockChecksum(const char *buf, size_t len)
{
    return KfsChecksum(kKfsNullChecksum, buf, len);
}

vector<uint32_t>
ComputeChecksums(const char *buf, size_t len)
{
    vector <uint32_t> cksums;

    if (len <= CHECKSUM_BLOCKSIZE) {
        uint32_t cks = ComputeBlockChecksum(buf, len);
        cksums.push_back(cks);
        return cksums;
    }
    
    size_t curr = 0;
    while (curr < len) {
        size_t tlen = min((size_t) CHECKSUM_BLOCKSIZE, len - curr);
        uint32_t cks = ComputeBlockChecksum(buf + curr, tlen);

        cksums.push_back(cks);
        curr += tlen;
    }
    return cksums;
}

uint32_t
ComputeBlockChecksum(uint32_t chksum, const IOBuffer *data, size_t len)
{
    uint32_t res = chksum;
    for (IOBuffer::iterator iter = data->begin();
         len > 0 && (iter != data->end()); ++iter) {
        size_t tlen = min((size_t) iter->BytesConsumable(), len);

        if (tlen == 0)
            continue;

        res = KfsChecksum(res, iter->Consumer(), tlen);
        len -= tlen;
    }
    return res;
}

uint32_t
ComputeBlockChecksum(const IOBuffer *data, size_t len)
{
    return ComputeBlockChecksum(kKfsNullChecksum, data, len);
}

uint32_t
ComputeBlockChecksum(const IOBuffer *d1, size_t l1,
    const IOBuffer *d2, size_t l2)
{
    uint32_t res = kKfsNullChecksum;
    res = ComputeBlockChecksum(res, d1, l1);
    res = ComputeBlockChecksum(res, d2, l2);
    return res;
}

vector<uint32_t>
ComputeChecksums(const IOBuffer *data, size_t len)
{
    vector<uint32_t> cksums;
    IOBuffer::iterator iter = data->begin();

    if (len < CHECKSUM_BLOCKSIZE) {
        uint32_t cks = ComputeBlockChecksum(data, len);
        cksums.push_back(cks);
        return cksums;
    }

    if (iter == data->end())
        return cksums;

    const char *buf = iter->Consumer();

    /// Compute checksum block by block
    while ((len > 0) && (iter != data->end())) {
        size_t currLen = 0;
        uint32_t res = kKfsNullChecksum;
        while (currLen < CHECKSUM_BLOCKSIZE) {
            unsigned navail = min((size_t) (iter->Producer() - buf), len);
            if (currLen + navail > CHECKSUM_BLOCKSIZE)
                navail = CHECKSUM_BLOCKSIZE - currLen;

            if (navail == 0) {
                iter++;
                if (iter == data->end())
                    break;
                buf = iter->Consumer();
                continue;
            }

            currLen += navail;
            len -= navail;
            res = KfsChecksum(res, buf, navail);
            buf += navail;
        }
        cksums.push_back(res);
    }
    return cksums;
}

}

