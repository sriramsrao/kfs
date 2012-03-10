//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: LeaseClerk.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2006/10/12
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
//
// \brief A lease clerk interacts with the metaserver for renewing
// read leases. 
//----------------------------------------------------------------------------

#ifndef LIBKFSCLIENT_LEASECLERK_H
#define LIBKFSCLIENT_LEASECLERK_H

#include "common/kfstypes.h"
#include "common/cxxutil.h"
#include <tr1/unordered_map>
#include <time.h>

namespace KFS {

struct LeaseInfo_t {
    int64_t leaseId;
    time_t expires;
    time_t renewTime;
};

// mapping from a chunk id to its lease
typedef std::tr1::unordered_map <kfsChunkId_t, LeaseInfo_t> LeaseMap;
typedef std::tr1::unordered_map <kfsChunkId_t, LeaseInfo_t>::iterator LeaseMapIter;

class LeaseClerk {
public:
    /// Before the lease expires at the server, we submit we a renew
    /// request, so that the lease remains valid.  So, back-off a few
    /// secs before the leases and submit the renew
    static const int LEASE_RENEW_INTERVAL_SECS = KFS::LEASE_INTERVAL_SECS - 30;

    LeaseClerk() { };
    ~LeaseClerk(){ };
    /// Register a lease with the clerk.
    /// @param[in] chunkId The chunk associated with the lease.
    /// @param[in] leaseId  The lease id to be registered with the clerk
    void RegisterLease(kfsChunkId_t chunkId, int64_t leaseId);
    void UnRegisterLease(kfsChunkId_t chunkId);
    
    /// Check if lease is still valid.
    /// @param[in] chunkId  The chunk whose lease we are checking for validity.
    bool IsLeaseValid(kfsChunkId_t chunkId);

    bool ShouldRenewLease(kfsChunkId_t chunkId);

    /// Get the leaseId for a chunk.
    int GetLeaseId(kfsChunkId_t chunkId, int64_t &leaseId);

    void LeaseRenewed(kfsChunkId_t chunkId);
    void LeaseRelinquished(kfsChunkId_t chunkId);

private:
    /// All the leases registered with the clerk
    LeaseMap mLeases;
};

}

#endif // LIBKFSCLIENT_LEASECLERK_H
