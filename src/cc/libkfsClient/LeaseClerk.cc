//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: LeaseClerk.cc 1552 2011-01-06 22:21:54Z sriramr $
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
// \brief Code for dealing with lease renewals.
//
//----------------------------------------------------------------------------

#include "LeaseClerk.h"
#include "common/log.h"

using namespace KFS;

void
LeaseClerk::RegisterLease(kfsChunkId_t chunkId, int64_t leaseId)
{
    time_t now = time(0);
    LeaseInfo_t lease;

    lease.leaseId = leaseId;
    lease.expires = now + KFS::LEASE_INTERVAL_SECS;
    lease.renewTime = now + LEASE_RENEW_INTERVAL_SECS;

    mLeases[chunkId] = lease;
    KFS_LOG_VA_DEBUG("Registered lease: chunk=%lld, lease=%lld",
                     chunkId, leaseId);
}

void
LeaseClerk::UnRegisterLease(kfsChunkId_t chunkId)
{
    LeaseMapIter iter = mLeases.find(chunkId);
    if (iter != mLeases.end()) {
        mLeases.erase(iter);
    }
    KFS_LOG_VA_DEBUG("Lease for chunk = %lld unregistered",
                     chunkId);

}

bool
LeaseClerk::IsLeaseValid(kfsChunkId_t chunkId)
{
    LeaseMapIter iter = mLeases.find(chunkId);
    if (iter == mLeases.end())
        return false;

    time_t now = time(NULL);
    LeaseInfo_t lease = iter->second;
    
    // now <= lease.expires ==> lease hasn't expired and is therefore
    // valid.
    return now <= lease.expires;
}


bool
LeaseClerk::ShouldRenewLease(kfsChunkId_t chunkId)
{
    LeaseMapIter iter = mLeases.find(chunkId);
    assert(iter != mLeases.end());
    if (iter == mLeases.end()) {
        return true;
    }

    time_t now = time(NULL);
    LeaseInfo_t lease = iter->second;

    // now >= lease.renewTime ==> it is time to renew lease
    return now >= lease.renewTime;
}

int
LeaseClerk::GetLeaseId(kfsChunkId_t chunkId, int64_t &leaseId)
{
    LeaseMapIter iter = mLeases.find(chunkId);
    if (iter == mLeases.end()) {
        return -1;
    }
    leaseId = iter->second.leaseId;

    return 0;
}

void
LeaseClerk::LeaseRenewed(kfsChunkId_t chunkId)
{
    LeaseMapIter iter = mLeases.find(chunkId);
    if (iter == mLeases.end())
        return;

    time_t now = time(NULL);
    LeaseInfo_t lease = iter->second;

    KFS_LOG_VA_DEBUG("Lease for chunk = %lld renewed",
                     chunkId);

    lease.expires = now + KFS::LEASE_INTERVAL_SECS;
    lease.renewTime = now + LEASE_RENEW_INTERVAL_SECS;
    mLeases[chunkId] = lease;
}

void
LeaseClerk::LeaseRelinquished(kfsChunkId_t chunkId)
{
    if (!IsLeaseValid(chunkId))
        return;

    LeaseMapIter iter = mLeases.find(chunkId);
    LeaseInfo_t lease = iter->second;

    mLeases.erase(iter);
    
}
