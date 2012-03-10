//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: LeaseClerk.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2006/10/09
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
// \brief Code for dealing with lease renewals.
//
//----------------------------------------------------------------------------

#include "LeaseClerk.h"
#include "libkfsIO/Globals.h"

#include "ChunkServer.h"
#include "ChunkManager.h"
#include "MetaServerSM.h"
#include "AtomicRecordAppender.h"

using namespace KFS;
using namespace KFS::libkfsio;

LeaseClerk KFS::gLeaseClerk;

inline time_t
LeaseClerk::Now()
{
    return libkfsio::globalNetManager().Now();
}

LeaseClerk::LeaseClerk()
{
    mLastLeaseCheckTime = 0;
    SET_HANDLER(this, &LeaseClerk::HandleEvent);
}

void
LeaseClerk::RegisterLease(kfsChunkId_t chunkId, int64_t leaseId)
{
    const time_t now = Now();
    LeaseInfo_t lease;
    // Get rid of the old lease if we had one
    LeaseMapIter iter = mLeases.find(chunkId);

    if (iter != mLeases.end()) {
        lease = iter->second;
        mLeases.erase(iter);
    }

    lease.leaseId = leaseId;
    lease.expires = now + LEASE_INTERVAL_SECS;
    lease.lastWriteTime = now;
    lease.leaseRenewSent = false;
    mLeases[chunkId] = lease;
    // Dont' print msgs for lease cleanup events.
    if (chunkId != 0) {
        KFS_LOG_STREAM_DEBUG <<
            "registered lease:"
            " chunk: " << chunkId <<
            " lease: " << leaseId <<
        KFS_LOG_EOM;
    }
}

void
LeaseClerk::UnRegisterLease(kfsChunkId_t chunkId)
{
    LeaseMapIter iter = mLeases.find(chunkId);
    if (iter != mLeases.end()) {
        mLeases.erase(iter);
    }
    KFS_LOG_STREAM_DEBUG <<
        "Lease for chunk: " << chunkId << " unregistered" <<
    KFS_LOG_EOM;
}

void
LeaseClerk::UnregisterAllLeases()
{
    KFS_LOG_STREAM_DEBUG <<
        "Unregistered all " << mLeases.size() << " leases" <<
    KFS_LOG_EOM;
    mLeases.clear();
}

void
LeaseClerk::DoingWrite(kfsChunkId_t chunkId)
{
    LeaseMapIter const iter = mLeases.find(chunkId);
    if (iter == mLeases.end()) {
        return;
    }
    iter->second.lastWriteTime = Now();
}

bool
LeaseClerk::IsLeaseValid(kfsChunkId_t chunkId)
{
    // now <= lease.expires ==> lease hasn't expired and is therefore
    // valid.
    LeaseMapIter const iter = mLeases.find(chunkId);
    return (iter != mLeases.end() && Now() <= iter->second.expires);
}

time_t
LeaseClerk::GetLeaseExpireTime(kfsChunkId_t chunkId)
{
    LeaseMapIter const iter = mLeases.find(chunkId);
    return (iter == mLeases.end() ? 0 : iter->second.expires);
}

void
LeaseClerk::LeaseRenewed(kfsChunkId_t chunkId)
{
    LeaseMapIter iter = mLeases.find(chunkId);

    if (iter == mLeases.end())
        return;

    time_t now = Now();
    LeaseInfo_t lease = iter->second;
    KFS_LOG_STREAM_INFO <<
        "lease renewed for:"
        " chunk: " << chunkId <<
        " lease: " << lease.leaseId <<
    KFS_LOG_EOM;
    lease.expires = now + LEASE_INTERVAL_SECS;
    lease.leaseRenewSent = false;
    mLeases[chunkId] = lease;
}

int
LeaseClerk::HandleEvent(int code, void *data)
{
#ifdef DEBUG
    verifyExecutingOnEventProcessor();
#endif

    switch(code) {
        case EVENT_CMD_DONE: {
	    // we got a reply for a lease renewal
	    const KfsOp* const op = reinterpret_cast<const KfsOp*>(data);
            if (! op) {
                break;
            }
            if (op->op == CMD_LEASE_RENEW) {
                const LeaseRenewOp* const renewOp =
                    static_cast<const LeaseRenewOp*>(op);
	        if (renewOp->status == 0) {
	            LeaseRenewed(renewOp->chunkId);
	        } else {
	            UnRegisterLease(renewOp->chunkId);
                }
            } else if (op->op != CMD_LEASE_RELINQUISH) {
                // Relinquish op shouldn't get here with its default handler.
                KFS_LOG_STREAM_DEBUG << "unexpected op: " << op->op <<
                KFS_LOG_EOM;
            }
	    delete op;
        }
	break;

        default:
            assert(!"Unknown event");
            break;
    }
    return 0;
}

void
LeaseClerk::CleanupExpiredLeases()
{
    const time_t now = Now();
    for (LeaseMapIter curr = mLeases.begin(); curr != mLeases.end(); ) {
        // messages could be in-flight...so wait for a full
        // lease-interval before discarding dead leases
        if (now - curr->second.expires > LEASE_INTERVAL_SECS) {
	    KFS_LOG_STREAM_INFO <<
                "cleanup lease: " << curr->second.leaseId <<
                " chunk: "        << curr->first <<
            KFS_LOG_EOM;
            mLeases.erase(curr++);
        } else {
            ++curr;
        }
    }
}

class LeaseRenewer {
    LeaseClerk * const lc;
    const time_t       now;
public:
    LeaseRenewer(LeaseClerk *l, time_t n) : lc(l), now(n) { }
    void operator()(std::tr1::unordered_map <kfsChunkId_t, LeaseInfo_t>::value_type &v) {
        kfsChunkId_t chunkId = v.first;
        LeaseInfo_t& lease = v.second;
        
        if ((now + LeaseClerk::LEASE_EXPIRE_WINDOW_SECS < lease.expires) || 
            (lease.leaseRenewSent)) {
            // if the lease is valid for a while or a lease renew is in flight, move on
            return;
        }
	// Renew the lease if a write is pending or a write
	// occured when we had a valid lease or if we are doing record
	// appends to the chunk and some client has space reserved or
	// there is some data buffered in the appender.
	if ((now <= lease.lastWriteTime + LEASE_INTERVAL_SECS) ||
                gAtomicRecordAppendManager.WantsToKeepLease(chunkId) ||
                gChunkManager.IsWritePending(chunkId)) {
	    // The seq # is something that the metaserverSM will fill
	    LeaseRenewOp* const op = new LeaseRenewOp(
                -1, chunkId, lease.leaseId, "WRITE_LEASE");
	    KFS_LOG_STREAM_INFO <<
                "sending lease renew for:"
                " chunk: "      << chunkId <<
                " lease: "      << lease.leaseId <<
                " expires in: " << (lease.expires - now) << " sec" <<
            KFS_LOG_EOM;
	    op->clnt = lc;
            lease.leaseRenewSent = true;
	    gMetaServerSM.EnqueueOp(op);
        }
    }
};

void
LeaseClerk::Timeout()
{
    const time_t now = Now();
    if (mLastLeaseCheckTime + 1 >= now) {
        return;
    }
    mLastLeaseCheckTime = now;
    // once per second, check the state of the leases
    CleanupExpiredLeases();
    for_each(mLeases.begin(), mLeases.end(), LeaseRenewer(this, now));
}

void
LeaseClerk::AvoidRenewingLease(kfsChunkId_t chunkId)
{
    LeaseMapIter iter = mLeases.find(chunkId);

    if (iter == mLeases.end())
        return;

    // set it back in time so that we won't renew the lease
    iter->second.lastWriteTime -= (LEASE_INTERVAL_SECS + 1);
}

void
LeaseClerk::RelinquishLease(kfsChunkId_t chunkId, off_t size, bool hasChecksum, uint32_t checksum)
{
    LeaseMapIter const iter = mLeases.find(chunkId);
    if (iter == mLeases.end()) {
        KFS_LOG_STREAM_DEBUG <<
            "lease relinquish: no lease exists for:"
            " chunk: "    << chunkId <<
            " size: "     << size    <<
            " checksum: " << (hasChecksum ? int64_t(checksum) : int64_t(-1)) <<
        KFS_LOG_EOM;
        return;
    }
    // Notify metaserver if the lease exists, even if lease expired or renew is
    // in flight, then delete the lease.
    const LeaseInfo_t& lease = iter->second;
    LeaseRelinquishOp *op = new LeaseRelinquishOp(-1, chunkId, lease.leaseId, "WRITE_LEASE");
    KFS_LOG_STREAM_INFO <<
        "sending lease relinquish for:"
        " chunk: "      << chunkId <<
        " lease: "      << lease.leaseId <<
        " expires in: " << (lease.expires - Now()) << " sec" <<
        " size: "       << size <<
        " checksum: "   << (hasChecksum ? int64_t(checksum) : int64_t(-1)) <<
    KFS_LOG_EOM;    
    op->hasChecksum   = hasChecksum;
    op->chunkChecksum = checksum;
    op->chunkSize     = size;
    op->clnt          = this;
    mLeases.erase(iter);
    gMetaServerSM.EnqueueOp(op);
}
