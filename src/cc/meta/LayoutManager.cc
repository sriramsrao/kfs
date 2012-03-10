//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: LayoutManager.cc 3518 2012-01-03 23:41:39Z sriramr $
//
// Created 2006/06/06
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
// \file LayoutManager.cc
// \brief Handlers for chunk layout.
//
//----------------------------------------------------------------------------

#include <algorithm>
#include <functional>
#include <sstream>
#include <boost/lexical_cast.hpp>
#include <openssl/rand.h>

#include "LayoutManager.h"
#include "kfstree.h"
#include "libkfsIO/Globals.h"
#include "common/log.h"
#include "common/properties.h"

using std::for_each;
using std::find;
using std::ptr_fun;
using std::mem_fun;
using std::mem_fun_ref;
using std::bind2nd;
using std::sort;
using std::random_shuffle;
using std::remove_if;
using std::set;
using std::vector;
using std::map;
using std::min;
using std::max;
using std::endl;
using std::istringstream;
using std::make_pair;
using std::pair;
using std::make_heap;
using std::pop_heap;

using namespace KFS;
using namespace KFS::libkfsio;

LayoutManager KFS::gLayoutManager;

/// Max # of concurrent read/write replications per node
///  -- write: is the # of chunks that the node can pull in from outside
///  -- read: is the # of chunks that the node is allowed to send out
///
int MAX_CONCURRENT_WRITE_REPLICATIONS_PER_NODE = 5;
int MAX_CONCURRENT_READ_REPLICATIONS_PER_NODE = 10;
/// When a chunkserver says it is done replicating a chunk, how much time do we
/// spend looking for a new block to re-replicate for that server
const float MAX_TIME_TO_FIND_ADDL_REPLICATION_WORK = 0.005;
/// How much do we spend on each internal RPC in chunk-replication-check to handout
/// replication work.
const float MAX_TIME_FOR_CHUNK_REPLICATION_CHECK = 0.5;
/// Once a month, check the replication of all blocks in the system
int NDAYS_PER_FULL_REPLICATION_CHECK = 30;

///
/// When placing chunks, we see the space available on the node as well as
/// we take our estimate of the # of writes on
/// the node as a hint for choosing servers; if a server is "loaded" we should
/// avoid sending traffic to it.  This value defines a watermark after which load
/// begins to be an issue.
///
const uint32_t CONCURRENT_WRITES_PER_NODE_WATERMARK = 10;

/// Helper functor that can be used to find a chunkid from a vector
/// of meta chunk info's.

static inline time_t TimeNow() {
	return libkfsio::globalNetManager().Now();
}


static inline seq_t RandomSeqNo() {
    seq_t ret = 0;
    RAND_pseudo_bytes(
        reinterpret_cast<unsigned char*>(&ret), int(sizeof(ret)));
    return ((ret < 0 ? -ret : ret) >> 1);
}

template<typename T> static T& ReallocIfNeeded(T& vec) {
	if (vec.size() <= (vec.capacity() >> 1)) {
		T n(vec);
		vec.swap(n);
	}
	return vec;
}

template<typename T, typename I> static T& EraseReallocIfNeeded(T& vec, I b, I e) {
	vec.erase(b, e);
	return ReallocIfNeeded(vec);
}

class ChunkIdMatcher {
	chunkId_t myid;
public:
	ChunkIdMatcher(chunkId_t c) : myid(c) { }
	bool operator() (MetaChunkInfo *c) {
		return c->chunkId == myid;
	}
};

inline bool LayoutManager::InRecoveryPeriod() const
{
	return (TimeNow() < mRecoveryStartTime + mRecoveryIntervalSecs);
}

inline bool LayoutManager::InRecovery() const
{
	return (
		mChunkServers.size() < mMinChunkserversToExitRecovery ||
		InRecoveryPeriod()
	);
}

inline bool LayoutManager::IsChunkServerRestartAllowed() const
{
	return (
		! InRecovery() &&
		mChunkServers.size() > mMinChunkserversToExitRecovery &&
		mHibernatingServers.empty()
	);
}

inline bool
ARAChunkCache::Invalidate(iterator it)
{
	assert(it != mMap.end() && ! mMap.empty());
	mMap.erase(it);
	return true;
}

inline bool
ARAChunkCache::Invalidate(fid_t fid, int rack)
{
	iterator const it = Find(fid, rack);
	if (it == mMap.end()) {
		return false;
	}
	mMap.erase(it);
	return true;
}

inline bool
ARAChunkCache::Invalidate(fid_t fid, int rack, chunkId_t chunkId)
{
	iterator const it = Find(fid, rack);
	if (it == mMap.end() || it->second.chunkId != chunkId) {
		return false;
	}
	mMap.erase(it);
	return true;
}

void
ARAChunkCache::RequestNew(MetaAllocate& req)
{
	if (req.offset < 0 || (req.offset % CHUNKSIZE) != 0 ||
			! req.appendChunk) {
		assert(! "invalid parameters");
		return;
	}
	// Find the end of the list, normally list should have only one element.
	MetaAllocate* last = &req;
	while (last->next) {
		last = last->next;
	}
	FidRack_t key(req.fid, req.clientRack);
	mMap[key] = Entry(
		req.chunkId,
		req.chunkVersion,
		req.offset,
		TimeNow(),
		last
	);
}

bool
ARAChunkCache::Entry::AddPending(MetaAllocate& req)
{
	assert(req.appendChunk);

	if (! lastPendingRequest || ! req.appendChunk) {
		return false;
	}
	assert(lastPendingRequest->suspended);
	MetaAllocate* last = &req;
	last->suspended = true;
	while (last->next) {
		last = last->next;
		last->suspended = true;
	}
	// Put request to the end of the queue.
	// Make sure that the last pointer is correct.
	while (lastPendingRequest->next) {
		lastPendingRequest = lastPendingRequest->next;
	}
	lastPendingRequest->next = last;
	lastPendingRequest = last;
	return true;
}

void
ARAChunkCache::RequestDone(const MetaAllocate& req)
{
	assert(req.appendChunk);

	iterator const it = Find(req.fid, req.clientRack);
	if (it == mMap.end()) {
		return;
	}
	Entry& entry = it->second;
	if (entry.chunkId != req.chunkId) {
		return;
	}
	if (req.status != 0) {
		// Failure, invalidate the cache.
		mMap.erase(it);
		return;
	}
	entry.offset             = req.offset;
	entry.lastPendingRequest = 0;
	entry.lastAccessedTime   = TimeNow();
}

void
ARAChunkCache::Timeout(time_t minTime)
{
	for (iterator it = mMap.begin(); it != mMap.end(); ) {
		const Entry& entry = it->second;
		if (entry.lastAccessedTime >= minTime ||
				entry.lastPendingRequest) {
			++it; // valid entry; keep going
		} else {
			mMap.erase(it++);
		}
	}
}

/// The rebalancing thresholds should be set in the emulator to get desired
/// behavior.

LayoutManager::LayoutManager() :
	mLeaseId(RandomSeqNo()),
	mNumOngoingReplications(0),
	mIsRebalancingEnabled(false),
	mMaxRebalanceSpaceUtilThreshold(0.0),
	mMinRebalanceSpaceUtilThreshold(0.0),
	mIsExecutingRebalancePlan(false),
	mLastChunkRebalanced(1),
	mLastChunkReplicated(1),
	mRecoveryStartTime(0),
	mStartTime(time(0)),
	mRecoveryIntervalSecs(KFS::LEASE_INTERVAL_SECS),
	mMinChunkserversToExitRecovery(1),
        mMastersCount(0),
        mSlavesCount(0),
	mAllMasters(false),
	mAssignMasterByIpFlag(false),
	mLeaseOwnerDownExpireDelay(30),
	mPercentLoadedNodesToAvoidForWrites(0.3),
	mMaxReservationSize(4 << 20),
	mReservationDecayStep(4), // decrease by factor of 2 every 4 sec
	mChunkReservationThreshold(KFS::CHUNKSIZE),
	mReservationOvercommitFactor(.25),
	mServerDownReplicationDelay(10 * 60),
	mMaxDownServersHistorySize(4 << 10),
        mChunkServersProps(),
	mCSToRestartCount(0),
	mMastersToRestartCount(0),
	mMaxCSRestarting(0),
	mMaxCSUptime(24 * 60 * 60),
	mCSGracefulRestartTimeout(15 * 60),
	mCSGracefulRestartAppendWithWidTimeout(40 * 60),
	mLastReplicationCheckTime(TimeNow()),
	mLastRecomputeDirsizeTime(TimeNow())
{
	// pthread_mutex_init(&mChunkServersMutex, NULL);

	mReplicationTodoStats = new Counter("Num Replications Todo");
	mChunksWithOneReplicaStats = new Counter("Chunks with one replica");
	mOngoingReplicationStats = new Counter("Num Ongoing Replications");
	mTotalReplicationStats = new Counter("Total Num Replications");
	mFailedReplicationStats = new Counter("Num Failed Replications");
	mStaleChunkCount = new Counter("Num Stale Chunks");
	// how much to be done before we are done
	globals().counterManager.AddCounter(mReplicationTodoStats);
	// how many chunks are "endangered"
	globals().counterManager.AddCounter(mChunksWithOneReplicaStats);
	// how much are we doing right now
	globals().counterManager.AddCounter(mOngoingReplicationStats);
	globals().counterManager.AddCounter(mTotalReplicationStats);
	globals().counterManager.AddCounter(mFailedReplicationStats);
	globals().counterManager.AddCounter(mStaleChunkCount);
}

void
LayoutManager::SetParameters(const Properties& props)
{
	MAX_CONCURRENT_READ_REPLICATIONS_PER_NODE = props.getValue(
		"metaServer.maxConcurrentReadReplicationsPerNode",
		MAX_CONCURRENT_READ_REPLICATIONS_PER_NODE);

	MAX_CONCURRENT_WRITE_REPLICATIONS_PER_NODE = props.getValue(
		"metaServer.maxConcurrentWriteReplicationsPerNode",
		MAX_CONCURRENT_WRITE_REPLICATIONS_PER_NODE);

	NDAYS_PER_FULL_REPLICATION_CHECK = props.getValue(
		"metaServer.ndaysPerFullReplicationCheck",
		NDAYS_PER_FULL_REPLICATION_CHECK);

	mAssignMasterByIpFlag = props.getValue(
		"metaServer.assignMasterByIp",
		mAssignMasterByIpFlag ? 1 : 0) != 0;
	mAllMasters = props.getValue(
		"metaServer.allMasters",
		mAllMasters ? 1 : 0) != 0;
	mLeaseOwnerDownExpireDelay = max(0, props.getValue(
		"metaServer.leaseOwnerDownExpireDelay",
		mLeaseOwnerDownExpireDelay));
	mMaxReservationSize = max(0, props.getValue(
		"metaServer.wappend.maxReservationSize",
		mMaxReservationSize));
	mReservationDecayStep = props.getValue(
		"metaServer.reservationDecayStep",
		mReservationDecayStep);
	mChunkReservationThreshold = props.getValue(
		"metaServer.reservationThreshold",
		mChunkReservationThreshold);
	mReservationOvercommitFactor = max(0., props.getValue(
		"metaServer.wappend.reservationOvercommitFactor",
		mReservationOvercommitFactor));
	/// On a startup, the # of secs to wait before we are open for reads/writes
	mRecoveryIntervalSecs = props.getValue(
		"metaServer.recoveryInterval", mRecoveryIntervalSecs);
	mServerDownReplicationDelay = props.getValue(
		"metaServer.serverDownReplicationDelay",
		 mServerDownReplicationDelay);
	mMaxDownServersHistorySize = props.getValue(
		"metaServer.maxDownServersHistorySize",
		 mMaxDownServersHistorySize);

	mMaxCSRestarting = props.getValue(
		"metaServer.maxCSRestarting",
		 mMaxCSRestarting);
	mMaxCSUptime = props.getValue(
		"metaServer.maxCSUptime",
		 mMaxCSUptime);
	mCSGracefulRestartTimeout = max((int64_t)0, props.getValue(
		"metaServer.CSGracefulRestartTimeout",
		 mCSGracefulRestartTimeout));
	mCSGracefulRestartAppendWithWidTimeout = max((int64_t)0, props.getValue(
		"metaServer.CSGracefulRestartAppendWithWidTimeout",
		mCSGracefulRestartAppendWithWidTimeout));

	const double percent = min(1.0, props.getValue(
		"metaServer.percentLoadedNodesToAvoidForWrites",
		mPercentLoadedNodesToAvoidForWrites));
	if (percent != mPercentLoadedNodesToAvoidForWrites) {
		KFS_LOG_STREAM_INFO <<
			"Setting the percent loaded nodes to avoid for writes to: " <<
			mPercentLoadedNodesToAvoidForWrites <<
		KFS_LOG_EOM;
        }

	SetChunkServersProperties(props);
}

void
LayoutManager::SetChunkServersProperties(const Properties& props)
{
	if (props.empty()) {
		return;
	}
	props.copyWithPrefix("chunkServer.", mChunkServersProps);
	if (mChunkServersProps.empty()) {
		return;
	}
	string display;
	mChunkServersProps.getList(display, "", ";");
	KFS_LOG_STREAM_INFO << "setting properties for " <<
		mChunkServers.size() << " chunk servers: " << display <<
	KFS_LOG_EOM;
	vector<ChunkServerPtr> const chunkServers(mChunkServers);
	for (vector<ChunkServerPtr>::const_iterator i = chunkServers.begin();
			i != chunkServers.end();
			++i) {
		(*i)->SetProperties(mChunkServersProps);
	}
}

CSCounters
LayoutManager::GetChunkServerCounters() const
{
	CSCounters ret;
	int i = 0;
	const size_t srvCount = mChunkServers.size();
	for (vector<ChunkServerPtr>::const_iterator it = mChunkServers.begin();
			it != mChunkServers.end();
			++it, ++i) {
		const Properties& props = (*it)->HeartBeatProperties();
		for (Properties::iterator pi = props.begin();
				pi != props.end();
				++pi) {
			if (pi->first == "CSeq") {
				continue;
			}
			ret[pi->first].resize(srvCount);
			ret[pi->first][i] = pi->second;
		}
		ostringstream os;
		os << (*it)->GetServerLocation().hostname << ":" <<
			(*it)->GetServerLocation().port;
		ret["XMeta-server-location"  ].push_back(os.str());
		ret["XMeta-server-retiring"  ].push_back(
			(*it)->IsRetiring()         ? "1" : "0");
		ret["XMeta-server-restarting"].push_back(
			(*it)->IsRestartScheduled() ? "1" : "0");
		ret["XMeta-server-responsive"].push_back(
			(*it)->IsResponsiveServer() ? "1" : "0");
		os.str(string());
		os << (*it)->GetAvailSpace();
		ret["XMeta-server-space-avail"].push_back(os.str());
		os.str(string());
		os << (TimeNow() - (*it)->TimeSinceLastHeartbeat());
		ret["XMeta-server-heartbeat-time"].push_back(os.str());
	}
	return ret;
}

string
KFS::ToString(const CSCounters& cntrs, string rowDelim)
{
	string ret;
	const char* prefix = "";
	size_t size = 1u << 20;
	for (CSCounters::const_iterator it = cntrs.begin();
			it != cntrs.end(); ++it) {
		ret += prefix + it->first;
		prefix = ",";
		size = min(size, it->second.size());
	}
	ret += rowDelim;
	for (size_t i = 0; i < size; i++) {
		const char* prefix = "";
		for (CSCounters::const_iterator it = cntrs.begin();
				it != cntrs.end(); ++it) {
			ret += prefix + it->second[i];
			prefix = ",";
		}
		ret += rowDelim;
	}
	return ret;
}

class MatchingServer {
	const ServerLocation loc;
public:
	MatchingServer(const ServerLocation &l) : loc(l) { }
	bool operator() (const ChunkServerPtr &s) {
		return s->MatchingServer(loc);
	}
};

//
// Try to match servers by hostname: for write allocation, we'd like to place
// one copy of the block on the same host on which the client is running.
//
class MatchServerByHost {
	string host;
public:
	MatchServerByHost(const string &s) : host(s) { }
	bool operator() (ChunkServerPtr &s) {
		ServerLocation l = s->GetServerLocation();

		return l.hostname == host;
	}
};


/// Add the newly joined server to the list of servers we have.  Also,
/// update our state to include the chunks hosted on this server.
void
LayoutManager::AddNewServer(MetaHello *r)
{
	if (r->server->IsDown()) {
		return;
	}
	const uint64_t allocSpace = r->chunks.size() * CHUNKSIZE;
        ChunkServer& srv = *r->server.get();
        srv.SetServerLocation(r->location);
        srv.SetSpace(r->totalSpace, r->usedSpace, allocSpace);
	srv.SetRack(r->rackId);

	const string srvId = r->location.ToString();
	vector <ChunkServerPtr>::iterator const existing = find_if(
		mChunkServers.begin(), mChunkServers.end(),
		MatchingServer(r->location));
	if (existing != mChunkServers.end()) {
		KFS_LOG_STREAM_DEBUG << "duplicate server: " << srvId <<
			" possible reconnect, taking: " <<
				(const void*)existing->get() << " down " <<
			" replacing with: " << (const void*)&srv <<
		KFS_LOG_EOM;
		ServerDown(existing->get());
		if (srv.IsDown()) {
			return;
		}
        }

	// Add server first, then add chunks, otherwise if/when the server goes
	// down in the process of adding chunks, taking out server from chunk
	// info will not work in ServerDown().
	//
	// prevent the network thread from wandering this list while we change it.
	// XXX: single threaded system now
	// pthread_mutex_lock(&mChunkServersMutex);
	mChunkServers.push_back(r->server);
	if (mAssignMasterByIpFlag) {
		// if the server node # is odd, it is master; else slave
		string ipaddr = r->peerName;
		string::size_type delimPos = ipaddr.rfind(':');
		if (delimPos != string::npos) {
			ipaddr.erase(delimPos);
		}
		delimPos = ipaddr.rfind('.');
		if (delimPos == string::npos) {
			srv.SetCanBeChunkMaster((rand() % 2) != 0);
		} else {
			string nodeNumStr = ipaddr.substr(delimPos + 1);
			int nodeNum = boost::lexical_cast<int>(nodeNumStr);
			srv.SetCanBeChunkMaster((nodeNum % 2) != 0);
		}
	} else {
		srv.SetCanBeChunkMaster(mAllMasters || (mSlavesCount >= mMastersCount));
	}
	if (srv.CanBeChunkMaster()) {
		mMastersCount++;
	} else {
		mSlavesCount++;
	}
	// pthread_mutex_unlock(&mChunkServersMutex);

        vector <chunkId_t> staleChunkIds;
	for (vector<ChunkInfo>::const_iterator it = r->chunks.begin();
			it != r->chunks.end() && ! srv.IsDown();
			++it) {
		const chunkId_t chunkId     = it->chunkId;
		const char*     staleReason = 0;
		CSMapIter const cmi         = mChunkToServerMap.find(chunkId);
		if (cmi == mChunkToServerMap.end()) {
			staleReason = "no chunk mapping exists";
                } else {
			const ChunkPlacementInfo& c      = cmi->second;
			const fid_t               fileId = c.fid;
			vector<ChunkServerPtr>::const_iterator const cs = find_if(
				c.chunkServers.begin(), c.chunkServers.end(),
				MatchingServer(srv.GetServerLocation())
			);
			if (cs != c.chunkServers.end()) {
				KFS_LOG_STREAM_ERROR << srvId <<
					" stable chunk: <" <<
						fileId << "/" <<
						it->allocFileId << "," <<
						chunkId << ">" <<
					" already hosted on: " <<
						(const void*)cs->get() <<
					" new server: " <<
						(const void*)&srv <<
					" has the same location: " <<
						srv.GetServerLocation().ToString() <<
					(cs->get() == &srv ?
						" duplicate chunk entry" :
						" possible stale chunk to"
						" server mapping entry"
					) <<
				KFS_LOG_EOM;
				if (cs->get() == &srv) {
					// Ignore duplicate chunk inventory entries.
					continue;
				}
			}
			vector<MetaChunkInfo *> v;
			metatree.getalloc(fileId, v);
			vector<MetaChunkInfo *>::const_iterator const ci = find_if(
				v.begin(), v.end(), ChunkIdMatcher(chunkId));
			if (ci == v.end()) {
				staleReason = "no chunk in file";
			} else {
				const seq_t chunkVersion = (*ci)->chunkVersion;
				if (chunkVersion > it->chunkVersion) {
					staleReason = "old chunk version";
				} else {
					// This chunk is non-stale.  Verify that there are
					// sufficient copies; if there are too many, nuke some.
					ChangeChunkReplication(chunkId);
					const int res = UpdateChunkToServerMapping(chunkId, &srv);
					assert(res >= 0);
					// get the chunksize for the last chunk of fid
					// stored on this server
					const MetaFattr * const fa = metatree.getFattr(fileId);
					// if ((fa->filesize < 0) || (fa->filesize < (off_t) (fa->chunkcount * CHUNKSIZE))) {
					if (fa && ((fa->filesize < 0) || ((fa->chunkcount > 0) &&
						(fa->filesize <= (off_t) ((fa->chunkcount - 1 ) * CHUNKSIZE))))) {
						// either we don't know the file's size
						// or our view of the file's size does
						// not include the last chunk, then ask
						// the chunkserver for the size.
						MetaChunkInfo *lastChunk = v.back();
						if (lastChunk->chunkId == chunkId) {
							KFS_LOG_STREAM_DEBUG << srvId <<
								" asking size of f=" << fileId <<
								", c=" << chunkId <<
							KFS_LOG_EOM;
							srv.GetChunkSize(fileId, chunkId, "");
						}
					}
					if (chunkVersion < it->chunkVersion) {
						// version #'s differ.  have the chunkserver reset
						// to what the metaserver has.
						// XXX: This is all due to the issue with not logging
						// the version # that the metaserver is issuing.  What is going
						// on here is that,
						//  -- client made a request
						//  -- metaserver bumped the version; notified the chunkservers
						//  -- the chunkservers write out the version bump on disk
						//  -- the metaserver gets ack; writes out the version bump on disk
						//  -- and then notifies the client
						// Now, if the metaserver crashes before it writes out the
						// version bump, it is possible that some chunkservers did the
						// bump, but not the metaserver.  So, fix up.  To avoid other whacky
						// scenarios, we increment the chunk version # by the incarnation stuff
						// to avoid reissuing the same version # multiple times.
						srv.NotifyChunkVersChange(fileId, chunkId, chunkVersion);

					}
					if (fa && fa->numReplicas <= (int)c.chunkServers.size() && ! srv.IsDown()) {
						CancelPendingMakeStable(fileId, chunkId);
					}
				}
			}
                }
		if (staleReason) {
		        KFS_LOG_STREAM_INFO << srvId <<
				" stable chunk: <" <<
				it->allocFileId << "," << chunkId << ">"
				" " << staleReason <<
				" => stale" <<
			KFS_LOG_EOM;
                        staleChunkIds.push_back(it->chunkId);
			mStaleChunkCount->Update(1);
		}
        }

	for (int i = 0; i < 2; i++) {
		const vector<ChunkInfo>& chunks = i == 0 ?
			r->notStableAppendChunks : r->notStableChunks;
		for (vector<ChunkInfo>::const_iterator it = chunks.begin();
				it != chunks.end() && ! srv.IsDown();
				++it) {
			const char* const staleReason = AddNotStableChunk(
				r->server,
				it->allocFileId,
				it->chunkId,
				it->chunkVersion,
				i == 0,
				srvId
			);
			KFS_LOG_STREAM_INFO << srvId <<
				" not stable chunk:" <<
				(i == 0 ? " append" : "") <<
				" <" <<
					it->allocFileId << "," << it->chunkId << ">"
				" " << (staleReason ? staleReason : "") <<
				(staleReason ? " => stale" : "added back") <<
			KFS_LOG_EOM;
			if (staleReason) {
                        	staleChunkIds.push_back(it->chunkId);
				mStaleChunkCount->Update(1);
			}
		}
	}
        if (! staleChunkIds.empty() && ! srv.IsDown()) {
                srv.NotifyStaleChunks(staleChunkIds);
        }
	if (! mChunkServersProps.empty() && ! srv.IsDown()) {
		srv.SetProperties(mChunkServersProps);
	}
	// All ops are queued at this point, make sure that the server is still up.
	if (srv.IsDown()) {
		KFS_LOG_STREAM_ERROR << srvId <<
			": went down in the process of adding it" <<
		KFS_LOG_EOM;
		return;
	}

	vector<RackInfo>::iterator const rackIter = find_if(
		mRacks.begin(), mRacks.end(), RackMatcher(r->rackId));
	if (rackIter != mRacks.end()) {
		rackIter->addServer(r->server);
	} else {
		RackInfo ri(r->rackId);
		ri.addServer(r->server);
		mRacks.push_back(ri);
	}

	// Update the list since a new server is in
	CheckHibernatingServersStatus();

	const char* msg = "added";
	if (IsChunkServerRestartAllowed() &&
			mCSToRestartCount < mMaxCSRestarting) {
		if (srv.Uptime() >= mMaxCSUptime &&
			! srv.IsDown() &&
			! srv.IsRestartScheduled()) {
			mCSToRestartCount++;
			if (srv.GetNumChunkWrites() <= 0 &&
					srv.GetNumAppendsWithWid() <= 0) {
				srv.Restart();
				msg = "restarted";
			} else {
				srv.ScheduleRestart(
					mCSGracefulRestartTimeout,
					mCSGracefulRestartAppendWithWidTimeout);
			}
		} else {
			ScheduleChunkServersRestart();
		}
	}
	KFS_LOG_STREAM_INFO <<
		msg << " chunk server: " << r->peerName << "/" << srv.ServerID() <<
		(srv.CanBeChunkMaster() ? " master" : " slave") <<
		" chunks: stable: " << r->chunks.size() <<
		" not stable: "     << r->notStableChunks.size() <<
		" append: "         << r->notStableAppendChunks.size() <<
		" +wid: "           << r->numAppendsWithWid <<
		" writes: "         << srv.GetNumChunkWrites() <<
		" +wid: "           << srv.GetNumAppendsWithWid() <<
		" masters: "        << mMastersCount <<
		" slaves: "         << mSlavesCount <<
		" total: "          << mChunkServers.size() <<
		" uptime: "         << srv.Uptime() <<
		" restart: "        << srv.IsRestartScheduled() <<
	KFS_LOG_EOM;
}

const char*
LayoutManager::AddNotStableChunk(
	ChunkServerPtr server,
	fid_t          allocFileId,
	chunkId_t      chunkId,
	seq_t          chunkVersion,
	bool           appendFlag,
	const string&  logPrefix)
{
	CSMapIter const cmi = mChunkToServerMap.find(chunkId);
	if (cmi == mChunkToServerMap.end()) {
		return "no chunk mapping exists";
	}
	ChunkPlacementInfo& pinfo  = cmi->second;
	const fid_t         fileId = pinfo.fid;
	vector<ChunkServerPtr>::const_iterator const cs = find_if(
		pinfo.chunkServers.begin(), pinfo.chunkServers.end(),
		MatchingServer(server->GetServerLocation())
	);
	if (cs != pinfo.chunkServers.end()) {
		KFS_LOG_STREAM_ERROR << logPrefix <<
			" not stable chunk:" <<
				(appendFlag ? " append " : "") <<
			" <"                   << fileId <<
			"/"                    << allocFileId <<
			","                    << chunkId << ">" <<
			" already hosted on: " << (const void*)cs->get() <<
			" new server: "        << (const void*)server.get() <<
			(cs->get() == server.get() ?
			" duplicate chunk entry" :
			" possible stale chunk to server mapping entry") <<
		KFS_LOG_EOM;
		return 0;
	}
	const char* staleReason = 0;
	if (AddServerToMakeStable(cmi->second, server,
			chunkId, chunkVersion, staleReason) || staleReason) {
		return staleReason;
	}
	// At this point it is known that no make chunk stable is in progress:
	// AddServerToMakeStable() invoked already.
	// Delete the replica if sufficient number of replicas already exists.
	const MetaFattr * const fa = metatree.getFattr(fileId);
	if (fa && fa->numReplicas <= (int)pinfo.chunkServers.size()) {
		CancelPendingMakeStable(fileId, chunkId);
		return "sufficient number of replicas exists";
	}
	// See if it is possible to add the chunk back before "[Begin] Make
	// Chunk Stable" ([B]MCS) starts. Expired lease cleanup lags behind. If
	// expired lease exists, and [B]MCS is not in progress, then add the
	// chunk back.
	vector<LeaseInfo>::const_iterator const li = find_if(
		pinfo.chunkLeases.begin(), pinfo.chunkLeases.end(),
		ptr_fun(&LeaseInfo::IsWriteLease)
	);
	const bool leaseExistsFlag = li != pinfo.chunkLeases.end();
	if (leaseExistsFlag && appendFlag != li->appendFlag) {
		return (appendFlag ? "not append lease" : "append lease");
	}
	if (! leaseExistsFlag && appendFlag) {
		PendingMakeStableMap::iterator const msi =
				mPendingMakeStable.find(chunkId);
		if (msi == mPendingMakeStable.end()) {
			return "no make stable info";
		}
		if (chunkVersion != msi->second.mChunkVersion) {
			return "pending make stable chunk version mismatch";
		}
		const bool beginMakeStableFlag = msi->second.mSize < 0;
		if (beginMakeStableFlag) {
			pinfo.chunkServers.push_back(server);
			if (InRecoveryPeriod() ||
					! mPendingBeginMakeStable.empty()) {
				// Allow chunk servers to connect back.
				mPendingBeginMakeStable.insert(chunkId);
				return 0;
			}
			MakeChunkStableInit(
				fileId, chunkId, pinfo.chunkOffsetIndex * CHUNKSIZE,
				"", // metatree.getPathname(fileId),
				pinfo.chunkServers, beginMakeStableFlag,
				-1, false, 0
			);
			return 0;
		}
		const bool kPendingAddFlag = true;
		server->MakeChunkStable(
			fileId, chunkId, chunkVersion,
			msi->second.mSize,
			msi->second.mHasChecksum,
			msi->second.mChecksum,
			kPendingAddFlag
		);
		return 0;
	}
	if (! leaseExistsFlag && ! appendFlag &&
			mPendingMakeStable.find(chunkId) !=
			mPendingMakeStable.end()) {
		return "chunk was open for append";
	}
	seq_t curChunkVersion = -1;
	if (metatree.getChunkVersion(fileId, chunkId, &curChunkVersion) != 0) {
		return "no such chunk";
	}
	if (chunkVersion < curChunkVersion) {
		return "chunk version mismatch";
	}
	if (curChunkVersion != chunkVersion) {
		server->NotifyChunkVersChange(fileId, chunkId, curChunkVersion);
		if (server->IsDown()) {
			return 0; // Went down while sending notification.
		}
	}
	// Adding server back can change replication chain (order) -- invalidate
	// record appender cache to prevent futher appends to this chunk.
	if (leaseExistsFlag) {
		if (appendFlag) {
			mARAChunkCache.Invalidate(fileId, server->GetRack(), chunkId);
		}
		pinfo.chunkServers.push_back(server);
	} else if (! appendFlag) {
		const bool kPendingAddFlag = true;
		server->MakeChunkStable(
			fileId, chunkId, curChunkVersion,
			-1, false, 0, kPendingAddFlag
		);
	}
	return 0;
}

void
LayoutManager::ProcessPendingBeginMakeStable()
{
	if (mPendingBeginMakeStable.empty()) {
		return;
	}
	ChunkIdSet pendingBeginMakeStable;
	pendingBeginMakeStable.swap(mPendingBeginMakeStable);
	// If there are too many pending entries, do not try to get path name,
	// since getPathname has exponential complexity.
	const bool getPathNameFlag = mPendingBeginMakeStable.size() <= 4;
	const bool kBeginMakeStableFlag = true;
	for (ChunkIdSet::const_iterator it = pendingBeginMakeStable.begin();
			it != pendingBeginMakeStable.end();
			++it) {
		chunkId_t const chunkId = *it;
		CSMapIter const cmi     = mChunkToServerMap.find(chunkId);
		if (cmi == mChunkToServerMap.end()) {
			continue;
		}
		ChunkPlacementInfo& pinfo  = cmi->second;
		const fid_t         fileId = pinfo.fid;
		MakeChunkStableInit(
			fileId, chunkId, pinfo.chunkOffsetIndex * CHUNKSIZE,
			getPathNameFlag ?
				metatree.getPathname(fileId) : string(),
			pinfo.chunkServers, kBeginMakeStableFlag,
			-1, false, 0
		);
	}
}

struct ExpireLeaseIfOwner
{
	const ChunkServer * const target;
	const time_t              expire;
	ExpireLeaseIfOwner(const ChunkServer *t)
		: target(t), expire(TimeNow() - 1)
	{}
	void operator () (LeaseInfo& li) {
		if (li.chunkServer.get() == target) {
			li.expires = expire;
			li.ownerWasDownFlag = li.ownerWasDownFlag ||
				(target && target->IsDown());
		}
	}
};

class MapPurger {
	ReplicationCandidates&   crset;
        ARAChunkCache&           araChunkCache;
	const ChunkServer* const target;
public:
	MapPurger(ReplicationCandidates &c, ARAChunkCache& ac, const ChunkServer *t)
		: crset(c), araChunkCache(ac), target(t)
		{}
	void operator () (CSMap::value_type& p) {
		ChunkPlacementInfo& c = p.second;
		//
		// only chunks hosted on the target need to be checked for
		// replication level
		//
		vector <ChunkServerPtr>::iterator const i = remove_if(
			c.chunkServers.begin(), c.chunkServers.end(),
			ChunkServerMatcher(target)
		);
		if (i == c.chunkServers.end()) {
			return;
		}
		for_each(c.chunkLeases.begin(), c.chunkLeases.end(),
			ExpireLeaseIfOwner(target));
		EraseReallocIfNeeded(c.chunkServers, i, c.chunkServers.end());
                // Chunk replication chain has changed: invalidate write append
                // cache entry, if any. It is an error to attempt to append to
                // after replication chain has changed. New chunk has to be
                // allocated.
                // Another way to handle this is to expire lease here, even in
                // the case if slave went offline.
                // For now let chunk master decide what to do, if it is
                // transient communication outage, the slave might come back,
                // and this will not affect other appenders if master can still
                // talk to it.
		araChunkCache.Invalidate(c.fid, p.first);
		// we need to check the replication level of this chunk
		crset.insert(p.first);
	}
};

class MapRetirer {
	ReplicationCandidates &crset;
	ChunkServer *retiringServer;
public:
	MapRetirer(ReplicationCandidates &c, ChunkServer *t):
		crset(c), retiringServer(t) { }
	void operator () (const CSMap::value_type& p) {
		const ChunkPlacementInfo& c = p.second;
        	vector <ChunkServerPtr>::const_iterator i;

		i = find_if(c.chunkServers.begin(), c.chunkServers.end(),
			ChunkServerMatcher(retiringServer));

		if (i == c.chunkServers.end())
			return;

		// we need to check the replication level of this chunk
		crset.insert(p.first);
		retiringServer->EvacuateChunk(p.first);
	}
};

class MapDumper {
	ofstream &ofs;
public:
	MapDumper(ofstream &o) : ofs(o) { }
	void operator () (const CSMap::value_type& p) {
		chunkId_t cid = p.first;
		const ChunkPlacementInfo& c = p.second;

		ofs << cid << ' ' << c.fid << ' ' << c.chunkServers.size() << ' ';
		for (uint32_t i = 0; i < c.chunkServers.size(); i++) {
			ofs << c.chunkServers[i]->ServerID() << ' '
				<< c.chunkServers[i]->GetRack() << ' ';
		}
		ofs << endl;
	}
};

class MapDumperStream {
	ostringstream &ofs;
public:
	MapDumperStream(ostringstream &o) : ofs(o) { }
	void operator () (const CSMap::value_type& p) {
		chunkId_t cid = p.first;
		const ChunkPlacementInfo& c = p.second;

		ofs << cid << ' ' << c.fid << ' ' << c.chunkServers.size() << ' ';
		for (uint32_t i = 0; i < c.chunkServers.size(); i++) {
			ofs << c.chunkServers[i]->ServerID() << ' '
				<< c.chunkServers[i]->GetRack() << ' ';
		}
		ofs << endl;
	}
};

class PrintChunkServerInfo {
	ofstream &ofs;
public:
	PrintChunkServerInfo(ofstream &o) : ofs(o) { }
	void operator() (ChunkServerPtr &c) {
		ofs << c->ServerID() << ' ' << c->GetRack() << ' '
			<< c->GetTotalSpace() << ' ' << c->GetUsedSpace() << endl;
	}
};

//
// Dump out the chunk block map to a file.  The output can be used in emulation
// modes where we setup the block map and experiment.
//
void
LayoutManager::DumpChunkToServerMap(const string &dirToUse)
{
	ofstream ofs;
	pid_t pid = getpid();

	//
	// to make offline rebalancing/re-replication easier, dump out where the
	// servers are and how much space each has.
	//
	string fn = dirToUse + "/network.def";
	ofs.open(fn.c_str());
	for_each(mChunkServers.begin(), mChunkServers.end(),
		PrintChunkServerInfo(ofs));
	ofs.flush();
	ofs.close();

	fn = dirToUse + "/chunkmap.txt." + boost::lexical_cast<string>(pid);
	ofs.open(fn.c_str());

	for_each(mChunkToServerMap.begin(), mChunkToServerMap.end(),
		MapDumper(ofs));
	ofs.flush();
	ofs.close();
}

void
LayoutManager::DumpChunkReplicationCandidates(ostringstream &os)
{
	for (ReplicationCandidates::const_iterator citer = mChunkReplicationCandidates.begin();
		citer != mChunkReplicationCandidates.end(); ++citer) {
		const chunkId_t chunkId = *citer;
		os << chunkId << ' ';
	}
}

void
LayoutManager::Fsck(ostringstream &os)
{
	ostringstream lost;
	ostringstream endangered;
	int lostBlocks = 0, endangeredBlocks = 0;
	for (CSMapConstIter citer = mChunkToServerMap.begin(); 	
			citer != mChunkToServerMap.end(); ++citer) {
		if (citer->second.chunkServers.size() == 0) {
			lost << citer->second.fid << ' ';
			lostBlocks++;
		}
		if (citer->second.chunkServers.size() == 1) {
			endangered << citer->second.fid << ' ';
			endangeredBlocks++;
		}
	}
	os << "Num endangered blocks: " << endangeredBlocks << endl;
	os << "Num lost blocks: " << lostBlocks << endl;
	os << "Endangered files: " << endangered.str() << endl;
	os << "Lost files: " << lost.str() << endl;
}

// Dump chunk block map to response stream
void
LayoutManager::DumpChunkToServerMap(ostringstream &os)
{
	for_each(mChunkToServerMap.begin(), mChunkToServerMap.end(),
			 MapDumperStream(os));
}

// Give preference to chunks at replication level of 1
void
LayoutManager::RebuildPriorityReplicationList()
{
	mPriorityChunkReplicationCandidates.clear();
	for (ReplicationCandidates::const_iterator citer = mChunkReplicationCandidates.begin();
		citer != mChunkReplicationCandidates.end(); ++citer) {
		const chunkId_t chunkId = *citer;
		CSMapIter iter = mChunkToServerMap.find(chunkId);

		if (iter == mChunkToServerMap.end())
			continue;
		if (iter->second.chunkServers.size() == 1)
			mPriorityChunkReplicationCandidates.insert(chunkId);
	}
}

void
LayoutManager::ServerDown(ChunkServer *server)
{
        vector <ChunkServerPtr>::iterator i =
		find_if(mChunkServers.begin(), mChunkServers.end(),
			ChunkServerMatcher(server));

	if (i == mChunkServers.end())
		return;

	if (! server->IsDown()) {
		server->ForceDown();
	}
	vector<RackInfo>::iterator rackIter;

	rackIter = find_if(mRacks.begin(), mRacks.end(), RackMatcher(server->GetRack()));
	if (rackIter != mRacks.end()) {
		rackIter->removeServer(server);
		if (rackIter->getServers().size() == 0) {
			// the entire rack of servers is gone
			// so, take the rack out
			KFS_LOG_STREAM_INFO << "All servers in rack " <<
				server->GetRack() << " are down; taking out the rack" <<
			KFS_LOG_EOM;
			mRacks.erase(rackIter);
		}
	}

	/// Fail all the ops that were sent/waiting for response from
	/// this server.
        const bool canBeMaster = server->CanBeChunkMaster();
	server->FailPendingOps();

	// check if this server was sent to hibernation
	bool isHibernating = false;
	for (uint32_t j = 0; j < mHibernatingServers.size(); j++) {
		if (mHibernatingServers[j].location == server->GetServerLocation()) {
			// record all the blocks that need to be checked for
			// re-replication later
			MapPurger purge(mHibernatingServers[j].blocks, mARAChunkCache, server);
			for_each(mChunkToServerMap.begin(), mChunkToServerMap.end(), purge);
			isHibernating = true;
			break;
		}
	}

	if (!isHibernating) {
		const int kMinReplicationDelay = 15;
		const int replicationDelay =
			mServerDownReplicationDelay - server->TimeSinceLastHeartbeat();
		if (replicationDelay > kMinReplicationDelay) {
			// Delay replication in case if the server reconnects back.
			HibernatingServerInfo_t hsi;
			hsi.location     = server->GetServerLocation();
			hsi.sleepEndTime = TimeNow() + replicationDelay;
			mHibernatingServers.push_back(hsi);
			MapPurger purge(mHibernatingServers.back().blocks, mARAChunkCache, server);
			for_each(mChunkToServerMap.begin(), mChunkToServerMap.end(), purge);
		} else {
			MapPurger purge(mChunkReplicationCandidates, mARAChunkCache, server);
			for_each(mChunkToServerMap.begin(), mChunkToServerMap.end(), purge);
		}
		RebuildPriorityReplicationList();
	}

	// for reporting purposes, record when it went down
	const time_t now = TimeNow();
	const ServerLocation loc = server->GetServerLocation();

	string reason = server->DownReason();
	if (isHibernating) {
		reason = "Hibernated";
	} else if (server->IsRetiring()) {
		reason = "Retired";
	} else if (reason.empty()) {
		reason = "Unreachable";
	}

	ostringstream os;
	os <<
		"s="        << loc.hostname <<
		", p="      << loc.port <<
		", down="   << timeToStr(now) <<
		", reason=" << reason <<
	"\t";
	while (mDownServers.size() >= mMaxDownServersHistorySize) {
		mDownServers.pop_front();
	}
	mDownServers.push_back(os.str());

        if (canBeMaster) {
            if (mMastersCount > 0) {
                mMastersCount--;
            }
        } else if (mSlavesCount > 0) {
            mSlavesCount--;
        }
	if (server->IsRestartScheduled()) {
		if (mCSToRestartCount > 0) {
			mCSToRestartCount--;
		}
		if (mMastersToRestartCount > 0 && server->CanBeChunkMaster()) {
			mMastersToRestartCount--;
		}
	}
	mChunkServers.erase(i);
	if (! mAssignMasterByIpFlag &&
			mMastersCount == 0 && ! mChunkServers.empty()) {
		assert(mSlavesCount > 0 &&
			! mChunkServers.front()->CanBeChunkMaster());
		mSlavesCount--;
		mMastersCount++;
		mChunkServers.front()->SetCanBeChunkMaster(true);
	}
	
}

int
LayoutManager::RetireServer(const ServerLocation &loc, int downtime)
{
	ChunkServerPtr retiringServer;
	vector <ChunkServerPtr>::iterator i;

	i = find_if(mChunkServers.begin(), mChunkServers.end(), MatchingServer(loc));
	if (i == mChunkServers.end())
		return -1;

	retiringServer = *i;

	retiringServer->SetRetiring();
	if (downtime > 0) {
		HibernatingServerInfo_t hsi;

		hsi.location = retiringServer->GetServerLocation();
		hsi.sleepEndTime = TimeNow() + downtime;
		mHibernatingServers.push_back(hsi);

		retiringServer->Retire();

		return 0;
	}

	MapRetirer retirer(mChunkReplicationCandidates, retiringServer.get());
	for_each(mChunkToServerMap.begin(), mChunkToServerMap.end(), retirer);

	return 0;
}

/*
 * Chunk-placement algorithm is rack-aware. At a high-level, the algorithm tries
 * to keep at most 1 copy of a chunk per rack:
 *  - Sort the racks based on space
 *  - From each rack, find one or more candidate servers
 * This approach will work when we are placing a chunk for the first time.
 * Whenever we need to copy/migrate a chunk, the intent is to keep the chunk
 * migration traffic within the same rack.  Specifically:
 * 1. Need to re-replicate a chunk because a node is dead
 *      -- here, we exclude the 2 racks on which the chunk is already placed and
 *      try to find a new spot.  By the time we get to finding a spot, we have
 *      removed the info about where the chunk was.  So, put it on a unique rack
 * 2. Need to re-replicate a chunk because a node is retiring
 *	-- here, since we know which node is retiring and the rack it is on, we
 *	can keep the new spot to be on the same rack
 * 3. Need to re-replicate a chunk because we are re-balancing amongst nodes
 *	-- we move data between nodes in the same rack
 *	-- if we a new rack got added, we move data between racks; here we need
 *	   to be a bit careful: in one iteration, we move data from one rack to
 *	   a newly added rack; in the next iteration, we could move another copy
 *	   of the chunk to the same rack...fill this in.
 * If we can't place the 3 copies on 3 different racks, we put the moved data
 * whereever we can find a spot.  (At a later time, we'll need the fix code: if
 * a new rack becomes available, we move the 3rd copy to the new rack and get
 * the copies on different racks).
 *
 */


/*
 * Return an ordered list of candidate racks
 */
void
LayoutManager::FindCandidateRacks(vector<int> &result)
{
	set<int> dummy;

	FindCandidateRacks(result, dummy);
}

void
LayoutManager::FindCandidateRacks(vector<int> &result, const set<int> &excludes)
{
	set<int>::const_iterator iter;
	int32_t numRacksToChoose = mRacks.size() - excludes.size();
	int32_t count = 0;
	int rackId, nodeId;
	set<int> chosenRacks;

	result.clear();

	if (numRacksToChoose == 0)
		return;

	for (uint32_t i = 0; i < mRacks.size(); i++) {
		// paranoia: each candidate rack better have at least one node
		if (!excludes.empty()) {
			iter = excludes.find(mRacks[i].id());
			if (iter != excludes.end())
				continue;
		}
		if (mRacks[i].getServers().size() == 0) {
			// there are no nodes in this rack
			numRacksToChoose--;
		}
	}

	if (numRacksToChoose == 0)
		return;

	count = 0;
	// choose a rack proportional to the # of nodes that rack
	while (count < numRacksToChoose) {
		nodeId = rand() % mChunkServers.size();
		rackId = mChunkServers[nodeId]->GetRack();
		if (!excludes.empty()) {
			iter = excludes.find(rackId);
			if (iter != excludes.end())
				// rack is in the exclude list
				continue;
		}
		iter = chosenRacks.find(rackId);
		if (iter != chosenRacks.end()) {
			// we have chosen this rack already
			continue;
		}
		chosenRacks.insert(rackId);
		result.push_back(rackId);
		count++;
	}
}

struct ServerSpace {
	uint32_t serverIdx;
	uint32_t loadEstimate;
	uint64_t availSpace;
	uint64_t usedSpace;

	// sort in decreasing order: Prefer the server with more free
	// space, or in the case of a tie, the one with less used space.
	// also, prefer servers that are lightly loaded

	bool operator < (const ServerSpace &other) const {

		if ((loadEstimate > CONCURRENT_WRITES_PER_NODE_WATERMARK) &&
			(loadEstimate != other.loadEstimate)) {
			// prefer server that is "lightly" loaded
			return loadEstimate < other.loadEstimate;
		}

		if (availSpace != other.availSpace)
			return availSpace > other.availSpace;
		else
			return usedSpace < other.usedSpace;
	}
};

struct ServerSpaceUtil {
	uint32_t serverIdx;
	float utilization;

	// sort in increasing order of space utilization
	bool operator < (const ServerSpaceUtil &other) const {
		return utilization < other.utilization;
	}
};

void
LayoutManager::FindCandidateServers(vector<ChunkServerPtr> &result,
				const vector<ChunkServerPtr> &excludes,
				int rackId)
{
	if (mChunkServers.size() < 1)
		return;

	if (rackId > 0) {
		vector<RackInfo>::iterator rackIter;

		rackIter = find_if(mRacks.begin(), mRacks.end(), RackMatcher(rackId));
		if (rackIter != mRacks.end()) {
			FindCandidateServers(result, rackIter->getServers(), excludes, rackId);
			return;
		}
	}
	FindCandidateServers(result, mChunkServers, excludes, rackId);
}

static bool
IsCandidateServer(const ChunkServerPtr &c)
{
	if ((c->GetAvailSpace() < ((int64_t) CHUNKSIZE)) || (!c->IsResponsiveServer())
		|| (c->IsRetiring()) || (c->IsRestartScheduled())) {
		// one of: no space, non-responsive, retiring...we leave
		// the server alone
		return false;
	}
	return true;
}

#if 0
static void
SortServersByCPULoad(vector<ChunkServerPtr> &servers)
{
	vector<ServerSpaceUtil> ss;
	vector<ChunkServerPtr> temp;

	ss.resize(servers.size());
	temp.resize(servers.size());

	// XXX: hack for now. utilization thinks it is space, but we are
	// sticking in CPU load
	for (vector<ChunkServerPtr>::size_type i = 0; i < servers.size(); i++) {
		ss[i].serverIdx = i;
		ss[i].utilization = servers[i]->GetCPULoadAvg();
		temp[i] = servers[i];
	}

	sort(ss.begin(), ss.end());
	for (vector<ChunkServerPtr>::size_type i = 0; i < servers.size(); i++) {
		servers[i] = temp[ss[i].serverIdx];
	}
}
#endif

void
LayoutManager::FindCandidateServers(vector<ChunkServerPtr> &result,
				const vector<ChunkServerPtr> &sources,
				const vector<ChunkServerPtr> &excludes,
				int rackId)
{
	if (sources.size() < 1)
		return;

	vector<ChunkServerPtr> candidates;
	vector<ChunkServerPtr>::size_type i;
	vector<ChunkServerPtr>::const_iterator iter;

	for (i = 0; i < sources.size(); i++) {
		ChunkServerPtr c = sources[i];

		if ((rackId >= 0) && (c->GetRack() != rackId))
			continue;

		if (!IsCandidateServer(c))
			continue;
		if (excludes.size() > 0) {
			iter = find(excludes.begin(), excludes.end(), c);
			if (iter != excludes.end()) {
				continue;
			}
		}
		// XXX: temporary measure: take only under-utilized servers
		// we need to move a model where we give preference to
		// under-utilized servers
		if (c->GetSpaceUtilization() > MAX_SERVER_SPACE_UTIL_THRESHOLD)
			continue;
		candidates.push_back(c);
	}
	if (candidates.size() == 0)
		return;
#if 0
	// do this on a heartbeat; not here
	SortServersByCPULoad(candidates);
	// drop the nodes which are in the bottom N%
	int32_t maxNodesToUse = candidates.size();
	maxNodesToUse -= int32_t(maxNodesToUse * mPercentLoadedNodesToAvoidForWrites);
	if ((maxNodesToUse > 0) && (maxNodesToUse < (int) candidates.size())) {
		candidates.resize(maxNodesToUse);
	}
#endif
	random_shuffle(candidates.begin(), candidates.end());
	for (i = 0; i < candidates.size(); i++) {
		result.push_back(candidates[i]);
	}
}

#if 0
void
LayoutManager::FindCandidateServers(vector<ChunkServerPtr> &result,
				const vector<ChunkServerPtr> &sources,
				const vector<ChunkServerPtr> &excludes,
				int rackId)
{
	if (sources.size() < 1)
		return;

	vector<ServerSpace> ss;
	vector<ChunkServerPtr>::size_type i, j;
	vector<ChunkServerPtr>::const_iterator iter;

	ss.resize(sources.size());

	for (i = 0, j = 0; i < sources.size(); i++) {
		ChunkServerPtr c = sources[i];

		if ((rackId >= 0) && (c->GetRack() != rackId))
			continue;
		if ((c->GetAvailSpace() < ((uint64_t) CHUNKSIZE)) || (!c->IsResponsiveServer())
			|| (c->IsRetiring())) {
			// one of: no space, non-responsive, retiring...we leave
			// the server alone
			continue;
		}
		if (excludes.size() > 0) {
			iter = find(excludes.begin(), excludes.end(), c);
			if (iter != excludes.end()) {
				continue;
			}
		}
		ss[j].serverIdx = i;
		ss[j].availSpace = c->GetAvailSpace();
		ss[j].usedSpace = c->GetUsedSpace();
		ss[j].loadEstimate = c->GetNumChunkWrites();
		j++;
	}

	if (j == 0)
		return;

	ss.resize(j);

	sort(ss.begin(), ss.end());

	result.reserve(ss.size());
	for (i = 0; i < ss.size(); ++i) {
		result.push_back(sources[ss[i].serverIdx]);
	}
}
#endif

void
LayoutManager::SortServersByUtilization(vector<ChunkServerPtr> &servers)
{
	vector<ServerSpaceUtil> ss;
	vector<ChunkServerPtr> temp;

	ss.resize(servers.size());
	temp.resize(servers.size());

	for (vector<ChunkServerPtr>::size_type i = 0; i < servers.size(); i++) {
		ss[i].serverIdx = i;
		ss[i].utilization = servers[i]->GetSpaceUtilization();
		temp[i] = servers[i];
	}

	sort(ss.begin(), ss.end());
	for (vector<ChunkServerPtr>::size_type i = 0; i < servers.size(); i++) {
		servers[i] = temp[ss[i].serverIdx];
	}
}


///
/// The algorithm for picking a set of servers to hold a chunk is: (1) pick
/// the server with the most amount of free space, and (2) to break
/// ties, pick the one with the least amount of used space.  This
/// policy has the effect of doing round-robin allocations.  The
/// allocated space is something that we track.  Note: We rely on the
/// chunk servers to tell us how much space is used up on the server.
/// Since servers can respond at different rates, doing allocations
/// based on allocated space ensures equitable distribution;
/// otherwise, if we were to do allocations based on the amount of
/// used space, then a slow responding server will get pummelled with
/// lots of chunks (i.e., used space will be updated on the meta
/// server at a slow rate, causing the meta server to think that the
/// chunk server has lot of space available).
///
int
LayoutManager::AllocateChunk(MetaAllocate *r)
{
	vector<ChunkServerPtr>::size_type i;
	vector<int> racks;

	r->servers.clear();
	if (r->numReplicas == 0) {
		// huh? allocate a chunk with 0 replicas???
            KFS_LOG_STREAM_DEBUG << "allocate chunk reaplicas: " << r->numReplicas <<
                " request: " << r->Show() <<
            KFS_LOG_EOM;
	    r->statusMsg = "0 replicas";
            return -EINVAL;
	}

	FindCandidateRacks(racks);
        const size_t numRacks = racks.size();
	if (numRacks <= 0) {
            KFS_LOG_STREAM_INFO << "allocate chunk no racks: " << numRacks <<
                " request: " << r->Show() <<
            KFS_LOG_EOM;
	    r->statusMsg = "no racks";
            return -ENOSPC;
        }

	r->servers.reserve(r->numReplicas);

	uint32_t numServersPerRack = r->numReplicas / numRacks;
	if (r->numReplicas % numRacks)
		numServersPerRack++;

	int appendChunkAffinityCount = 0;
	if (r->appendChunk && (r->clientRack != -1)) {
            // make the client rack the first one in the list
            int32_t idx = -1;
            for (uint32_t i = 0; i < racks.size(); i++) {
                if (racks[i] == r->clientRack) {
                    idx = i;
                    break;
                }
            }
            if (idx != -1) {
                // swap
                racks[idx] = racks[0];
                racks[0] = r->clientRack;
                // take both replicas from that rack
                numServersPerRack = r->numReplicas;
            }
	}

	// for non-record append case, take the server local to the machine on
	// which the client is on make that the master; this avoids a network transfer.
	// for the record append case, to avoid deadlocks when writing out large
	// records, we are doing hierarchical allocation: a chunkserver that is
	// a chunk master is never made a slave.
	// XXX: Avoid local host optimization for record append case; since multiple writers
	// will be writing to the chunk, one machine can get inundated with chunks
	ChunkServerPtr localserver;
	int replicaCnt = 0;
	vector <ChunkServerPtr>::iterator const li = find_if(
            mChunkServers.begin(), mChunkServers.end(), MatchServerByHost(r->clientHost));
	/*
	if ((li != mChunkServers.end()) && (IsCandidateServer(*li)) &&
			(! r->appendChunk || (*li)->CanBeChunkMaster())) {
		localserver = *li;
		replicaCnt++;
	}
	if (r->appendChunk || localserver) {
		r->servers.push_back(localserver);
	}
	*/
	bool isMasterChosen = false;
	if ((li != mChunkServers.end()) && (IsCandidateServer(*li)) &&
			(! r->appendChunk )) {
		localserver = *li;
		replicaCnt++;
		r->servers.push_back(localserver);
		isMasterChosen = true;
	}
	else if (r->appendChunk) {
		ARAChunkCache::Entry* const entry = mARAChunkCache.Get(r->fid, r->clientRack);
		if (entry) {
			// Look for affinity and see if we can put the block there
			appendChunkAffinityCount = entry->appendChunkAffinityCount;
			// disable it for now.
			if ((appendChunkAffinityCount > 0) && (appendChunkAffinityCount < 1)) {
       				CSMapConstIter const iter = mChunkToServerMap.find(entry->chunkId);
       				if (iter != mChunkToServerMap.end()) {
					const ChunkPlacementInfo& v = iter->second;
					if (! v.chunkServers.empty()) {
						// put the chunk here
						localserver = v.chunkServers[0];
						replicaCnt++;
						isMasterChosen = true;
					}
				}
			}
		} 
		// Create one entry at the head where we stash master
		r->servers.push_back(localserver);
	}
	int    mastersSkipped = 0;
	int    slavesSkipped  = 0;
	size_t numCandidates = 0;
	for (uint32_t idx = 0;
			replicaCnt < r->numReplicas && idx < numRacks;
			idx++) {
		vector<ChunkServerPtr> candidates, dummy;
		FindCandidateServers(candidates, dummy, racks[idx]);
		numCandidates += candidates.size();
		// take as many as we can from this rack
		uint32_t n = (localserver && (racks[idx] == localserver->GetRack())) ? 1 : 0;
		for (vector<ChunkServerPtr>::const_iterator i = candidates.begin();
				i != candidates.end() && n < numServersPerRack &&
				replicaCnt < r->numReplicas;
                                ++i) {
			const ChunkServerPtr& cs = *i;
			if (r->appendChunk) {
				// for record appends, to avoid deadlocks for
				// buffer allocation during atomic record
				// appends, use hierarchical chunkserver
				// selection
				if (cs->CanBeChunkMaster()) {
					// if (r->servers.front()) {
					if (isMasterChosen) {
						mastersSkipped++;
						continue;
					}
					r->servers.front() = cs;
					isMasterChosen = true;
				} else {
					if (r->servers.size() >= (size_t)r->numReplicas) {
						slavesSkipped++;
						continue;
					}
					r->servers.push_back(cs);
				}
			} else {
				if (cs == localserver) {
					continue;
				}
				r->servers.push_back(cs);
				isMasterChosen = true;
			}
			n++;
			replicaCnt++;
		}
	}
	if (r->servers.empty() || (!isMasterChosen)) {
		int dontLikeCount[2]      = { 0, 0 };
		int outOfSpaceCount[2]    = { 0, 0 };
		int notResponsiveCount[2] = { 0, 0 };
		int retiringCount[2]      = { 0, 0 };
		int restartingCount[2]    = { 0, 0 };
		for (vector<ChunkServerPtr>::const_iterator it =
				mChunkServers.begin();
				it != mChunkServers.end();
				++it) {
			const int i = (*it)->CanBeChunkMaster() ? 0 : 1;
			if (! IsCandidateServer(*it)) {
				dontLikeCount[i]++;
			}
			if ((*it)->GetAvailSpace() < int64_t(CHUNKSIZE)) {
				outOfSpaceCount[i]++;
			}
			if (! (*it)->IsResponsiveServer()) {
				notResponsiveCount[i]++;
			}
			if ((*it)->IsRetiring()) {
				retiringCount[i]++;
			}
			if ((*it)->IsRestartScheduled()) {
				restartingCount[i]++;
			}
		}
		if (r->appendChunk) {
			// Allocation for append shouldn't fail
			// unless we REALLY are out of space.
			assert(!"Can't happen...");
		}
		const size_t numFound = r->servers.size();
		r->servers.clear();
		KFS_LOG_STREAM_INFO << "allocate chunk no " <<
			(!isMasterChosen ? "master" : "servers") <<
			" repl: "       << r->numReplicas <<
				"/" << replicaCnt <<
			" servers: "    << numFound <<
				"/" << mChunkServers.size() <<
			" dont like: "  << dontLikeCount[0] <<
				"/" << dontLikeCount[1] <<
			" no space: "   << outOfSpaceCount[0] <<
				"/" << outOfSpaceCount[1] <<
			" slow: "   <<  notResponsiveCount[0] <<
				"/" << notResponsiveCount[1] <<
			" retire: "     << retiringCount[0] <<
				"/" << retiringCount[1] <<
			" restart: "    << restartingCount[0] <<
				"/" << restartingCount[1] <<
                        " racks: "      << numRacks <<
			" candidates: " << numCandidates <<
			" masters: "    << mastersSkipped <<
				"/" << mMastersCount <<
			" slaves: "     << slavesSkipped <<
				"/" << mSlavesCount <<
			" to restart: " << mCSToRestartCount <<
				"/"    << mMastersToRestartCount <<
			" request: "    << r->Show() <<
		KFS_LOG_EOM;
	    	r->statusMsg = (!isMasterChosen) ? "no master" : "no servers";
		return -ENOSPC;
	}

	const LeaseInfo l(WRITE_LEASE, mLeaseId, r->servers[0], r->pathname, r->appendChunk);
	mLeaseId++;

	r->master = r->servers[0];

	ChunkPlacementInfo v;

	v.fid = r->fid;
	// r->offset is a multiple of CHUNKSIZE
	assert((r->offset % CHUNKSIZE) == 0);
	v.chunkOffsetIndex = (r->offset / CHUNKSIZE);
	v.chunkServers = r->servers;
	v.chunkLeases.push_back(l);

	mChunkToServerMap[r->chunkId] = v;

	mChunksWithLeases.insert(r->chunkId);

	if (r->servers.size() < (uint32_t) r->numReplicas)
		ChangeChunkReplication(r->chunkId);

	for (i = r->servers.size(); i-- > 0; ) {
		r->servers[i]->AllocateChunk(r, i == 0 ? l.leaseId : -1);
	}
	if (! r->servers.empty() && r->appendChunk) {
		mARAChunkCache.RequestNew(*r);
		ARAChunkCache::Entry* entry = mARAChunkCache.Get(r->fid, r->clientRack);
		entry->appendChunkAffinityCount = appendChunkAffinityCount + 1;
	}
	return 0;
}

int
LayoutManager::GetChunkWriteLease(MetaAllocate *r, bool &isNewLease)
{
	vector<ChunkServerPtr>::size_type i;
	vector<LeaseInfo>::iterator l;

	// XXX: This is a little too conservative.  We should
	// check if any server has told us about a lease for this
	// file; if no one we know about has a lease, then deny
	// issuing the lease during recovery---because there could
	// be some server who has a lease and hasn't told us yet.
	if (InRecovery()) {
		KFS_LOG_STREAM_INFO <<
			"GetChunkWriteLease: InRecovery() => EBUSY" <<
		KFS_LOG_EOM;
		return -EBUSY;
	}

	// if no allocation has been done, can't grab any lease
        CSMapIter iter = mChunkToServerMap.find(r->chunkId);
        if (iter == mChunkToServerMap.end())
                return -EINVAL;

	ChunkPlacementInfo& v = iter->second;
	if (v.chunkServers.empty())
		// all the associated servers are dead...so, fail
		// the allocation request.
		return -KFS::EDATAUNAVAIL;

	if (v.ongoingReplications > 0) {
		// don't issue a write lease to a chunk that is being
		// re-replicated; this prevents replicas from diverging
		KFS_LOG_STREAM_INFO << "Write lease: " << r->chunkId <<
			" is being re-replicated => EBUSY" <<
		KFS_LOG_EOM;
		return -EBUSY;
	}

	l = find_if(v.chunkLeases.begin(), v.chunkLeases.end(),
			ptr_fun(LeaseInfo::IsValidWriteLease));
	if (l != v.chunkLeases.end()) {
#ifdef DEBUG
		const time_t now = TimeNow();
		assert(now <= l->expires);
		KFS_LOG_STREAM_DEBUG << "write lease exists, no version bump" <<
		KFS_LOG_EOM;
#endif
		// valid write lease; so, tell the client where to go
		KFS_LOG_STREAM_INFO << "Valid write lease exists for " << r->chunkId <<
			" expires=" << timeToStr(l->expires) <<
		KFS_LOG_EOM;
		isNewLease = false;
		r->servers = v.chunkServers;
		r->master = l->chunkServer;
		return 0;
	}
	// there is no valid write lease; to issue a new write lease, we
	// need to do a version # bump.  do that only if we haven't yet
	// handed out valid read leases
	if (! ExpiredLeaseCleanup(r->chunkId, TimeNow())) {
		KFS_LOG_STREAM_DEBUG <<
			"GetChunkWriteLease: read lease for chunk " <<
				r->chunkId << " => EBUSY" <<
		KFS_LOG_EOM;
		return -EBUSY;
	}
	// Check if make stable is in progress.
	// It is crucial to check the after invoking ExpiredLeaseCleanup()
	// Expired lease cleanup the above can start make chunk stable.
	if (! IsChunkStable(r->chunkId)) {
		KFS_LOG_STREAM_INFO <<
			"Chunk " << r->chunkId << " isn't yet stable => EBUSY" <<
		KFS_LOG_EOM;
		return -EBUSY;
	}
        // Check if servers vector has changed:
        // chunk servers can go down in ExpiredLeaseCleanup()
	if (v.chunkServers.empty())
		// all the associated servers are dead...so, fail
		// the allocation request.
		return -KFS::EDATAUNAVAIL;

	// Need space on the servers..otherwise, fail it
	r->servers = v.chunkServers;
	for (i = 0; i < r->servers.size(); i++) {
		if (r->servers[i]->GetAvailSpace() < (int64_t)CHUNKSIZE)
			return -ENOSPC;
	}

	isNewLease = true;

	// when issuing a new lease, bump the version # by the increment
	r->chunkVersion += chunkVersionInc;
	const LeaseInfo lease(WRITE_LEASE, mLeaseId, r->servers[0], r->pathname, r->appendChunk);
	mLeaseId++;

	v.chunkLeases.push_back(lease);

	mChunksWithLeases.insert(r->chunkId);

	r->master = r->servers[0];
	KFS_LOG_STREAM_INFO <<
		"New write lease issued for " << r->chunkId <<
		"; version=" << r->chunkVersion <<
	KFS_LOG_EOM;

	for (i = r->servers.size(); i-- > 0; ) {
		r->servers[i]->AllocateChunk(r, i == 0 ? lease.leaseId : -1);
	}
	return 0;
}

/*
 * \brief During atomic record appends, a client tries to allocate a block.
 * Since the client doesn't know the file size, the client notifies the
 * metaserver it is trying to append.  Simply allocating a new chunk for each
 * such request will cause too many chunks.  Instead, the metaserver picks one
 * of the existing chunks of the file which has a valid write lease (presumably,
 * that chunk is not full), and returns that info.  When the client gets the
 * info, it is possible that the chunk became full. In such a scenario, the client may have to try
 * multiple times until it finds a chunk that it can write to.
 */
int
LayoutManager::AllocateChunkForAppend(MetaAllocate *req)
{
	ARAChunkCache::Entry* const entry = mARAChunkCache.Get(req->fid, req->clientRack);
	if (! entry) {
		return -1;
	}

	KFS_LOG_STREAM_DEBUG << "Append on file " << req->fid <<
		" with offset " << req->offset <<
                " max offset  " << entry->offset <<
		(entry->IsAllocationPending() ?
			" allocation in progress" : "") <<
	KFS_LOG_EOM;

	if (entry->offset < 0 || (entry->offset % CHUNKSIZE) != 0) {
		assert(! "invalid offset");
		mARAChunkCache.Invalidate(req->fid, req->clientRack);
		return -1;
	}
	// The client is providing an offset hint in the case when it needs a
	// new chunk: space allocation failed because chunk is full, or it can
	// not talk to the chunk server.
	// 
	// If allocation has already finished, then cache entry offset is valid,
	// otherwise the offset is equal to EOF at the time the initial request
	// has started. The client specifies offset just to indicate that it
	// wants a new chunk, and when the allocation finishes it will get the
	// new chunk.
	if (entry->offset < req->offset && ! entry->IsAllocationPending()) {
		return -1;
	}

       	CSMapConstIter const iter = mChunkToServerMap.find(entry->chunkId);
       	if (iter == mChunkToServerMap.end()) {
		return -1;
	}
	const ChunkPlacementInfo& v = iter->second;
	if ((v.chunkServers.empty()) || (v.ongoingReplications > 0)) {
		return -1;
	}
	vector<LeaseInfo>::const_iterator const l =
		find_if(v.chunkLeases.begin(), v.chunkLeases.end(),
			ptr_fun(LeaseInfo::IsValidWriteLease));
	if (l == v.chunkLeases.end()) {
		return -1;
	}
	// Since there is no un-reservation mechanism, decay reservation by
	// factor of 2 every mReservationDecayStep sec.
	// The goal is primarily to decrease # or rtt and meta server cpu
	// consumption due to chunk space reservation contention between
	// multiple concurrent appenders, while keeping chunk size as large as
	// possible.
	const time_t now = TimeNow();
	if (mReservationDecayStep > 0 &&
			entry->lastDecayTime +
			mReservationDecayStep <= now) {
		const size_t exp = (now - entry->lastDecayTime) /
			mReservationDecayStep;
		if (exp >= sizeof(entry->spaceReservationSize) * 8) {
			entry->spaceReservationSize = 0;
		} else {
			entry->spaceReservationSize >>= exp;
		}
		entry->lastDecayTime = now;
	}
	const int reservationSize = (int)(min(double(mMaxReservationSize),
		mReservationOvercommitFactor *
		max(1, req->spaceReservationSize)));
	if (entry->spaceReservationSize + reservationSize >
			mChunkReservationThreshold) {
		return -1;
	}
	// valid write lease; so, tell the client where to go
	req->chunkId = entry->chunkId;
	req->offset = entry->offset;
	req->chunkVersion = entry->chunkVersion;
	req->servers = v.chunkServers;
	req->master = l->chunkServer;
	entry->numAppendersInChunk++;
	entry->lastAccessedTime = now;
	entry->spaceReservationSize += reservationSize;
	const bool pending = entry->AddPending(*req);
	KFS_LOG_STREAM_DEBUG <<
		"Valid write lease exists for " << req->chunkId <<
		" expires in " << (l->expires - TimeNow()) << " sec" <<
		" space: " << entry->spaceReservationSize <<
		" (+" << reservationSize <<
		"," << req->spaceReservationSize << ")" <<
		" num appenders: " << entry->numAppendersInChunk <<
		(pending ? " allocation in progress" : "") <<
	KFS_LOG_EOM;
	return 0;
}

/*
 * The chunk files are named <fid, chunkid, version>. The fid is now ignored by
 * the meta server.
*/
void
LayoutManager::CoalesceBlocks(const vector<chunkId_t>& srcChunks, fid_t srcFid,
				fid_t dstFid, const off_t dstStartOffset)
{
	assert("!Not supported...fix to handle the per-rack chunk files");

	// mARAChunkCache.Invalidate(srcFid);

	// All src chunks moved to the the of dst file -- update offset indexes.
	assert(dstStartOffset >= 0);
	const uint32_t chunkOffsetIndexAdd =
		(uint32_t)(dstStartOffset / CHUNKSIZE);
	for (vector<chunkId_t>::const_iterator it = srcChunks.begin();
			it != srcChunks.end(); ++it) {
		CSMapIter const cs = mChunkToServerMap.find(*it);
		if (cs == mChunkToServerMap.end()) {
			KFS_LOG_STREAM_ERROR <<
				"Coalesce blocks: unknown chunk: " << *it <<
			KFS_LOG_EOM;
		} else {
			if (cs->second.fid != srcFid) {
				KFS_LOG_STREAM_ERROR <<
					"Coalesce blocks: chunk: " << *it <<
					" undexpected file id: " << cs->second.fid <<
					" expect: " << srcFid <<
				KFS_LOG_EOM;
			}
			cs->second.fid = dstFid;
			cs->second.chunkOffsetIndex += chunkOffsetIndexAdd;
		}
	}
}

/*
 * \brief Process a reqeuest for a READ lease.
*/
int
LayoutManager::GetChunkReadLease(MetaLeaseAcquire *req)
{
	if (InRecovery()) {
		KFS_LOG_STREAM_INFO << "GetChunkReadLease: inRecovery() => EBUSY" <<
		KFS_LOG_EOM;
		return -EBUSY;
	}

        CSMapIter iter = mChunkToServerMap.find(req->chunkId);
        if (iter == mChunkToServerMap.end())
                return -EINVAL;

	ChunkPlacementInfo& v = iter->second;
	vector<LeaseInfo>::iterator l;
	l = find_if(v.chunkLeases.begin(), v.chunkLeases.end(),
			ptr_fun(&LeaseInfo::IsWriteLease));
	if (l != v.chunkLeases.end()) {
		KFS_LOG_STREAM_DEBUG <<
			(LeaseInfo::IsValidWriteLease(*l) ? "Valid" : "Expired") <<
			" write lease exists for chunk " << req->chunkId <<
			" => EBUSY" <<
		KFS_LOG_EOM;
		return -EBUSY;
	}
	//
	// Even if there is no write lease, wait until the chunk is stable
	// before the client can read the data.  We could optimize by letting
	// the client read from servers where the data is stable, but that
	// requires more book-keeping; so, we'll defer for now.
	//
	if (!IsChunkStable(req->chunkId)) {
		KFS_LOG_STREAM_INFO << "Chunk " << req->chunkId <<
			" isn't yet stable => EBUSY" <<
		KFS_LOG_EOM;
		return -EBUSY;
	}

	// issue a read lease
	const LeaseInfo lease(READ_LEASE, mLeaseId, false);
	mLeaseId++;

	v.chunkLeases.push_back(lease);
	req->leaseId = lease.leaseId;

	mChunksWithLeases.insert(req->chunkId);

	return 0;
}

class ValidLeaseIssued {
	const CSMap &chunkToServerMap;
public:
	ValidLeaseIssued(const CSMap &m) : chunkToServerMap(m) { }
	bool operator() (MetaChunkInfo *c) {
		CSMapConstIter const iter = chunkToServerMap.find(c->chunkId);
		if (iter == chunkToServerMap.end())
			return false;
		const ChunkPlacementInfo& v = iter->second;
		return (
			find_if(v.chunkLeases.begin(), v.chunkLeases.end(),
				ptr_fun(LeaseInfo::IsValidLease)) !=
			v.chunkLeases.end()
		);
	}
};

bool
LayoutManager::IsValidLeaseIssued(const vector <MetaChunkInfo *> &c)
{
	vector <MetaChunkInfo *>::const_iterator i;

	i = find_if(c.begin(), c.end(), ValidLeaseIssued(mChunkToServerMap));
	if (i == c.end())
		return false;
	KFS_LOG_STREAM_DEBUG << "Valid lease issued on chunk: " <<
			(*i)->chunkId << KFS_LOG_EOM;
	return true;
}

bool
LayoutManager::IsValidWriteLeaseIssued(chunkId_t chunkId)
{
        CSMapIter const iter = mChunkToServerMap.find(chunkId);
	if (iter == mChunkToServerMap.end()) {
		return false;
	}
	ChunkPlacementInfo& v = iter->second;
	vector<LeaseInfo>::iterator l = find_if(v.chunkLeases.begin(), v.chunkLeases.end(),
			ptr_fun(LeaseInfo::IsValidWriteLease));
	return (l != v.chunkLeases.end());
}


class LeaseIdMatcher {
	const int64_t myid;
public:
	LeaseIdMatcher(int64_t id) : myid(id) { }
	bool operator() (const LeaseInfo &l) {
		return l.leaseId == myid;
	}
};

int
LayoutManager::LeaseRenew(MetaLeaseRenew *req)
{
	vector<LeaseInfo>::iterator l;

        CSMapIter iter = mChunkToServerMap.find(req->chunkId);
        if (iter == mChunkToServerMap.end()) {
		if (InRecovery() && req->leaseId >= mLeaseId) {
			mLeaseId = req->leaseId + 1;
		}
                return -EINVAL;

	}
	ChunkPlacementInfo& v = iter->second;
	l = find_if(v.chunkLeases.begin(), v.chunkLeases.end(),
			LeaseIdMatcher(req->leaseId));
	if (l == v.chunkLeases.end()) {
		return -EINVAL;
	}
	const time_t now = TimeNow();
	if (now > l->expires) {
		// can't renew dead leases; get a new one
		return -ELEASEEXPIRED;
	}
	l->expires = now + LEASE_INTERVAL_SECS;
	mChunksWithLeases.insert(req->chunkId);
	return 0;
}

///
/// Handling a corrupted chunk involves removing the mapping
/// from chunk id->chunkserver that we know has it.
///
void
LayoutManager::ChunkCorrupt(MetaChunkCorrupt *r)
{
	if (! r->isChunkLost)
		r->server->IncCorruptChunks();

        CSMapIter iter = mChunkToServerMap.find(r->chunkId);
	if (iter == mChunkToServerMap.end())
		return;

	ChunkPlacementInfo& v = iter->second;
        const size_t prevNumSrv = v.chunkServers.size();
	EraseReallocIfNeeded(v.chunkServers, remove_if(
		v.chunkServers.begin(), v.chunkServers.end(),
		ChunkServerMatcher(r->server.get())), v.chunkServers.end());
	for_each(v.chunkLeases.begin(), v.chunkLeases.end(),
		ExpireLeaseIfOwner(r->server.get()));
        if (prevNumSrv != v.chunkServers.size()) {
            // Invalidate cache.
            mARAChunkCache.Invalidate(r->fid, r->server->GetRack());
	    // check the replication state when the replicaiton checker gets to it
	    ChangeChunkReplication(r->chunkId);
        }
	KFS_LOG_STREAM_INFO << "Server " << r->server->ServerID() <<
		" claims file/chunk: <" <<
                v.fid << "/" << r->fid << "," << r->chunkId <<
		"> to be " << (r->isChunkLost ? "lost" : "corrupt") <<
                " servers: " << prevNumSrv << " -> " << v.chunkServers.size() <<
	KFS_LOG_EOM;

	// this chunk has to be replicated from elsewhere; since this is no
	// longer hosted on this server, take it out of its list of blocks
	r->server->MovingChunkDone(r->chunkId);
	if (r->server->IsRetiring()) {
		r->server->EvacuateChunkDone(r->chunkId);
	}
}

class ChunkDeletor {
	const chunkId_t chunkId;
public:
	ChunkDeletor(chunkId_t c)
		: chunkId(c)
		{}
	void operator () (const ChunkServerPtr &c) {
		c->DeleteChunk(chunkId);
	}
};

///
/// Deleting a chunk involves two things: (1) removing the
/// mapping from chunk id->chunk server that has it; (2) sending
/// an RPC to the associated chunk server to nuke out the chunk.
///
void
LayoutManager::DeleteChunk(chunkId_t chunkId)
{
        // if we know anything about this chunk at all, then we
        // process the delete request.
        CSMapIter const iter = mChunkToServerMap.find(chunkId);
	if (iter == mChunkToServerMap.end()) {
		return;
	}

	vector<ChunkServerPtr> const c(iter->second.chunkServers);
	// remove the mapping
	mChunkToServerMap.erase(iter);
	mPendingBeginMakeStable.erase(chunkId);
	mPendingMakeStable.erase(chunkId);
	mChunkReplicationCandidates.erase(chunkId);

	// submit an RPC request
	for_each(c.begin(), c.end(), ChunkDeletor(chunkId));
}

class Truncator {
    chunkId_t chunkId;
    off_t sz;
public:
    Truncator(chunkId_t c, off_t s) : chunkId(c), sz(s) { }
    void operator () (ChunkServerPtr &c) { c->TruncateChunk(chunkId, sz); }
};

///
/// To truncate a chunk, find the server that holds the chunk and
/// submit an RPC request to it.
///
void
LayoutManager::TruncateChunk(chunkId_t chunkId, off_t sz)
{
	vector<ChunkServerPtr> c;

        // if we know anything about this chunk at all, then we
        // process the truncate request.
	if (GetChunkToServerMapping(chunkId, c) != 0)
		return;

	// submit an RPC request
        Truncator doTruncate(chunkId, sz);
	for_each(c.begin(), c.end(), doTruncate);
}

void
LayoutManager::AddChunkToServerMapping(chunkId_t chunkId, fid_t fid,
					off_t offset,
					ChunkServer *c)
{
	ChunkPlacementInfo v;

        if (c == NULL) {
		// Store an empty mapping to signify the presence of this
		// particular chunkId.
		v.fid = fid;
		mChunkToServerMap[chunkId] = v;
		return;
        }

	assert(ValidServer(c));

	KFS_LOG_STREAM_DEBUG << "Laying out chunk=" << chunkId << " on server  " <<
		c->GetServerName() << KFS_LOG_EOM;

	if (UpdateChunkToServerMapping(chunkId, c) == 0)
            return;

	v.fid = fid;
	assert((offset % CHUNKSIZE) == 0);
	v.chunkOffsetIndex = (offset / CHUNKSIZE);
        v.chunkServers.push_back(c->shared_from_this());
        mChunkToServerMap[chunkId] = v;
}

void
LayoutManager::RemoveChunkToServerMapping(chunkId_t chunkId)
{
        mChunkToServerMap.erase(chunkId);
	mPendingBeginMakeStable.erase(chunkId);
	mPendingMakeStable.erase(chunkId);
}

int
LayoutManager::UpdateChunkToServerMapping(chunkId_t chunkId, ChunkServer *c)
{
        // If the chunkid isn't present in the mapping table, it could be a
        // stale chunk
        CSMapIter iter = mChunkToServerMap.find(chunkId);
        if (iter == mChunkToServerMap.end())
                return -1;

	/*
	KFS_LOG_STREAM_DEBUG << "chunk=" << chunkId << " was laid out on server " <<
		c->GetServerName() << KFS_LOG_EOM;
	*/
        iter->second.chunkServers.push_back(c->shared_from_this());

        return 0;
}

bool
LayoutManager::GetChunkFileId(chunkId_t chunkId, fid_t& fileId)
{
        CSMapConstIter const it = mChunkToServerMap.find(chunkId);
	if (it == mChunkToServerMap.end()) {
		return false;
	}
	fileId = it->second.fid;
	return true;
}

int
LayoutManager::GetChunkToServerMapping(chunkId_t chunkId, vector<ChunkServerPtr> &c)
{
        CSMapConstIter iter = mChunkToServerMap.find(chunkId);
        if ((iter == mChunkToServerMap.end()) ||
		(iter->second.chunkServers.size() == 0))
                return -1;

        c = iter->second.chunkServers;
        return 0;
}

/// Wrapper class due to silly template/smart-ptr madness
class Dispatcher {
public:
	Dispatcher() { }
	void operator() (ChunkServerPtr &c) {
		c->Dispatch();
	}
};

void
LayoutManager::Dispatch()
{
	// this method is called in the context of the network thread.
	// lock out the request processor to prevent changes to the list.
	// XXX: Above comment doesn't apply anymore.

	// pthread_mutex_lock(&mChunkServersMutex);

	for_each(mChunkServers.begin(), mChunkServers.end(), Dispatcher());

	// pthread_mutex_unlock(&mChunkServersMutex);
}

class Heartbeater {
public:
	Heartbeater() { }
	void operator() (ChunkServerPtr &c) {
		c->Heartbeat();
	}
};

void
LayoutManager::HeartbeatChunkServers()
{
	// this method is called in the context of the network thread.
	// lock out the request processor to prevent changes to the list.
	// XXX: Above comment doesn't apply anymore.

	// pthread_mutex_lock(&mChunkServersMutex);

	for_each(mChunkServers.begin(), mChunkServers.end(), Heartbeater());

	// pthread_mutex_unlock(&mChunkServersMutex);
}

bool
LayoutManager::ValidServer(ChunkServer *c)
{
	vector <ChunkServerPtr>::const_iterator i;

	i = find_if(mChunkServers.begin(), mChunkServers.end(),
		ChunkServerMatcher(c));
	return (i != mChunkServers.end());
}

class Pinger {
	string &result;
	// return the total/used for all the nodes in the cluster
	uint64_t &totalSpace;
	uint64_t &usedSpace;
public:
	Pinger(string &r, uint64_t &t, uint64_t &u) :
		result(r), totalSpace(t), usedSpace(u) { }
	void operator () (ChunkServerPtr &c)
	{
		c->Ping(result);
		totalSpace += c->GetTotalSpace();
		usedSpace += c->GetUsedSpace();
	}
};

class RetiringStatus {
	string &result;
public:
	RetiringStatus(string &r):result(r) { }
	void operator () (ChunkServerPtr &c) { c->GetRetiringStatus(result); }
};

void
LayoutManager::Ping(string &systemInfo, string &upServers, string &downServers, string &retiringServers)
{
	uint64_t totalSpace = 0, usedSpace = 0;
	Pinger doPing(upServers, totalSpace, usedSpace);
	for_each(mChunkServers.begin(), mChunkServers.end(), doPing);
	downServers.clear();
	for (DownServers::const_iterator it = mDownServers.begin();
			it != mDownServers.end();
			++it) {
		downServers += *it;
	}
	for_each(mChunkServers.begin(), mChunkServers.end(), RetiringStatus(retiringServers));

	ostringstream os;

	os << "Up since= " << timeToStr(mStartTime) << '\t';
	os << "Total space= " << totalSpace << '\t';
	os << "Used space= " << usedSpace;
	systemInfo = os.str();
}

class UpServersList {
    ostringstream &os;
public:
	UpServersList(ostringstream &os): os(os) { }
    void operator () (ChunkServerPtr &c) {
        os << c->GetServerLocation().ToString() << endl;
    }
};

void
LayoutManager::UpServers(ostringstream &os)
{
    UpServersList upServers(os);
    for_each(mChunkServers.begin(), mChunkServers.end(), upServers);
}

class ReReplicationCheckIniter {
	CRCandidateSet &crset;
	CRCandidateSet &prioritySet;
public:
	ReReplicationCheckIniter(CRCandidateSet &c, CRCandidateSet &p) : crset(c), prioritySet(p) { }
	void operator () (const CSMap::value_type p) {
		crset.insert(p.first);
		if (p.second.chunkServers.size() == 1)
			prioritySet.insert(p.first);
	}
};


// Periodically, check the replication level of ALL chunks in the system.
void
LayoutManager::InitCheckAllChunks()
{
	mPriorityChunkReplicationCandidates.clear();
	for_each(mChunkToServerMap.begin(), mChunkToServerMap.end(),
		ReReplicationCheckIniter(mChunkReplicationCandidates, 
					mPriorityChunkReplicationCandidates));
}

/// functor to tell if a lease has expired
class LeaseExpired {
	time_t now;
public:
	LeaseExpired(time_t n): now(n) { }
	bool operator () (const LeaseInfo &l) { return now >= l.expires; }

};

/// If the write lease on a chunk is expired, then decrement the # of writes
/// on the servers that are involved in the write.
class DecChunkWriteCount {
	const fid_t     fid;
	const chunkId_t chunkId;
	const CSMap&    chunkMap;
	int             writeLeaseCount;
public:
	DecChunkWriteCount(const CSMap& map, fid_t fid, chunkId_t chunkId)
		: fid(fid),
		  chunkId(chunkId),
		  chunkMap(map),
		  writeLeaseCount(0)
		{}
	void operator() (const LeaseInfo &l) {
		if (l.leaseType != WRITE_LEASE) {
			return;
		}
		if (++writeLeaseCount > 1) {
			KFS_LOG_STREAM_ERROR << "decrement write count:" <<
				" <" << fid << "," << chunkId << ">"
				" name: " << l.pathname <<
				" write lease count: " << writeLeaseCount <<
				", extraneous write" <<
					(l.appendFlag ? " append" : "") <<
				" lease ignored" <<
			KFS_LOG_EOM;
			return;
		}
		CSMapConstIter const ci = chunkMap.find(chunkId);
		if (ci == chunkMap.end()) {
			return;
		}
		if (l.relinquishedFlag) {
			return;
		}
		gLayoutManager.MakeChunkStableInit(
			fid, chunkId, ci->second.chunkOffsetIndex * CHUNKSIZE,
			l.pathname, ci->second.chunkServers, l.appendFlag,
			-1, false, 0
		);
	}
};

bool
LayoutManager::ExpiredLeaseCleanup(
	chunkId_t                 chunkId,
	time_t                    now,
	int                       ownerDownExpireDelay /* = 0 */,
	const CRCandidateSetIter* chunksWithLeasesIt   /* = 0 */)
{
	CSMapIter const iter = mChunkToServerMap.find(chunkId);
	if (iter == mChunkToServerMap.end()) {
		if (chunksWithLeasesIt) {
			mChunksWithLeases.erase(*chunksWithLeasesIt);
		}
		return true;
	}
	ChunkPlacementInfo& c = iter->second;
	vector<LeaseInfo>::iterator const i = remove_if(
		c.chunkLeases.begin(), c.chunkLeases.end(), LeaseExpired(now));
	if (ownerDownExpireDelay > 0 &&
			i != c.chunkLeases.end() &&
			i->ownerWasDownFlag &&
			LeaseInfo::IsWriteLease(*i) &&
			i->expires + ownerDownExpireDelay > now) {
		return false;
	}
	if (i != c.chunkLeases.end() && i->appendFlag) {
		if (i->chunkServer)
			mARAChunkCache.Invalidate(c.fid, i->chunkServer->GetRack(), chunkId);
	}
	// The call to DecChunkWriteCount() can cause the chunk to get deleted:
	// for instance, if there was an allocation request outstanding, the
	// allocate will fail because one of the servers went down, causing us
	// to free chunkLeases.  Then, we come back here, boom.  To fix, stash
	// away what needs to be deleted into a local and then operate on that.
	vector<LeaseInfo> const leases(i, c.chunkLeases.end());
	// trim the list
	const bool retVal = EraseReallocIfNeeded(
		c.chunkLeases, i, c.chunkLeases.end()).empty();
	if (retVal && chunksWithLeasesIt) {
		mChunksWithLeases.erase(*chunksWithLeasesIt);
	}
	for_each(leases.begin(), leases.end(),
		DecChunkWriteCount(mChunkToServerMap, c.fid, chunkId));
	// If the chunk disappeared or cleaned up in the process then most likely
	// mChunksWithLeases is already cleaned up, if not then it will be
	// cleaned on the next timer run. mChunksWithLeases is only used for
	// lease cleanup, stale entry should not cause a problem.
	return retVal;
}

bool
LayoutManager::ExpiredLeaseCleanup(chunkId_t chunkId)
{
	if (! ExpiredLeaseCleanup(chunkId, TimeNow())) {
		return false;
	}
	mChunksWithLeases.erase(chunkId);
	return true;
}

struct UptimeLess :
	public std::binary_function<ChunkServerPtr, ChunkServerPtr, bool>
{
	bool operator() (const ChunkServerPtr& lhs, ChunkServerPtr& rhs) const {
		return (lhs.get()->Uptime() < rhs.get()->Uptime());
	}
};

void
LayoutManager::LeaseCleanup()
{
	const time_t now = TimeNow();
	for (CRCandidateSetIter citer = mChunksWithLeases.begin();
		citer != mChunksWithLeases.end(); ) {
		CRCandidateSetIter it = citer++;
		ExpiredLeaseCleanup(*it, now, mLeaseOwnerDownExpireDelay, &it);
	}
	// also clean out the ARACache of old entries
	mARAChunkCache.Timeout(now - ARA_CHUNK_CACHE_EXPIRE_INTERVAL);
	if (now - mLastReplicationCheckTime > NDAYS_PER_FULL_REPLICATION_CHECK * 60 * 60 * 24) {
		KFS_LOG_STREAM_INFO <<
			"Initiating a replication check of all chunks..." <<
		KFS_LOG_EOM;
		InitCheckAllChunks();
		mLastReplicationCheckTime = now;
	}
	if (now - mLastRecomputeDirsizeTime > 60 * 60) {
		KFS_LOG_STREAM_INFO << "Doing a recompute dir size..." <<
		KFS_LOG_EOM;
		metatree.recomputeDirSize();
		mLastRecomputeDirsizeTime = now;
		KFS_LOG_STREAM_INFO << "Recompute dir size is done..." <<
		KFS_LOG_EOM;
	}
	ScheduleChunkServersRestart();
}

void LayoutManager::ScheduleChunkServersRestart()
{
	if (! IsChunkServerRestartAllowed()) {
		return;
	}
	vector<ChunkServerPtr> servers(mChunkServers);
	make_heap(servers.begin(), servers.end(), UptimeLess());
	while (! servers.empty()) {
		ChunkServer& srv = *servers.front().get();
		if (srv.Uptime() < mMaxCSUptime) {
			break;
		}
		bool restartFlag = srv.IsRestartScheduled();
		if (! restartFlag && mCSToRestartCount < mMaxCSRestarting) {
			// Make sure that there are enough masters.
			restartFlag = ! srv.CanBeChunkMaster() ||
					mMastersCount > mMastersToRestartCount +
						max(size_t(1), mSlavesCount / 2 * 3);
			if (! restartFlag && ! mAssignMasterByIpFlag) {
				for (vector<ChunkServerPtr>::iterator
						it = servers.begin();
						it != servers.end();
						++it) {
					if (! (*it)->CanBeChunkMaster() &&
							! (*it)->IsRestartScheduled() &&
							IsCandidateServer(*it)) {
						(*it)->SetCanBeChunkMaster(true);
						srv.SetCanBeChunkMaster(false);
						restartFlag = true;
						break;
					}
				}
			}
			if (restartFlag) {
				mCSToRestartCount++;
				if (srv.CanBeChunkMaster()) {
					mMastersToRestartCount++;
				}
			}
		}
		if (restartFlag &&
				srv.ScheduleRestart(
					mCSGracefulRestartTimeout,
					mCSGracefulRestartAppendWithWidTimeout)) {
			KFS_LOG_STREAM_INFO <<
				"initiated restart sequence for: " <<
				servers.front()->ServerID() <<
			KFS_LOG_EOM;
			break;
		}
		pop_heap(servers.begin(), servers.end(), UptimeLess());
		servers.pop_back();
	}
}

// This call is internally generated: an allocation failed; invalidate the write
// lease so that subsequent allocation request will force a version # bump and
// a new write lease to be issued.
void
LayoutManager::InvalidateWriteLease(chunkId_t chunkId)
{
        CSMapIter const iter = mChunkToServerMap.find(chunkId);
	if (iter == mChunkToServerMap.end()) {
		return;
	}
	ChunkPlacementInfo& v = iter->second;
	vector<LeaseInfo>::iterator l = find_if(v.chunkLeases.begin(), v.chunkLeases.end(),
			ptr_fun(LeaseInfo::IsValidWriteLease));
	if (l != v.chunkLeases.end()) {
		// Invalidate the lease; the normal cleanup will fix things up.
		l->expires = 0;
	}
}

int
LayoutManager::LeaseRelinquish(MetaLeaseRelinquish *req)
{
        CSMapIter const iter = mChunkToServerMap.find(req->chunkId);
	if (iter == mChunkToServerMap.end()) {
		return -ELEASEEXPIRED;
	}
	ChunkPlacementInfo& v = iter->second;
	vector<LeaseInfo>::iterator const l =
		find_if(v.chunkLeases.begin(), v.chunkLeases.end(),
			LeaseIdMatcher(req->leaseId));
	if (l == v.chunkLeases.end()) {
		return -EINVAL;
	}
	const time_t now = TimeNow();
	if (now > l->expires) {
		return -ELEASEEXPIRED;
	}
	const bool hadLeaseFlag = ! l->relinquishedFlag;
	l->relinquishedFlag = true;
	// the owner of the lease is giving up the lease; update the expires so
	// that the normal lease cleanup will work out.
	l->expires = 0;
	if (l->leaseType == WRITE_LEASE && hadLeaseFlag) {
		// For write append lease checksum and size always have to be
		// specified for make chunk stable, otherwise run begin make
		// chunk stable.
		const bool beginMakeChunkStableFlag = l->appendFlag &&
			(! req->hasChunkChecksum || req->chunkSize < 0);
		if (l->appendFlag) {
			mARAChunkCache.Invalidate(v.fid, v.chunkServers[0]->GetRack(), req->chunkId);
		}
		MakeChunkStableInit(
			v.fid, req->chunkId, (v.chunkOffsetIndex * CHUNKSIZE), 
			l->pathname, v.chunkServers,
			beginMakeChunkStableFlag,
			req->chunkSize, req->hasChunkChecksum, req->chunkChecksum
		);
	}
	return 0;
}

// Periodically, check the status of all the leases
// This is an expensive call...use sparingly....
void
LayoutManager::CheckAllLeases()
{
	const time_t now = TimeNow();
	for (CSMapIter it = mChunkToServerMap.first();
			it != mChunkToServerMap.end();
			it = mChunkToServerMap.next()) {
		ExpiredLeaseCleanup(it->first, now, mLeaseOwnerDownExpireDelay);
	}
}

/*

Make chunk stable protocol description.

The protocol is mainly designed for write append, though it is also partially
used for random write.

The protocol is needed to solve consensus problem, i.e. make all chunk replicas
identical. This also allows replication participants (chunk servers) to
determine the status of a particular write append operation, and, if requested,
to convey this status to the write append client(s).

The fundamental idea is that the meta server always makes final irrevocable
decision what stable chunk replicas should be: chunk size and chunk checksum.
The meta server selects exactly one variant of the replica, and broadcast this
information to all replication participants (the hosting chunk servers). More
over, the meta server maintains this information until sufficient number or
replicas become "stable" as a result of "make chunk stable" operation, or as a
result of re-replication from already "stable" replica(s).

The meta server receives chunk size and chunk checksum from the chunk servers.
There are two ways meta server can get this information:
1. Normally chunk size, and checksum are conveyed by the write master in the
write lease release request.
2. The meta server declares chunk master nonoperational, and broadcasts "begin
make chunk stable" request to all remaining operational replication participants
(slaves). The slaves reply with chunk size and chunk checksum. The meta server
always selects one reply that has the smallest chunk size, in the hope that
other participants can converge their replicas to this chunk size, and checksum
by simple truncation. Begin make chunk stable repeated until the meta server
gets at least one valid reply.

The meta server writes this decision: chunk version, size, and checksum into the
log before broadcasting make chunk stable request with these parameters to all
operational replication participants. This guarantees that the decision is
final: it can never change as long as the log write is persistent. Once log
write completes successfully the meta server broadcasts make chunk stable
request to all operational
replication participants.

The chunk server maintains the information about chunk state: stable -- read
only, or not stable -- writable, and if the chunk was open for write append or
for random write. This information conveyed (back) to the meta server in the
chunk server hello message. The hello message contains 3 chunk lists: stable,
not stable write append, and not stable random write. This is needed to make
appropriate decision when chunk server establishes communication with the meta
server.

The chunk server can never transition not stable chunk replica into stable
replica, unless it receives make chunk stable request from the meta server.
The chunk server discards all non stable replicas on startup (restart).

For stable chunks the server is added to the list of the servers hosting the
chunk replica, as long as the corresponding chunk meta data exists, and version
of the replica matches the meta data version.

In case of a failure, the meta server declares chunk replica stale and conveys
this decision to the chunk server, then the chunk server discards the stale
replica.

For not stable random write chunk replicas the same checks are performed, plus
additional step: make chunk stable request is issued, The request in this case
does not specify the chunk size, and checksum. When make chunk stable completes
successfully the server added to the list of servers hosting the chunk replica.

With random writes version number is used to detect missing writes, and the task
of making chunk replicas consistent left entirely up to the writer (client).
Write lease mechanism is used to control write concurrency: for random write
only one concurrent writer per chunk is allowed.

Not stable write append chunk handling is more involved, because multiple
concurrent write appenders are allowed to append to the same chunk.

First, the same checks for chunk meta data existence, and the version match are
applied.  If successful, then the check for existing write lease is performed.
If the write (possibly expired) lease exists the server is added to the list of
servers hosting the replica. If the write lease exists, and begin make chunk
stable or make chunk stable operation for the corresponding chunk is in
progress, the chunk server is added to the operation.

If no lease exists, and begin make chunk stable was never successfully completed
(no valid pending make chunk stable info exists), then the meta server issues
begin make chunk stable request.

Once begin make chunks stable successfully completes the meta server writes
"mkstable" log record with the chunk version, size, and checksum into the log,
and adds this information to in-memory pending make chunk stable table. Make
chunk stable request is issued after log write successfully completes.

The make chunk stable info is kept in memory, and in the checkpoint file until
sufficient number of stable replicas created, or chunk ceases to exist. Once
sufficient number of replicas is created, the make chunk stable info is purged
from memory, and the "mkstabledone" record written to the log. If chunk ceases
to exist then only in-memory information purged, but no log write performed.
"Mkstabledone" log records effectively cancels "mkstable" record.

Chunk allocation log records have an additional "append" attribute set to 1. Log
replay process creates in-memory make chunk stable entry with chunk size
attribute set to -1 for every chunk allocation record with the append attribute
set to 1. In memory entries with size set to -1 mark not stable chunks for which
chunk size and chunk checksum are not known. For such chunks begin make stable
has to be issued first. The "mkstable" records are used to update in-memory
pending make stable info with the corresponding chunk size and checksum. The
"mkstabledone" records are used to delete the corresponding in-memory pending
make stable info. Chunk delete log records also purge the corresponding
in-memory pending make stable info.

In memory pending delete info is written into the checkpoint file, after the
meta (tree) information. One "mkstable" entry for every chunk that is not
stable, or does not have sufficient number of replicas.

During "recovery" period, begin make chunk stable is not issued, instead these
are delayed until recovery period ends, in the hope that begin make stable with
more servers has higher chances of succeeding, and can potentially produce more
stable replicas.

*/

class BeginMakeChunkStable
{
	const fid_t     fid;
	const chunkId_t chunkId;
	const seq_t     chunkVersion;
public:
	BeginMakeChunkStable(fid_t f, chunkId_t c, seq_t v)
		: fid(f), chunkId(c), chunkVersion(v)
	{}
	void operator()(const ChunkServerPtr &c) const {
            c->BeginMakeChunkStable(fid, chunkId, chunkVersion);
        }
};

class MakeChunkStable
{
	const fid_t     fid;
	const chunkId_t chunkId;
	const seq_t     chunkVersion;
        const off_t     chunkSize;
        const bool      hasChunkChecksum;
        const uint32_t  chunkChecksum;
public:
	MakeChunkStable(fid_t f, chunkId_t c, seq_t v,
		off_t s, bool hasCs, uint32_t cs)
		: fid(f), chunkId(c), chunkVersion(v),
                  chunkSize(s), hasChunkChecksum(hasCs), chunkChecksum(cs)
	{}
	void operator() (const ChunkServerPtr &c) const {
            c->MakeChunkStable(fid, chunkId, chunkVersion,
	    	chunkSize, hasChunkChecksum, chunkChecksum);
        }
};

void
LayoutManager::MakeChunkStableInit(
	fid_t                         fid,
	chunkId_t                     chunkId,
	off_t                         chunkOffsetInFile,
	string                        pathname,
	const vector<ChunkServerPtr>& servers,
	bool                          beginMakeStableFlag,
	off_t                         chunkSize,
	bool                          hasChunkChecksum,
	uint32_t                      chunkChecksum)
{
	const char* const logPrefix = beginMakeStableFlag ? "BMCS:" : "MCS:";
	if (servers.empty()) {
		if (beginMakeStableFlag) {
			// Ensure that there is at least pending begin make
			// stable.
			// Append allocations are marked as such, and log replay
			// adds begin make stable entries if necessary.
			pair<PendingMakeStableMap::iterator, bool> const res =
				mPendingMakeStable.insert(make_pair(
					chunkId, PendingMakeStableEntry()));
			if (res.second) {
				MetaChunkInfo* cinfo = 0;
				const int ret = metatree.getalloc(
					fid, chunkOffsetInFile, &cinfo);
				if (cinfo) {
					res.first->second.mChunkVersion =
						cinfo->chunkVersion;
				} else {
					KFS_LOG_STREAM_ERROR << logPrefix <<
						" <" << fid <<
						"," << chunkId << ">"
						" name: " << pathname <<
						" unable to get version"
						" status: " << ret <<
					KFS_LOG_EOM;
					if (ret == -ENOENT) {
						mPendingMakeStable.erase(res.first);
					}
				}
				
			}
		}
		KFS_LOG_STREAM_INFO << logPrefix <<
			" <" << fid << "," << chunkId << ">"
			" name: "     << pathname <<
			" no servers" <<
		KFS_LOG_EOM;
		return;
	}
	pair<NonStableChunksMap::iterator, bool> const ret =
		mNonStableChunks.insert(make_pair(chunkId,
			MakeChunkStableInfo(
				(int)servers.size(),
				beginMakeStableFlag,
				pathname
		)));
	if (! ret.second) {
		KFS_LOG_STREAM_INFO << logPrefix <<
			" <" << fid << "," << chunkId << ">"
			" name: " << pathname <<
			" already in progress" <<
		KFS_LOG_EOM;
		return;
	}
	MetaChunkInfo* cinfo = 0;
	const int res = metatree.getalloc(fid, chunkOffsetInFile, &cinfo);
	const seq_t chunkVersion = cinfo ? cinfo->chunkVersion : -1;
	ret.first->second.chunkVersion = chunkVersion;
	if (chunkVersion < 0) {
		KFS_LOG_STREAM_ERROR << logPrefix <<
			" <" << fid << "," << chunkId << ">"
			" name: " << pathname <<
			" unable to get version"
			" status: " << res <<
		KFS_LOG_EOM;
		// Ignore the error and make non existent chunk stable anyway?
	}
	KFS_LOG_STREAM_INFO << logPrefix <<
		" <" << fid << "," << chunkId << ">"
		" name: "     << pathname <<
		" version: "  << chunkVersion <<
		" servers: "  << servers.size() <<
		" size: "     << chunkSize <<
		" checksum: " << (hasChunkChecksum ?
			(int64_t)chunkChecksum : (int64_t)-1) <<
	KFS_LOG_EOM;
	// Make a local copy of servers.
	// "Placement info" can change while iterating if any servers are down
	// go down while sending the request.
	if (beginMakeStableFlag) {
		const vector<ChunkServerPtr> srv(servers);
		for_each(srv.begin(), srv.end(),
			BeginMakeChunkStable(fid, chunkId, chunkVersion));
	} else if (hasChunkChecksum || chunkSize >= 0) {
		// Remember chunk check sum and size.
		PendingMakeStableEntry const pmse(
			chunkSize,
			hasChunkChecksum,
			chunkChecksum,
			chunkVersion
		);
		pair<PendingMakeStableMap::iterator, bool> const res =
			mPendingMakeStable.insert(make_pair(chunkId, pmse));
		if (! res.second) {
			KFS_LOG_STREAM((res.first->second.mSize >= 0 ||
					res.first->second.mHasChecksum) ?
					MsgLogger::kLogLevelWARN :
					MsgLogger::kLogLevelDEBUG) <<
				logPrefix <<
				" <" << fid << "," << chunkId << ">"
				" updating existing pending MCS: " <<
				" chunkId: "  << chunkId <<
				" version: "  <<
					res.first->second.mChunkVersion <<
				"=>"          << pmse.mChunkVersion <<
				" size: "     << res.first->second.mSize <<
				"=>"          << pmse.mSize <<
				" checksum: " <<
					(res.first->second.mHasChecksum ?
					int64_t(res.first->second.mChecksum) :
					int64_t(-1)) <<
				"=>"          << (pmse.mHasChecksum ?
					int64_t(pmse.mChecksum) :
					int64_t(-1)) <<
			KFS_LOG_EOM;
			res.first->second = pmse;
		}
		ret.first->second.logMakeChunkStableFlag = true;
		submit_request(new MetaLogMakeChunkStable(
			fid, chunkId, chunkVersion,
			chunkSize, hasChunkChecksum, chunkChecksum, chunkId
		));
	} else {
		const vector<ChunkServerPtr> srv(servers);
		for_each(srv.begin(), srv.end(), MakeChunkStable(
			fid, chunkId, chunkVersion,
			chunkSize, hasChunkChecksum, chunkChecksum
		));
	}
}

bool
LayoutManager::AddServerToMakeStable(
	ChunkPlacementInfo& placementInfo,
	ChunkServerPtr      server,
	chunkId_t           chunkId,
	seq_t               chunkVersion,
	const char*&        errMsg)
{
	errMsg = 0;
	NonStableChunksMap::iterator const it = mNonStableChunks.find(chunkId);
	if (it == mNonStableChunks.end()) {
		return false; // Not in progress
	}
	MakeChunkStableInfo& info = it->second;
	if (info.chunkVersion != chunkVersion) {
		errMsg = "version mismatch";
		return false;
	}
	vector<ChunkServerPtr>& servers = placementInfo.chunkServers;
	if (find_if(servers.begin(), servers.end(),
			MatchingServer(server->GetServerLocation())
			) != servers.end()) {
		// Already there, duplicate chunk? Same as in progress.
		return true;
	}
	KFS_LOG_STREAM_DEBUG << 
		(info.beginMakeStableFlag ? "B" :
			info.logMakeChunkStableFlag ? "L" : "") <<
		"MCS:"
		" <" << placementInfo.fid << "," << chunkId << ">"
		" adding server: " << server->ServerID() <<
		" name: "          << info.pathname <<
		" servers: "       << info.numAckMsg <<
		"/"                << info.numServers <<
		"/"                << servers.size() <<
		" size: "          << info.chunkSize <<
		" checksum: "      << info.chunkChecksum <<
		" added: "         << info.serverAddedFlag <<
	KFS_LOG_EOM;
	servers.push_back(server);
	info.numServers++;
	info.serverAddedFlag = true;
	if (info.beginMakeStableFlag) {
		server->BeginMakeChunkStable(
			placementInfo.fid, chunkId, info.chunkVersion);
	} else if (! info.logMakeChunkStableFlag) {
		server->MakeChunkStable(
			placementInfo.fid,
			chunkId,
			info.chunkVersion,
	    		info.chunkSize,
			info.chunkSize >= 0,
			info.chunkChecksum
		);
	}
	// If log make stable is in progress, then make stable or begin make
	// stable will be started when logging is done.
	return true;
}

void
LayoutManager::BeginMakeChunkStableDone(const MetaBeginMakeChunkStable* req)
{
	const char* const                  logPrefix = "BMCS: done";
	NonStableChunksMap::iterator const it        =
		mNonStableChunks.find(req->chunkId);
	if (it == mNonStableChunks.end() || ! it->second.beginMakeStableFlag) {
		KFS_LOG_STREAM_DEBUG << logPrefix <<
			" <" << req->fid << "," << req->chunkId << ">"
			" " << req->Show() <<
			" ignored: " <<
			(it == mNonStableChunks.end() ?
				"not in progress" : "MCS in progress") <<
		KFS_LOG_EOM;
		return;
	}
	MakeChunkStableInfo& info = it->second;
	KFS_LOG_STREAM_DEBUG << logPrefix <<
		" <" << req->fid << "," << req->chunkId << ">"
		" name: "     << info.pathname <<
		" servers: "  << info.numAckMsg << "/" << info.numServers <<
		" size: "     << info.chunkSize <<
		" checksum: " << info.chunkChecksum <<
		" " << req->Show() <<
	KFS_LOG_EOM;
	CSMapConstIter ci = mChunkToServerMap.end();
	bool noSuchChunkFlag = false;
	if (req->status != 0 || req->chunkSize < 0) {
		if (req->status == 0 && req->chunkSize < 0) {
			KFS_LOG_STREAM_ERROR << logPrefix <<
				" <" << req->fid << "," << req->chunkId  << ">"
				" invalid chunk size: " << req->chunkSize <<
				" declaring chunk replica corrupt" <<
				" " << req->Show() <<
			KFS_LOG_EOM;
		}
		ci = mChunkToServerMap.find(req->chunkId);
		if (ci != mChunkToServerMap.end()) {
			vector<ChunkServerPtr>::const_iterator const si = find_if(
				ci->second.chunkServers.begin(),
				ci->second.chunkServers.end(),
				MatchingServer(req->serverLoc)
			);
			if (si != ci->second.chunkServers.end() &&
					! (*si)->IsDown()) {
				MetaChunkCorrupt cc(-1, req->fid, req->chunkId);
				cc.server = *si;
				ChunkCorrupt(&cc);
			}
		} else {
			noSuchChunkFlag = true;
		}
	} else if (req->chunkSize < info.chunkSize || info.chunkSize < 0) {
		// Pick the smallest good chunk.
		info.chunkSize     = req->chunkSize;
		info.chunkChecksum = req->chunkChecksum;
	}
	if (++info.numAckMsg < info.numServers) {
		return;
	}
	if (! noSuchChunkFlag && ci == mChunkToServerMap.end()) {
		ci = mChunkToServerMap.find(req->chunkId);
		noSuchChunkFlag = ci == mChunkToServerMap.end();
	}
	if (noSuchChunkFlag) {
		KFS_LOG_STREAM_DEBUG << logPrefix <<
			" <" << req->fid << "," << req->chunkId  << ">"
			" no such chunk, cleaning up" <<
		KFS_LOG_EOM;
		mNonStableChunks.erase(it);
		mPendingMakeStable.erase(req->chunkId);
		return;
	}
	info.beginMakeStableFlag    = false;
	info.logMakeChunkStableFlag = true;
	info.serverAddedFlag        = false;
	// Remember chunk check sum and size.
	PendingMakeStableEntry const pmse(
		info.chunkSize,
		info.chunkSize >= 0,
		info.chunkChecksum,
		req->chunkVersion
	);
	pair<PendingMakeStableMap::iterator, bool> const res =
		mPendingMakeStable.insert(make_pair(req->chunkId, pmse));
	assert(
		res.second ||
		(res.first->second.mSize < 0 &&
		res.first->second.mChunkVersion == pmse.mChunkVersion)
	);
	if (! res.second && pmse.mSize >= 0) {
		res.first->second = pmse;
	}
	if (res.first->second.mSize < 0) {
		int numUpServers = 0;
		for (vector<ChunkServerPtr>::const_iterator
				si = ci->second.chunkServers.begin();
				si != ci->second.chunkServers.end();
				++si) {
			if (! (*si)->IsDown()) {
				numUpServers++;
			}
		}
		if (numUpServers <= 0) {
			KFS_LOG_STREAM_DEBUG << logPrefix <<
				" <" << req->fid << "," << req->chunkId  << ">"
				" no servers up, retry later" <<
			KFS_LOG_EOM;
		} else {
			// Shouldn't get here.
			KFS_LOG_STREAM_WARN << logPrefix <<
				" <" << req->fid << "," << req->chunkId  << ">"
				" internal error:"
				" up servers: "         << numUpServers <<
				" invalid chunk size: " <<
					res.first->second.mSize <<
			KFS_LOG_EOM;
		}
		// Try again later.
		mNonStableChunks.erase(it);
		return;
	}
	submit_request(new MetaLogMakeChunkStable(
		req->fid, req->chunkId, req->chunkVersion,
		info.chunkSize, info.chunkSize >= 0, info.chunkChecksum,
		req->opSeqno
	));
}

void
LayoutManager::LogMakeChunkStableDone(const MetaLogMakeChunkStable* req)
{
	const char* const                  logPrefix = "LMCS: done";
	NonStableChunksMap::iterator const it        =
		mNonStableChunks.find(req->chunkId);
	if (it == mNonStableChunks.end() ||
			! it->second.logMakeChunkStableFlag) {
		KFS_LOG_STREAM_DEBUG << logPrefix <<
			" <" << req->fid << "," << req->chunkId  << ">" <<
			" " << req->Show() <<
			" ignored: " <<
			(it == mNonStableChunks.end() ?
				"not in progress" :
				it->second.beginMakeStableFlag ? "B" : "") <<
				"MCS in progress" <<
		KFS_LOG_EOM;
		return;
	}
	MakeChunkStableInfo& info = it->second;
	CSMapConstIter const ci = mChunkToServerMap.find(req->chunkId);
	if (ci == mChunkToServerMap.end() || ci->second.chunkServers.empty()) {
		KFS_LOG_STREAM_INFO << logPrefix <<
			" <" << req->fid << "," << req->chunkId  << ">" <<
			" name: " << info.pathname <<
			(ci == mChunkToServerMap.end() ?
				" does not exist, cleaning up" :
				" no servers, run MCS later") <<
		KFS_LOG_EOM;
		if (ci == mChunkToServerMap.end()) {
			// If chunk was deleted, do not emit mkstabledone log
			// entry. Only ensure that no stale pending make stable
			// entry exists.
			mPendingMakeStable.erase(req->chunkId);
		}
		mNonStableChunks.erase(it);
		return;
	}
	// Make a local copy of servers.
	// "Placement info" can change while iterating, if any servers go down
	// while broadcasting the request.
	const bool                   serverWasAddedFlag = info.serverAddedFlag;
	const int                    prevNumServer      = info.numServers;
	const vector<ChunkServerPtr> servers(ci->second.chunkServers);
	info.numServers             = (int)servers.size();
	info.numAckMsg              = 0;
	info.beginMakeStableFlag    = false;
	info.logMakeChunkStableFlag = false;
	info.serverAddedFlag        = false;
	info.chunkSize              = req->chunkSize;
	info.chunkChecksum          = req->chunkChecksum;
	KFS_LOG_STREAM_INFO << logPrefix <<
		" <" << req->fid << "," << req->chunkId  << ">"
		" starting MCS"
		" version: "  << req->chunkVersion  <<
		" name: "     << info.pathname <<
		" size: "     << info.chunkSize     <<
		" checksum: " << info.chunkChecksum <<
		" servers: "  << prevNumServer << "->" << info.numServers <<
		" "           << (serverWasAddedFlag ? "new servers" : "") <<
	KFS_LOG_EOM;
	if (serverWasAddedFlag && info.chunkSize < 0) {
		// Retry make chunk stable with newly added servers.
		info.beginMakeStableFlag = true;
		for_each(servers.begin(), servers.end(), BeginMakeChunkStable(
			ci->second.fid, req->chunkId, info.chunkVersion
		));
		return;
	}
	for_each(servers.begin(), servers.end(), MakeChunkStable(
		req->fid, req->chunkId, req->chunkVersion,
		req->chunkSize, req->hasChunkChecksum, req->chunkChecksum
	));
}

void
LayoutManager::MakeChunkStableDone(const MetaChunkMakeStable* req)
{
	const char* const                  logPrefix      = "MCS: done";
	string                             pathname;
	const ChunkPlacementInfo*          pinfo          = 0;
	bool                               updateSizeFlag = false;
	NonStableChunksMap::iterator const it             =
		mNonStableChunks.find(req->chunkId);
	if (req->addPending) {
		// Make chunk stable started in AddNotStableChunk() is now
		// complete. Sever can be added if nothing has changed since
		// the op was started.
		// It is also crucial to ensure to the server with the
		// identical location is not already present in the list of
		// servers hosting the chunk before declaring chunk stale.
		bool        notifyStaleFlag = true;
		const char* res             = 0;
		PendingMakeStableMap::iterator msi;
		if (it != mNonStableChunks.end()) {
			res = "not stable again";
		} else {
			msi = mPendingMakeStable.find(req->chunkId);
		}
		if (res) {
			// Has already failed.
		} else if (req->chunkSize >= 0 || req->hasChunkChecksum) {
			if (msi == mPendingMakeStable.end()) {
				// Chunk went away, or already sufficiently
				// replicated.
				res = "no pending make stable info";
			} else if (msi->second.mChunkVersion !=
						req->chunkVersion ||
					msi->second.mSize != req->chunkSize ||
					msi->second.mHasChecksum !=
						req->hasChunkChecksum ||
					msi->second.mChecksum !=
						req->chunkChecksum) {
				// Stale request.
				res = "pending make stable info has changed";
			}
		} else if (msi != mPendingMakeStable.end()) {
			res = "pending make stable info now exists";
		}
		if (req->server->IsDown()) {
			res = "server down";
			notifyStaleFlag = false;
		} else if (req->status != 0) {
			res = "request failed";
			notifyStaleFlag = false;
		} else {
			seq_t           chunkVersion = -1;
			CSMapIter const ci           =
				mChunkToServerMap.find(req->chunkId);
			if (ci == mChunkToServerMap.end()) {
				res = "no such chunk";
			} else if (find_if(
					ci->second.chunkServers.begin(),
					ci->second.chunkServers.end(),
					MatchingServer(
						req->server->GetServerLocation())
					) != ci->second.chunkServers.end()) {
				res = "already added";
				notifyStaleFlag = false;
			} else if (find_if(
					ci->second.chunkLeases.begin(),
					ci->second.chunkLeases.end(),
					ptr_fun(&LeaseInfo::IsWriteLease)
					) != ci->second.chunkLeases.end()) {
				// No write lease existed when this was started.
				res = "new write lease exists";
			} else if (metatree.getChunkVersion(
					ci->second.fid,
					req->chunkId,
					&chunkVersion
					) != 0 ||
					req->chunkVersion != chunkVersion) {
				res = "chunk version has changed";
			} else {
				pinfo          = &ci->second;
				updateSizeFlag = pinfo->chunkServers.empty();
				ci->second.chunkServers.push_back(req->server);
				/* if (updateSizeFlag) {
					pathname = metatree.getPathname(
						pinfo->fid);
				} */
				notifyStaleFlag = false;
			}
		}
		if (res) {
			KFS_LOG_STREAM_DEBUG << logPrefix <<
				" <" << req->fid << "," << req->chunkId  << ">"
				" "  << req->server->ServerID() <<
				" not added: " << res <<
				"; " << req->Show() <<
			KFS_LOG_EOM;
			if (notifyStaleFlag) {
				req->server->NotifyStaleChunk(req->chunkId);
			}
			// List of servers hosting the chunk remains unchanged.
			return;
		}
	} else {
		if (it == mNonStableChunks.end() ||
				it->second.beginMakeStableFlag ||
				it->second.logMakeChunkStableFlag) {
			KFS_LOG_STREAM_ERROR << "MCS"
				" " << req->Show() <<
				" ignored: BMCS in progress" <<
			KFS_LOG_EOM;
			return;
		}
		MakeChunkStableInfo& info = it->second;
		KFS_LOG_STREAM_DEBUG << logPrefix <<
			" <" << req->fid << "," << req->chunkId  << ">"
			" name: "     << info.pathname <<
			" servers: "  << info.numAckMsg <<
				"/" << info.numServers <<
			" size: "     << req->chunkSize <<
				"/" << info.chunkSize <<
			" checksum: " << req->chunkChecksum <<
				"/" << info.chunkChecksum <<
			" " << req->Show() <<
		KFS_LOG_EOM;
		if (req->status != 0 && ! req->server->IsDown()) {
			MetaChunkCorrupt cc(-1, req->fid, req->chunkId);
			cc.server = req->server;
			ChunkCorrupt(&cc);
		}
		if (++info.numAckMsg < info.numServers) {
			return;
		}
		// Cleanup mNonStableChunks, after the lease cleanup, for extra
		// safety: this will prevent make chunk stable from restarting
		// recursively, in the case if there are double or stale
		// write lease.
		ExpiredLeaseCleanup(req->chunkId);
		pathname = info.pathname;
		mNonStableChunks.erase(it);
		// "&info" is invalid at this point.
		updateSizeFlag = true;
	}
	if (! pinfo) {
		CSMapConstIter const ci = mChunkToServerMap.find(req->chunkId);
		if (ci == mChunkToServerMap.end()) {
			KFS_LOG_STREAM_INFO << logPrefix <<
				" <"      << req->fid <<
				","       << req->chunkId  << ">" <<
				" name: " << pathname <<
				" does not exist, skipping size update" <<
			KFS_LOG_EOM;
			return;
		}
		pinfo = &ci->second;
	}
	const fid_t    fileId         = pinfo->fid;
	int            numServers     = 0;
	int            numDownServers = 0;
	ChunkServerPtr goodServer;
	for (vector<ChunkServerPtr>::const_iterator csi =
				pinfo->chunkServers.begin();
			csi != pinfo->chunkServers.end();
			++csi) {
		if ((*csi)->IsDown()) {
			numDownServers++;
		} else {
			numServers++;
			if (! goodServer) {
				goodServer = *csi;
			}
		}
	}
	int replicas = -1;
	if (mChunkReplicationCandidates.find(req->chunkId) ==
			mChunkReplicationCandidates.end()) {
		const MetaFattr* const fa = metatree.getFattr(fileId);
		if (fa && (replicas = fa->numReplicas) != numServers) {
			mChunkReplicationCandidates.insert(req->chunkId);
		} else {
			CancelPendingMakeStable(fileId, req->chunkId);
		}
	}
	KFS_LOG_STREAM_INFO << logPrefix <<
		" <" << req->fid << "," << req->chunkId  << ">"
		" fid: "              << fileId <<
		" version: "          << req->chunkVersion  <<
		" name: "             << pathname <<
		" size: "             << req->chunkSize <<
		" checksum: "         << req->chunkChecksum <<
		" replicas: "         << replicas <<
		" is now stable on: " << numServers <<
		" down: "             << numDownServers <<
		" server(s)" <<
	KFS_LOG_EOM;
	if (! updateSizeFlag || numServers <= 0) {
		return; // if no servers, can not update size
	}
	if (req->chunkSize >= 0) {
		// Already know the size, update it.
		// The following will invoke GetChunkSizeDone(),
		// and update the log.
		MetaChunkSize* const op = new MetaChunkSize(
			0, // seq #
			0, // chunk server
			fileId, req->chunkId, pathname
		);
		op->chunkSize = req->chunkSize;
		submit_request(op);
	} else {
		// Get the chunk's size from one of the servers.
		goodServer->GetChunkSize(fileId, req->chunkId, pathname);
	}
}

void
LayoutManager::ReplayPendingMakeStable(
	chunkId_t chunkId,
	seq_t     chunkVersion,
	off_t     chunkSize,
	bool      hasChunkChecksum,
	uint32_t  chunkChecksum,
	bool      addFlag)
{
	const char*          res             = 0;
	seq_t                curChunkVersion = -1;
	CSMapConstIter const ci              = mChunkToServerMap.find(chunkId);
	MsgLogger::LogLevel logLevel = MsgLogger::kLogLevelDEBUG;
	if (ci == mChunkToServerMap.end()) {
		res = "no such chunk";
	} else if (metatree.getChunkVersion(
			ci->second.fid, chunkId, &curChunkVersion) != 0 ||
			curChunkVersion != chunkVersion) {
		res      = "chunk version mismatch";
		logLevel = MsgLogger::kLogLevelERROR;
	}
	if (res) {
		// Failure.
	} else if (addFlag) {
		const PendingMakeStableEntry entry(
			chunkSize,
			hasChunkChecksum,
			chunkChecksum,
			chunkVersion
		);
		pair<PendingMakeStableMap::iterator, bool> const res =
			mPendingMakeStable.insert(make_pair(chunkId, entry));
		if (! res.second) {
			KFS_LOG_STREAM((res.first->second.mHasChecksum ||
					res.first->second.mSize >= 0) ?
					MsgLogger::kLogLevelWARN :
					MsgLogger::kLogLevelDEBUG) <<
				"replay MCS add:" <<
				" update:"
				" chunkId: "  << chunkId <<
				" version: "  <<
					res.first->second.mChunkVersion <<
				"=>"          << entry.mChunkVersion <<
				" size: "     << res.first->second.mSize <<
				"=>"          << entry.mSize <<
				" checksum: " <<
					(res.first->second.mHasChecksum ?
					int64_t(res.first->second.mChecksum) :
					int64_t(-1)) <<
				"=>"          << (entry.mHasChecksum ?
					int64_t(entry.mChecksum) :
					int64_t(-1)) <<
			KFS_LOG_EOM;
			res.first->second = entry;
		}
	} else {
		PendingMakeStableMap::iterator const it =
			mPendingMakeStable.find(chunkId);
		if (it == mPendingMakeStable.end()) {
			res      = "no such entry";
			logLevel = MsgLogger::kLogLevelERROR;
		} else {
			const bool warn =
				it->second.mChunkVersion != chunkVersion ||
				(it->second.mSize >= 0 && (
					it->second.mSize != chunkSize ||
					it->second.mHasChecksum !=
						hasChunkChecksum ||
					(hasChunkChecksum &&
					it->second.mChecksum != chunkChecksum
				)));
			KFS_LOG_STREAM(warn ?
					MsgLogger::kLogLevelWARN :
					MsgLogger::kLogLevelDEBUG) <<
				"replay MCS remove:"
				" chunkId: "  << chunkId <<
				" version: "  << it->second.mChunkVersion <<
				"=>"          << chunkVersion <<
				" size: "     << it->second.mSize <<
				"=>"          << chunkSize <<
				" checksum: " << (it->second.mHasChecksum ?
					int64_t(it->second.mChecksum) :
					int64_t(-1)) <<
				"=>"          << (hasChunkChecksum ?
					int64_t(chunkChecksum) : int64_t(-1)) <<
			KFS_LOG_EOM;
			mPendingMakeStable.erase(it);
		}
	}
	KFS_LOG_STREAM(logLevel) <<
		"replay MCS: " <<
		(addFlag ? "add" : "remove") <<
		" "           << (res ? res : "ok") <<
		" total: "    << mPendingMakeStable.size() <<
		" chunkId: "  << chunkId <<
		" version: "  << chunkVersion <<
		" cur vers: " << curChunkVersion <<
		" size: "     << chunkSize <<
		" checksum: " << (hasChunkChecksum ?
			int64_t(chunkChecksum) : int64_t(-1)) <<
	KFS_LOG_EOM;
}

int
LayoutManager::WritePendingMakeStable(ostream& os) const
{
	// Write all entries in restore_makestable() format.
	for (PendingMakeStableMap::const_iterator it =
				mPendingMakeStable.begin();
			it != mPendingMakeStable.end() && os;
			++it) {
		os <<
			"mkstable"
			"/chunkId/"      << it->first <<
			"/chunkVersion/" << it->second.mChunkVersion  <<
			"/size/"         << it->second.mSize <<
			"/checksum/"     << it->second.mChecksum <<
			"/hasChecksum/"  << (it->second.mHasChecksum ? 1 : 0) <<
		"\n";
	}
	return (os ? 0 : -EIO);
}

void
LayoutManager::CancelPendingMakeStable(fid_t fid, chunkId_t chunkId)
{
	PendingMakeStableMap::iterator const it =
		mPendingMakeStable.find(chunkId); 
	if (it == mPendingMakeStable.end()) {
		return;
	}
	NonStableChunksMap::iterator const nsi = mNonStableChunks.find(chunkId);
	if (nsi != mNonStableChunks.end()) {
		KFS_LOG_STREAM_ERROR << 
			"delete pending MCS:"
			" <" << fid << "," << chunkId << ">" <<
			" attempt to delete while " <<
			(nsi->second.beginMakeStableFlag ? "B" :
				(nsi->second.logMakeChunkStableFlag ? "L" : "")) <<
			"MCS is in progress denied" <<
		KFS_LOG_EOM;
		return;
	}
	// Emit done log record -- this "cancels" "mkstable" log record.
	// Do not write if begin make stable wasn't started before the
	// chunk got deleted.
	MetaLogMakeChunkStableDone* const op =
		(it->second.mSize < 0 || it->second.mChunkVersion < 0) ? 0 :
		new MetaLogMakeChunkStableDone(
			fid, chunkId, it->second.mChunkVersion,
			it->second.mSize, it->second.mHasChecksum,
			it->second.mChecksum, chunkId
		);
	mPendingMakeStable.erase(it);
	mPendingBeginMakeStable.erase(chunkId);
	KFS_LOG_STREAM_DEBUG <<
		"delete pending MCS:"
		" <" << fid << "," << chunkId << ">" <<
		" total: " << mPendingMakeStable.size() <<
		" " << (op ? op->Show() : string("size < 0")) <<
	KFS_LOG_EOM;
	if (op) {
		submit_request(op);
	}
}

int
LayoutManager::GetChunkSizeDone(MetaChunkSize* req)
{
	MetaFattr * const fa = metatree.getFattr(req->fid);
	if (! fa || fa->type != KFS_FILE) {
		return 0;
	}
	vector<MetaChunkInfo*> chunkInfo;
        const int status = metatree.getalloc(fa->id(), chunkInfo);
	off_t spaceUsageDelta = 0;

        if ((status != 0) || chunkInfo.empty()) {
		// don't write out a log entry
                return -1;
        }
	if (fa->filesize > 0) {
		// stash the value for doing the delta calc
		spaceUsageDelta = fa->filesize;
	}
	// only if we are looking at the last chunk of the file can we
	// set the size.
        MetaChunkInfo* lastChunk = chunkInfo.back();
	off_t sizeEstimate = fa->filesize;
	if (req->chunkId == lastChunk->chunkId) {
		sizeEstimate = (fa->chunkcount - 1) * CHUNKSIZE +
				req->chunkSize;
		fa->filesize = sizeEstimate;
	}
	if (fa->filesize == 0) {
		// 0-length files are strange beasts: they typically
		// shouldn't happen (unless they are kept to exchange
		// status between processes); if we have such a file,
		// ask the client to work out the size.
		fa->filesize = -1;
		return -1;
	}
	KFS_LOG_STREAM_INFO <<
		"For file: "  << req->fid <<
		" chunk: "    << req->chunkId <<
		" got size: " << req->chunkSize <<
		" filesize: " << fa->filesize <<
	KFS_LOG_EOM;

	// fa->filesize = max(fa->filesize, sizeEstimate);
	// stash the value away so that we can log it.
	req->filesize = fa->filesize;
	//
	// we possibly updated fa->filesize; so, compute the delta from
	// before and after
	//
	spaceUsageDelta = fa->filesize - spaceUsageDelta;
	if (spaceUsageDelta != 0) {
		/*
		if (pathname == "") {
			pathname = metatree.getPathname(fid);
		}
		*/
		if (! req->pathname.empty()) {
			metatree.updateSpaceUsageForPath(req->pathname, spaceUsageDelta);
		} else {
			KFS_LOG_STREAM_INFO <<
                            "Got size " << req->chunkSize <<
                            " for file " << req->fid << ", chunk " << req->chunkId <<
                            "; Will need to force recomputation of dir size" <<
                        KFS_LOG_EOM;
			// don't write out a log entry
			// status = -1;
		}
	}
	return 0;
}

bool
LayoutManager::IsChunkStable(chunkId_t chunkId)
{
	return (mNonStableChunks.find(chunkId) == mNonStableChunks.end());
}


class RetiringServerPred {
public:
	RetiringServerPred() { }
	bool operator()(const ChunkServerPtr &c) {
		return c->IsRetiring();
	}
};

class ReplicationDoneNotifier {
	chunkId_t cid;
public:
	ReplicationDoneNotifier(chunkId_t c) : cid(c) { }
	void operator()(ChunkServerPtr &s) {
		s->EvacuateChunkDone(cid);
	}
};

class RackSetter {
	set<int> &racks;
	bool excludeRetiringServers;
public:
	RackSetter(set<int> &r, bool excludeRetiring = false) :
		racks(r), excludeRetiringServers(excludeRetiring) { }
	void operator()(const ChunkServerPtr &s) {
		if (excludeRetiringServers && s->IsRetiring())
			return;
		racks.insert(s->GetRack());
	}
};

int
LayoutManager::ReplicateChunk(chunkId_t chunkId, ChunkPlacementInfo &clli,
				uint32_t extraReplicas)
{
	vector<int> racks;
	set<int> excludeRacks;
	// find a place
	vector<ChunkServerPtr> candidates;

	// two steps here: first, exclude the racks on which chunks are already
	// placed; if we can't find a unique rack, then put it wherever
	// for accounting purposes, ignore the rack(s) which contain a retiring
	// chunkserver; we'd like to move the block within that rack if
	// possible.

	for_each(clli.chunkServers.begin(), clli.chunkServers.end(),
		RackSetter(excludeRacks, true));

	FindCandidateRacks(racks, excludeRacks);
	if (racks.size() == 0) {
		// no new rack is available to put the chunk
		// take what we got
		FindCandidateRacks(racks);
		if (racks.size() == 0)
			// no rack is available
			return 0;
	}

	uint32_t numServersPerRack = extraReplicas / racks.size();
	if (extraReplicas % racks.size())
		numServersPerRack++;

	for (uint32_t idx = 0; idx < racks.size(); idx++) {
		if (candidates.size() >= extraReplicas)
			break;
		vector<ChunkServerPtr> servers;

		// find candidates other than those that are already hosting the
		// chunk
		FindCandidateServers(servers, clli.chunkServers, racks[idx]);

		// take as many as we can from this rack
		for (uint32_t i = 0; i < servers.size() && i < numServersPerRack; i++) {
			if (candidates.size() >= extraReplicas)
				break;
			candidates.push_back(servers[i]);
		}
	}

	if (candidates.size() == 0)
		return 0;

	return ReplicateChunk(chunkId, clli, extraReplicas, candidates);
}

int
LayoutManager::ReplicateChunk(chunkId_t chunkId, ChunkPlacementInfo &clli,
	uint32_t extraReplicas, const vector<ChunkServerPtr> &candidates)
{
	vector<MetaChunkInfo *> v;
	vector<MetaChunkInfo *>::iterator chunk;
	fid_t fid = clli.fid;
	int numDone = 0;

	/*
	metatree.getalloc(fid, v);
	chunk = find_if(v.begin(), v.end(), ChunkIdMatcher(chunkId));
	if (chunk == v.end()) {
		panic("missing chunk", true);
	}

	MetaChunkInfo *mci = *chunk;
	*/

	for (uint32_t i = 0; i < candidates.size() && i < extraReplicas; i++) {

		ChunkServerPtr const c = candidates[i];
		// Don't send too many replications to a server
		if (c->IsDown() ||
				c->GetNumChunkReplications() >
				MAX_CONCURRENT_WRITE_REPLICATIONS_PER_NODE)
			continue;
		// verify that we got good candidates
		assert(
			find(clli.chunkServers.begin(), clli.chunkServers.end(), c) ==
			clli.chunkServers.end()
		);
		// prefer a server that is being retired to the other nodes as
		// the source of the chunk replication
		vector<ChunkServerPtr>::const_iterator const iter = find_if(
			clli.chunkServers.begin(), clli.chunkServers.end(),
			RetiringServerPred());

		const char *reason;
		ChunkServerPtr dataServer;
		if (iter != clli.chunkServers.end()) {
			reason = " evacuating chunk ";
			if (((*iter)->GetReplicationReadLoad() <
				MAX_CONCURRENT_READ_REPLICATIONS_PER_NODE) &&
				(*iter)->IsResponsiveServer())
				dataServer = *iter;
		} else {
			reason = " re-replication ";
		}

		// if we can't find a retiring server, pick a server that has read b/w available
		for (uint32_t j = 0; (!dataServer) &&
				(j < clli.chunkServers.size()); j++) {
			if ((clli.chunkServers[j]->GetReplicationReadLoad() >=
				MAX_CONCURRENT_READ_REPLICATIONS_PER_NODE) ||
				(!(clli.chunkServers[j]->IsResponsiveServer())))
				continue;
			dataServer = clli.chunkServers[j];
		}
		if (dataServer) {
			ServerLocation srcLocation = dataServer->GetServerLocation();
			ServerLocation dstLocation = c->GetServerLocation();
			KFS_LOG_STREAM_INFO <<
				"Starting re-replication for chunk " << chunkId <<
				" from: " << srcLocation.ToString() <<
				" to " << dstLocation.ToString() <<
				" reason: " << reason <<
			KFS_LOG_EOM;
			dataServer->UpdateReplicationReadLoad(1);
			// have the chunkserver get the version
			assert(clli.ongoingReplications >= 0 && mNumOngoingReplications >= 0);
			// Bump counters here, completion can be invoked
			// immediately, for example when send fails.
			mNumOngoingReplications++;
			clli.ongoingReplications++;
			mLastChunkReplicated = chunkId;
			mOngoingReplicationStats->Update(1);
			mTotalReplicationStats->Update(1);
			c->ReplicateChunk(fid, chunkId, -1,
				dataServer->GetServerLocation());
			numDone++;
		}
		dataServer.reset();
	}
	return numDone;
}

bool
LayoutManager::CanReplicateChunkNow(chunkId_t chunkId,
				ChunkPlacementInfo &c,
				int &extraReplicas,
				bool &noSuchChunkFlag)
{
	noSuchChunkFlag = false;
	extraReplicas = 0;
	// Don't replicate chunks for which a write lease
	// has been issued.
	vector<LeaseInfo>::iterator const l = find_if(
		c.chunkLeases.begin(), c.chunkLeases.end(),
		ptr_fun(&LeaseInfo::IsWriteLease));

	if (l != c.chunkLeases.end()) {
		KFS_LOG_STREAM_DEBUG <<
			"re-replication delayed chunk:"
			" <" << c.fid << "," << chunkId << ">"
			" " << (LeaseInfo::IsValidWriteLease(*l) ?
				"valid" : "expired") <<
			" write lease exists" <<
		KFS_LOG_EOM;
		return false;
	}
	if (! IsChunkStable(chunkId)) {
		KFS_LOG_STREAM_DEBUG <<
			"re-replication delayed chunk:"
			" <" << c.fid << "," << chunkId << ">"
			" is not stable yet" <<
		KFS_LOG_EOM;
		return false;
	}

	// Can't re-replicate a chunk if we don't have a copy! so,
	// take out this chunk from the candidate set.
	if (c.chunkServers.empty()) {
		KFS_LOG_STREAM_DEBUG <<
			"can not re-replicate chunk:"
			" <" << c.fid << "," << chunkId << ">"
			" no copies left,"
			" canceling re-replication" <<
		KFS_LOG_EOM;
		return true;
	}
	// check if the chunk still exists
	vector<MetaChunkInfo *> v;
	metatree.getalloc(c.fid, v);
	vector<MetaChunkInfo *>::iterator const chunk = find_if(
		v.begin(), v.end(), ChunkIdMatcher(chunkId));
	if (chunk == v.end()) {
		// This chunk doesn't exist in this file anymore.
		// So, take out this chunk from the candidate set.
		KFS_LOG_STREAM_ERROR <<
			"can not re-replicate chunk:"
			" <" << c.fid << "," << chunkId << ">"
			" no such chunk,"
			" possible stale chunk to server mapping," <<
			" canceling re-replication" <<
		KFS_LOG_EOM;
		noSuchChunkFlag = true;
		return true;
	}
	MetaFattr * const fa = metatree.getFattr(c.fid);
	if (! fa) {
		// No file attr.  So, take out this chunk
		// from the candidate set.
		KFS_LOG_STREAM_ERROR <<
			"can not re-replicate chunk:"
			" <" << c.fid << "," << chunkId << ">"
			" no file attribute exists,"
			" canceling re-replication" <<
		KFS_LOG_EOM;
		return true;
	}
	// if any of the chunkservers are retiring, we need to make copies
	// so, first determine how many copies we need because one of the
	// servers hosting the chunk is going down
	// May need to re-replicate this chunk:
	//    - extraReplicas > 0 means make extra copies;
	//    - extraReplicas == 0, take out this chunkid from the candidate set
	//    - extraReplicas < 0, means we got too many copies; delete some
	const int numRetiringServers = (int)count_if(
		c.chunkServers.begin(), c.chunkServers.end(),
		RetiringServerPred());
	// now, determine if we have sufficient copies
	if (numRetiringServers <= 0 ||
		c.chunkServers.size() <=
			(size_t)(fa->numReplicas + numRetiringServers)) {
		// we need to make this many copies: # of servers that are
		// retiring plus the # this chunk is under-replicated
		extraReplicas = (fa->numReplicas + numRetiringServers) -
			c.chunkServers.size();
	}
	//
	// If additional copies need to be deleted, check if there is a valid
	// (read) lease issued on the chunk. In case if lease exists leave the
	// chunk alone for now; we'll look at deleting it when the lease has
	// expired.  This is for safety: if a client was reading from the copy
	// of the chunk that we are trying to delete, the client will see the
	// deletion and will have to failover; avoid unnecessary failovers
	//
	const bool ret = extraReplicas >= 0 || find_if(
		c.chunkLeases.begin(), c.chunkLeases.end(),
		ptr_fun(LeaseInfo::IsValidLease)
	) == c.chunkLeases.end();
	KFS_LOG_STREAM_DEBUG <<
		"re-replicate: chunk:"
		" <" << c.fid << "," << chunkId << ">"
		" retiring: " << numRetiringServers <<
		" target: "   << fa->numReplicas <<
		" needed: "   << extraReplicas <<
		(ret ? " start now" : " check later") <<
 	KFS_LOG_EOM;
	return ret;
}

class EvacuateChunkChecker {
	ReplicationCandidates &candidates;
	CSMap &chunkToServerMap;
public:
	EvacuateChunkChecker(ReplicationCandidates &c, CSMap &m) :
		candidates(c), chunkToServerMap(m) {}
	void operator()(ChunkServerPtr c) {
		if (!c->IsRetiring())
			return;
		CRCandidateSet leftover = c->GetEvacuatingChunks();
		for (CRCandidateSetIter citer = leftover.begin(); citer !=
			leftover.end(); ++citer) {
			const chunkId_t chunkId = *citer;
			CSMapIter iter = chunkToServerMap.find(chunkId);

			if (iter == chunkToServerMap.end()) {
				c->EvacuateChunkDone(chunkId);
				KFS_LOG_STREAM_INFO <<
					c->GetServerLocation().ToString() <<
					" has bogus block " << chunkId <<
				KFS_LOG_EOM;
			} else {
				// XXX
				// if we don't think this chunk is on this
				// server, then we should update the view...

				candidates.insert(chunkId);
				KFS_LOG_STREAM_INFO <<
					c->GetServerLocation().ToString() <<
					" has block " << chunkId  <<
					" that wasn't in replication candidates" <<
				KFS_LOG_EOM;
			}
		}
	}
};

void
LayoutManager::CheckHibernatingServersStatus()
{
	const time_t now = TimeNow();

	vector <HibernatingServerInfo_t>::iterator iter = mHibernatingServers.begin();
	vector<ChunkServerPtr>::iterator i;

	while (iter != mHibernatingServers.end()) {
		i = find_if(mChunkServers.begin(), mChunkServers.end(),
			MatchingServer(iter->location));
		if ((i == mChunkServers.end()) && (now < iter->sleepEndTime)) {
			// within the time window where the server is sleeping
			// so, move on
			iter++;
			continue;
		}
		if (i != mChunkServers.end()) {
			KFS_LOG_STREAM_INFO <<
				"Hibernated server " << iter->location.ToString()  <<
				" is back as promised" <<
			KFS_LOG_EOM;
		} else {
			// server hasn't come back as promised...so, check
			// re-replication for the blocks that were on that node
			KFS_LOG_STREAM_INFO <<
				"Hibernated server " << iter->location.ToString()  <<
				" is NOT back as promised" <<
			KFS_LOG_EOM;
			if (mChunkReplicationCandidates.empty()) {
				mChunkReplicationCandidates.swap(iter->blocks);
			} else {
				mChunkReplicationCandidates.insert(
					iter->blocks.begin(), iter->blocks.end());
			}
		}
		iter = mHibernatingServers.erase(iter);
	}
}

bool
LayoutManager::IsAnyServerAvailForReReplication() const
{
	int anyAvail = 0;
	for (uint32_t i = 0; i < mChunkServers.size(); i++) {
		const ChunkServerPtr c = mChunkServers[i];
		if (c->GetSpaceUtilization() > MAX_SERVER_SPACE_UTIL_THRESHOLD)
			continue;
		if (c->GetNumChunkReplications() > MAX_CONCURRENT_WRITE_REPLICATIONS_PER_NODE)
			continue;
		anyAvail++;
	}
	return anyAvail > 0;
}

void
LayoutManager::HandoutChunkReplicationWork(CRCandidateSet &candidates,
						CRCandidateSet &delset)
{
	// There is a set of chunks that are affected: their server went down
	// or there is a change in their degree of replication.  in either
	// case, walk this set of chunkid's and work on their replication amount.

	struct timeval start;

	const int kCheckTime = 32;
	int       pass       = 0;

	for (ReplicationCandidates::const_iterator citer = candidates.begin();
		citer != candidates.end(); ++citer) {
		if (--pass <= 0) {
			struct timeval now;
			gettimeofday(&now, 0);
			if (pass < 0) {
				start = now;
			}
			pass = kCheckTime;
			if (ComputeTimeDiff(start, now) > MAX_TIME_FOR_CHUNK_REPLICATION_CHECK)
				// if we have spent more than 1 second here, stop
				// serve other requests
				break;
		}

		if (!IsAnyServerAvailForReReplication())
			break;

		const chunkId_t chunkId = *citer;
        	CSMapIter const iter    = mChunkToServerMap.find(chunkId);
        	if (iter == mChunkToServerMap.end()) {
			delset.insert(chunkId);
			continue;
		}
		// if the chunk is already in the delset, don't process it
		// further
		ReplicationCandidates::const_iterator alreadyDeleted = delset.find(chunkId);
		if (alreadyDeleted != delset.end())
			continue;

		if (iter->second.ongoingReplications > 0)
			// this chunk is being re-replicated; we'll check later
			continue;

		int  extraReplicas   = 0;
		bool noSuchChunkFlag = false;
		if (!CanReplicateChunkNow(iter->first, iter->second,
				extraReplicas, noSuchChunkFlag))
			continue;

		if (noSuchChunkFlag) {
			// Delete stale mapping.
			delset.insert(chunkId);
			mChunkToServerMap.erase(iter);
		} else if (extraReplicas > 0) {
			ReplicateChunk(iter->first, iter->second, extraReplicas);
		} else if (extraReplicas == 0) {
			delset.insert(chunkId);
		} else {
			DeleteAddlChunkReplicas(iter->first, iter->second, -extraReplicas);
			delset.insert(chunkId);
		}
		if (mNumOngoingReplications > (int64_t)mChunkServers.size() *
			MAX_CONCURRENT_WRITE_REPLICATIONS_PER_NODE)
			// throttle...we are handing out
			break;
	}
}

void
LayoutManager::ChunkReplicationChecker()
{
	if (! mPendingBeginMakeStable.empty() && ! InRecoveryPeriod()) {
		ProcessPendingBeginMakeStable();
	}
	if (InRecovery()) {
		return;
	}

	CheckHibernatingServersStatus();

	CRCandidateSet delset;
	HandoutChunkReplicationWork(mPriorityChunkReplicationCandidates, delset);
	HandoutChunkReplicationWork(mChunkReplicationCandidates, delset);

	for (CRCandidateSet::const_iterator citer = delset.begin();
			citer != delset.end();
			++citer) {
		// Notify the retiring servers of any of their chunks have
		// been evacuated---such as, if there were too many copies of those
		// chunks, we are done evacuating them
		const chunkId_t chunkId = *citer;
        	CSMapIter const iter = mChunkToServerMap.find(chunkId);
		if (iter != mChunkToServerMap.end() &&
				! iter->second.chunkServers.empty()) {
			for_each(iter->second.chunkServers.begin(),
				iter->second.chunkServers.end(),
				ReplicationDoneNotifier(chunkId));
			// Sufficient replicas, now no need to make chunk
			// stable.
			CancelPendingMakeStable(iter->second.fid, *citer);
		}
		mPriorityChunkReplicationCandidates.erase(*citer);
		mChunkReplicationCandidates.erase(*citer);
	}

	if (mChunkReplicationCandidates.empty()) {
		mPriorityChunkReplicationCandidates.clear();
		// if there are any retiring servers, we need to make sure that
		// the servers don't think there is a block to be replicated
		// if there is any such, let us get them into the set of
		// candidates...need to know why this happens
		for_each(mChunkServers.begin(), mChunkServers.end(),
			EvacuateChunkChecker(mChunkReplicationCandidates, mChunkToServerMap));
	}

	RebalanceServers();

	mReplicationTodoStats->Set(mChunkReplicationCandidates.size());
	mChunksWithOneReplicaStats->Set(mPriorityChunkReplicationCandidates.size());
}

void
LayoutManager::FindReplicationWorkForServer(ChunkServerPtr &server, chunkId_t chunkReplicated)
{
	vector<ChunkServerPtr> c;

	if (server->IsRetiring())
		return;

	c.push_back(server);

	// try to start where we were done with this server
	ReplicationCandidates::iterator citer =
		mChunkReplicationCandidates.find(chunkReplicated + 1);
	if (citer == mChunkReplicationCandidates.end()) {
		// try to start where we left off last time; if that chunk has
		// disappeared, find something "closeby"
		citer = mChunkReplicationCandidates.upper_bound(mLastChunkReplicated);
	}

	if (citer == mChunkReplicationCandidates.end()) {
		mLastChunkReplicated = 1;
		citer = mChunkReplicationCandidates.begin();
	}

	struct timeval start, now;

	gettimeofday(&start, NULL);

	for (; citer != mChunkReplicationCandidates.end(); ++citer) {
		gettimeofday(&now, NULL);

		if (ComputeTimeDiff(start, now) > MAX_TIME_TO_FIND_ADDL_REPLICATION_WORK)
			// if we have spent more than 5 m-seconds here, stop
			// serve other requests
			break;

		const chunkId_t chunkId = *citer;
        	CSMapIter iter = mChunkToServerMap.find(chunkId);
        	if (iter == mChunkToServerMap.end())
			continue;

		if (iter->second.ongoingReplications > 0)
			continue;

		// if the chunk is already hosted on this server, the chunk isn't a candidate for
		// work to be sent to this server.
		int  extraReplicas   = 0;
		bool noSuchChunkFlag = false;
		if (IsChunkHostedOnServer(iter->second.chunkServers, server) ||
				(!CanReplicateChunkNow(iter->first, iter->second,
					extraReplicas, noSuchChunkFlag)))
			continue;

		if (noSuchChunkFlag) {
			mChunkToServerMap.erase(iter);
		} else if (extraReplicas > 0) {
			if (mRacks.size() > 1) {
				// when there is more than one rack, since we
				// are re-replicating a chunk, we don't want to put two
				// copies of a chunk on the same rack.  if one
				// of the nodes is retiring, we don't want to
				// count the node in this rack set---we want to
				// put a block on to the same rack.
				set<int> excludeRacks;
				for_each(iter->second.chunkServers.begin(), iter->second.chunkServers.end(),
					RackSetter(excludeRacks, true));
				if (excludeRacks.find(server->GetRack()) != excludeRacks.end())
					continue;
			}
			ReplicateChunk(iter->first, iter->second, 1, c);
		} else if (! iter->second.chunkServers.empty()) {
			CancelPendingMakeStable(iter->second.fid, iter->first);
		}

		if (server->GetNumChunkReplications() > MAX_CONCURRENT_WRITE_REPLICATIONS_PER_NODE)
			break;
	}
	// if there is any room left to do more work...
	ExecuteRebalancePlan(server);
}

void
LayoutManager::ChunkReplicationDone(MetaChunkReplicate *req)
{
	KFS_LOG_STREAM_DEBUG <<
		"Meta chunk replicate finished for"
		" chunk: "   << req->chunkId <<
		" version: " << req->chunkVersion <<
		" status: "  << req->status <<
		" replications in flight: " << mNumOngoingReplications <<
		" server: " << req->server->ServerID() <<
		" " << (req->server->IsDown() ? "down" : "OK") <<
	KFS_LOG_EOM;

	mOngoingReplicationStats->Update(-1);
	assert(mNumOngoingReplications > 0);
	mNumOngoingReplications--;

	// Book-keeping....
	int             chunkInFlight;
	CSMapIter const iter = mChunkToServerMap.find(req->chunkId);
	if (iter != mChunkToServerMap.end()) {
		assert(iter->second.ongoingReplications > 0);
		iter->second.ongoingReplications--;
		chunkInFlight = iter->second.ongoingReplications;
	} else {
		chunkInFlight = -1;
		KFS_LOG_STREAM_INFO <<
			"chunk " << req->chunkId << " mapping no longer exists" <<
		KFS_LOG_EOM;
	}

	req->server->ReplicateChunkDone(req->chunkId);
	vector<ChunkServerPtr>::iterator const source = find_if(
		mChunkServers.begin(), mChunkServers.end(),
		MatchingServer(req->srcLocation));
	if (source !=  mChunkServers.end()) {
		(*source)->UpdateReplicationReadLoad(-1);
	}

	if (req->status != 0) {
		// Replication failed...we will try again later
		KFS_LOG_STREAM_INFO <<
			req->server->GetServerLocation().ToString() <<
			": re-replication for chunk " << req->chunkId <<
			" failed, status: " << req->status <<
			" chunk replications in flight: " << chunkInFlight <<
		KFS_LOG_EOM;
		mFailedReplicationStats->Update(1);
		return;
	}
	// replication succeeded: book-keeping

	// if any of the hosting servers were being "retired", notify them that
	// re-replication of any chunks hosted on them is finished
	if (iter != mChunkToServerMap.end()) {
		for_each(iter->second.chunkServers.begin(), iter->second.chunkServers.end(),
			ReplicationDoneNotifier(req->chunkId));
	}

	// validate that the server got the latest copy of the chunk
	vector<MetaChunkInfo *> v;
	metatree.getalloc(iter->second.fid, v);
	vector<MetaChunkInfo *>::const_iterator const chunk = find_if(
            v.begin(), v.end(), ChunkIdMatcher(req->chunkId));
	if (chunk == v.end()) {
		// Chunk disappeared -> stale; this chunk will get nuked
		KFS_LOG_STREAM_INFO <<
			req->server->GetServerLocation().ToString() <<
			" re-replicate: chunk " << req->chunkId <<
			" disappeared => stale" <<
		KFS_LOG_EOM;
		mFailedReplicationStats->Update(1);
		req->server->NotifyStaleChunk(req->chunkId);
		return;
	}
	const MetaChunkInfo *mci = *chunk;
	if (mci->chunkVersion != req->chunkVersion) {
		// Version that we replicated has changed...so, stale
		KFS_LOG_STREAM_INFO <<
			req->server->GetServerLocation().ToString() <<
			" re-replicate: chunk " << req->chunkId <<
			" version changed was: " << req->chunkVersion <<
			" now " << mci->chunkVersion << " => stale" <<
		KFS_LOG_EOM;
		mFailedReplicationStats->Update(1);
		req->server->NotifyStaleChunk(req->chunkId);
		return;
	}

	// Yaeee...all good...
	KFS_LOG_STREAM_DEBUG <<
		req->server->GetServerLocation().ToString() <<
		" reports that re-replication for chunk " << req->chunkId <<
		" is all done" <<
	KFS_LOG_EOM;
	UpdateChunkToServerMapping(req->chunkId, req->server.get());

	// since this server is now free, send more work its way...
	if (req->server->GetNumChunkReplications() <= 1) {
		FindReplicationWorkForServer(req->server, req->chunkId);
	}
}

//
// To delete additional copies of a chunk, find the servers that have the least
// amount of space and delete the chunk from there.  In addition, also pay
// attention to rack-awareness: if two copies are on the same rack, then we pick
// the server that is the most loaded and delete it there
//
void
LayoutManager::DeleteAddlChunkReplicas(chunkId_t chunkId, ChunkPlacementInfo &clli,
				uint32_t extraReplicas)
{
	vector<ChunkServerPtr> servers = clli.chunkServers, copiesToDiscard;
	uint32_t numReplicas = servers.size() - extraReplicas;
	set<int> chosenRacks;

	// if any of the copies are on nodes that are retiring, leave the copies
	// alone; we will reclaim space later if needed
        int numRetiringServers = count_if(servers.begin(), servers.end(),
					RetiringServerPred());
	if (numRetiringServers > 0)
		return;

	// We get servers sorted by increasing amount of space utilization; so the candidates
	// we want to delete are at the end
	SortServersByUtilization(servers);

	for_each(servers.begin(), servers.end(), RackSetter(chosenRacks));
	if (chosenRacks.size() == numReplicas) {
		// we need to keep as many copies as racks.  so, find the extra
		// copies on a given rack and delete them
		clli.chunkServers.clear();
		chosenRacks.clear();
		for (uint32_t i = 0; i < servers.size(); i++) {
			if (chosenRacks.find(servers[i]->GetRack()) ==
				chosenRacks.end()) {
				chosenRacks.insert(servers[i]->GetRack());
				clli.chunkServers.push_back(servers[i]);
			} else {
				// second copy on the same rack
				copiesToDiscard.push_back(servers[i]);
			}
		}
	} else {
		clli.chunkServers = servers;
		// Get rid of the extra stuff from the end
		clli.chunkServers.resize(numReplicas);

		// The first N are what we want to keep; the rest should go.
		copiesToDiscard.insert(copiesToDiscard.end(), servers.begin() + numReplicas, servers.end());
	}
	mChunkToServerMap[chunkId] = clli;

	ostringstream msg;
	msg << "Chunk " << chunkId << " lives on:\n";
	for (uint32_t i = 0; i < clli.chunkServers.size(); i++) {
		msg << clli.chunkServers[i]->GetServerLocation().ToString() << ' '
			<< clli.chunkServers[i]->GetRack() << "; ";
	}
	msg << "\nDiscarding chunk on: ";
	for (uint32_t i = 0; i < copiesToDiscard.size(); i++) {
		msg << copiesToDiscard[i]->GetServerLocation().ToString() << ' '
			<< copiesToDiscard[i]->GetRack() << " ";
	}
	msg << "\n";
	KFS_LOG_STREAM_INFO << msg.str() << KFS_LOG_EOM;

	for_each(copiesToDiscard.begin(), copiesToDiscard.end(), ChunkDeletor(chunkId));
}

void
LayoutManager::ChangeChunkReplication(chunkId_t chunkId)
{
	mChunkReplicationCandidates.insert(chunkId);
}

//
// Check if the server is part of the set of the servers hosting the chunk
//
bool
LayoutManager::IsChunkHostedOnServer(const vector<ChunkServerPtr> &hosters,
					const ChunkServerPtr &server)
{
	vector<ChunkServerPtr>::const_iterator iter;
	iter = find(hosters.begin(), hosters.end(), server);
	return iter != hosters.end();
}

class LoadedServerPred {
	double maxServerSpaceUtilThreshold;
public:
	LoadedServerPred(double m) : maxServerSpaceUtilThreshold(m) { }
	bool operator()(const ChunkServerPtr &s) const {
		return s->GetSpaceUtilization() > maxServerSpaceUtilThreshold;
	}
};

//
// We are trying to move a chunk between two servers on the same rack.  For a
// given chunk, we try to find as many "migration pairs" (source/destination
// nodes) within the respective racks.
//
void
LayoutManager::FindIntraRackRebalanceCandidates(vector<ChunkServerPtr> &candidates,
			const vector<ChunkServerPtr> &nonloadedServers,
			const ChunkPlacementInfo &clli)
{
	vector<ChunkServerPtr>::const_iterator iter;

	for (uint32_t i = 0; i < clli.chunkServers.size(); i++) {
		vector<ChunkServerPtr> servers;

		if (clli.chunkServers[i]->GetSpaceUtilization() <
			mMaxRebalanceSpaceUtilThreshold) {
			continue;
		}
		//we have a loaded server; find another non-loaded
		//server within the same rack (case 1 from above)
		FindCandidateServers(servers, nonloadedServers, clli.chunkServers,
					clli.chunkServers[i]->GetRack());
		if (servers.size() == 0) {
			// nothing available within the rack to do the move
			continue;
		}
		// make sure that we are not putting 2 copies of a chunk on the
		// same server
		for (uint32_t j = 0; j < servers.size(); j++) {
			iter = find(candidates.begin(), candidates.end(), servers[j]);
			if (iter == candidates.end()) {
				candidates.push_back(servers[j]);
				break;
			}
		}
	}
}


//
// For rebalancing, for a chunk, we could not find a candidate server on the same rack as a
// loaded server.  Hence, we are trying to move the chunk between two servers on two
// different racks.  So, find a migration pair: source/destination on two different racks.
//
void
LayoutManager::FindInterRackRebalanceCandidate(ChunkServerPtr &candidate,
			const vector<ChunkServerPtr> &nonloadedServers,
			const ChunkPlacementInfo &clli)
{
	vector<ChunkServerPtr> servers;

	FindCandidateServers(servers, nonloadedServers, clli.chunkServers);
	if (servers.size() == 0) {
		return;
	}
	// XXX: in emulation mode, we have 0 racks due to compile issues
	if (mRacks.size() <= 1)
		return;

	// if we had only one rack then the intra-rack move should have found a
	// candidate.
	assert(mRacks.size() > 1);
	if (mRacks.size() <= 1)
		return;

	// For the candidate we pick, we want to enforce the property that all
	// the copies of the chunks are on different racks.
	set<int> excludeRacks;
        for_each(clli.chunkServers.begin(), clli.chunkServers.end(),
	                RackSetter(excludeRacks));

	for (uint32_t i = 0; i < servers.size(); i++) {
		set<int>::iterator iter = excludeRacks.find(servers[i]->GetRack());
		if (iter == excludeRacks.end()) {
			candidate = servers[i];
			return;
		}
	}
}

//
// Periodically, if we find that some chunkservers have LOT (> 80% free) of space
// and if others are loaded (i.e., < 30% free space), move chunks around.  This
// helps with keeping better disk space utilization (and maybe load).
//
int
LayoutManager::RebalanceServers()
{
	if ((InRecovery()) || (mChunkServers.size() == 0)) {
		return 0;
	}

	// if we are doing rebalancing based on a plan, execute as
	// much of the plan as there is room.

	ExecuteRebalancePlan();

	for_each(mRacks.begin(), mRacks.end(), mem_fun_ref(&RackInfo::computeSpace));

	if (!mIsRebalancingEnabled)
		return 0;

	vector<ChunkServerPtr> servers = mChunkServers;
	vector<ChunkServerPtr> loadedServers, nonloadedServers;
	int extraReplicas, numBlocksMoved = 0, numSkipped = 0;

	for (uint32_t i = 0; i < servers.size(); i++) {
		if (servers[i]->IsRetiring())
			continue;
		if (servers[i]->GetSpaceUtilization() < mMinRebalanceSpaceUtilThreshold)
			nonloadedServers.push_back(servers[i]);
		else if (servers[i]->GetSpaceUtilization() > mMaxRebalanceSpaceUtilThreshold)
			loadedServers.push_back(servers[i]);
	}

	if ((nonloadedServers.size() == 0) || (loadedServers.size() == 0))
		return 0;

	bool allbusy = false;
	// try to start where we left off last time; if that chunk has
	// disappeared, find something "closeby"
	CSMapIter iter = mChunkToServerMap.find(mLastChunkRebalanced);
	if (iter == mChunkToServerMap.end())
		iter = mChunkToServerMap.upper_bound(mLastChunkRebalanced);

	for (; iter != mChunkToServerMap.end(); iter++) {

		allbusy = true;
		for (uint32_t i = 0; i < nonloadedServers.size(); i++) {
			if (nonloadedServers[i]->GetNumChunkReplications() <
				MAX_CONCURRENT_WRITE_REPLICATIONS_PER_NODE) {
				allbusy = false;
				break;
			}
		}

		if (allbusy || (numBlocksMoved > 200) || (numSkipped > 500)) {
			allbusy = true;
			break;
		}

		chunkId_t chunkId = iter->first;
		ChunkPlacementInfo &clli = iter->second;
		vector<ChunkServerPtr> candidates;

		// chunk could be moved around if it is hosted on a loaded server
		vector<ChunkServerPtr>::const_iterator csp;
		csp = find_if(clli.chunkServers.begin(), clli.chunkServers.end(),
				LoadedServerPred(mMaxRebalanceSpaceUtilThreshold));
		if (csp == clli.chunkServers.end())
			continue;

		// we have seen this chunkId; next time, we'll start time from
		// around here
		mLastChunkRebalanced = chunkId;

		// If this chunk is already being replicated or it is busy, skip
		bool noSuchChunkFlag = false;
		if ((clli.ongoingReplications > 0) ||
			(!CanReplicateChunkNow(chunkId, clli, extraReplicas, noSuchChunkFlag)))
				continue;
		if (noSuchChunkFlag) {
			mChunkToServerMap.erase(iter--);
			continue;
		}
		// if we got too many copies of this chunk, don't bother
		if (extraReplicas < 0)
			continue;


		// there are two ways nodes can be added:
		//  1. new nodes to an existing rack: in this case, we want to
		//  migrate chunk within a rack
		//  2. new rack of nodes gets added: in this case, we want to
		//  migrate chunks to the new rack as long as we don't get more
		//  than one copy onto the same rack

		FindIntraRackRebalanceCandidates(candidates, nonloadedServers, clli);
		if (candidates.size() == 0) {
			ChunkServerPtr cand;

			FindInterRackRebalanceCandidate(cand, nonloadedServers, clli);
			if (cand)
				candidates.push_back(cand);
		}

		if (candidates.size() == 0)
			// no candidates :-(
			continue;
		// get the chunk version
		vector<MetaChunkInfo *> v;
		vector<MetaChunkInfo *>::iterator chunk;

		metatree.getalloc(clli.fid, v);
		chunk = find_if(v.begin(), v.end(), ChunkIdMatcher(chunkId));
		if (chunk == v.end())
			continue;

		uint32_t numCopies = min(candidates.size(), clli.chunkServers.size());
		uint32_t numOngoing = 0;

		numOngoing = ReplicateChunkToServers(chunkId, clli, numCopies,
					candidates);
		if (numOngoing > 0) {
                        numBlocksMoved++;
		} else {
			numSkipped++;
		}
	}
	if (!allbusy)
		// reset
		mLastChunkRebalanced = 1;

        return numBlocksMoved;
}

int
LayoutManager::ReplicateChunkToServers(chunkId_t chunkId, ChunkPlacementInfo &clli,
					uint32_t numCopies,
					vector<ChunkServerPtr> &candidates)
{
	for (uint32_t i = 0; i < candidates.size(); i++) {
		assert(!IsChunkHostedOnServer(clli.chunkServers, candidates[i]));
	}

	const int numStarted = ReplicateChunk(chunkId, clli, numCopies, candidates);
	if (numStarted > 0) {
		// add this chunk to the target set of chunkIds that we are tracking
		// for replication status change
		ChangeChunkReplication(chunkId);
	}
	return numStarted;
}

class RebalancePlanExecutor {
	LayoutManager *mgr;
public:
	RebalancePlanExecutor(LayoutManager *l) : mgr(l) { }
	void operator()(ChunkServerPtr &c) {
		if ((c->IsRetiring()) || (!c->IsResponsiveServer()))
			return;
		mgr->ExecuteRebalancePlan(c);
	}
};

int
LayoutManager::LoadRebalancePlan(const string &planFn)
{
	// load the plan from the specified file
	int fd = open(planFn.c_str(), O_RDONLY);

	if (fd < 0) {
		KFS_LOG_STREAM_INFO << "Unable to open: " << planFn << KFS_LOG_EOM;
		return -1;
	}

	RebalancePlanInfo_t rpi;
	int rval;

	while (1) {
		rval = read(fd, &rpi, sizeof(RebalancePlanInfo_t));
		if (rval != sizeof(RebalancePlanInfo_t))
			break;
		ServerLocation loc;
		istringstream ist(rpi.dst);
		vector <ChunkServerPtr>::iterator j;

		ist >> loc.hostname;
		ist >> loc.port;
		j = find_if(mChunkServers.begin(), mChunkServers.end(),
			MatchingServer(loc));
		if (j == mChunkServers.end())
			continue;
		ChunkServerPtr c = *j;
		c->AddToChunksToMove(rpi.chunkId);
	}
	close(fd);

	mIsExecutingRebalancePlan = true;
	KFS_LOG_STREAM_INFO << "Setup for rebalance plan execution from " <<
		planFn << " is done" <<
	KFS_LOG_EOM;

	return 0;
}

void
LayoutManager::ExecuteRebalancePlan()
{
	if (!mIsExecutingRebalancePlan)
		return;

	for_each(mChunkServers.begin(), mChunkServers.end(),
		RebalancePlanExecutor(this));

	bool alldone = true;

	for (vector<ChunkServerPtr>::iterator iter = mChunkServers.begin();
		iter != mChunkServers.end(); iter++) {
		ChunkServerPtr c = *iter;
		if (!c->GetChunksToMove().empty()) {
			alldone = false;
			break;
		}
	}

	if (alldone) {
		KFS_LOG_STREAM_INFO << "Execution of rebalance plan is complete" <<
		KFS_LOG_EOM;
		mIsExecutingRebalancePlan = false;
	}
}

void
LayoutManager::ExecuteRebalancePlan(ChunkServerPtr &c)
{
	vector<ChunkServerPtr> candidates;

	if (!mIsExecutingRebalancePlan)
		return;

	if (c->GetSpaceUtilization() > MAX_SERVER_SPACE_UTIL_THRESHOLD) {
		KFS_LOG_STREAM_INFO <<
			"Terminating rebalance plan execution for overloaded server " <<
			c->ServerID() <<
		KFS_LOG_EOM;
		c->ClearChunksToMove();
		return;
	}

	candidates.push_back(c);

	const ChunkIdSet chunksToMove = c->GetChunksToMove();
	for (ChunkIdSet::const_iterator citer = chunksToMove.begin();
		citer != chunksToMove.end(); citer++) {
		if (c->GetNumChunkReplications() >
			MAX_CONCURRENT_WRITE_REPLICATIONS_PER_NODE)
			return;

		const chunkId_t cid = *citer;

		CSMapIter const iter = mChunkToServerMap.find(cid);

		if (iter == mChunkToServerMap.end()) {
			// chunk got deleted from the time the plan was created
			// to now.
			c->MovingChunkDone(cid);
			continue;
		}

		int  extraReplicas   = 0;
		bool noSuchChunkFlag = false;
		if ((iter->second.ongoingReplications > 0) ||
			(!CanReplicateChunkNow(cid, iter->second,
				extraReplicas, noSuchChunkFlag)))
			continue;
		if (IsChunkHostedOnServer(iter->second.chunkServers, c)) {
			// Paranoia...
			c->MovingChunkDone(cid);
			continue;
		}
		if (noSuchChunkFlag) {
			mChunkToServerMap.erase(iter);
		} else {
			ReplicateChunkToServers(cid, iter->second, 1, candidates);
		}
	}
}

class OpenFileChecker {
	set<fid_t> &readFd, &writeFd;
public:
	OpenFileChecker(set<fid_t> &r, set<fid_t> &w) :
		readFd(r), writeFd(w) { }
	void operator() (const CSMap::value_type& p) {
		const ChunkPlacementInfo& c = p.second;
		vector<LeaseInfo>::const_iterator l;
		l = find_if(c.chunkLeases.begin(), c.chunkLeases.end(),
			ptr_fun(LeaseInfo::IsValidWriteLease));
		if (l != c.chunkLeases.end()) {
			writeFd.insert(c.fid);
			return;
		}
		l = find_if(c.chunkLeases.begin(), c.chunkLeases.end(),
				ptr_fun(LeaseInfo::IsValidLease));
		if (l != c.chunkLeases.end()) {
			readFd.insert(c.fid);
			return;
		}
	}
};

void
LayoutManager::GetOpenFiles(string &openForRead, string &openForWrite)
{
	set<fid_t> readFd, writeFd;
	for_each(mChunkToServerMap.begin(), mChunkToServerMap.end(),
		OpenFileChecker(readFd, writeFd));
		// XXX: fill me in..map from fd->path name

}
