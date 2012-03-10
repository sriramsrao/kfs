//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: LayoutManager.h 3489 2011-12-15 00:36:06Z sriramr $
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
// \file LayoutManager.h
// \brief Layout manager is responsible for laying out chunks on chunk
// servers.  Model is that, when a chunkserver connects to the meta
// server, the layout manager gets notified; the layout manager then
// uses the chunk server for data placement.
//
//----------------------------------------------------------------------------

#ifndef META_LAYOUTMANAGER_H
#define META_LAYOUTMANAGER_H

#include <map>
#include <tr1/unordered_map>
#include <vector>
#include <set>
#include <sstream>
#include <boost/pool/pool_alloc.hpp> 

#include "kfstypes.h"
#include "meta.h"
#include "queue.h"
#include "ChunkServer.h"
#include "LeaseCleaner.h"
#include "ChunkReplicator.h"
#include "ChunkServer.h"

#include "libkfsIO/Counter.h"
#include "common/properties.h"

namespace KFS
{

	///
	/// For disk space utilization balancing, we say that a server
	/// is "under utilized" if is below XXX% full; we say that a server
	/// is "over utilized" if it is above YYY% full.  For rebalancing, we
	/// move data from servers that are over-utilized to servers that are
	/// under-utilized.  These #'s are intentionally set conservatively; we
	/// don't want the system to constantly move stuff between nodes when
	/// there isn't much to be gained by it.
	///

	const float MIN_SERVER_SPACE_UTIL_THRESHOLD = 0.3;
	const float MAX_SERVER_SPACE_UTIL_THRESHOLD = 0.9;

	/// Model for leases: metaserver assigns write leases to chunkservers;
	/// clients/chunkservers can grab read lease on a chunk at any time.
	/// The server will typically renew unexpired leases whenever asked.
	/// As long as the lease is valid, server promises not to chnage
	/// the lease's version # (also, chunk won't disappear as long as
	/// lease is valid).
	struct LeaseInfo {
		LeaseInfo(LeaseType t, int64_t i, bool append)
			: leaseType(t),
			  leaseId(i),
			  chunkServer(),
			  pathname(),
			  appendFlag(append),
                          relinquishedFlag(false),
                          ownerWasDownFlag(false),
			  expires(time(0) + LEASE_INTERVAL_SECS)
			{}
		LeaseInfo(LeaseType t, int64_t i, ChunkServerPtr &c, const std::string &p, bool append)
			: leaseType(t),
			  leaseId(i),
			  chunkServer(c),
			  pathname(p),
			  appendFlag(append),
                          relinquishedFlag(false),
                          ownerWasDownFlag(false),
			  expires(time(0) + LEASE_INTERVAL_SECS)
			{}
		LeaseInfo(const LeaseInfo& lease)
			: leaseType(lease.leaseType),
			  leaseId(lease.leaseId),
			  chunkServer(lease.chunkServer),
			  pathname(lease.pathname),
			  appendFlag(lease.appendFlag),
                          relinquishedFlag(lease.relinquishedFlag),
                          ownerWasDownFlag(lease.ownerWasDownFlag),
			  expires(lease.expires)
			{}
		LeaseInfo& operator=(const LeaseInfo& lease)
		{
			Mutable(leaseType)        = lease.leaseType;
			Mutable(leaseId)          = lease.leaseId;
			Mutable(chunkServer)      = lease.chunkServer;
			Mutable(pathname)         = lease.pathname;
			Mutable(appendFlag)       = lease.appendFlag;
			Mutable(relinquishedFlag) = lease.relinquishedFlag;
                        Mutable(ownerWasDownFlag) = lease.ownerWasDownFlag;
			Mutable(expires)          = lease.expires;
			return *this;
		}
		static bool IsValidLease(const LeaseInfo &l)
		{
			return time(0) <= l.expires;
		}
		static bool IsValidWriteLease(const LeaseInfo &l)
		{
			return (l.leaseType == WRITE_LEASE) &&
				IsValidLease(l);
		}
		static bool IsWriteLease(const LeaseInfo &l)
		{
			return (l.leaseType == WRITE_LEASE);
		}
		const LeaseType leaseType;
		const int64_t leaseId;
		// set for a write lease
		const ChunkServerPtr chunkServer;
		// for a write lease, record the pathname; we can use the path
		// to traverse the dir. tree and update space used at each level
		// of the tree
		const std::string pathname;
                const bool appendFlag:1;
                bool       relinquishedFlag:1;
                bool       ownerWasDownFlag:1;
		time_t expires;
	private:
		template<typename T> static T& Mutable(const T& val)
		{
			return const_cast<T&>(val);
		}
	};

	// Chunks are made stable by a message from the metaserver ->
	// chunkservers.  To prevent clients from seeing non-stable chunks, the
	// metaserver delays issuing a read lease to a client if there is a make
	// stable message in-flight.  Whenever the metaserver receives acks for
	// the make stable message, it needs to update its state. This structure
	// tracks the # of messages sent out and how many have been ack'ed.
	// When all the messages have been ack'ed the entry for a particular
	// chunk can be cleaned up.
	struct MakeChunkStableInfo {
		MakeChunkStableInfo(
			int         nServers          = 0,
			bool        beginMakeStable   = false,
			std::string name              = std::string())
		: beginMakeStableFlag(beginMakeStable),
		  logMakeChunkStableFlag(false),
		  serverAddedFlag(false),
		  numServers(nServers),
		  numAckMsg(0),
		  pathname(name),
		  chunkChecksum(0),
		  chunkSize(-1)
		{}
		bool              beginMakeStableFlag:1;
		bool              logMakeChunkStableFlag:1;
		bool              serverAddedFlag:1;
		int               numServers;
		int               numAckMsg;
		const std::string pathname;
		uint32_t          chunkChecksum;
		off_t             chunkSize;
		seq_t             chunkVersion;
	};
	typedef std::map <chunkId_t, MakeChunkStableInfo,
            std::less<chunkId_t>,
            boost::fast_pool_allocator<
                std::pair<const chunkId_t, MakeChunkStableInfo> >
        > NonStableChunksMap;

	struct PendingMakeStableEntry {
		PendingMakeStableEntry(
			off_t    size        = -1,
			bool     hasChecksum = false,
			uint32_t checksum    = 0,
			seq_t    version     = -1)
			: mSize(size),
			  mHasChecksum(hasChecksum),
			  mChecksum(checksum),
			  mChunkVersion(version)
			{}
		off_t    mSize;
		bool     mHasChecksum;
		uint32_t mChecksum;
		seq_t    mChunkVersion;
        };
        typedef std::map <chunkId_t, PendingMakeStableEntry,
            std::less<chunkId_t>,
            boost::fast_pool_allocator<
                std::pair<const chunkId_t, PendingMakeStableEntry> >
	> PendingMakeStableMap;

	// Given a chunk-id, where is stored and who has the lease(s)
	struct ChunkPlacementInfo {
		ChunkPlacementInfo() :
			fid(-1), chunkOffsetIndex(0), ongoingReplications(0) { }
		// For cross-validation, we store the fid here.  This
		// is also useful during re-replication: given a chunk, we
		// can get its fid and from all the attributes of the file
		fid_t fid;
		// Offset within in the file where this chunk is located.  The
		// metatree is key'ed using <fid, offset> to get the chunk info.
		// So, given a chunkid, use the key to get at the chunk info.
		// To save memory, store <offset / CHUNKSIZE>
		uint32_t chunkOffsetIndex;
		/// is this chunk being (re) replicated now?  if so, how many
		int ongoingReplications;
		std::vector<ChunkServerPtr> chunkServers;
		std::vector<LeaseInfo> chunkLeases;
	};

	// To support rack-aware placement, we need an estimate of how much
	// space is available on each given rack.  Once the set of candidate
	// racks are ordered, we walk down the sorted list to pick the
	// desired # of servers.  For ordering purposes, we track how much space
	// each machine on the rack exports and how much space we have parceled
	// out; this gives us an estimate of availble space (we are
	// over-counting because the space we parcel out may not be fully used).
	class RackInfo {
		uint32_t mRackId;
		uint64_t mTotalSpace;
		uint64_t mAllocSpace;
		// set of servers on this rack
		std::vector<ChunkServerPtr> mServers;
	public:
		RackInfo(int id) : mRackId(id), mTotalSpace(0), mAllocSpace(0) { }

		inline uint32_t id() const {
			return mRackId;
		}
		void clear() {
			mTotalSpace = mAllocSpace = 0;
		}
		void addServer(ChunkServerPtr &s) {
			mTotalSpace += s->GetTotalSpace();
			mAllocSpace += s->GetUsedSpace();
			mServers.push_back(s);
		}
		void removeServer(ChunkServer *server) {
			std::vector <ChunkServerPtr>::iterator iter;

			iter = find_if(mServers.begin(), mServers.end(),
					ChunkServerMatcher(server));
			if (iter == mServers.end())
				return;

			mTotalSpace -= server->GetTotalSpace();
			mAllocSpace -= server->GetUsedSpace();
			mServers.erase(iter);
		}
		void computeSpace() {
			clear();
			for(std::vector<ChunkServerPtr>::iterator iter = mServers.begin();
				iter != mServers.end(); iter++) {
				ChunkServerPtr s = *iter;
				mTotalSpace += s->GetTotalSpace();
				mAllocSpace += s->GetUsedSpace();
			}
		}
		const std::vector<ChunkServerPtr> &getServers() {
			return mServers;
		}

		uint64_t availableSpace() const {
			if (mTotalSpace < mAllocSpace)
				// paranoia...
				return 0;
			return mTotalSpace - mAllocSpace;
		}
		// want to sort in decreasing order so that racks with more
		// space are at the head of list (and so, a node from them will
		// get chosen).
		bool operator < (const RackInfo &other) const {
			uint64_t mine;

			mine = availableSpace();
			if (mine == 0)
				return false;
			return mine < other.availableSpace();
		}
	};

	// Functor to enable matching of a rack-id with a RackInfo
	class RackMatcher {
		uint32_t id;
	public:
		RackMatcher(uint32_t rackId) : id(rackId) { }
		bool operator()(const RackInfo &rack) const {
			return rack.id() == id;
		}
	};

	// chunkid to server(s) map
	class CSMap {
	private:
		typedef std::map <chunkId_t, ChunkPlacementInfo,
        	    std::less<chunkId_t>,
        	    boost::fast_pool_allocator<
                	std::pair<const chunkId_t, ChunkPlacementInfo> >
        	> Map;
		void update() {
			if ((mKeyValidFlag = mIt != mMap.end())) {
				mKey = mIt->first;
			}
		}
	public:
		typedef Map::iterator       iterator;
		typedef Map::const_iterator const_iterator;
		typedef Map::value_type     value_type;
		typedef Map::key_type       key_type;
		typedef Map::mapped_type    mapped_type;
		typedef Map::size_type      size_type;

		CSMap() : mMap(), mIt(mMap.end()), mKey(), mKeyValidFlag(false) {}
		 ~CSMap() {}
		const_iterator find(const key_type& key) const {
			return const_iterator(const_cast<CSMap*>(this)->find(key));
		}
		iterator find(const key_type& key) {
			if (mKeyValidFlag && mKey == key) {
				return mIt;
			}
			mIt           = mMap.find(key);
			mKey          = key;
			mKeyValidFlag = true;
			return mIt;
		}
		void clear() {
			mMap.clear();
			mIt           = mMap.end();
			mKey          = key_type();
			mKeyValidFlag = false;
		}
		size_type size() const {
			return mMap.size();
		}
		bool empty() const {
			return mMap.empty();
		}
		size_type erase(const key_type& key) {
			if (mIt != mMap.end() && mIt->first == key) {
				mMap.erase(mIt++);
				update();
				return 1;
			}
			return mMap.erase(key);
		}
		void erase(iterator it) {
			if (mIt == it) {
				mMap.erase(mIt++);
				update();
			} else {
				mMap.erase(it);
			}
		}
		std::pair<iterator, bool> insert(const value_type& val) {
			if (mIt == mMap.end() || mIt->first != val.first) {
				std::pair<iterator, bool> const ret = mMap.insert(val);
				mIt           = ret.first;
				mKey          = mIt->first;
				mKeyValidFlag = true;
				return ret;
			}
			return std::make_pair(mIt, false);
		}
		mapped_type& operator[](const key_type& key) {
			return insert(value_type(key, mapped_type())).first->second;
		}
		iterator begin() {
			return mMap.begin();
		}
		iterator end() {
			return mMap.end();
		}
		const_iterator begin() const {
			return mMap.begin();
		}
		const_iterator end() const {
			return mMap.end();
		}
		iterator lower_bound(const key_type& key) {
			return mMap.lower_bound(key);
		}
		iterator upper_bound(const key_type& key) {
			return mMap.upper_bound(key);
		}
		const_iterator lower_bound(const key_type& key) const {
			return mMap.lower_bound(key);
		}
		const_iterator upper_bound(const key_type& key) const {
			return mMap.upper_bound(key);
		}
		size_type count(const key_type& key) const {
			return (mMap.find(key) == mMap.end() ? 0 : 1);
		}
		iterator first() {
			mIt = mMap.begin();
			update();
			return mIt;
		}
		iterator next() {
			if (mIt != mMap.end()) {
				++mIt;
				update();
			}
			return mIt;
		}
		void copyInto(CSMap& map) const {
			map.clear();
			map.mMap = mMap;
		}
	private:
		CSMap(const CSMap&);
		CSMap& operator=(const CSMap&);
	private:
		Map      mMap;
		iterator mIt;
		key_type mKey;
		bool     mKeyValidFlag;
	};
	typedef CSMap::const_iterator CSMapConstIter;
	typedef CSMap::iterator CSMapIter;

	// candidate set of chunks whose replication needs checking
	typedef ChunkServer::ChunkIdSet ChunkIdSet;
	typedef ChunkIdSet CRCandidateSet;
	typedef CRCandidateSet::iterator CRCandidateSetIter;
        typedef CRCandidateSet ReplicationCandidates;

	//
	// For maintenance reasons, we'd like to schedule downtime for a server.
	// When the server is taken down, a promise is made---the server will go
	// down now and come back up by a specified time. During this window, we
	// are willing to tolerate reduced # of copies for a block.  Now, if the
	// server doesn't come up by the promised time, the metaserver will
	// initiate re-replication of blocks on that node.  This ability allows
	// us to schedule downtime on a node without having to incur the
	// overhead of re-replication.
	//
	struct HibernatingServerInfo_t {
		// the server we put in hibernation
		ServerLocation location;
		// the blocks on this server
		ReplicationCandidates blocks;
		// when is it likely to wake up
		time_t sleepEndTime;
	};

	// use a 10 min. interval to expire entries in the ARA cache.
	const uint32_t ARA_CHUNK_CACHE_EXPIRE_INTERVAL = 600;

	// For atomic record append, we'd like to assign multiple clients to the
	// same block.  Whenever an allocation request comes in, we'll try to
	// assign the last block of the file to the client; if that doesn't
	// work (because the client passed in a hint saying it wanted a block
	// past the current EOF), we'll allocate a new block.  Trying to find
	// the last block of a file is expensive, particularly, when the system
	// is scaled to handle few 100's of record appenders.  To optimize,
	// stash the currently known last block of a file.
	// A downside of this approach is that, if any of the blocks in the
	// middle are non-full, we won't fill them up.
	// If this structure works out, we'll need to extend this to hold a list
	// of blocks that can be re-used for allocation (and thereby avoid the
	// non-full problem).

	class ARAChunkCache
	{
	public:
		struct Entry {
			Entry(
				chunkId_t     cid = -1,
				seq_t         cv  = -1,
				off_t         co  = -1,
				time_t        lat = 0,
				MetaAllocate* req = 0)
				: chunkId(cid),
				  chunkVersion(cv),
				  offset(co),
				  lastAccessedTime(lat),
				  lastDecayTime(lat),
				  spaceReservationSize(0),
				  numAppendersInChunk(0),
				  appendChunkAffinityCount(0),
				  lastPendingRequest(req)
				{}
			bool AddPending(MetaAllocate& req);
			bool IsAllocationPending() const {
				return (lastPendingRequest != 0);
			}
			// index into chunk->server map to work out where the block lives
			chunkId_t chunkId;
			seq_t     chunkVersion;
			// the file offset corresponding to the last chunk
			off_t     offset;
			// when was this info last accessed; use this to cleanup 
			time_t    lastAccessedTime;
                	time_t    lastDecayTime;
			// chunk space reservation approximation
			int  spaceReservationSize;
			// # of appenders to which this chunk was used for allocation
			int  numAppendersInChunk;
			// for rack-local Ifiles, it is desirable to provide chunk affinity.
			// such affinity will allow a sorter to combine data from multiple blocks 
			// and sort them, giving us bigger runs.  To get such affinity, we place
			// N consecutive chunks on a node.   Once this value is hit, we move to
			// a new node.  This ensures that during merge (as in reduce phase), we
			// use all nodes equally; otherwise, we will be stuck on a single node.
			// These blocks are allocated on the node that holds the chunk corresponding to chunkId.
			int appendChunkAffinityCount;
		private:
			MetaAllocate* lastPendingRequest;
			friend class ARAChunkCache;
        	};

		// To support rack-local I-files, for each fid, track the chunk
		// of that fid stored in a rack.  Each allocation request also
		// includes the rack where the client is located.
		struct FidRack_t {
			fid_t fid;
			int rack;
			FidRack_t(fid_t f, int r) : fid(f), rack(r) { }
			bool operator< (const FidRack_t &other) const {
				if (fid == other.fid)
					return rack < other.rack;
				return fid < other.fid;
			}
		};

		/* WAS
		typedef std::map <fid_t, Entry, std::less<fid_t>,
			boost::fast_pool_allocator<
		    		std::pair<const fid_t, Entry> >
		> Map;
		*/
		typedef std::map <FidRack_t, Entry, std::less<FidRack_t>, 
			boost::fast_pool_allocator<
		    		std::pair<const FidRack_t, Entry> >
		> Map;
		typedef Map::const_iterator const_iterator;
		typedef Map::iterator       iterator;

		ARAChunkCache()
			: mMap()
			{}
		~ARAChunkCache()
			{ mMap.empty(); }
		void RequestNew(MetaAllocate& req);
		void RequestDone(const MetaAllocate& req);
		void Timeout(time_t now);
		inline bool Invalidate(fid_t fid, int rack);
		inline bool Invalidate(fid_t fid, int rack, chunkId_t chunkId);
		inline bool Invalidate(iterator it);
		iterator Find(fid_t fid, int rack) {
			FidRack_t key(fid, rack);
			return mMap.find(key);
		}
		const_iterator Find(fid_t fid, int rack) const {
			FidRack_t key(fid, rack);
			return mMap.find(key);
		}
		const Entry* Get(const_iterator it) const {
			return (it == mMap.end() ? 0 : &it->second);
		}
		Entry* Get(iterator it) {
			return (it == mMap.end() ? 0 : &it->second);
		}
		const Entry* Get(fid_t fid, int rack) const {
			return Get(Find(fid, rack));
		}
		Entry* Get(fid_t fid, int rack) {
			return Get(Find(fid, rack));
		}
	private:
		Map mMap;
	};
	typedef std::set<ServerLocation, std::less<ServerLocation>,
            boost::fast_pool_allocator<ServerLocation>
	> ServerLocationSet;
	typedef std::map <std::string, std::vector<std::string>,
            std::less<std::string>,
            boost::fast_pool_allocator<
                std::pair<const std::string, std::vector<std::string> > >
        > CSCounters;
	std::string ToString(const CSCounters& cntrs, std::string rowDelim);

        ///
        /// LayoutManager is responsible for write allocation:
        /// it determines where to place a chunk based on metrics such as,
        /// which server has the most space, etc.  Will eventually be
        /// extend this model to include replication.
        ///
        /// Allocating space for a chunk is a 3-way communication:
        ///  1. Client sends a request to the meta server for
        /// allocation
        ///  2. Meta server picks a chunkserver to hold the chunk and
        /// then sends an RPC to that chunkserver to create a chunk.
        ///  3. The chunkserver creates a chunk and replies to the
        /// meta server's RPC.
        ///  4. Finally, the metaserver logs the allocation request
        /// and then replies to the client.
        ///
        /// In this model, the layout manager picks the chunkserver
        /// location and queues the RPC to the chunkserver.  All the
        /// communication is handled by a thread in NetDispatcher
        /// which picks up the RPC request and sends it on its merry way.
        ///
	class LayoutManager {
	public:
		LayoutManager();

		virtual ~LayoutManager() { }

		/// On a startup, # of secs to wait before we are open for
		/// reads/writes/re-replication.
		void SetRecoveryInterval(int secs) { 
			mRecoveryIntervalSecs = secs;
		}

                /// A new chunk server has joined and sent a HELLO message.
                /// Use it to configure information about that server
                /// @param[in] r  The MetaHello request sent by the
                /// new chunk server.
		void AddNewServer(MetaHello *r);

                /// Our connection to a chunkserver went down.  So,
                /// for all chunks hosted on this server, update the
                /// mapping table to indicate that we can't
                /// get to the data.
                /// @param[in] server  The server that is down
		void ServerDown(ChunkServer *server);

		/// A server is being taken down: if downtime is > 0, it is a
		/// value in seconds that specifies the time interval within
		/// which the server will connect back.  If it doesn't connect
		/// within that interval, the server is assumed to be down and
		/// re-replication will start.
		int RetireServer(const ServerLocation &loc, int downtime);

                /// Allocate space to hold a chunk on some
                /// chunkserver.
                /// @param[in] r The request associated with the
                /// write-allocation call.
                /// @retval 0 on success; -1 on failure
		int AllocateChunk(MetaAllocate *r);

		/// When allocating a chunk for append, we try to re-use an
		/// existing chunk for a which a valid write lease exists.  
                /// @param[in/out] r The request associated with the
                /// write-allocation call.  When an existing chunk is re-used,
		/// the chunkid/version is returned back to the caller.
		/// @retval 0 on success; -1 on failure
		int AllocateChunkForAppend(MetaAllocate *r);

		void CoalesceBlocks(const vector<chunkId_t>& srcChunks, fid_t srcFid, 
					fid_t dstFid, const off_t dstStartOffset);

		/// A chunkid has been previously allocated.  The caller
		/// is trying to grab the write lease on the chunk. If a valid
		/// lease exists, we return it; otherwise, we assign a new lease,
		/// bump the version # for the chunk and notify the caller.
		///
                /// @param[in] r The request associated with the
                /// write-allocation call.
		/// @param[out] isNewLease  True if a new lease has been
		/// issued, which tells the caller that a version # bump
		/// for the chunk has been done.
                /// @retval status code
		int GetChunkWriteLease(MetaAllocate *r, bool &isNewLease);

                /// Delete a chunk on the server that holds it.
                /// @param[in] chunkId The id of the chunk being deleted
		void DeleteChunk(chunkId_t chunkId);

                /// A chunkserver is notifying us that a chunk it has is
		/// corrupt; so update our tables to reflect that the chunk isn't
		/// hosted on that chunkserver any more; re-replication will take
		/// care of recovering that chunk.
                /// @param[in] r  The request that describes the corrupted chunk
		void ChunkCorrupt(MetaChunkCorrupt *r);

                /// Truncate a chunk to the desired size on the server that holds it.
                /// @param[in] chunkId The id of the chunk being
                /// truncated
		/// @param[in] sz    The size to which the should be
                /// truncated to.
		void TruncateChunk(chunkId_t chunkId, off_t sz);

		/// Handlers to acquire and renew leases.  Unexpired leases
		/// will typically be renewed.
		int GetChunkReadLease(MetaLeaseAcquire *r);
		int LeaseRenew(MetaLeaseRenew *r);

		/// Handler to let a lease owner relinquish a lease.
		int LeaseRelinquish(MetaLeaseRelinquish *r);

		/// Internally generated: whenever an allocation fails,
		/// invalidate the write lease, so that a subsequent allocation
		/// will cause a version # bump and a new lease to be issued.
		void InvalidateWriteLease(chunkId_t chunkId);

		/// Is a valid lease issued on any of the chunks in the
		/// vector of MetaChunkInfo's?
		bool IsValidLeaseIssued(const std::vector <MetaChunkInfo *> &c);

		/// Is a valid lease issued on the chunk.
		bool IsValidWriteLeaseIssued(chunkId_t chunkId);

		void MakeChunkStableInit(
			fid_t                         fid,
			chunkId_t                     chunkId,
			off_t                         chunkOffsetInFile,
			std::string                   pathname,
			const vector<ChunkServerPtr>& servers,
			bool                          beginMakeStableFlag,
			off_t                         chunkSize,
			bool                          hasChunkChecksum,
			uint32_t                      chunkChecksum);
		bool AddServerToMakeStable(
			ChunkPlacementInfo& placementInfo,
			ChunkServerPtr      server,
			chunkId_t           chunkId,
			seq_t               chunkVersion,
			const char*&        errMsg);
                void BeginMakeChunkStableDone(const MetaBeginMakeChunkStable* req);
		void LogMakeChunkStableDone(const MetaLogMakeChunkStable* req);
		void MakeChunkStableDone(const MetaChunkMakeStable* req);
		void ReplayPendingMakeStable(
			chunkId_t chunkId,
			seq_t     chunkVersion,
			off_t     chunkSize,
			bool      hasChunkChecksum,
			uint32_t  chunkChecksum,
			bool      addFlag);
                int WritePendingMakeStable(ostream& os) const;
		void CancelPendingMakeStable(fid_t fid, chunkId_t chunkId);
                int GetChunkSizeDone(MetaChunkSize* req);
		bool IsChunkStable(chunkId_t chunkId);
		const char* AddNotStableChunk(
			ChunkServerPtr server,
			fid_t          allocFileId,
			chunkId_t      chunkId,
			seq_t          chunkVersion,
			bool           appendFlag,
			const string&  logPrefix);
		void ProcessPendingBeginMakeStable();

                /// Add a mapping from chunkId -> server.
                /// @param[in] chunkId  chunkId that has been stored
                /// on server c
                /// @param[in] fid  fileId associated with this chunk.
                /// @param[in] offset  offset in the file associated with this chunk.
                /// @param[in] c   server that stores chunk chunkId.
                ///   If c == NULL, then, we update the table to
                /// reflect chunk allocation; whenever chunk servers
                /// start up and tell us what chunks they have, we
                /// line things up and see which chunk is stored where.
		void AddChunkToServerMapping(chunkId_t chunkId, fid_t fid, off_t offset, 
						ChunkServer *c);

		/// Remove the mappings for a chunk.
                /// @param[in] chunkId  chunkId for which mapping needs to be nuked.
		void RemoveChunkToServerMapping(chunkId_t chunkId);

                /// Update the mapping from chunkId -> server.
		/// @param[in] chunkId  chunkId that has been stored
		/// on server c
		/// @param[in] c   server that stores chunk chunkId.
		/// @retval  0 if update is successful; -1 otherwise
		/// Update will fail if chunkId is not present in the
		/// chunkId -> server mapping table.
		int UpdateChunkToServerMapping(chunkId_t chunkId, ChunkServer *c);

                /// Get the mapping from chunkId -> server.
                /// @param[in] chunkId  chunkId that has been stored
                /// on some server(s)
                /// @param[out] c   server(s) that stores chunk chunkId
                /// @retval 0 if a mapping was found; -1 otherwise
                ///
		int GetChunkToServerMapping(chunkId_t chunkId, std::vector<ChunkServerPtr> &c);

                /// Get the mapping from chunkId -> file id.
                /// @param[in] chunkId  chunkId
                /// @param[out] fileId  file id the chunk belongs to
                /// @retval true if a mapping was found; false otherwise
                ///
                bool GetChunkFileId(chunkId_t chunkId, fid_t& fileId);

		/// Dump out the chunk location map to a file.  The file is
		/// written to the specified dir.  The filename: 
		/// <dir>/chunkmap.txt.<pid>
		///
		void DumpChunkToServerMap(const std::string &dir);

		/// Dump out the chunk location map to a string stream.
		void DumpChunkToServerMap(ostringstream &os);

		/// Dump out the list of chunks that are currently replication
		/// candidates.
		void DumpChunkReplicationCandidates(ostringstream &os);

		/// Check the replication level of all the blocks and report
		/// back files that are under-replicated.
		/// Returns true if the system is healthy.
		void Fsck(ostringstream &os);

		///
		/// How many blocks are at replication level of 1.
		///
		size_t GetNumUnderReplicatedBlocks() const {
			return mPriorityChunkReplicationCandidates.size();
		}

                /// Ask each of the chunkserver's to dispatch pending RPCs
		void Dispatch();

		/// For monitoring purposes, dump out state of all the
		/// connected chunk servers.
		/// @param[out] systemInfo A string that describes system status
		///   such as, the amount of space in cluster
		/// @param[out] upServers  The string containing the
		/// state of the up chunk servers.
		/// @param[out] downServers  The string containing the
		/// state of the down chunk servers.
		/// @param[out] retiringServers  The string containing the
		/// state of the chunk servers that are being retired for
		/// maintenance.
		void Ping(string &systemInfo, string &upServers, string &downServers, string &retiringServers);

		/// Return a list of alive chunk servers
		void UpServers(ostringstream &os);

		/// Periodically, walk the table of chunk -> [location, lease]
		/// and remove out dead leases.
		void LeaseCleanup();

		/// Periodically, re-check the replication level of all chunks
		/// the system; this call initiates the checking work, which
		/// gets done over time.
		void InitCheckAllChunks();

		/// Is an expensive call; use sparingly
		void CheckAllLeases();

		/// Cleanup the lease for a particular chunk
		/// @param[in] chunkId  the chunk for which leases need to be cleaned up
		/// @param[in] v   the placement/lease info for the chunk
		void LeaseCleanup(chunkId_t chunkId, ChunkPlacementInfo &v);
		bool ExpiredLeaseCleanup(chunkId_t chunkId);

		/// Handler that loops thru the chunk->location map and determines
		/// if there are sufficient copies of each chunk.  Those chunks with
		/// fewer copies are (re) replicated.
		void ChunkReplicationChecker();

		/// A set of nodes have been put in hibernation by an admin.
		/// This is done for scheduled downtime.  During this period, we
		/// don't want to pro-actively replicate data on the down nodes;
		/// if the node doesn't come back as promised, we then start
		/// re-replication.  Periodically, check the status of
		/// hibernating nodes.
		void CheckHibernatingServersStatus();

		/// A chunk replication operation finished.  If the op was successful,
		/// then, we update the chunk->location map to record the presence
		/// of a new replica.
		/// @param[in] req  The op that we sent to a chunk server asking
		/// it to do the replication.
		void ChunkReplicationDone(MetaChunkReplicate *req);

		/// Degree of replication for chunk has changed.  When the replication
		/// checker runs, have it check the status for this chunk.
		/// @param[in] chunkId  chunk whose replication level needs checking
		///
		void ChangeChunkReplication(chunkId_t chunkId);

		/// Get all the fid's for which there is an open lease (read/write).
		/// This is useful for reporting purposes.
		/// @param[out] openForRead, openForWrite: the pathnames of files
		/// that are open for reading/writing respectively
		void GetOpenFiles(std::string &openForRead, std::string &openForWrite);

		void InitRecoveryStartTime()
		{
			mRecoveryStartTime = time(0);
		}

		void SetMinChunkserversToExitRecovery(uint32_t n) {
			mMinChunkserversToExitRecovery = n;
		}

		void ToggleRebalancing(bool v) {
			mIsRebalancingEnabled = v;
		}

		/// Methods for doing "planned" rebalancing of data.
		/// Read in the file that lays out the plan
		/// Return 0 if we can open the file; -1 otherwise
		int LoadRebalancePlan(const std::string &planFn);

		/// Execute the plan for all servers
		void ExecuteRebalancePlan();

		/// Execute planned rebalance for server c
		void ExecuteRebalancePlan(ChunkServerPtr &c);

		/// Send a heartbeat message to all responsive chunkservers
		void HeartbeatChunkServers();

                void SetParameters(const Properties& props);
		void SetChunkServersProperties(const Properties& props);

		/// For layout emulator.
		void GetChunkToServerMap(CSMap& map) {
			mChunkToServerMap.copyInto(map);
		}

		CSCounters GetChunkServerCounters() const;

		void AllocateChunkForAppendDone(MetaAllocate& req) {
			mARAChunkCache.RequestDone(req);
		}
        protected:
		/// A rolling counter for tracking leases that are issued to
		/// to clients/chunkservers for reading/writing chunks
		int64_t mLeaseId;

		/// A counter to track the # of ongoing chunk replications
		int mNumOngoingReplications;


		/// A switch to toggle rebalancing: if the system is under load,
		/// we'd like to turn off rebalancing.  We can enable it a
		/// suitable time.
		bool mIsRebalancingEnabled;

		/// For the purposes of rebalancing, what is the range we want 
		/// a node to be in.  If a node is outside the range, it is
		/// either underloaded (in which case, it can take blocks) or it
		/// is overloaded (in which case, it can give up blocks).
		double mMaxRebalanceSpaceUtilThreshold;
		double mMinRebalanceSpaceUtilThreshold;

		/// Set when a rebalancing plan is being excuted.
		bool mIsExecutingRebalancePlan;

		/// On each iteration, we try to rebalance some # of blocks;
		/// this counter tracks the last chunk we checked
		kfsChunkId_t mLastChunkRebalanced;

		/// When a server goes down or needs retiring, we start
		/// replicating blocks.  Whenever a replication finishes, we
		/// find the next candidate.  We need to track "where" we left off
		/// on a previous iteration, so that we can start from there and
		/// run with it.
		kfsChunkId_t mLastChunkReplicated;

		/// After a crash, track the recovery start time.  For a timer
		/// period that equals the length of lease interval, we only grant
		/// lease renews and new leases to new chunks.  We however,
		/// disallow granting new leases to existing chunks.  This is
		/// because during the time period that corresponds to a lease interval,
		/// we may learn about leases that we had handed out before crashing.
		time_t mRecoveryStartTime;
		/// To keep track of uptime.
		const time_t mStartTime;

		/// Defaults to the width of a lease window
		int mRecoveryIntervalSecs;

		/// Periodically clean out dead leases
		LeaseCleaner mLeaseCleaner;

		/// Similar to the lease cleaner: periodically check if there are
		/// sufficient copies of each chunk.
		ChunkReplicator mChunkReplicator;

		uint32_t mMinChunkserversToExitRecovery;

                /// List of connected chunk servers.
                std::vector <ChunkServerPtr> mChunkServers;
		/// Whenever the list of chunkservers has to be modified, this
		/// lock is used to serialize access.  
		/// XXX: The code is now single threaded.  Don't need this mutex
		/// anymore.
		// pthread_mutex_t mChunkServersMutex;

		/// List of servers that are hibernating; if they don't wake up
		/// the time the hibernation period ends, the blocks on those
		/// nodes needs to be re-replicated.  This provides us the ability
		/// to take a node down for maintenance and bring it back up
		/// without incurring re-replication overheads.
		std::vector <HibernatingServerInfo_t> mHibernatingServers;

		/// Track when servers went down so we can report it
                typedef std::deque<string> DownServers;
		DownServers mDownServers;

		/// State about how each rack (such as, servers/space etc)
		std::vector<RackInfo> mRacks;

                /// Mapping from a chunk to its location(s).
                CSMap mChunkToServerMap;

                /// Candidate set of chunks whose replication needs checking
                ReplicationCandidates mChunkReplicationCandidates;

		/// These are chunks with 1 copy; replicate first before we do
		/// the rest that needs replication
		CRCandidateSet mPriorityChunkReplicationCandidates;

		/// chunks to which a lease has been handed out; whenever we
		/// cleanup the leases, this set is walked 
		CRCandidateSet mChunksWithLeases;

		/// For files that are being atomic record appended to, track the last
		/// chunk of the file that we can use for subsequent allocations
		ARAChunkCache mARAChunkCache;

		/// Set of chunks that are in the process being made stable: a
		/// message has been sent to the associated chunkservers which are
		/// flushing out data to disk.
		NonStableChunksMap   mNonStableChunks;
		ChunkIdSet           mPendingBeginMakeStable;
		PendingMakeStableMap mPendingMakeStable;

		/// Counters to track chunk replications
		Counter *mOngoingReplicationStats;
		Counter *mTotalReplicationStats;
		/// how much todo before we are all done (estimate of the size
		/// of the chunk-replication candidates set).
		Counter *mReplicationTodoStats;
		/// # of chunks for which there is only a single copy
		Counter *mChunksWithOneReplicaStats;
		/// Track the # of replication ops that failed
		Counter *mFailedReplicationStats;
		/// Track the # of stale chunks we have seen so far
		Counter *mStaleChunkCount;
                size_t mMastersCount;
                size_t mSlavesCount;
		/// Are all chunkservers masters?
                bool   mAllMasters;
                bool   mAssignMasterByIpFlag;
                int    mLeaseOwnerDownExpireDelay;
                double mPercentLoadedNodesToAvoidForWrites;
                // Write append space reservation accounting.
                int    mMaxReservationSize;
                int    mReservationDecayStep;
                int    mChunkReservationThreshold;
                double mReservationOvercommitFactor;
		// Delay replication when connection breaks.
		int    mServerDownReplicationDelay;
                uint64_t mMaxDownServersHistorySize;
                // Chunk server properties broadcasted to all chunk servers.
		Properties mChunkServersProps;
		string     mChunkServersPropsFileName;
		bool       mReloadChunkServersPropertiesFlag;
		// Chunk server restart logic.
		int     mCSToRestartCount;
                int     mMastersToRestartCount;
		int     mMaxCSRestarting;
		int64_t mMaxCSUptime;
		int64_t mCSGracefulRestartTimeout;
                int64_t mCSGracefulRestartAppendWithWidTimeout;
                time_t  mLastReplicationCheckTime;
                time_t  mLastRecomputeDirsizeTime;

		bool ExpiredLeaseCleanup(
	            chunkId_t                 chunkId,
	            time_t                    now,
	            int                       ownerDownExpireDelay = 0,
	            const CRCandidateSetIter* chunksWithLeasesIt   = 0);
		/// Find a set of racks to place a chunk on; the racks are
		/// ordered by space.
		void FindCandidateRacks(std::vector<int> &result);

		/// Find a set of racks to place a chunk on; the racks are
		/// ordered by space.  The set excludes defines the set of racks
		/// that should be excluded from consideration.
		void FindCandidateRacks(std::vector<int> &result, const std::set<int> &excludes);

		/// Helper function to generate candidate servers
		/// for hosting a chunk.  The list of servers returned is
		/// ordered in decreasing space availability.
		/// @param[out] result  The set of available servers
		/// @param[in] excludes  The set of servers to exclude from
		///    candidate generation.
		/// @param[in] rackId   The rack to restrict the candidate
		/// selection to; if rackId = -1, then all servers are fair game
		void FindCandidateServers(std::vector<ChunkServerPtr> &result,
					const std::vector<ChunkServerPtr> &excludes,
					int rackId = -1);

		/// Helper function to generate candidate servers from
		/// the specified set of sources for hosting a chunk.
		/// The list of servers returned is
		/// ordered in decreasing space availability.
		/// @param[out] result  The set of available servers
		/// @param[in] sources  The set of possible source servers
		/// @param[in] excludes  The set of servers to exclude from
		/// @param[in] rackId   The rack to restrict the candidate
		/// selection to; if rackId = -1, then all servers are fair game
		void FindCandidateServers(std::vector<ChunkServerPtr> &result,
					const std::vector<ChunkServerPtr> &sources,
					const std::vector<ChunkServerPtr> &excludes,
					int rackId = -1);

		/// Helper function that takes a set of servers and sorts
		/// them by space utilization.  The list of servers returned is
		/// ordered on increasing space utilization (i.e., decreasing
		/// space availability).
		/// @param[in/out] servers  The set of servers we want sorted
		void SortServersByUtilization(vector<ChunkServerPtr> &servers);

		/// Check the # of copies for the chunk and return true if the
		/// # of copies is less than targeted amount.  We also don't replicate a chunk
		/// if it is currently being written to (i.e., if a write lease
		/// has been issued).
		/// @param[in] chunkId   The id of the chunk which we are checking
		/// @param[in] clli  The lease/location information about the chunk.
		/// @param[out] extraReplicas  The target # of additional replicas for the chunk
		/// @retval true if the chunk is to be replicated; false otherwise
		bool CanReplicateChunkNow(chunkId_t chunkId,
				ChunkPlacementInfo &clli,
				int &extraReplicas,
				bool &noSuchChunkFlag);

		/// Replicate a chunk.  This involves finding a new location for
		/// the chunk that is different from the existing set of replicas
		/// and asking the chunkserver to get a copy.
		/// @param[in] chunkId   The id of the chunk which we are checking
		/// @param[in] clli  The lease/location information about the chunk.
		/// @param[in] extraReplicas  The target # of additional replicas for the chunk
		/// @param[in] candidates   The set of servers on which the additional replicas
		/// 				should be stored
		/// @retval  The # of actual replications triggered
		int ReplicateChunk(chunkId_t chunkId, ChunkPlacementInfo &clli,
				uint32_t extraReplicas);
		int ReplicateChunk(chunkId_t chunkId, ChunkPlacementInfo &clli,
				uint32_t extraReplicas, const std::vector<ChunkServerPtr> &candidates);

		/// The server has finished re-replicating a chunk.  If there is more
		/// re-replication to be done, send it the server's way.
		/// @param[in] server  The server to which re-replication work should be sent
		/// @param[in] chunkReplicated  The chunkid that the server says
		///     it finished replication.
		void FindReplicationWorkForServer(ChunkServerPtr &server, chunkId_t chunkReplicated);

		/// From the candidates, handout work to nodes.  If any chunks are
		/// over-replicated/chunk is deleted from system, add them to delset.
		void HandoutChunkReplicationWork(CRCandidateSet &candidates,
						CRCandidateSet &delset);
		/// From the list of candidates, build a priority list---a list
		/// of chunks with replication level of 1.
		void RebuildPriorityReplicationList();

		/// There are more replicas of a chunk than the requested amount.  So,
		/// delete the extra replicas and reclaim space.  When deleting the addtional
		/// copies, find the servers that are low on space and delete from there.
		/// As part of deletion, we update our mapping of where the chunk is stored.
		/// @param[in] chunkId   The id of the chunk which we are checking
		/// @param[in] clli  The lease/location information about the chunk.
		/// @param[in] extraReplicas  The # of replicas that need to be deleted
		void DeleteAddlChunkReplicas(chunkId_t chunkId, ChunkPlacementInfo &clli,
				uint32_t extraReplicas);

		/// Helper function to check set membership.
		/// @param[in] hosters  Set of servers hosting a chunk
		/// @param[in] server   The server we want to check for membership in hosters.
		/// @retval true if server is a member of the set of hosters;
		///         false otherwise
		bool IsChunkHostedOnServer(const vector<ChunkServerPtr> &hosters,
						const ChunkServerPtr &server);

		/// Periodically, update our estimate of how much space is
		/// used/available in each rack.
		void UpdateRackSpaceUsageCounts();

		/// Does any server have space/write-b/w available for
		/// re-replication
		bool IsAnyServerAvailForReReplication() const;

		/// Periodically, rebalance servers by moving chunks around from
		/// "over utilized" servers to "under utilized" servers.
		/// @retval # of blocks that were moved around
		int RebalanceServers();
		void FindIntraRackRebalanceCandidates(vector<ChunkServerPtr> &candidates,
				const vector<ChunkServerPtr> &nonloadedServers,
				const ChunkPlacementInfo &clli);

		void FindInterRackRebalanceCandidate(ChunkServerPtr &candidate,
				const vector<ChunkServerPtr> &nonloadedServers,
				const ChunkPlacementInfo &clli);


		/// Helper method to replicate a chunk to given set of
		/// candidates.
		/// Returns the # of copies that were triggered.
		int ReplicateChunkToServers(chunkId_t chunkId, ChunkPlacementInfo &clli,
				uint32_t numCopies,
				std::vector<ChunkServerPtr> &candidates);

		/// Return true if c is a server in mChunkServers[].
		bool ValidServer(ChunkServer *c);

		/// For a time period that corresponds to the length of a lease interval,
		/// we are in recovery after a restart.
		/// Also, if the # of chunkservers that are connected to us is
		/// less than some threshold, we are in recovery mode.
		inline bool InRecovery() const;
		inline bool InRecoveryPeriod() const;

                inline bool IsChunkServerRestartAllowed() const;
                void ScheduleChunkServersRestart();
        };

	// When the rebalance planner it works out a plan that specifies
	// which chunk has to be moved from src->dst
	struct RebalancePlanInfo_t {
		static const int hostnamelen = 256;
		chunkId_t chunkId;
		char dst[hostnamelen];
		char src[hostnamelen];
	};

        extern LayoutManager gLayoutManager;
}

#endif // META_LAYOUTMANAGER_H
