//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: ChunkServer.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2006/06/05
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
// \file ChunkServer.h
// \brief Object that handles the communication with an individual
// chunk server. Model here is the following:
//  - For write-allocation, layout manager asks the ChunkServer object
// to send an RPC to the chunk server.
//  - The ChunkServer object sends the RPC and holds on to the request
// that triggered the RPC.
//  - Eventually, when the RPC reply is received, the request is
// re-activated (alongwith the response) and is sent back down the pike.
//
//----------------------------------------------------------------------------

#ifndef META_CHUNKSERVER_H
#define META_CHUNKSERVER_H

#include <string>
#include <sstream>
#include <set>
using std::string;
using std::ostringstream;

#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/pool/pool_alloc.hpp> 

#include <time.h>

#include "libkfsIO/KfsCallbackObj.h"
#include "libkfsIO/NetConnection.h"
#include "request.h"
#include "queue.h"

#include "common/properties.h"


namespace KFS
{
        /// Chunk server connects to the meta server, sends a HELLO
        /// message to configure its state with the meta server,  and
        /// from then onwards, the meta server then drives the RPCs.
        /// Types of messages:
        ///   Meta server --> Chunk server: Allocate, Free, Heartbeat
        ///

        class ChunkServer : public KfsCallbackObj,
		public boost::enable_shared_from_this<ChunkServer> {
        public:
		typedef std::set <chunkId_t, std::less<chunkId_t>,
        	    boost::fast_pool_allocator<chunkId_t>
        	> ChunkIdSet;
                ///
                /// Sequence:
                ///  Chunk server connects.
                ///   - A new chunkserver sm is born
                ///   - chunkserver sends a HELLO with config info
                ///   - send/recv messages with that chunkserver.
                ///
                ChunkServer();
                ChunkServer(NetConnectionPtr &conn);
                ~ChunkServer();

		bool CanBeChunkMaster() const {
			return mCanBeChunkMaster;
		}
                void SetCanBeChunkMaster(bool flag) {
                    mCanBeChunkMaster = flag;
                }

                /// Generic event handler to handle network
                /// events. This method gets from the net manager when
                /// it sees some data is available on the socket.
                int HandleRequest(int code, void *data);

                /// Send an RPC to allocate a chunk on this server.
                /// An RPC request is enqueued and the call returns.
                /// When the server replies to the RPC, the request
                /// processing resumes.
                /// @param[in] r the request associated with the RPC call.
		/// @param[in] leaseId the id associated with the write lease.
                /// @retval 0 on success; -1 on failure
                ///
                int AllocateChunk(MetaAllocate *r, int64_t leaseId);

                /// Send an RPC to delete a chunk on this server.
                /// An RPC request is enqueued and the call returns.
                /// When the server replies to the RPC, the request
                /// processing resumes.
                /// @param[in] chunkId name of the chunk that is being
                ///  deleted.
                /// @retval 0 on success; -1 on failure
                ///
                int DeleteChunk(chunkId_t chunkId);

                /// Send an RPC to truncate a chunk.
                /// An RPC request is enqueued and the call returns.
                /// When the server replies to the RPC, the request
                /// processing resumes.
                /// @param[in] chunkId name of the chunk that is being
                ///  truncated
                /// @param[in] s   size to which chunk is being truncated to.
                /// @retval 0 on success; -1 on failure
                ///
		int TruncateChunk(chunkId_t chunkId, off_t s);

		///
		/// Send a message to the server asking it to go down.
		///
		void Retire();

                void Restart();

		/// Method to get the size of a chunk from a chunkserver.
		int GetChunkSize(fid_t fid, chunkId_t chunkId, const std::string &pathname);

		/// Methods to handle (re) replication of a chunk.  If there are
		/// insufficient copies of a chunk, we replicate it.
		int ReplicateChunk( fid_t fid, chunkId_t chunkId, seq_t chunkVersion,
                                const ServerLocation &loc);
                /// Start write append recovery when chunk master is non operational.
		int BeginMakeChunkStable(fid_t fid, chunkId_t chunkId, seq_t chunkVersion);
		/// Notify a chunkserver that the writes to a chunk are done;
		/// the chunkserver in turn should flush dirty data and make the
		/// chunk "stable".
		int MakeChunkStable(fid_t fid, chunkId_t chunkId, seq_t chunkVersion,
			off_t chunkSize, bool hasChunkChecksum, uint32_t chunkChecksum,
			bool addPending = false);

		/// Replication of a chunk finished.  Update statistics
		void ReplicateChunkDone(chunkId_t chunkId) {
			mNumChunkWriteReplications--;
			assert(mNumChunkWriteReplications >= 0);
			if (mNumChunkWriteReplications < 0)
				mNumChunkWriteReplications = 0;
			MovingChunkDone(chunkId);
		}


		/// Accessor method to get # of replications that are being
		/// handled by this server.
		int GetNumChunkReplications() const {
			return mNumChunkWriteReplications;
		}

		/// During re-replication, we want to track how much b/w is
		/// being spent read requests for replication by the server.  This
		/// is to prevent a server being overloaded and becoming
		/// unresponsive as we try to increase the # of replicas.
		int GetReplicationReadLoad() const {
			return mNumChunkReadReplications;
		}

		void UpdateReplicationReadLoad(int count) {
			mNumChunkReadReplications += count;
			if (mNumChunkReadReplications < 0)
				mNumChunkReadReplications = 0;
		}

                /// Periodically, send a heartbeat message to the
                /// chunk server.  The message is enqueued to the list
                /// of RPCs that need to be dispatched; whenever the
                /// dispatcher sends them out, the message goes.
                void Heartbeat();

		/// If a chunkserver isn't responding, don't send any
		/// write load towards it.  We detect loaded servers to be
		/// those that don't respond to heartbeat messages.
		bool IsResponsiveServer() const {
			return (! mDown && ! mHeartbeatSkipped);
		}

		/// To support scheduled down-time and allow maintenance to be
		/// done on the server node, we could "retire" a server; when the
		/// server is being retired, we evacuate the blocks on that server
		/// and re-replicate them elsewhere (on non-retiring nodes).
		/// During the stage where the server is being retired, we don't
		/// want to send any new write traffic to the server. 
		///
		void SetRetiring();

		bool IsRetiring() const {
			return mIsRetiring;
		}

		void IncCorruptChunks() {
			mNumCorruptChunks++;
		}

		/// Provide some stats...useful for ops
		void GetRetiringStatus(string &result);

		/// Notify the server object that the chunk needs evacuation.
		void EvacuateChunk(chunkId_t chunkId) {
			mEvacuatingChunks.insert(chunkId);
		}

		const ChunkIdSet& GetEvacuatingChunks() {
			return mEvacuatingChunks;
		}

		/// When the plan is read in, the set of chunks that
		/// need to be moved to this node is updated.
		void AddToChunksToMove(chunkId_t chunkId) {
			mChunksToMove.insert(chunkId);
		}

		const ChunkIdSet& GetChunksToMove() {
			return mChunksToMove;
		}

		void ClearChunksToMove() {
			mChunksToMove.clear();
		}

		/// Whenever this node re-replicates a chunk that was targeted
		/// for rebalancing, update the set.
		void MovingChunkDone(chunkId_t chunkId) {
			mChunksToMove.erase(chunkId);
		}

		/// Evacuation of a chunk that maybe hosted on this server is
		/// done; if this server is retiring and all chunks on this are
		/// evacuated, we can tell the server to retire.
		void EvacuateChunkDone(chunkId_t chunkId);

                /// Whenever the layout manager determines that this
                /// server has stale chunks, it queues an RPC to
                /// notify the chunk server of the stale data.
                void NotifyStaleChunks(const vector<chunkId_t> &staleChunks);
                void NotifyStaleChunk(chunkId_t staleChunk);

                /// There is a difference between the version # as stored
		/// at the chunkserver and what is on the metaserver.  By sending
		/// this message, the metaserver is asking the chunkserver to change
		/// the version # to what is passed in.
                void NotifyChunkVersChange(fid_t fid, chunkId_t chunkId, seq_t chunkVers);

		/// Dispatch all the pending RPCs to the chunk server.
		void Dispatch();

		/// An op has been dispatched.  Stash a pointer to that op
		/// in the list of dispatched ops.
		/// @param[in] op  The op that was just dispatched
		void Dispatched(MetaChunkRequest *r) {
			mDispatchedReqs.push_back(r);
		}

                /// Accessor method to get the host name/port
                ServerLocation GetServerLocation() const {
			return mLocation;
                }

		string ServerID() const
		{
			return mLocation.ToString();
		}

		/// Check if the hostname/port matches what is passed in
		/// @param[in] name  name to match
		/// @param[in] port  port # to match
		/// @retval true  if a match occurs; false otherwise
		bool MatchingServer(const ServerLocation &loc) const {
			return mLocation == loc;
		}

                /// Setter method to set the host name/port
                void SetServerLocation(const ServerLocation &loc) {
			mLocation = loc;
                }

                /// Setter method to set space
                void SetSpace(int64_t total, int64_t used, int64_t alloc) {
			mTotalSpace = total;
			mUsedSpace = used;
			mAllocSpace = alloc; 
                }

                const char *GetServerName() {
                        return mLocation.hostname.c_str();
                }

		void SetRack(int rackId) {
			mRackId = rackId;
		}
		/// Return the unique identifier for the rack on which the
		/// server is located.
		int GetRack() const {
			return mRackId;
		}

		double GetCPULoadAvg() const {
			return mCpuLoadAvg;
		}

		/// Available space is defined as the difference
		/// between the total storage space available
		/// on the server and the amount of space that
		/// has been parceled out for outstanding writes 
		/// by the meta server.  THat is, alloc space is tied
		/// to the chunks that have been write-leased.  This
		/// has the effect of keeping alloc space tied closely
		/// to used space.
                int64_t GetAvailSpace() const {
			return (mTotalSpace > mAllocSpace ?
                            mTotalSpace - mAllocSpace : 0
                        );
                }

		/// Accessor to that returns an estimate of the # of
		/// concurrent writes that are being handled by this server
		int GetNumChunkWrites() const {
			return mNumChunkWrites;

		}

                int64_t GetNumAppendsWithWid() const {
			return mNumAppendsWithWid;
		}

                int64_t GetTotalSpace() const {
                        return mTotalSpace;
                }

                int64_t GetUsedSpace() const {
                        return mUsedSpace;
                }

		int GetNumChunks () const {
			return mNumChunks;
		}

		/// Return an estimate of disk space utilization on this server.
		/// The estimate is between [0..1]
		float GetSpaceUtilization() const {
			if (mTotalSpace == 0)
				return 0.0;
			return (float) mUsedSpace / (float) mTotalSpace;

		}

		bool IsDown() const {
			return mDown;
		}

                ///
                /// The chunk server went down.  So, fail all the
                /// outstanding ops. 
                ///
                void FailPendingOps();

		/// For monitoring purposes, dump out state as a string.
		/// @param [out] result   The state of this server
		///
		void Ping(string &result);

		seq_t NextSeq() { return mSeqNo++; }
		int TimeSinceLastHeartbeat() const;
                void ForceDown();
		static void SetParameters(const Properties& prop);
                void SetProperties(const Properties& props);
                int64_t Uptime() const { return mUptime; }
                bool ScheduleRestart(int64_t gracefulRestartTimeout, int64_t gracefulRestartAppendWithWidTimeout);
                bool IsRestartScheduled() const {
                    return (mRestartScheduledFlag || mRestartQueuedFlag);
                }
                string DownReason() const {
                    return mDownReason;
                }
                const Properties& HeartBeatProperties() const {
                    return mHeartbeatProperties;
                }

        protected:
		/// Enqueue a request to be dispatched to this server
		/// @param[in] r  the request to be enqueued.
		void Enqueue(MetaChunkRequest *r);

                /// A sequence # associated with each RPC we send to
                /// chunk server.  This variable tracks the seq # that
                /// we should use in the next RPC.
                seq_t mSeqNo;
                /// A handle to the network connection
                NetConnectionPtr mNetConnection;

                /// Are we thru with processing HELLO message
                bool mHelloDone;

		/// Boolean that tracks whether this server is down
		bool mDown;

                /// Is there a heartbeat message for which we haven't
		/// recieved a reply yet?  If yes, dont' send one more
                bool   mHeartbeatSent;

		/// did we skip the sending of a heartbeat message?
                bool mHeartbeatSkipped;

                time_t mLastHeartbeatSent;
                static int sHeartbeatTimeout;
                static int sHeartbeatInterval;
                static int sHeartbeatLogInterval;

		/// For record append's, can this node be a chunk master
		bool mCanBeChunkMaster;

		/// is the server being retired
                bool mIsRetiring;
		/// when we did we get the retire request
		time_t mRetireStartTime;
		
		/// when did we get the last heartbeat reply
		time_t mLastHeard;

		/// Set of chunks on this server that need to be evacuated
		/// whenever this node is to be retired; when evacuation set is
		/// empty, the server can be retired.
		ChunkIdSet mEvacuatingChunks;

		/// Set of chunks that need to be moved to this server.
		/// This set was previously computed by the rebalance planner.
		ChunkIdSet mChunksToMove;

                /// Location of the server at which clients can
                /// connect to
		ServerLocation mLocation;

		/// A unique id to denote the rack on which the server is located.
		/// -1 signifies that we don't what rack the server is on and by
		/// implication, all servers are on same rack
		int mRackId;

		/// Keep a count of how many corrupt chunks we are seeing on
		/// this node; an indicator of the node in trouble?
		int mNumCorruptChunks;

                /// total space available on this server
                int64_t mTotalSpace;
                /// space that has been used by chunks on this server
                int64_t mUsedSpace;

                /// space that has been allocated for chunks: this
                /// corresponds to the allocations that have been
                /// made, but not all of the allocated space is used.
                /// For instance, when we have partially filled
                /// chunks, there is space is allocated for a chunk
                /// but that space hasn't been fully used up.
                int64_t mAllocSpace;

		/// # of chunks hosted on this server; useful for
		/// reporting purposes
		long mNumChunks;

		/// An estimate of the CPU load average as reported by the
		/// chunkserver.  When selecting nodes for block allocation, we
		/// can use this info to weed out the most heavily loaded N% of
		/// the nodes.
		double mCpuLoadAvg;

		/// Chunkserver returns the # of drives on the node in a
		/// heartbeat response; we can then show this value on the UI
		int mNumDrives;

		/// An estimate of the # of writes that are being handled
		/// by this server.  We use this value to update mAllocSpace
		/// The problem we have is that, we can end up with lots of
		/// partial chunks and over time such drift can significantly
		/// reduce the available space on the server (space is held
		/// down for by the partial chunks that may never be written to).
		/// Since writes can occur only when someone gets a valid write lease,
		/// we track the # of write leases that are issued and where the
		/// writes are occurring.  So, whenever we get a heartbeat, we
		/// can update alloc space as a sum of the used space and the # of
		/// writes that are currently being handled by this server.
		int     mNumChunkWrites;
                int64_t mNumAppendsWithWid;


		/// Track the # of chunk replications (write/read) that are going on this server
		int mNumChunkWriteReplications;
		int mNumChunkReadReplications;

		typedef MetaQueue <MetaChunkRequest> PendingReqs;
                /// list of RPCs that need to be sent to this chunk
                /// server.  This list is shared between the main
                /// event processing loop and the network thread.
                PendingReqs mPendingReqs;
                /// list of RPCs that we have sent to this chunk
                /// server.  This list is operated by the network
                /// thread.
		typedef std::list <MetaChunkRequest *> DispatchedReqs;
                DispatchedReqs mDispatchedReqs;
                int64_t    mLostChunks;
                int64_t    mUptime;
                Properties mHeartbeatProperties;
                bool       mRestartScheduledFlag;
                bool       mRestartQueuedFlag;
                time_t     mRestartScheduledTime;
                time_t     mLastHeartBeatLoggedTime;
                string     mDownReason;

                ///
                /// We have received a message from the chunk
                /// server. Do something with it.
                /// @param[in] iobuf  An IO buffer stream with message
                /// received from the chunk server.
                /// @param[in] msgLen  Length in bytes of the message.
                /// @retval 0 if message was processed successfully;
                /// -1 if there was an error
                ///
                int HandleMsg(IOBuffer *iobuf, int msgLen);

		/// Handlers for the 3 types of messages we could get:
		/// 1. Hello message from a chunkserver
		/// 2. An RPC from a chunkserver
		/// 3. A reply to an RPC that we have sent previously.

		int HandleHelloMsg(IOBuffer *iobuf, int msgLen);
		int HandleCmd(IOBuffer *iobuf, int msgLen);
		int HandleReply(IOBuffer *iobuf, int msgLen);

		/// Send a response message to the MetaRequest we got.
		void SendResponse(MetaChunkRequest *op);

                ///
                /// Given a response from a chunkserver, find the
                /// associated request that we previously sent.
                /// Request/responses are matched based on sequence
                /// numbers in the messages.
                ///
                /// @param[in] cseq The sequence # of the op we are
                /// looking for.
                /// @retval The matching request if one exists; NULL
                /// otherwise
                ///
                MetaChunkRequest *FindMatchingRequest(seq_t cseq);

		MetaRequest* GetOp(IOBuffer& iobuf, int msgLen, const char* errMsgPrefix);

                ///
                /// The response sent by a chunkserver is of the form:
                /// OK \r\n
                /// Cseq: <seq #>\r\n
                /// Status: <status> \r\n\r\n
                /// Extract out Cseq, Status
                ///
                /// @param[in] buf Buffer containing the response
                /// @param[in] bufLen length of buf
                /// @param[out] prop  Properties object with the response header/values
                ///
                bool ParseResponse(std::istream& is, Properties &prop);
                ///
                /// The chunk server went down.  So, stop the network timer event;
		/// also, fail all the dispatched ops.
                ///
		void Error(const char* errorMsg);
		void FailDispatchedOps();
        };

	class ChunkServerMatcher {
		const ChunkServer * const target;

	public:
		ChunkServerMatcher(const ChunkServer *t): target(t) { };
		bool operator() (const ChunkServerPtr &c) {
			return c.get() == target;
		}
	};

	extern void ChunkServerHeartbeaterInit();
}

#endif // META_CHUNKSERVER_H
