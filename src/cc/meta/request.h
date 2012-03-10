/*!
 * $Id: request.h 3489 2011-12-15 00:36:06Z sriramr $
 *
 * \file request.h
 * \brief protocol requests to KFS metadata server
 *
 * The model is that various receiver threads handle network
 * connections and extract RPC parameters, then queue a request
 * of the appropriate type for the metadata server to process.
 * When the operation is finished, the server calls back to the
 * receiver with status and any results.
 *
 * Copyright 2008 Quantcast Corp.
 * Copyright 2006-2008 Kosmix Corp.
 *
 * This file is part of Kosmos File System (KFS).
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
#if !defined(KFS_REQUEST_H)
#define KFS_REQUEST_H

#include "common/kfsdecls.h"
#include "kfstypes.h"
#include "meta.h"
#include "thread.h"
#include "util.h"
#include <deque>
#include <istream>
#include <fstream>
#include <sstream>
#include <vector>

#include "libkfsIO/KfsCallbackObj.h"
#include "common/properties.h"

using std::ofstream;
using std::vector;
using std::ostringstream;
using std::ostream;

namespace KFS {

/*!
 * \brief Metadata server operations
 */
enum MetaOp {
	// Client -> Metadata server ops
	META_LOOKUP,
	META_LOOKUP_PATH,
	META_CREATE,
	META_MKDIR,
	META_REMOVE,
	META_RMDIR,
	META_READDIR,
	META_READDIRPLUS,
	META_GETALLOC,
	META_GETLAYOUT,
	META_ALLOCATE,
	META_TRUNCATE,
	META_RENAME,
	META_MAKE_CHUNKS_STABLE, //!< Client is asking us to make all the chunks of the file stable
	META_SETMTIME, //!< Set the mtime on a specific file to support cp -p
	META_CHANGE_FILE_REPLICATION, //! < Client is asking for a change in file's replication factor
	META_COALESCE_BLOCKS, //!< Client is asking for blocks from one file to be coalesced with another
	//!< Admin is notifying us to retire a chunkserver
	META_RETIRE_CHUNKSERVER,
	//!< Admin is notifying us to toggle rebalancing
	META_TOGGLE_REBALANCING,
	//!< Admin is notifying us to execute a rebalance plan
	META_EXECUTE_REBALANCEPLAN,
	//!< Read a config and update some of the settings
	META_READ_CONFIG,
	META_TOGGLE_WORM, //!< Toggle metaserver's WORM mode
	//!< Metadata server <-> Chunk server ops
	META_HELLO,  //!< Hello RPC sent by chunkserver on startup
	META_BYE,  //!< Internally generated op whenever a chunkserver goes down
	META_CHUNK_HEARTBEAT, //!< Periodic heartbeat from meta->chunk
	META_CHUNK_ALLOCATE, //!< Allocate chunk RPC from meta->chunk
	META_CHUNK_DELETE,  //!< Delete chunk RPC from meta->chunk
	META_CHUNK_TRUNCATE, //!< Truncate chunk RPC from meta->chunk
	META_CHUNK_STALENOTIFY, //!< Stale chunk notification RPC from meta->chunk
        META_BEGIN_MAKE_CHUNK_STABLE,
	META_CHUNK_MAKE_STABLE, //!< Notify a chunkserver to make a chunk stable
	META_CHUNK_COALESCE_BLOCK, //!< Notify a chunkserver to coalesce a chunk from file to another
	META_CHUNK_VERSCHANGE, //!< Notify chunkserver of version # change from meta->chunk
	META_CHUNK_REPLICATE, //!< Ask chunkserver to replicate a chunk
	META_CHUNK_SIZE, //!< Ask chunkserver for the size of a chunk
	META_CHUNK_REPLICATION_CHECK, //!< Internally generated
	META_CHUNK_CORRUPT, //!< Chunkserver is notifying us that a chunk is corrupt
	//!< All the blocks on the retiring server have been evacuated and the
	//!< server can safely go down.  We are asking the server to take a graceful bow
	META_CHUNK_RETIRE,
	//!< Lease related messages
	META_LEASE_ACQUIRE,
	META_LEASE_RENEW,
	META_LEASE_RELINQUISH,
	//!< Internally generated to cleanup leases
	META_LEASE_CLEANUP,
	//!< Internally generated to update the increment for chunk version #'s
	META_CHANGE_CHUNKVERSIONINC,

	//!< Metadata server monitoring
	META_PING, //!< Print out chunkserves and their configs
	META_STATS, //!< Print out whatever statistics/counters we have
	META_RECOMPUTE_DIRSIZE, //! < Do a top-down size update
	META_DUMP_CHUNKTOSERVERMAP, //! < Dump out the chunk -> location map
	META_DUMP_CHUNKREPLICATIONCANDIDATES, //! < Dump out the list of chunks being re-replicated
	META_FSCK, //!< Check all blocks and report files that have missing blocks
	META_CHECK_LEASES, //! < Check all the leases and clear out expired ones
	META_OPEN_FILES, //!< Print out open files---for which there is a valid read/write lease
	META_UPSERVERS, //!< Print out live chunk servers
        META_LOG_MAKE_CHUNK_STABLE, //!< Emit log record with chunk length and checksum
	META_LOG_MAKE_CHUNK_STABLE_DONE, //!< Emit log record with successful completion of make chunk stable.
	META_SET_CHUNK_SERVERS_PROPERTIES,
        META_CHUNK_SERVER_RESTART,
        META_CHUNK_SET_PROPERTIES,
        META_GET_CHUNK_SERVERS_COUNTERS,

        META_NUM_OPS_COUNT // must be the last one
};

/*!
 * \brief Meta request base class
 */
struct MetaRequest {
	const MetaOp op; //!< type of request
	int status;	//!< returned status
	int clientProtoVers; //!< protocol version # sent by client
        std::string statusMsg;
	seq_t opSeqno;	//!< command sequence # sent by the client
	seq_t seqno;	//!< sequence no. in log
	const bool mutation; //!< mutates metatree
	bool suspended;  //!< is this request suspended somewhere
	KfsCallbackObj *clnt; //!< a handle to the client that generated this request.
	time_t enqueueTime; //!< when was it parsed/put in Q at client
	time_t submitTime; //!< when was it submitted for processing
	time_t doneTime;  //! when did processing finish
	MetaRequest(MetaOp o, seq_t ops, int pv, bool mu):
		op(o), status(0), clientProtoVers(pv), statusMsg(), opSeqno(ops), seqno(0), mutation(mu),
		suspended(false), clnt(NULL), 
		enqueueTime(0), submitTime(0), doneTime(0) { }
	virtual ~MetaRequest() { }

        virtual void handle();
	//!< when an op finishes execution, we send a response back to
	//!< the client.  This function should generate the appropriate
	//!< response to be sent back as per the KFS protocol.
	virtual void response(ostream &os)
	{
		(void) os; // XXX avoid spurious compiler warnings
	};
	virtual int log(ofstream &file) const = 0; //!< write request to log
	virtual string Show() const { return ""; }
};

extern void process_request(MetaRequest *r);
extern void submit_request(MetaRequest *r);

/*!
 * \brief look up a file name
 */
struct MetaLookup: public MetaRequest {
	fid_t dir;	//!< parent directory fid
	string name;	//!< name to look up
	std::string result; //!< reply
	MetaLookup(seq_t s, int  pv, fid_t d, string n):
		MetaRequest(META_LOOKUP, s, pv, false), dir(d), name(n) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		ostringstream os;

		os << "lookup: name = " << name;
		os << " (parent fid = " << dir << ")";
		return os.str();
	}
};

/*!
 * \brief look up a complete path
 */
struct MetaLookupPath: public MetaRequest {
	fid_t root;	//!< fid of starting directory
	string path;	//!< path to look up
	std::string result; //!< reply
	MetaLookupPath(seq_t s, int pv, fid_t r, string p):
		MetaRequest(META_LOOKUP_PATH, s, pv, false), root(r), path(p) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		ostringstream os;

		os << "lookup_path: path = " << path;
		os << " (root fid = " << root << ")";
		return os.str();
	}
};

/*!
 * \brief create a file
 */
struct MetaCreate: public MetaRequest {
	fid_t dir;	//!< parent directory fid
	string name;	//!< name to create
	fid_t fid;	//!< file ID of new file
	int16_t numReplicas; //!< desired degree of replication
	bool exclusive;  //!< model the O_EXCL flag
	MetaCreate(seq_t s, int pv, fid_t d, string n, int16_t r, bool e):
		MetaRequest(META_CREATE, s, pv, true), dir(d),
		name(n), numReplicas(r), exclusive(e) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		ostringstream os;

		os << "create: name = " << name;
		os << " (parent fid = " << dir << ")";
		os << " desired replication = " << numReplicas;
		return os.str();
	}
};

/*!
 * \brief create a directory
 */
struct MetaMkdir: public MetaRequest {
	fid_t dir;	//!< parent directory fid
	string name;	//!< name to create
	fid_t fid;	//!< file ID of new directory
	MetaMkdir(seq_t s, int pv, fid_t d, string n):
		MetaRequest(META_MKDIR, s, pv, true), dir(d), name(n) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		ostringstream os;

		os << "mkdir: name = " << name;
		os << " (parent fid = " << dir << ")";
		return os.str();
	}
};

/*!
 * \brief remove a file
 */
struct MetaRemove: public MetaRequest {
	fid_t dir;	//!< parent directory fid
	string name;	//!< name to remove
	string pathname; //!< full pathname to remove
	off_t filesize;	//!< size of file that was freed (debugging info)
	MetaRemove(seq_t s, int pv, fid_t d, string n):
		MetaRequest(META_REMOVE, s, pv, true), dir(d), name(n), filesize(0) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		ostringstream os;

		os << "remove: path = " << pathname << " (name = " << name << ")";
		os << " (parent fid = " << dir << ") : filesize: " << filesize;
		return os.str();
	}
};

/*!
 * \brief remove a directory
 */
struct MetaRmdir: public MetaRequest {
	fid_t dir;	//!< parent directory fid
	string name;	//!< name to remove
	string pathname; //!< full pathname to remove
	MetaRmdir(seq_t s, int pv, fid_t d, string n):
		MetaRequest(META_RMDIR, s, pv, true), dir(d), name(n) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		ostringstream os;

		os << "rmdir: path = " << pathname << " (name = " << name << ")";
		os << " (parent fid = " << dir << ")";
		return os.str();
	}
};

/*!
 * \brief read directory contents
 */
struct MetaReaddir: public MetaRequest {
	fid_t dir;	//!< directory to read
	vector <MetaDentry *> v; //!< vector of results
	MetaReaddir(seq_t s, int pv, fid_t d):
		MetaRequest(META_READDIR, s, pv, false), dir(d) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		ostringstream os;

		os << "readdir: dir fid = " << dir;
		return os.str();
	}
};

/*!
 * \brief read directory contents and get file attributes
 */
struct MetaReaddirPlus: public MetaRequest {
	fid_t dir;	//!< directory to read
	ostringstream v; //!< results built out into a string
	int numEntries; //!< # of entries in the directory
	MetaReaddirPlus(seq_t s, int pv, fid_t d):
		MetaRequest(META_READDIRPLUS, s, pv, false), dir(d) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		ostringstream os;

		os << "readdir plus: dir fid = " << dir;
		return os.str();
	}
};

/*!
 * \brief get allocation info. a chunk for a file
 */
struct MetaGetalloc: public MetaRequest {
	fid_t fid;	//!< file for alloc info is needed
	chunkOff_t offset; //!< offset of chunk within file
	chunkId_t chunkId; //!< Id of the chunk corresponding to offset
	seq_t chunkVersion; //!< version # assigned to this chunk
	vector<ServerLocation> locations; //!< where the copies of the chunks are
	std::string pathname; //!< pathname of the file (useful to print in debug msgs)
	MetaGetalloc(seq_t s, int pv, fid_t f, chunkOff_t o, std::string n):
		MetaRequest(META_GETALLOC, s, pv, false), fid(f), offset(o), 
		chunkId(-1), chunkVersion(-1), pathname(n)
	{}
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		ostringstream os;

		os << "getalloc: " << pathname << " (fid = " << fid << ")";
		os << " offset = " << offset;
		return os.str();
	}
};

/*!
 * \brief layout information for a chunk
 */
struct ChunkLayoutInfo {
	chunkOff_t offset; //!< offset of chunk within file
	chunkId_t chunkId; //!< Id of the chunk corresponding to offset
	seq_t chunkVersion; //!< version # assigned to this chunk
	vector<ServerLocation> locations; //!< where the copies of the chunks are
	string toString()
	{
		ostringstream os;

		os << offset << " " << chunkId << " " ;
		os << chunkVersion << " " << locations.size() << " ";
		for (vector<ServerLocation>::size_type i = 0;
			i < locations.size(); ++i) {
			os << locations[i].hostname << " " << locations[i].port << " ";
		}
		return os.str();
	}
};

/*!
 * \brief get allocation info. for all chunks of a file
 */
struct MetaGetlayout: public MetaRequest {
	fid_t fid;	//!< file for layout info is needed
	vector <ChunkLayoutInfo> v; //!< vector of results
	MetaGetlayout(seq_t s, int pv, fid_t f):
		MetaRequest(META_GETLAYOUT, s, pv, false), fid(f) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		ostringstream os;

		os << "getlayout: fid = " << fid;
		return os.str();
	}
};

class ChunkServer;
typedef boost::shared_ptr<ChunkServer> ChunkServerPtr;

/*!
 * \brief allocate a chunk for a file
 */
struct MetaAllocate: public MetaRequest {
	MetaRequest *req; //!< req that triggered allocation (e.g., truncate)
	fid_t fid;		//!< file for which space has to be allocated
	chunkOff_t offset;	//!< offset of chunk within file
	chunkId_t chunkId;	//!< Id of the chunk that was allocated
	seq_t chunkVersion;	//!< version # assigned to this chunk
	std::string pathname;   //!< full pathname that corresponds to fid
	std::string clientHost; //!< the host from which request was received
	int16_t  numReplicas;	//!< inherited from file's fattr
	int16_t  clientRack;    //!< rack from where the request is originating from
	bool layoutDone;	//!< Has layout of chunk been done
	//!< when set, the allocation request is asking the metaserver to append
	//!< a chunk to the file and let the client know the offset at which it was
	//!< appended.
	bool appendChunk;	
	//!< Write append only: the space reservation size that will follow the
        //!< chunk allocation.
        int spaceReservationSize;
	//!< Suggested max # of concurrent appenders per chunk
	int maxAppendersPerChunk;
	//!< Server(s) on which this chunk has been placed
	vector <ChunkServerPtr> servers;
	//!< For replication, the master that runs the transaction
	//!< for completing the write.
	ChunkServerPtr master;
	uint32_t numServerReplies;
        bool logFlag;
        MetaAllocate* next;
	MetaAllocate(seq_t s, int pv, fid_t f, chunkOff_t o):
		MetaRequest(META_ALLOCATE, s, pv, true),
		req(NULL),
		fid(f),
		offset(o),
		chunkId(-1),
		pathname(),
		clientHost(),
		numReplicas(0),
		clientRack(-1),
		layoutDone(false),
		appendChunk(false),
		spaceReservationSize(1 << 20),
		maxAppendersPerChunk(64),
		master(),
		numServerReplies(0),
                logFlag(true),
                next(0)
	{}
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const;
        void LayoutDone();
};

/*!
 * \brief truncate a file
 */
struct MetaTruncate: public MetaRequest {
	fid_t fid;	//!< file for which space has to be allocated
	chunkOff_t offset; //!< offset to truncate the file to
	string pathname; //!< full pathname for file being truncated
	//!< set if the blks from the beginning of the file to the offset have
	//!< to be deleted.
	bool pruneBlksFromHead; 
	MetaTruncate(seq_t s, int pv, fid_t f, chunkOff_t o):
		MetaRequest(META_TRUNCATE, s, pv, true), fid(f), offset(o), 
		pruneBlksFromHead(false) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		ostringstream os;

		if (pruneBlksFromHead)
			os << "prune from head: ";
		else
			os << "truncate: ";
		os << "path = " << pathname << " fid = " << fid;
		os << " offset = " << offset;

		return os.str();
	}
};

/*!
 * \brief rename a file or directory
 */
struct MetaRename: public MetaRequest {
	fid_t dir;	//!< parent directory
	string oldname;	//!< old file name
	string newname;	//!< new file name
	string oldpath; //!< fully-qualified old pathname
	bool overwrite; //!< overwrite newname if it exists
	MetaRename(seq_t s, int pv, fid_t d, const char *o, const char *n, bool c):
		MetaRequest(META_RENAME, s, pv, true), dir(d),
			oldname(o), newname(n), overwrite(c) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		ostringstream os;

		os << "rename: oldname = " << oldpath << "(name = " << oldname << ")";
		os << " (fid = " << dir << ")";
		os << " newname = " << newname;
		return os.str();
	}
};

/*!
 * \brief make all the chunks in a file "stable".
 */

struct MetaMakeChunksStable: public MetaRequest {
	fid_t fid;		//!< fid whose chunks need to be made stable
	string pathname;	//!< full path name
	MetaMakeChunksStable(seq_t s, int pv, fid_t f, string p):
		MetaRequest(META_MAKE_CHUNKS_STABLE, s, pv, false), 
		fid(f), pathname(p) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		ostringstream os;

		os << "make-all-chunks-stable: path = " << pathname;
		os << " (fid = " << fid << ")";
		return os.str();
	}
};

/*!
 * \brief set the mtime for a file or directory
 */
struct MetaSetMtime: public MetaRequest {
	fid_t fid;		//!< stash the fid for logging
	string pathname;	//!< absolute path for which we want to set the mtime
	struct timeval mtime; 	//!< the mtime to set
	MetaSetMtime(seq_t s, int pv, string p, struct timeval &m):
		MetaRequest(META_SETMTIME, s, pv, true), pathname(p), mtime(m) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		ostringstream os;

		os << "setmtime: path = " << pathname << " ; mtime: " << showtime(mtime);
		return os.str();
	}
};


/*!
 * \brief change a file's replication factor
 */
struct MetaChangeFileReplication: public MetaRequest {
	fid_t fid;	//!< fid whose replication has to be changed
	int16_t numReplicas; //!< desired degree of replication
	MetaChangeFileReplication(seq_t s, int pv, fid_t f, int16_t n):
		MetaRequest(META_CHANGE_FILE_REPLICATION, s, pv, true), fid(f), numReplicas(n) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		ostringstream os;

		os << "change-file-replication: fid = " << fid;
		os << " new # of replicas: " << numReplicas << ' ';
		return os.str();
	}
};

/*!
 * \brief coalesce blocks of one file with another by appending the blocks from
 * src->dest.  After the coalesce is done, src will be of size 0.
 */
struct MetaCoalesceBlocks: public MetaRequest {
	string srcPath; //!< fully-qualified pathname
	string dstPath; //!< fully-qualified pathname
	fid_t  srcFid;
	fid_t  dstFid;
	//!< output: the offset in dst at which the first
	//!< block of src was moved to.
	off_t  dstStartOffset;
	vector<chunkId_t> srcChunks;
	MetaCoalesceBlocks(seq_t s, int pv, const char *o, const char *d):
		MetaRequest(META_COALESCE_BLOCKS, s, pv, true), 
			srcPath(o), dstPath(d),
                        srcFid(-1), dstFid(-1), dstStartOffset(-1), srcChunks() {}
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		ostringstream os;

		os << "coalesce blocks: src = " << srcPath;
		os << " dst = " << dstPath;
		return os.str();
	}
};

/*!
 * \brief Notification to hibernate/retire a chunkserver:
 * Hibernation: when the server is put
 * in hibernation mode, the server is taken down temporarily with a promise that
 * it will come back N secs later; if the server doesnt' come up as promised
 * then re-replication starts.
 *
 * Retirement: is extended downtime.  The server is taken down and we don't know
 * if it will ever come back.  In this case, we use this server (preferably)
 * to evacuate/re-replicate all the blocks off it before we take it down.
 */

struct MetaRetireChunkserver : public MetaRequest {
	ServerLocation location; //<! Location of this server
	int nSecsDown; //<! set to -1, we retire; otherwise, # of secs of down time
	MetaRetireChunkserver(seq_t s, int pv, const ServerLocation &l, int d) :
		MetaRequest(META_RETIRE_CHUNKSERVER, s, pv, false), location(l),
		nSecsDown(d) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	string Show() const
	{
		if (nSecsDown > 0)
			return "Hibernating server: " + location.ToString();
		else
			return "Retiring server: " + location.ToString();
	}
};

/*!
 * \brief Notification to toggle rebalancing state.
 */

struct MetaToggleRebalancing : public MetaRequest {
	bool value; // !< Enable/disable rebalancing
	MetaToggleRebalancing(seq_t s, int pv, bool v) :
		MetaRequest(META_TOGGLE_REBALANCING, s, pv, false), value(v) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		if (value)
			return "Toggle rebalancing: Enable";
		else
			return "Toggle rebalancing: Disable";
	}
};

/*!
 * \brief Execute a rebalance plan that was constructed offline.
*/

struct MetaExecuteRebalancePlan : public MetaRequest {
	std::string planPathname; //<! full path to the file with the plan
	MetaExecuteRebalancePlan(seq_t s, int pv, const std::string &p) :
		MetaRequest(META_EXECUTE_REBALANCEPLAN, s, pv, false), planPathname(p) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		return "Execute rebalance plan : " + planPathname;
	}
};

/*!
 * \brief Read a config file and update params (whatever can be in a running
 * system).
*/
struct MetaReadConfig : public MetaRequest {
	std::string configFn; //<! full path to the file with the config
	MetaReadConfig(seq_t s, int pv, const std::string &p) :
		MetaRequest(META_READ_CONFIG, s, pv, false), configFn(p) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show()
	{
		return "Execute read config : " + configFn;
	}
};

/*!
 * \brief change the number which is used to increment
 * chunk version numbers.  This is an internally
 * generated op whenever (1) the system restarts after a failure
 * or (2) a write-allocation fails because a replica went down.
*/
struct MetaChangeChunkVersionInc : public MetaRequest {
	seq_t cvi;
	//!< The request that depends on the chunk-version-inc # being
	//!< logged to disk.  Once the logging is done, the request
	//!< processing can resume.
	MetaRequest *req;
	MetaChangeChunkVersionInc(seq_t n, MetaRequest *r):
		MetaRequest(META_CHANGE_CHUNKVERSIONINC, 0, 0, true),
		cvi(n), req(r) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual string Show() const
	{
		ostringstream os;

		os << "changing chunk version inc: value = " << cvi;
		return os.str();
	}
};

struct ChunkInfo {
	fid_t     allocFileId; // file id when chunk was allocated
	chunkId_t chunkId;
	seq_t     chunkVersion;
};

/*!
 * \brief hello RPC from a chunk server on startup
 */
struct MetaHello: public MetaRequest {
	ChunkServerPtr server; //!< The chunkserver that sent the hello message
	ServerLocation location; //<! Location of this server
	std::string peerName;
	uint64_t totalSpace; //!< How much storage space does the
			//!< server have (bytes)
	uint64_t usedSpace; //!< How much storage space is used up (in bytes)
        int64_t uptime; //!< Chunk server uptime.
	int rackId; //!< the rack on which the server is located
	int numChunks; //!< # of chunks hosted on this server
        int numNotStableAppendChunks; //!< # of not stable append chunks hosted on this server
	int numNotStableChunks; //!< # of not stable chunks hosted on this server
	int contentLength; //!< Length of the message body
        int64_t numAppendsWithWid;
	vector<ChunkInfo> chunks; //!< Chunks  hosted on this server
	vector<ChunkInfo> notStableChunks;
	vector<ChunkInfo> notStableAppendChunks;
	MetaHello(seq_t s): MetaRequest(META_HELLO, s, 0, false) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		return "Chunkserver Hello";
	}
};

/*!
 * \brief whenever a chunk server goes down, this message is used to clean up state.
 */
struct MetaBye: public MetaRequest {
	ChunkServerPtr server; //!< The chunkserver that went down
	MetaBye(seq_t s, ChunkServerPtr c):
		MetaRequest(META_BYE, s, 0, false), server(c) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual string Show() const
	{
		return "Chunkserver Bye";
	}
};

/*!
 * \brief RPCs that go from meta server->chunk server are
 * MetaRequest's that define a method to generate the RPC
 * request.
 */
struct MetaChunkRequest: public MetaRequest {
	MetaChunkRequest(MetaOp o, seq_t s, bool mu, ChunkServer *c):
		MetaRequest(o, s, 0, mu), server(c) {}
	//!< generate a request message (in string format) as per the
	//!< KFS protocol.
	virtual int log(ofstream &file) const { return 0; }
	virtual void request(ostream &os) = 0;
        virtual void handleReply(const Properties& prop) {}
        virtual void resume() = 0;
private:
	const ChunkServer * const server; // The chunkserver to send this RPC to debug only
};

/*!
 * \brief Allocate RPC from meta server to chunk server
 */
struct MetaChunkAllocate: public MetaChunkRequest {
	int64_t leaseId;
	MetaAllocate * const req;
	MetaChunkAllocate(seq_t n, MetaAllocate *r, ChunkServer *s, int64_t l):
		MetaChunkRequest(META_CHUNK_ALLOCATE, n, false, s),
		leaseId(l), req(r) { }
	virtual void request(ostream &os);
        virtual void resume();
	virtual string Show() const
	{
		return  "meta->chunk allocate: ";
	}
};

/*!
 * \brief Chunk version # change RPC from meta server to chunk server
 */
struct MetaChunkVersChange: public MetaChunkRequest {
	fid_t fid;
	chunkId_t chunkId; //!< The chunk id to free
	seq_t chunkVersion;	//!< version # assigned to this chunk
	MetaChunkVersChange(seq_t n, ChunkServer *s, fid_t f, chunkId_t c, seq_t v):
		MetaChunkRequest(META_CHUNK_VERSCHANGE, n, false, s),
		fid(f), chunkId(c), chunkVersion(v) { }
	virtual void request(ostream &os);
        virtual void resume()
	{
		delete this;
	}           
	virtual string Show() const
	{
		ostringstream os;

		os <<  "meta->chunk vers change: ";
		os << " fid = " << fid;
		os << " chunkId = " << chunkId;
		os << " chunkVersion = " << chunkVersion;
		return os.str();
	}
};

/*!
 * \brief Delete RPC from meta server to chunk server
 */
struct MetaChunkDelete: public MetaChunkRequest {
	chunkId_t chunkId; //!< The chunk id to free
	MetaChunkDelete(seq_t n, ChunkServer *s, chunkId_t c):
		MetaChunkRequest(META_CHUNK_DELETE, n, false, s), chunkId(c) { }
	virtual void request(ostream &os);
        virtual void resume()
	{
		delete this;
	}           
	virtual string Show() const
	{
		ostringstream os;

		os <<  "meta->chunk delete: ";
		os << " chunkId = " << chunkId;
		return os.str();
	}
};

/*!
 * \brief Truncate chunk RPC from meta server to chunk server
 */
struct MetaChunkTruncate: public MetaChunkRequest {
	chunkId_t chunkId; //!< The id of chunk to be truncated
	size_t chunkSize; //!< The size to which chunk should be truncated
	MetaChunkTruncate(seq_t n, ChunkServer *s, chunkId_t c, size_t sz):
		MetaChunkRequest(META_CHUNK_TRUNCATE, n, false, s),
		chunkId(c), chunkSize(sz) { }
	virtual void request(ostream &os);
        virtual void resume()
	{
		delete this;
	}           
	virtual string Show() const
	{
		ostringstream os;

		os <<  "meta->chunk truncate: ";
		os << " chunkId = " << chunkId;
		os << " chunkSize = " << chunkSize;
		return os.str();
	}
};

/*!
 * \brief Replicate RPC from meta server to chunk server.  This
 * message is sent to a "destination" chunk server---that is, a chunk
 * server is told to create a copy of chunk from some source that is
 * already hosting the chunk.  This model allows the destination to
 * replicate the chunk at its convenieance.
 */
struct MetaChunkReplicate: public MetaChunkRequest {
	fid_t fid;  //!< input: we tell the chunkserver what it is
	chunkId_t chunkId; //!< The chunk id to replicate
	seq_t chunkVersion; //!< output: the chunkservers tells us what it did
	ServerLocation srcLocation; //!< where to get a copy from
	ChunkServerPtr server;  //!< "dest" on which we put a copy
	MetaChunkReplicate(seq_t n, ChunkServer *s,
			fid_t f, chunkId_t c, seq_t v,
			const ServerLocation &l):
		MetaChunkRequest(META_CHUNK_REPLICATE, n, false, s),
		fid(f), chunkId(c), chunkVersion(v), srcLocation(l) { }
	virtual void handle();
	virtual void request(ostream &os);
	virtual void handleReply(const Properties& prop)
	{
		fid          = (fid_t)prop.getValue("File-handle",   (long long) 0);
		chunkVersion = (seq_t)prop.getValue("Chunk-version", (long long) 0);
	}
        virtual void resume()
	{
		submit_request(this);
	}
	virtual string Show() const
	{
		ostringstream os;

		os <<  "meta->chunk replicate: ";
		os << " fileId = " << fid;
		os << " chunkId = " << chunkId;
		os << " chunkVersion = " << chunkVersion;
		return os.str();
	}
};

/*!
 * \brief As a chunkserver for the size of a particular chunk.  We use this RPC
 * to compute the filesize: whenever the lease on the last chunk of the file
 * expires, we get the chunk's size and then determine the filesize.
 */
struct MetaChunkSize: public MetaChunkRequest {
	fid_t fid;  //!< input: we use the tuple <fileid, chunkid> to
			//!< find the entry we need.
	chunkId_t chunkId; //!< input: the chunk whose size we need
	off_t chunkSize; //!< output: the chunk size
	off_t filesize; //!< for logging purposes: the size of the file
	/// input: given the pathname, we can update space usage for the path
	/// hierarchy corresponding to pathname; this will enable us to make "du"
	/// instantaneous.
	std::string pathname; 
	MetaChunkSize(seq_t n, ChunkServer *s, fid_t f, chunkId_t c, 
			const std::string &p) :
		MetaChunkRequest(META_CHUNK_SIZE, n, true, s),
		fid(f), chunkId(c), chunkSize(-1), filesize(-1), pathname(p) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void request(ostream &os);
	virtual void handleReply(const Properties& prop)
	{
		chunkSize = prop.getValue("Size", (off_t) -1);
	}
        virtual void resume()
	{
		submit_request(this);
	}
	virtual string Show() const
	{
		ostringstream os;

		os <<  "meta->chunk size: " << pathname;
		os << " fileId = " << fid;
		os << " chunkId = " << chunkId;
		return os.str();
	}
};

/*!
 * \brief Heartbeat RPC from meta server to chunk server.  We can
 * ask the chunk server for lots of stuff; for now, we ask it
 * how much is available/used up.
 */
struct MetaChunkHeartbeat: public MetaChunkRequest {
	MetaChunkHeartbeat(seq_t n, ChunkServer *s):
		MetaChunkRequest(META_CHUNK_HEARTBEAT, n, false, s) { }
	virtual void request(ostream &os);
        virtual void resume()
	{
		delete this;
	}           
	virtual string Show() const
	{
		return "meta->chunk heartbeat";
	}
};

/*!
 * \brief Stale chunk notification message from meta->chunk.  This
 * tells the chunk servers the id's of stale chunks, which the chunk
 * server should get rid of.
 */
struct MetaChunkStaleNotify: public MetaChunkRequest {
	MetaChunkStaleNotify(seq_t n, ChunkServer *s):
		MetaChunkRequest(META_CHUNK_STALENOTIFY, n, false, s) { }
	vector<chunkId_t> staleChunkIds; //!< chunk ids that are stale
	virtual void request(ostream &os);
        virtual void resume()
	{
		delete this;
	}           
	virtual string Show() const
	{
		return "meta->chunk stale notify";
	}
};

struct MetaBeginMakeChunkStable : public MetaChunkRequest {
	const fid_t          fid;           // input
	const chunkId_t      chunkId;       // input
	const seq_t          chunkVersion;  // input
        const ServerLocation serverLoc;     // processing this cmd
	int64_t              chunkSize;     // output
	uint32_t             chunkChecksum; // output
	MetaBeginMakeChunkStable(seq_t n, ChunkServer *s,
			const ServerLocation& l, fid_t f, chunkId_t c, seq_t v) :
		MetaChunkRequest(META_BEGIN_MAKE_CHUNK_STABLE, n, false, s),
		fid(f), chunkId(c), chunkVersion(v), serverLoc(l),
                chunkSize(-1), chunkChecksum(0)
		{}
        virtual void handle();
	virtual void request(ostream &os);
	virtual void handleReply(const Properties& prop)
	{
		chunkSize     =           prop.getValue("Chunk-size",     (int64_t) -1);
		chunkChecksum = (uint32_t)prop.getValue("Chunk-checksum", (uint64_t)0);
        }
        virtual void resume()
	{
		submit_request(this);
	}
	virtual string Show() const {
		ostringstream os;
		os << "begin-make-chunk-stable:"
                " server: "        << serverLoc.ToString() <<
		" seq: "           << opSeqno <<
                " status: "        << status <<
                    (statusMsg.empty() ? "" : " ") << statusMsg <<
		" fileid: "        << fid <<
		" chunkid: "       << chunkId <<
		" chunkvers: "     << chunkVersion <<
                " chunkSize: "     << chunkSize <<
                " chunkChecksum: " << chunkChecksum;
		return os.str();
	}
};

struct MetaLogMakeChunkStable : public MetaRequest, public  KfsCallbackObj {
	const fid_t     fid;              // input
	const chunkId_t chunkId;          // input
	const seq_t     chunkVersion;     // input
	const int64_t   chunkSize;        // input
	const uint32_t  chunkChecksum;    // input
	const bool      hasChunkChecksum; // input
	MetaLogMakeChunkStable(fid_t fileId, chunkId_t id, seq_t version,
		int64_t size, bool hasChecksum, uint32_t checksum, seq_t seqNum,
		bool logDoneTypeFlag = false)
		: MetaRequest(logDoneTypeFlag ?
		  	META_LOG_MAKE_CHUNK_STABLE_DONE :
		  	META_LOG_MAKE_CHUNK_STABLE, seqNum, 0, true),
		  KfsCallbackObj(),
		  fid(fileId),
		  chunkId(id),
		  chunkVersion(version),
		  chunkSize(size),
		  chunkChecksum(checksum),
		  hasChunkChecksum(hasChecksum)
	{
		SET_HANDLER(this, &MetaLogMakeChunkStable::logDone);
		clnt = this;
	}
	virtual void handle() { status = 0; }
	virtual string Show() const {
		ostringstream os;
		os << (op == META_LOG_MAKE_CHUNK_STABLE ?
			"log-make-chunk-stable:" :
			"log-make-chunk-stable-done:") <<
		" fleid: "         << fid <<
		" chunkid: "       << chunkId <<
		" chunkvers: "     << chunkVersion <<
		" chunkSize: "     << chunkSize <<
                " chunkChecksum: " << (hasChunkChecksum ?
			int64_t(chunkChecksum) : int64_t(-1));
		return os.str();
	}
	virtual int log(ofstream &file) const;
	int logDone(int code, void *data);
};

struct MetaLogMakeChunkStableDone : public MetaLogMakeChunkStable {
	MetaLogMakeChunkStableDone(fid_t fileId, chunkId_t id, seq_t version,
		int64_t size, bool hasChecksum, uint32_t checksum, seq_t seqNum)
		: MetaLogMakeChunkStable(fileId, id, version, size, hasChecksum,
			checksum, seqNum, true)
		{}
};

/*!
 * \brief Notification message from meta->chunk asking the server to make a
 * chunk.  This tells the chunk server that the writes to a chunk are done and
 * that the chunkserver should flush any dirty data.  
 */
struct MetaChunkMakeStable: public MetaChunkRequest {
	MetaChunkMakeStable(
		seq_t          inSeqNo,
		ChunkServerPtr inServer,
		fid_t          inFileId,
		chunkId_t      inChunkId,
		seq_t          inChunkVersion,
		off_t          inChunkSize,
		bool           inHasChunkChecksum,
		uint32_t       inChunkChecksum,
		bool           inAddPending)
		: MetaChunkRequest(META_CHUNK_MAKE_STABLE, inSeqNo, false, inServer.get()),
		  fid(inFileId),
		  chunkId(inChunkId),
		  chunkVersion(inChunkVersion),
                  chunkSize(inChunkSize),
		  hasChunkChecksum(inHasChunkChecksum),
		  addPending(inAddPending),
		  chunkChecksum(inChunkChecksum),
                  server(inServer)
                {}
	const fid_t          fid;          //!< input: we tell the chunkserver what it is
	const chunkId_t      chunkId;      //!< The chunk id to make stable
	const seq_t          chunkVersion; //!< The version tha the chunk should be in
        const off_t          chunkSize;
        const bool           hasChunkChecksum:1;
	const bool           addPending:1;
        const uint32_t       chunkChecksum;
	const ChunkServerPtr server;        //!< The chunkserver that sent us this message
        virtual void handle();
	virtual void request(ostream &os);
        virtual void resume()
	{
		submit_request(this);
	}
	virtual string Show() const;
};


/*!
 * For scheduled downtime, we evacaute all the chunks on a server; when
 * we know that the evacuation is finished, we tell the chunkserver to retire.
 */
struct MetaChunkRetire: public MetaChunkRequest {
	MetaChunkRetire(seq_t n, ChunkServer *s):
		MetaChunkRequest(META_CHUNK_RETIRE, n, false, s) { }
	virtual void request(ostream &os);
        virtual void resume()
	{
		delete this;
	}           
	virtual string Show() const
	{
		return "chunkserver retire";
	}
};

struct MetaChunkSetProperties: public MetaChunkRequest {
	const string serverProps;
	MetaChunkSetProperties(
			seq_t n, ChunkServer *s, const Properties& props)
		: MetaChunkRequest(META_CHUNK_SET_PROPERTIES, n, false, s),
		  serverProps(Properties2Str(props))
	{}
	virtual void request(ostream &os);
        virtual void resume()
	{
		delete this;
	}           
	virtual string Show() const
	{
		return "chunkserver set properties";
	}
	static string Properties2Str(const Properties& props)
	{
		string ret;
		props.getList(ret, "");
		return ret;
	}
};

struct MetaChunkServerRestart : public MetaChunkRequest {
	MetaChunkServerRestart(seq_t n, ChunkServer *s)
		: MetaChunkRequest(META_CHUNK_SERVER_RESTART, n, false, s)
		{}
	virtual void request(ostream &os);
        virtual void resume()
	{
		delete this;
	}           
	virtual string Show() const
	{
		return "chunkserver restart";
	}
};

/*!
 * \brief For monitoring purposes, a client/tool can send a PING
 * request.  In response, the server replies with the list of all
 * connected chunk servers and their locations as well as some state
 * about each of those servers.
 */
struct MetaPing: public MetaRequest {
	string systemInfo; //!< result that describes system info (space etc)
	string servers; //!< result that contains info about chunk servers
	string retiringServers; //!< info about servers that are being retired
	string downServers; //!< info about servers that have gone down
	MetaPing(seq_t s, int pv):
		MetaRequest(META_PING, s, pv, false) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		return "ping";
	}
};

/*!
 * \brief For monitoring purposes, a client/tool can request metaserver
 * to provide a list of live chunkservers.
 */
struct MetaUpServers: public MetaRequest {
	ostringstream stringStream;
	MetaUpServers(seq_t s, int pv):
		MetaRequest(META_UPSERVERS, s, pv, false) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		return "upservers";
	}
};

/*!
 * \brief To toggle WORM mode of metaserver a client/tool can send a
 * TOGGLE_WORM request. In response, the server changes its WORM state.
 */
struct MetaToggleWORM: public MetaRequest {
	bool value; // !< Enable/disable WORM
	MetaToggleWORM(seq_t s, int pv, bool v):
		MetaRequest(META_TOGGLE_WORM, s, pv, false), value(v) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		if (value)
			return "Toggle WORM: Enabled";
		else
			return "Toggle WORM: Disabled";
	}
};
/*!
 * \brief For monitoring purposes, a client/tool can send a STATS
 * request.  In response, the server replies with the list of all
 * counters it keeps.
 */
struct MetaStats: public MetaRequest {
	string stats; //!< result
	MetaStats(seq_t s, int pv):
		MetaRequest(META_STATS, s, pv, false) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		return "stats";
	}
};

/*!
 * \brief For debugging purposes, recompute the size of the dir tree
 */
struct MetaRecomputeDirsize: public MetaRequest {
	MetaRecomputeDirsize(seq_t s, int pv):
		MetaRequest(META_RECOMPUTE_DIRSIZE, s, pv, false) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		return "recompute dir size";
	}
};

/*!
 * \brief For debugging purposes, dump out the chunk->location map
 * to a file.
 */
struct MetaDumpChunkToServerMap: public MetaRequest {
	string chunkmapFile; //!< file to which the chunk map was written to
	MetaDumpChunkToServerMap(seq_t s, int pv):
		MetaRequest(META_DUMP_CHUNKTOSERVERMAP, s, pv, false) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		return "dump chunk2server map";
	}
};

/*!
 * \brief For debugging purposes, check the status of all the leases
 */
struct MetaCheckLeases: public MetaRequest {
	MetaCheckLeases(seq_t s, int pv):
		MetaRequest(META_CHECK_LEASES, s, pv, false) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		return "checking all leases";
	}
};

/*!
 * \brief For debugging purposes, dump out the set of blocks that are currently
 * being re-replicated.
 */
struct MetaDumpChunkReplicationCandidates: public MetaRequest {
	MetaDumpChunkReplicationCandidates(seq_t s, int pv):
		MetaRequest(META_DUMP_CHUNKREPLICATIONCANDIDATES, s, pv, false) { }
	// list of blocks that are being re-replicated
	std::string blocks;
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		return "dump chunk replication candidates";
	}
};

/*!
 * \brief Check the replication level of all blocks in the system.  Return back
 * a list of files that have blocks missing.
*/
struct MetaFsck: public MetaRequest {
	MetaFsck(seq_t s, int pv):
		MetaRequest(META_FSCK, s, pv, false) { }
	// a status message about what is missing/endangered
	std::string fsckStatus;
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		return "fsck";
	}
};

/*!
 * \brief For monitoring purposes, a client/tool can send a OPEN FILES
 * request.  In response, the server replies with the list of all
 * open files---files for which there is a valid lease
 */
struct MetaOpenFiles: public MetaRequest {
	string openForRead; //!< result
	string openForWrite; //!< result
	MetaOpenFiles(seq_t s, int pv):
		MetaRequest(META_OPEN_FILES, s, pv, false) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		return "open files";
	}
};

struct MetaSetChunkServersProperties : public MetaRequest {
	Properties properties; // input
	MetaSetChunkServersProperties(seq_t s, int pv)
		: MetaRequest(META_SET_CHUNK_SERVERS_PROPERTIES, s, pv, false),
		  properties()
		{}
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		std::string ret("set chunk servers properties ");
		properties.getList(ret, "", ";");
		return ret;
	}
};

struct MetaGetChunkServersCounters : public MetaRequest {
	std::string resp; 
	MetaGetChunkServersCounters(seq_t s, int pv)
		: MetaRequest(META_GET_CHUNK_SERVERS_COUNTERS, s, pv, false),
		  resp()
		{}
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		return std::string("get chunk servers counters ");
	}
};

/*!
 * \brief Op for handling a notify of a corrupt chunk
 */
struct MetaChunkCorrupt: public MetaRequest {
	fid_t fid; //!< input
	chunkId_t chunkId; //!< input
	int isChunkLost; //! < input
	ChunkServerPtr server; //!< The chunkserver that sent us this message
	MetaChunkCorrupt(seq_t s, fid_t f, chunkId_t c):
		MetaRequest(META_CHUNK_CORRUPT, s, 0, false),
		fid(f), chunkId(c), isChunkLost(0) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		ostringstream os;

		os << (isChunkLost ? "lost" : "corrupt") << " chunk: fid = " << fid << " chunkid = " << chunkId;
		return os.str();
	}
};

/*!
 * \brief Op for acquiring a lease on a chunk of a file.
 */
struct MetaLeaseAcquire: public MetaRequest {
	LeaseType leaseType; //!< input
	std::string pathname;   //!< full pathname of the file that owns chunk
	chunkId_t chunkId; //!< input
	int64_t leaseId; //!< result
	MetaLeaseAcquire(seq_t s, int pv, chunkId_t c, std::string n):
		MetaRequest(META_LEASE_ACQUIRE, s, pv, false),
		leaseType(READ_LEASE), pathname(n), chunkId(c), leaseId(-1) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		ostringstream os;

		os << "lease acquire: " << pathname << " ";
		if (leaseType == READ_LEASE)
			os << "read lease ";
		else
			os << "write lease ";

		os << " chunkId = " << chunkId;
		return os.str();
	}
};

/*!
 * \brief Op for renewing a lease on a chunk of a file.
 */
struct MetaLeaseRenew: public MetaRequest {
	LeaseType leaseType; //!< input
	std::string pathname;   //!< full pathname of the file that owns chunk
	chunkId_t chunkId; //!< input
	int64_t leaseId; //!< input
	MetaLeaseRenew(seq_t s, int pv, LeaseType t, chunkId_t c, int64_t l, std::string n):
		MetaRequest(META_LEASE_RENEW, s, pv, false),
		leaseType(t), pathname(n), chunkId(c), leaseId(l) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		ostringstream os;

		os << "lease renew: " << pathname << " ";
		if (leaseType == READ_LEASE)
			os << "read lease ";
		else
			os << "write lease ";

		os << " chunkId = " << chunkId;
		return os.str();
	}
};

/*!
 * \brief Op for relinquishing a lease on a chunk of a file.
 */
struct MetaLeaseRelinquish: public MetaRequest {
	const LeaseType leaseType; //!< input
	const chunkId_t chunkId; //!< input
	const int64_t leaseId; //!< input
        const off_t chunkSize;
        const bool hasChunkChecksum;
        const uint32_t chunkChecksum;
	MetaLeaseRelinquish(seq_t s, int pv, LeaseType t, chunkId_t c, int64_t l,
            off_t size, bool hasCs, uint32_t checksum):
		MetaRequest(META_LEASE_RELINQUISH, s, pv, false),
		leaseType(t), chunkId(c), leaseId(l), chunkSize(size),
                hasChunkChecksum(hasCs), chunkChecksum(checksum) { }
        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual void response(ostream &os);
	virtual string Show() const
	{
		ostringstream os;

		os << "lease relinquish: ";
		if (leaseType == READ_LEASE)
			os << "read lease ";
		else
			os << "write lease ";

		os << " chunkId: " << chunkId << " leaseId: " << leaseId <<
                    " chunkSize: " << chunkSize;
                if (hasChunkChecksum) {
                    os << " checksum: " << chunkChecksum;
                }
		return os.str();
	}
};

/*!
 * \brief An internally generated op to force the cleanup of
 * dead leases thru the main event processing loop.
 */
struct MetaLeaseCleanup: public MetaRequest {
	MetaLeaseCleanup(seq_t s, KfsCallbackObj *c):
		MetaRequest(META_LEASE_CLEANUP, s, 0, false) { clnt = c; }

        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual string Show() const
	{
		return "lease cleanup";
	}
};

/*!
 * \brief An internally generated op to check that the degree
 * of replication for each chunk is satisfactory.  This op goes
 * thru the main event processing loop.
 */
struct MetaChunkReplicationCheck : public MetaRequest {
	MetaChunkReplicationCheck(seq_t s, KfsCallbackObj *c):
		MetaRequest(META_CHUNK_REPLICATION_CHECK, s, 0, false) { clnt = c; }

        virtual void handle();
	virtual int log(ofstream &file) const;
	virtual string Show() const
	{
		return "chunk replication check";
	}
};

extern int ParseCommand(std::istream& is, MetaRequest **res);

extern void initialize_request_handlers();
extern void printleaves();

extern void ChangeIncarnationNumber(MetaRequest *r);
extern void RegisterCounters();
extern void setClusterKey(const char *key);
extern void setMD5SumFn(const char *md5sumFn);
extern void setWORMMode(bool value);
extern void setMaxReplicasPerFile(int16_t value);
extern void setChunkmapDumpDir(string dir);

/* update counters for # of files/dirs/chunks in the system */
extern void UpdateNumDirs(int count);
extern void UpdateNumFiles(int count);
extern void UpdateNumChunks(int count);

}
#endif /* !defined(KFS_REQUEST_H) */
