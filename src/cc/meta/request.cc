/*!
 * $Id: request.cc 3496 2011-12-15 03:13:48Z sriramr $
 *
 * \file request.cc
 * \brief process queue of outstanding metadata requests
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

#include <map>
#include <boost/lexical_cast.hpp>
#include <string.h>

#include "common/Version.h"

#include "kfstree.h"
#include "queue.h"
#include "request.h"
#include "logger.h"
#include "checkpoint.h"
#include "util.h"
#include "LayoutManager.h"
#include "ChildProcessTracker.h"

#include "libkfsIO/Globals.h"
#include "common/log.h"

using std::map;
using std::string;
using std::istringstream;
using std::ifstream;
using std::min;
using std::max;

using namespace KFS;
using namespace KFS::libkfsio;

namespace KFS {
typedef int (*ParseHandler)(Properties &, MetaRequest **);

static int parseHandlerLookup(Properties &prop, MetaRequest **r);
static int parseHandlerLookupPath(Properties &prop, MetaRequest **r);
static int parseHandlerCreate(Properties &prop, MetaRequest **r);
static int parseHandlerRemove(Properties &prop, MetaRequest **r);
static int parseHandlerRename(Properties &prop, MetaRequest **r);
static int parseHandlerMakeChunksStable(Properties &prop, MetaRequest **r);
static int parseHandlerMkdir(Properties &prop, MetaRequest **r);
static int parseHandlerRmdir(Properties &prop, MetaRequest **r);
static int parseHandlerReaddir(Properties &prop, MetaRequest **r);
static int parseHandlerReaddirPlus(Properties &prop, MetaRequest **r);
static int parseHandlerGetalloc(Properties &prop, MetaRequest **r);
static int parseHandlerGetlayout(Properties &prop, MetaRequest **r);
static int parseHandlerAllocate(Properties &prop, MetaRequest **r);
static int parseHandlerTruncate(Properties &prop, MetaRequest **r);
static int parseHandlerCoalesceBlocks(Properties &prop, MetaRequest **r);
static int parseHandlerSetMtime(Properties &prop, MetaRequest **r);
static int parseHandlerChangeFileReplication(Properties &prop, MetaRequest **r);
static int parseHandlerRetireChunkserver(Properties &prop, MetaRequest **r);
static int parseHandlerToggleRebalancing(Properties &prop, MetaRequest **r);
static int parseHandlerExecuteRebalancePlan(Properties &prop, MetaRequest **r);
static int parseHandlerReadConfig(Properties &prop, MetaRequest **r);

static int parseHandlerLeaseAcquire(Properties &prop, MetaRequest **r);
static int parseHandlerLeaseRenew(Properties &prop, MetaRequest **r);
static int parseHandlerLeaseRelinquish(Properties &prop, MetaRequest **r);
static int parseHandlerChunkCorrupt(Properties &prop, MetaRequest **r);

static int parseHandlerHello(Properties &prop, MetaRequest **r);

static int parseHandlerPing(Properties &prop, MetaRequest **r);
static int parseHandlerStats(Properties &prop, MetaRequest **r);
static int parseHandlerCheckLeases(Properties &prop, MetaRequest **r);
static int parseHandlerRecomputeDirsize(Properties &prop, MetaRequest **r);
static int parseHandlerDumpChunkToServerMap(Properties &prop, MetaRequest **r);
static int parseHandlerDumpChunkReplicationCandidates(Properties &prop, MetaRequest **r);
static int parseHandlerFsck(Properties &prop, MetaRequest **r);
static int parseHandlerOpenFiles(Properties &prop, MetaRequest **r);
static int parseHandlerToggleWORM(Properties &prop, MetaRequest **r);
static int parseHandlerUpServers(Properties &prop, MetaRequest **r);
static int parseHandlerSetChunkServersProperties(Properties &prop, MetaRequest **r);
static int parseHandlerGetChunkServerCounters(Properties &prop, MetaRequest **r);

/// command -> parsehandler map
typedef map<string, ParseHandler> ParseHandlerMap;
typedef map<string, ParseHandler>::iterator ParseHandlerMapIter;

// handlers for parsing
ParseHandlerMap gParseHandlers;

// mapping for the counters
typedef map<MetaOp, Counter *> OpCounterMap;
typedef map<MetaOp, Counter *>::iterator OpCounterMapIter;
OpCounterMap gCounters;
Counter *gNumFiles, *gNumDirs, *gNumChunks;
Counter *gPathToFidCacheHit, *gPathToFidCacheMiss;

// see the comments in setClusterKey()
string gClusterKey;
string gMD5SumFn;
bool gWormMode = false;
static int16_t gMaxReplicasPerFile = MAX_REPLICAS_PER_FILE;
static string gChunkmapDumpDir = ".";

static bool
file_exists(fid_t fid)
{
	return metatree.getFattr(fid) != NULL;
}

static bool
path_exists(const string &pathname)
{
	MetaFattr *fa = metatree.lookupPath(KFS::ROOTFID, pathname);
	return fa != NULL;
}

static bool
is_dir(fid_t fid)
{
	MetaFattr *fa = metatree.getFattr(fid);
	return fa != NULL && fa->type == KFS_DIR;
}

static void
AddCounter(const char *name, MetaOp opName)
{
	Counter *c = new Counter(name);
	globals().counterManager.AddCounter(c);
	gCounters[opName] = c;
}

void
RegisterCounters()
{
	static int calledOnce = 0;
	if (calledOnce)
		return;
	calledOnce = 1;

	AddCounter("Get alloc", META_GETALLOC);
	AddCounter("Get layout", META_GETLAYOUT);
	AddCounter("Lookup", META_LOOKUP);
	AddCounter("Lookup Path", META_LOOKUP_PATH);
	AddCounter("Allocate", META_ALLOCATE);
	AddCounter("Truncate", META_TRUNCATE);
	AddCounter("Create", META_CREATE);
	AddCounter("Remove", META_REMOVE);
	AddCounter("Rename", META_RENAME);
	AddCounter("Make-all-chunks-stable", META_MAKE_CHUNKS_STABLE);
	AddCounter("Set Mtime", META_SETMTIME);
	AddCounter("Mkdir", META_MKDIR);
	AddCounter("Rmdir", META_RMDIR);
	AddCounter("Change File Replication", META_CHANGE_FILE_REPLICATION);
	AddCounter("Lease Acquire", META_LEASE_ACQUIRE);
	AddCounter("Lease Renew", META_LEASE_RENEW);
	AddCounter("Lease Cleanup", META_LEASE_CLEANUP);
	AddCounter("Corrupt Chunk ", META_CHUNK_CORRUPT);
	AddCounter("Chunkserver Hello ", META_HELLO);
	AddCounter("Chunkserver Bye ", META_BYE);
	AddCounter("Chunkserver Retire Start", META_RETIRE_CHUNKSERVER);
	// AddCounter("Chunkserver Retire Done", META_CHUNK_RETIRE_DONE);
	AddCounter("Replication Checker ", META_CHUNK_REPLICATION_CHECK);
	AddCounter("Replication Done ", META_CHUNK_REPLICATE);

	gNumFiles = new Counter("Number of Files");
	gNumDirs = new Counter("Number of Directories");
	gNumChunks = new Counter("Number of Chunks");
	gPathToFidCacheHit = new Counter("Number of Hits in Path->Fid Cache");
	gPathToFidCacheMiss = new Counter("Number of Misses in Path->Fid Cache");

	globals().counterManager.AddCounter(gNumFiles);
	globals().counterManager.AddCounter(gNumDirs);
	globals().counterManager.AddCounter(gNumChunks);
	globals().counterManager.AddCounter(gPathToFidCacheHit);
	globals().counterManager.AddCounter(gPathToFidCacheMiss);
}

static void
UpdateCounter(MetaOp opName)
{
	Counter *c;
	OpCounterMapIter iter;

	iter = gCounters.find(opName);
	if (iter == gCounters.end())
		return;
	c = iter->second;
	c->Update(1);
}

void
UpdateNumDirs(int count)
{
	if (gNumDirs == NULL)
		return;

	if ((int64_t) gNumDirs->GetValue() + count < 0)
		gNumDirs->Reset();
	else
		gNumDirs->Update(count);
}

void
UpdateNumFiles(int count)
{
	if (gNumFiles == NULL)
		return;

	if ((int64_t) gNumDirs->GetValue() + count < 0)
		gNumFiles->Reset();
	else
		gNumFiles->Update(count);
}

void
UpdateNumChunks(int count)
{
	if (gNumChunks == NULL)
		return;

	if ((int64_t) gNumDirs->GetValue() + count < 0)
		gNumChunks->Reset();
	else
		gNumChunks->Update(count);
}

/*
 * Submit a request to change the increment used for bumping up chunk version #.
 * @param[in] r  The request that depends on chunk-version-increment being written
 * out to disk as part of completing the request processing.
 */
void
ChangeIncarnationNumber(MetaRequest *r)
{
	if (chunkVersionInc < 1)
		// disable this bumping for now
		++chunkVersionInc;
	MetaChangeChunkVersionInc *ccvi = new MetaChangeChunkVersionInc(chunkVersionInc, r);

	submit_request(ccvi);
}

/*
 * Set the "key" for this cluster.  All chunkservers connecting to the meta-data
 * server should provide this key in the hello message.
 * @param[in] key  The desired cluster key
*/
void
setClusterKey(const char *key)
{
	gClusterKey = key;
}

/*
 * A way of doing admission control on chunkservers is to ensure that they run
 * an "approved" binary.  The approved list is defined by the MD5Sum of the
 * binary in an MD5Sum file.  All chunkservers connecting to 
 * the metaserver should provide their md5sum in the hello message.  If the
 * md5sum is in the approved list, then the chunkserver is admitted to the
 * system.
 * @param[in] md5sumFn  The filename with the list of MD5Sums
*/
void
setMD5SumFn(const char *md5sumFn)
{
	gMD5SumFn = md5sumFn;
}

/*
 * Set WORM mode. In WORM mode, deletes are disabled.
 */
void
setWORMMode(bool value)
{
	gWormMode = value;
}

void
setMaxReplicasPerFile(int16_t val)
{
	gMaxReplicasPerFile = val;
}

void
setChunkmapDumpDir(string d)
{
	gChunkmapDumpDir = d;
}

static inline string FattrReply(const MetaFattr *fa)
{
	if (! fa) {
		return string();
	}
	static const string fname[] = { "empty", "file", "dir" };
	ostringstream os;
	os << "File-handle: " << toString(fa->id()) << "\r\n";
	os << "Type: " << fname[fa->type] << "\r\n";
	os << "Chunk-count: " << toString(fa->chunkcount) << "\r\n";
	os << "File-size: " << toString(fa->filesize) << "\r\n";
	os << "Replication: " << toString(fa->numReplicas) << "\r\n";
	sendtime(os, "M-Time:", fa->mtime, "\r\n");
	sendtime(os, "C-Time:", fa->ctime, "\r\n");
	sendtime(os, "CR-Time:", fa->crtime, "\r\n");
	return os.str();
}

/* virtual */ void
MetaLookup::handle()
{
	MetaFattr *fa = metatree.lookup(dir, name);
	status = (fa == NULL) ? -ENOENT : 0;
	result = FattrReply(fa);
}

/* virtual */ void
MetaLookupPath::handle()
{
	MetaFattr *fa = metatree.lookupPath(root, path);
	status = (fa == NULL) ? -ENOENT : 0;
	result = FattrReply(fa);
}

/* virtual */ void
MetaCreate::handle()
{
	if (!is_dir(dir)) {
		status = -ENOTDIR;
		return;
	}
        fid = 0;
	status = metatree.create(dir, name, &fid,
					numReplicas, exclusive);
}

/* virtual */ void
MetaMkdir::handle()
{
	if (!is_dir(dir)) {
		status = -ENOTDIR;
		return;
	}
        fid = 0;
	status = metatree.mkdir(dir, name, &fid);
}


/*!
 * Specially named files (such as, those that end with ".tmp") can be
 * mutated by remove/rename.  Otherwise, in WORM no deletes/renames are allowed.
 */
static bool
isWormMutationAllowed(const string &pathname)
{
	string::size_type pos;

	pos = pathname.rfind(".tmp");
	return pos != string::npos;
}

/*!
 * \brief Remove a file in a directory.  Also, remove the chunks
 * associated with the file.  For removing chunks, we send off
 * RPCs to the appropriate chunkservers.
 */

/* virtual */ void
MetaRemove::handle()
{
	if (gWormMode && (!isWormMutationAllowed(name))) {
		// deletes are disabled in WORM mode except for specially named
		// files
		status = -EPERM;
		return;
	}
	status = metatree.remove(dir, name, pathname, &filesize);
}

/* virtual */ void
MetaRmdir::handle()
{
	if (gWormMode && (!isWormMutationAllowed(name))) {
		// deletes are disabled in WORM mode
		status = -EPERM;
		return;
	}
	status = metatree.rmdir(dir, name, pathname);
}

/* virtual */ void
MetaReaddir::handle()
{
        // Since we took out threads in the code, we can revert the change back to version 71.  
        // This piece of code was changed with svn version 75.
	MetaFattr * const fa = metatree.getFattr(dir);
        status = (! fa) ? -ENOENT : (fa->type != KFS_DIR ? -ENOTDIR :
            metatree.readdir(dir, v));
}

class EnumerateLocations {
	vector <ServerLocation> &v;
public:
	EnumerateLocations(vector <ServerLocation> &result): v(result) { }
	void operator () (ChunkServerPtr c)
	{
		ServerLocation l = c->GetServerLocation();
		v.push_back(l);
	}
};

class ListServerLocations {
	ostream &os;
public:
	ListServerLocations(ostream &out): os(out) { }
	void operator () (const ServerLocation &s)
	{
		os << " " <<  s.ToString();
	}
};

class EnumerateReaddirPlusInfo {
	ostream &os;
public:
	EnumerateReaddirPlusInfo(ostream &o) : os(o) { }
	void operator()(MetaDentry *entry) {
		static string fname[] = { "empty", "file", "dir" };
		MetaFattr *fa = metatree.getFattr(entry->id());

		os << "Begin-entry" << "\r\n";

		if (fa == NULL) {
			return;
		}
		// when we readdir on "/", we get an entry for "/".  We need to
		// supress that from the listing for "/".
		if ((fa->id() == ROOTFID) && (entry->getName() == "/"))
			return;

		os << "Name: " << entry->getName() << "\r\n";
		os << "File-handle: " << toString(fa->id()) << "\r\n";
		os << "Type: " << fname[fa->type] << "\r\n";
		sendtime(os, "M-Time:", fa->mtime, "\r\n");
		sendtime(os, "C-Time:", fa->ctime, "\r\n");
		sendtime(os, "CR-Time:", fa->crtime, "\r\n");
		if (fa->type == KFS_DIR) {
			return;
		}
		// for a file, get the layout and provide location of last chunk
		// so that the client can compute filesize
		vector<MetaChunkInfo*> chunkInfo;
		vector<ChunkServerPtr> c;
		int status = metatree.getalloc(fa->id(), chunkInfo);

		if ((status != 0) || (chunkInfo.size() == 0)) {
			os << "Chunk-count: 0\r\n";
			os << "File-size: 0\r\n";
			os << "Replication: " << toString(fa->numReplicas) << "\r\n";
			return;
		}
		MetaChunkInfo* lastChunk = chunkInfo.back();
		ChunkLayoutInfo l;

		l.offset = lastChunk->offset;
		l.chunkId = lastChunk->chunkId;
		l.chunkVersion = lastChunk->chunkVersion;
		if (gLayoutManager.GetChunkToServerMapping(l.chunkId, c) != 0) {
			// if all the servers hosting the chunk are
			// down...sigh...
			os << "Chunk-count: 0\r\n";
			os << "File-size: 0\r\n";
			os << "Replication: " << toString(fa->numReplicas) << "\r\n";
			return;
		}
		//
		// we give the client all the info about the last block of the
		// file; we also tell the client what we know about the
		// filesize.  if the value we send is -1, the client will figure
		// out the size.
		//
		for_each(c.begin(), c.end(), EnumerateLocations(l.locations));
		os << "Chunk-count: " << toString(fa->chunkcount) << "\r\n";
		os << "File-size: " << toString(fa->filesize) << "\r\n";
		os << "Replication: " << toString(fa->numReplicas) << "\r\n";
		os << "Chunk-offset: " << l.offset << "\r\n";
		os << "Chunk-handle: " << l.chunkId << "\r\n";
		os << "Chunk-version: " << l.chunkVersion << "\r\n";
		os << "Num-replicas: " << l.locations.size() << "\r\n";
		os << "Replicas: ";
		for_each(l.locations.begin(), l.locations.end(), ListServerLocations(os));
		os << "\r\n";
	}

};

/* virtual */ void
MetaReaddirPlus::handle()
{
	MetaFattr * const fa = metatree.getFattr(dir);
	if (! fa) {
		status = -ENOENT;
        } else if (fa->type != KFS_DIR) {
		status = -ENOTDIR;
	} else {
		vector<MetaDentry *> res;
		status = metatree.readdir(dir, res);
		if (status == 0) {
			// now that we have the entire directory read, for each entry in the
			// directory, get the attributes out.
			numEntries = res.size();
			for_each(res.begin(), res.end(), EnumerateReaddirPlusInfo(v));
		}
	}
}

/*!
 * \brief Get the allocation information for a specific chunk in a file.
 */
/* virtual */ void
MetaGetalloc::handle()
{
	if (!file_exists(fid)) {
		KFS_LOG_STREAM_DEBUG << "handle_getalloc: no such file " << fid <<
		KFS_LOG_EOM;
		status = -ENOENT;
		return;
	}

	MetaChunkInfo *chunkInfo = 0;
	status = metatree.getalloc(fid, offset, &chunkInfo);
	if (status != 0) {
		KFS_LOG_STREAM_DEBUG <<
			"handle_getalloc(" << fid << "," << offset <<
			") = " << status << ": kfsop failed" <<
		KFS_LOG_EOM;
		return;
	}

	chunkId = chunkInfo->chunkId;
	chunkVersion = chunkInfo->chunkVersion;
	vector<ChunkServerPtr> c;
	if (gLayoutManager.GetChunkToServerMapping(chunkId, c) != 0) {
		KFS_LOG_STREAM_DEBUG <<
			"handle_getalloc(" << fid << "," << chunkId << "," << offset <<
			"): no chunkservers" <<
		KFS_LOG_EOM;
		status = -EAGAIN;
		return;
	}
	for_each(c.begin(), c.end(), EnumerateLocations(locations));
	status = 0;
}

/*!
 * \brief Get the allocation information for a file.  Determine
 * how many chunks there and where they are located.
 */
/* virtual */ void
MetaGetlayout::handle()
{
	vector<MetaChunkInfo*> chunkInfo;
	vector<ChunkServerPtr> c;

	if (!file_exists(fid)) {
		status = -ENOENT;
		return;
	}

	status = metatree.getalloc(fid, chunkInfo);
	if (status != 0)
		return;

	for (vector<MetaChunkInfo*>::size_type i = 0; i < chunkInfo.size(); i++) {
		ChunkLayoutInfo l;

		l.offset = chunkInfo[i]->offset;
		l.chunkId = chunkInfo[i]->chunkId;
		l.chunkVersion = chunkInfo[i]->chunkVersion;
		if (gLayoutManager.GetChunkToServerMapping(l.chunkId, c) == 0) {
			// If we know the locations, pass it back
			for_each(c.begin(), c.end(), EnumerateLocations(l.locations));
		}
		v.push_back(l);
	}
	status = 0;
}

class ChunkVersionChanger {
	fid_t fid;
	chunkId_t chunkId;
	seq_t chunkVers;
public:
	ChunkVersionChanger(fid_t f, chunkId_t c, seq_t v) :
		fid(f), chunkId(c), chunkVers(v) { }
	void operator() (ChunkServerPtr p) {
		p->NotifyChunkVersChange(fid, chunkId, chunkVers);
	}
};

/*!
 * \brief handle an allocation request for a chunk in a file.
 * \param[in] r		write allocation request
 *
 * Write allocation proceeds as follows:
 *  1. The client has sent a write allocation request which has been
 * parsed and turned into an RPC request (which is handled here).
 *  2. We first get a unique chunk identifier (after validating the
 * fileid).
 *  3. We send the request to the layout manager to pick a location
 * for the chunk.
 *  4. The layout manager picks a location and sends an RPC to the
 * corresponding chunk server to create the chunk.
 *  5. When the RPC is going on, processing for this request is
 * suspended.
 *  6. When the RPC reply is received, this request gets re-activated
 * and we come back to this function.
 *  7. Assuming that the chunk server returned a success,  we update
 * the metatree to link the chunkId with the fileid (from this
 * request).
 *  8. Processing for this request is now complete; it is logged and
 * a reply is sent back to the client.
 *
 * Versioning/Leases introduces a few wrinkles to the above steps:
 * In step #2, the metatree could return -EEXIST if an allocation
 * has been done for the <fid, offset>.  In such a case, we need to
 * check with the layout manager to see if a new lease is required.
 * If a new lease is required, the layout manager bumps up the version
 * # for the chunk and notifies the chunkservers.  The message has to
 * be suspended until the chunkservers ack.  After the message is
 * restarted, we need to update the metatree to reflect the newer
 * version # before notifying the client.
 *
 * On the other hand, if a new lease isn't required, then the layout
 * manager tells us where the data has been placed; the process for
 * the request is therefore complete.
 */
/* virtual */ void
MetaAllocate::handle()
{
	suspended = false;
	if (layoutDone) {
		return;
	}
	KFS_LOG_STREAM_DEBUG << "Starting layout for req: " << opSeqno <<
        KFS_LOG_EOM;
	if (appendChunk) {
		// pick a chunk for which a write lease exists
		status = gLayoutManager.AllocateChunkForAppend(this);
		if (status == 0) {
			// all good
			KFS_LOG_STREAM_DEBUG <<
                            "For append re-using chunk " << chunkId <<
			    (suspended ? "; allocation in progress" : "") <<
                        KFS_LOG_EOM;
			logFlag = false; // Do not emit redundant log record.
			return;
		}
		offset = -1; // Allocate a new chunk past eof.
	}
	// force an allocation
	chunkId = 0;
	// start at step #2 above.
	status = metatree.allocateChunkId(
			fid, offset, &chunkId,
			&chunkVersion, &numReplicas);
	if ((status != 0) && (status != -EEXIST || appendChunk)) {
		// we have a problem
		return;
	}
	if (status == -EEXIST) {
		bool isNewLease = false;
		// Get a (new) lease if possible
		status = gLayoutManager.GetChunkWriteLease(this, isNewLease);
		if (status != 0) {
			// couln't get the lease...bail
			return;
		}
		if (!isNewLease) {
			KFS_LOG_STREAM_DEBUG << "Got valid lease for req:" << opSeqno <<
                        KFS_LOG_EOM;
			// we got a valid lease.  so, return
			return;
		}
		// new lease and chunkservers have been notified
		// so, wait for them to ack

	} else if (gLayoutManager.AllocateChunk(this) != 0) {
		// we have a problem
		status = -ENOSPC;
		return;
	}
	// we have queued an RPC to the chunkserver.  so, hold
	// off processing (step #5)
	// If all allocate ops fail synchronously (all servers are down), then
	// the op is not suspended, and can proceed immediately.
	suspended =! layoutDone;
}

void
MetaAllocate::LayoutDone()
{
	const bool wasSuspended = suspended;
	suspended  = false;
	layoutDone = true;
	KFS_LOG_STREAM_DEBUG <<
		"Layout is done for req: " << opSeqno << " status: " << status <<
	KFS_LOG_EOM;
	if (status == 0) {
		// Check if all servers are still up, and didn't go down
		// and reconnected back.
		// In the case of reconnect smart pointers should be different:
		// the the previous server incarnation always taken down on
		// reconnect.
		// Since the chunk is "dangling" up until this point, then in
		// the case of reconnect the chunk becomes "stale", and chunk
		// server is instructed to delete its replica of this new chunk.
		for (vector<ChunkServerPtr>::const_iterator i = servers.begin();
				i != servers.end(); ++i) {
			if ((*i)->IsDown()) {
				KFS_LOG_STREAM_DEBUG << (*i)->ServerID() <<
					" went down during allocation, alloc failed" <<
				KFS_LOG_EOM;
				status = -EIO;
				break;
			}
		}
	}
	if (status != 0) {
		seq_t oldvers = -1;
		// we have a problem: it is possible that the server
		// went down.  ask the client to retry....
		status = -KFS::EALLOCFAILED;

		gLayoutManager.InvalidateWriteLease(chunkId);
		metatree.getChunkVersion(fid, chunkId, &oldvers);
		if (oldvers > 0) {
			// For allocation requests that cause a version bump, we
			// have notified some of the chunkservers of the version bump.
			// If we need to revert the version # and can't send them that
			// message, we got a problem.  That particular version # is
			// poisoned and we aren't taking it out of the system.  By
			// letting the message to flow thru, we update the metatree with
			// the version # and then when the client does a new allocation,
			// we'll avoid re-using the poisoned version #.
			metatree.assignChunkId(fid, offset,
						chunkId, chunkVersion);
		} else {
			// this is the first time the chunk was allocated.
			// since the allocation failed, remove existence of this chunk
			// on the metaserver.
			gLayoutManager.DeleteChunk(chunkId);
		}
		// processing for this message is all done
	} else {
		// layout is complete (step #6)

		// update the tree (step #7) and since we turned off the
		// suspend flag, the request will be logged and go on its
		// merry way.
		//
		// There could be more than one append allocation request set
		// (each set one or more request with the same chunk) in flight.
		// The append request sets can finish in any order.
		// The append request sets can potentially all have the same
		// offset: the eof at the time the first request in each set
		// started.
		// For append requests assignChunkId assigns past eof offset,
		// if it succeeds, and returns the value in appendOffset.
		chunkOff_t appendOffset = offset;
		status = metatree.assignChunkId(fid, offset,
						chunkId, chunkVersion,
						appendChunk ? &appendOffset : 0);
		if (status == 0) {
			// Offset can change in the case of append.
			offset = appendOffset;
			gLayoutManager.CancelPendingMakeStable(fid, chunkId);
			// assignChunkId() forces a recompute of the file's size.
		} else {
			assert(! appendChunk || status != -EEXIST);
			KFS_LOG_STREAM((appendChunk && status == -EEXIST) ?
					MsgLogger::kLogLevelERROR :
					MsgLogger::kLogLevelDEBUG) <<
				"Assign chunk id failed for"
				" <" << fid << "," << offset << ">"
				" status: " << status <<
                	KFS_LOG_EOM; 
		}
	}
	if (appendChunk) {
		gLayoutManager.AllocateChunkForAppendDone(*this);
	}
	if (! wasSuspended) {
		// Do need need to resume, if it wasn't suspended: this method
		// is [indirectly] invoked from handle().
		// Presently the only way to get here is from synchronous chunk
		// server allocation failure. The request can not possibly have
		// non empty request list in this case, as it wasn't ever
		// suspened.
		if (next) {
			panic(
				"non empty allocation queue,"
				" for request that was not suspended",
				false
			);
		}
		return;
	}
	// Currently the ops queue only used for append allocations.
	assert(appendChunk || ! next);
	// Clone status for all ops in the queue.
	// Submit the replies in the same order as requests.
	// "this" might get deleted after submit_request()
	MetaAllocate* n = this;
	do {
		MetaAllocate& c = *n;
		n = c.next;
		c.next = 0;
		if (n) {
			MetaAllocate& q = *n;
			assert(q.fid == c.fid);
			q.status           = c.status;
			q.statusMsg        = c.statusMsg;
			q.suspended        = false;
			q.fid              = c.fid;
			q.offset           = c.offset;
			q.chunkId          = c.chunkId;
			q.chunkVersion     = c.chunkVersion;
			q.pathname         = c.pathname;
			q.numReplicas      = c.numReplicas;
			q.layoutDone       = c.layoutDone;
			q.clientRack       = c.clientRack;
			q.appendChunk      = c.appendChunk;
			q.servers          = c.servers;
			q.master           = c.master;
			q.numServerReplies = c.numServerReplies;
		}
		submit_request(&c);
	} while (n);
}

/* virtual */ void
MetaChunkAllocate::resume()
{
        assert(req && (req->op == META_ALLOCATE));

	// if there is a non-zero status, don't throw it away
	if (req->status == 0) {
                req->status = status;
	}
        MetaAllocate& alloc = *req;
	delete this;                

        alloc.numServerReplies++;
        // wait until we get replies from all servers
	if (alloc.numServerReplies == alloc.servers.size()) {
		// The ops are no longer suspended
		alloc.LayoutDone();
        }
}

string
MetaAllocate::Show() const
{
	ostringstream os;
	os << "allocate:"
            " seq:  "     << opSeqno     <<
            " path: "     << pathname    <<
            " fid: "      << fid         <<
            " chunkId: "  << chunkId     <<
            " offset: "   << offset      <<
            " client: "   << clientHost  <<
            " replicas: " << numReplicas <<
            " append: "   << appendChunk <<
	    " log: "      << logFlag
        ;
        for (vector<ChunkServerPtr>::const_iterator i = servers.begin();
                i != servers.end(); ++i) {
            os << " " << (*i)->ServerID();
        }
	return os.str();
}

/* virtual */ void
MetaTruncate::handle()
{
	chunkOff_t allocOffset = 0;

	if (gWormMode) {
		status = -EPERM;
		return;
	}

	if (pruneBlksFromHead) {
		status = metatree.pruneFromHead(fid, offset);
		return;
	}

	status = metatree.truncate(fid, offset, &allocOffset);
	if (status > 0) {
		// an allocation is needed
		MetaAllocate *alloc = new MetaAllocate(opSeqno, KFS_CLIENT_PROTO_VERS, fid,
							allocOffset);

		KFS_LOG_STREAM_DEBUG << "Suspending truncation due to alloc at offset: " <<
				allocOffset << KFS_LOG_EOM;

		// tie things together
		alloc->req = this;
		suspended = true;
		alloc->handle();
	}
}

/* virtual */ void
MetaRename::handle()
{
	if (gWormMode && ((!isWormMutationAllowed(oldname)) ||
                          path_exists(newname))) {
		// renames are disabled in WORM mode: otherwise, we could
		// overwrite an existing file
		status = -EPERM;
		return;
	}
	status = metatree.rename(dir, oldname, newname,
					oldpath, overwrite);
}

/* virtual */ void
MetaMakeChunksStable::handle()
{
	vector<MetaChunkInfo*> chunkInfo;
	vector<ChunkServerPtr> c;

	if (!file_exists(fid)) {
		status = -ENOENT;
		return;
	}

	status = metatree.getalloc(fid, chunkInfo);
	if (status != 0)
		return;

	for (vector<MetaChunkInfo*>::size_type i = 0; i < chunkInfo.size(); i++) {
		vector<ChunkServerPtr> c;

		if (!gLayoutManager.IsValidWriteLeaseIssued(chunkInfo[i]->chunkId))
			continue;

		if (gLayoutManager.GetChunkToServerMapping(chunkInfo[i]->chunkId, c) != 0)
			continue;

		KFS_LOG_STREAM_INFO << "Make chunk stable init on: " <<
			pathname << " < " << fid << "," <<
			chunkInfo[i]->chunkId << " > " << KFS_LOG_EOM;
				
		gLayoutManager.MakeChunkStableInit(fid, chunkInfo[i]->chunkId, 
			chunkInfo[i]->offset, pathname, c,
			true, -1, false, 0);
	}
	status = 0;
}

/* virtual */ void
MetaSetMtime::handle()
{
	MetaFattr *fa = metatree.lookupPath(ROOTFID, pathname);

	if (fa != NULL) {
		fa->mtime = mtime;
		fid    = fa->id();
		status = 0;
	} else {
		status = -ENOENT;
		fid    = -1;
	}

}

/* virtual */ void
MetaChangeFileReplication::handle()
{
	if (file_exists(fid))
		status = metatree.changePathReplication(fid, numReplicas);
	else
		status = -ENOENT;
}

/*
 * Move chunks from src file into the end chunk boundary of the dst file.
 */
/* virtual */ void
MetaCoalesceBlocks::handle()
{
	chunkOff_t appendOffset = -1;
	if ((status = metatree.coalesceBlocks(
			srcPath, dstPath, srcFid, srcChunks, dstFid,
			appendOffset)) == 0) {
		dstStartOffset = appendOffset;
		gLayoutManager.CoalesceBlocks(srcChunks, srcFid, dstFid, dstStartOffset);
	} else {
		dstStartOffset = -1;
	}
	KFS_LOG_STREAM(status == 0 ?
			MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelERROR) <<
		"Coalesce blocks " << srcPath << "->" << dstPath <<
			" status: " << status <<
			" offset: " << dstStartOffset <<
	KFS_LOG_EOM;
}

/* virtual */ void
MetaRetireChunkserver::handle()
{
	status = gLayoutManager.RetireServer(location, nSecsDown);
}

/* virtual */ void
MetaToggleRebalancing::handle()
{
	gLayoutManager.ToggleRebalancing(value);
	status = 0;
}

/* virtual */ void
MetaToggleWORM::handle()
{
   	setWORMMode(value);
	status = 0;
}

/* virtual */ void
MetaExecuteRebalancePlan::handle()
{
	status = gLayoutManager.LoadRebalancePlan(planPathname);
}

/* virtual */ void
MetaReadConfig::handle()
{
	Properties prop;

	if (prop.loadProperties(configFn.c_str(), '=', true) != 0) {
		status = -1;
		return;
	}
	KFS_LOG_STREAM_WARN << "loading config from: " << configFn.c_str() << KFS_LOG_EOM;

	setMaxReplicasPerFile(prop.getValue("metaServer.maxReplicasPerFile",
				MAX_REPLICAS_PER_FILE));
	gLayoutManager.SetParameters(prop);
	MsgLogger::GetLogger()->SetLogLevel(
		prop.getValue("metaServer.loglevel",
				MsgLogger::GetLogLevelNamePtr(
				 MsgLogger::GetLogger()->GetLogLevel())));

	status = 0;
}

/* virtual */ void
MetaHello::handle()
{
	if (status < 0) {
		// bad hello request...possible cluster key mismatch
		return;
	}
	gLayoutManager.AddNewServer(this);
	status = 0;
}

/* virtual */ void
MetaBye::handle()
{
	gLayoutManager.ServerDown(server.get());
	status = 0;
}

/* virtual */ void
MetaLeaseAcquire::handle()
{
	status = gLayoutManager.GetChunkReadLease(this);
}

/* virtual */ void
MetaLeaseRenew::handle()
{
	status = gLayoutManager.LeaseRenew(this);
}

/* virtual */ void
MetaLeaseRelinquish::handle()
{
	status = gLayoutManager.LeaseRelinquish(this);
	KFS_LOG_STREAM(status == 0 ?
			MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
		Show() << " status: " << status <<
        KFS_LOG_EOM;
}

/* virtual */ void
MetaLeaseCleanup::handle()
{
	gLayoutManager.LeaseCleanup();
	// some leases are gone.  so, cleanup dumpster
	metatree.cleanupDumpster();
	metatree.cleanupPathToFidCache();
	status = 0;
}

/* virtual */ void
MetaChunkCorrupt::handle()
{
	gLayoutManager.ChunkCorrupt(this);
	status = 0;
}

/* virtual */ void
MetaChunkReplicationCheck::handle()
{
	gLayoutManager.ChunkReplicationChecker();
	status = 0;
}

/* virtual */ void
MetaBeginMakeChunkStable::handle()
{
	gLayoutManager.BeginMakeChunkStableDone(this);
	status = 0;
}

int
MetaLogMakeChunkStable::logDone(int code, void *data)
{
	assert(
		code == EVENT_CMD_DONE &&
		data == static_cast<MetaRequest*>(this)
	);
	if (op == META_LOG_MAKE_CHUNK_STABLE) {
		gLayoutManager.LogMakeChunkStableDone(this);
	}
	delete this;
	return 0;
}

/* virtual */ void
MetaChunkMakeStable::handle()
{
	gLayoutManager.MakeChunkStableDone(this);
	status = 0;
}

/* virtual */ string
MetaChunkMakeStable::Show() const
{
	ostringstream os;
	os <<
		"make-chunk-stable:"
		" server: "        << server->GetServerLocation().ToString() <<
		" seq: "           << opSeqno <<
		" status: "        << status <<
		    (statusMsg.empty() ? "" : " ") << statusMsg <<
		" fileid: "        << fid <<
		" chunkid: "       << chunkId <<
		" chunkvers: "     << chunkVersion <<
		" chunkSize: "     << chunkSize <<
		" chunkChecksum: " << chunkChecksum
	;
	return os.str();
}

/* virtual */ void
MetaChunkSize::handle()
{
	if (chunkSize < 0) {
		status = -1;
		return;
	}
	status = gLayoutManager.GetChunkSizeDone(this);
}

/* virtual */ void
MetaChunkReplicate::handle()
{
	gLayoutManager.ChunkReplicationDone(this);
}

/* virtual */ void
MetaChangeChunkVersionInc::handle()
{
	status = 0;
}

/* virtual */ void
MetaPing::handle()
{
	status = 0;
	gLayoutManager.Ping(systemInfo, servers, downServers, retiringServers);

}

/* virtual */ void
MetaUpServers::handle()
{
	status = 0;
	gLayoutManager.UpServers(stringStream);
}

/* virtual */ void
MetaRecomputeDirsize::handle()
{
	status = 0;
	KFS_LOG_STREAM_INFO << "Processing a recompute dir size..." << KFS_LOG_EOM;
	metatree.recomputeDirSize();
}

/* virtual */ void
MetaDumpChunkToServerMap::handle()
{
	pid_t pid;

	if ((pid = fork()) == 0) {
		// In the child process, we didn't setup the poll vector.  To
		// avoid random crashes due to the d'tors trying to clear out
		// entres from the poll vector, update the netManager so that it
		// can "fake out" the closes.
                MsgLogger::Stop();
		globalNetManager().SetForkedChild();

		// let the child write out the map; if the map is large, this'll
		// take several seconds.  we get the benefits of writing out the
		// map in the background while the metaserver continues to
		// process other RPCs
		gLayoutManager.DumpChunkToServerMap(gChunkmapDumpDir);
		exit(0);
	}
	KFS_LOG_VA_INFO("child that is writing out the chunk->server map has pid: %d", pid);
	// if fork() failed, let the sender know
	if (pid < 0) {
		status = -1;
		return;
	}
	// hold on to the request until the child  finishes
	chunkmapFile = gChunkmapDumpDir + "/chunkmap.txt." + boost::lexical_cast<string>(pid);
	suspended = true;
	gChildProcessTracker.Track(pid, this);
}

/* virtual */ void
MetaDumpChunkReplicationCandidates::handle()
{
	ostringstream os;
	status = 0;
	gLayoutManager.DumpChunkReplicationCandidates(os);
	blocks = os.str();
}

/* virtual */ void
MetaFsck::handle()
{
	ostringstream os;
	status = 0;
	gLayoutManager.Fsck(os);
	fsckStatus = os.str();
}

/* virtual */ void
MetaCheckLeases::handle()
{
	status = 0;
	gLayoutManager.CheckAllLeases();
}

/* virtual */ void
MetaStats::handle()
{
	ostringstream os;
	status = 0;
	globals().counterManager.Show(os);
	stats = os.str();

}

/* virtual */ void
MetaOpenFiles::handle()
{
	status = 0;
	gLayoutManager.GetOpenFiles(openForRead, openForWrite);
}

/* virtual */ void
MetaSetChunkServersProperties::handle()
{
	status = (int)properties.size();
	gLayoutManager.SetChunkServersProperties(properties);
}

/* virtual */ void
MetaGetChunkServersCounters::handle()
{
	status = 0;
	resp = ToString(gLayoutManager.GetChunkServerCounters(), string("\n"));
}

/*
 * Map request types to the functions that handle them.
 */
static void
setup_handlers()
{
	gParseHandlers["LOOKUP"] = parseHandlerLookup;
	gParseHandlers["LOOKUP_PATH"] = parseHandlerLookupPath;
	gParseHandlers["CREATE"] = parseHandlerCreate;
	gParseHandlers["MKDIR"] = parseHandlerMkdir;
	gParseHandlers["REMOVE"] = parseHandlerRemove;
	gParseHandlers["RMDIR"] = parseHandlerRmdir;
	gParseHandlers["READDIR"] = parseHandlerReaddir;
	gParseHandlers["READDIRPLUS"] = parseHandlerReaddirPlus;
	gParseHandlers["GETALLOC"] = parseHandlerGetalloc;
	gParseHandlers["GETLAYOUT"] = parseHandlerGetlayout;
	gParseHandlers["ALLOCATE"] = parseHandlerAllocate;
	gParseHandlers["TRUNCATE"] = parseHandlerTruncate;
	gParseHandlers["RENAME"] = parseHandlerRename;
	gParseHandlers["MAKE_CHUNKS_STABLE"] = parseHandlerMakeChunksStable;
	gParseHandlers["SET_MTIME"] = parseHandlerSetMtime;
	gParseHandlers["CHANGE_FILE_REPLICATION"] = parseHandlerChangeFileReplication;
	gParseHandlers["COALESCE_BLOCKS"] = parseHandlerCoalesceBlocks;

	gParseHandlers["RETIRE_CHUNKSERVER"] = parseHandlerRetireChunkserver;
	gParseHandlers["EXECUTE_REBALANCEPLAN"] = parseHandlerExecuteRebalancePlan;
	gParseHandlers["READ_CONFIG"] = parseHandlerReadConfig;
	gParseHandlers["TOGGLE_REBALANCING"] = parseHandlerToggleRebalancing;

	// Lease related ops
	gParseHandlers["LEASE_ACQUIRE"] = parseHandlerLeaseAcquire;
	gParseHandlers["LEASE_RENEW"] = parseHandlerLeaseRenew;
	gParseHandlers["LEASE_RELINQUISH"] = parseHandlerLeaseRelinquish;
	gParseHandlers["CORRUPT_CHUNK"] = parseHandlerChunkCorrupt;

	// Meta server <-> Chunk server ops
	gParseHandlers["HELLO"] = parseHandlerHello;

	gParseHandlers["PING"] = parseHandlerPing;
	gParseHandlers["UPSERVERS"] = parseHandlerUpServers;
	gParseHandlers["TOGGLE_WORM"] = parseHandlerToggleWORM;
	gParseHandlers["STATS"] = parseHandlerStats;
	gParseHandlers["CHECK_LEASES"] = parseHandlerCheckLeases;
	gParseHandlers["RECOMPUTE_DIRSIZE"] = parseHandlerRecomputeDirsize;
	gParseHandlers["DUMP_CHUNKTOSERVERMAP"] = parseHandlerDumpChunkToServerMap;
	gParseHandlers["DUMP_CHUNKREPLICATIONCANDIDATES"] = parseHandlerDumpChunkReplicationCandidates;
	gParseHandlers["FSCK"] = parseHandlerFsck;
	gParseHandlers["OPEN_FILES"] = parseHandlerOpenFiles;
	gParseHandlers["SET_CHUNK_SERVERS_PROPERTIES"] = &parseHandlerSetChunkServersProperties;
	gParseHandlers["GET_CHUNK_SERVERS_COUNTERS"] = &parseHandlerGetChunkServerCounters;
}

/*!
 * \brief request queue initialization
 */
void
initialize_request_handlers()
{
	setup_handlers();
}

/* virtual */ void
MetaRequest::handle()
{
    status = -ENOSYS;  // Not implemented
}

/*!
 * \brief remove successive requests for the queue and carry them out.
 */
void
process_request(MetaRequest *r)
{
        r->handle();
	if (!r->suspended) {
		UpdateCounter(r->op);
		oplog.dispatch(r);
	}
}

/*!
 * \brief add a new request to the queue: we used to have threads before; at
 * that time, the requests would be dropped into the queue and the request
 * processor would pick it up.  We have taken out threads; so this method is
 * just pass thru
 * \param[in] r the request
 */
void
submit_request(MetaRequest *r)
{
	struct timeval s, e;
	// stash the string lest r get deleted after calling process_request()
	// and we need to print a message because it took too long.
	string msg = r->Show();

	gettimeofday(&s, NULL);

	process_request(r);

	gettimeofday(&e, NULL);

	float timeSpent = ComputeTimeDiff(s, e);

	// if we spend more than 200 ms/msg, inquiring minds'd like to know ;-)
	if (timeSpent > 0.2) {
		KFS_LOG_STREAM_INFO << "Time spent processing: " << msg
				<< " is: " << timeSpent << KFS_LOG_EOM;
	}
}

/*!
 * \brief print out the leaf nodes for debugging
 */
void
printleaves()
{
	metatree.printleaves();
}

/*!
 * \brief log lookup request (nop)
 */
int
MetaLookup::log(ofstream &file) const
{
	return 0;
}

/*!
 * \brief log lookup path request (nop)
 */
int
MetaLookupPath::log(ofstream &file) const
{
	return 0;
}

/*!
 * \brief log a file create
 */
int
MetaCreate::log(ofstream &file) const
{
	// use the log entry time as a proxy for when the file was created
	struct timeval t;
	gettimeofday(&t, NULL);

	file << "create/dir/" << dir << "/name/" << name <<
		"/id/" << fid << "/numReplicas/" << (int) numReplicas << 
		"/ctime/" << showtime(t) << '\n';
	return file.fail() ? -EIO : 0;
}

/*!
 * \brief log a directory create
 */
int
MetaMkdir::log(ofstream &file) const
{
	struct timeval t;
	gettimeofday(&t, NULL);

	file << "mkdir/dir/" << dir << "/name/" << name <<
		"/id/" << fid << "/ctime/" << showtime(t) << '\n';
	return file.fail() ? -EIO : 0;
}

/*!
 * \brief log a file deletion
 */
int
MetaRemove::log(ofstream &file) const
{
	file << "remove/dir/" << dir << "/name/" << name << '\n';
	return file.fail() ? -EIO : 0;
}

/*!
 * \brief log a directory deletion
 */
int
MetaRmdir::log(ofstream &file) const
{
	file << "rmdir/dir/" << dir << "/name/" << name << '\n';
	return file.fail() ? -EIO : 0;
}

/*!
 * \brief log directory read (nop)
 */
int
MetaReaddir::log(ofstream &file) const
{
	return 0;
}

/*!
 * \brief log directory read (nop)
 */
int
MetaReaddirPlus::log(ofstream &file) const
{
	return 0;
}

/*!
 * \brief log getalloc (nop)
 */
int
MetaGetalloc::log(ofstream &file) const
{
	return 0;
}

/*!
 * \brief log getlayout (nop)
 */
int
MetaGetlayout::log(ofstream &file) const
{
	return 0;
}

/*!
 * \brief log a chunk allocation
 */
int
MetaAllocate::log(ofstream &file) const
{
	if (! logFlag) {
		return 0;
	}
	// use the log entry time as a proxy for when the block was created/file
	// was modified
	struct timeval t;
	gettimeofday(&t, NULL);

	file << "allocate/file/" << fid << "/offset/" << offset
	     << "/chunkId/" << chunkId
	     << "/chunkVersion/" << chunkVersion 
	     << "/mtime/" << showtime(t)
	     << "/append/" << (appendChunk ? 1 : 0)
	     << '\n';
	return file.fail() ? -EIO : 0;
}

/*!
 * \brief log a file truncation
 */
int
MetaTruncate::log(ofstream &file) const
{
	// use the log entry time as a proxy for when the file was modified
	struct timeval t;
	gettimeofday(&t, NULL);

	if (pruneBlksFromHead) {
		file << "pruneFromHead/file/" << fid << "/offset/" << offset 
	     		<< "/mtime/" << showtime(t) << '\n';
	} else {
		file << "truncate/file/" << fid << "/offset/" << offset 
	     		<< "/mtime/" << showtime(t) << '\n';
	}
	return file.fail() ? -EIO : 0;
}

/*!
 * \brief log a rename
 */
int
MetaRename::log(ofstream &file) const
{
	file << "rename/dir/" << dir << "/old/" <<
		oldname << "/new/" << newname << '\n';
	return file.fail() ? -EIO : 0;
}

/*!
 * \brief log a block coalesce
 */
int
MetaCoalesceBlocks::log(ofstream &file) const
{
	file << "coalesce/old/" << srcFid << "/new/" << dstFid 
		<< "/count/" << srcChunks.size() << '\n';
	return file.fail() ? -EIO : 0;
}

/*!
 * \brief log a setmtime
 */
int
MetaSetMtime::log(ofstream &file) const
{
	file << "setmtime/file/" << fid 
		<< "/mtime/" << showtime(mtime) << '\n';
	return file.fail() ? -EIO : 0;
}

/*!
 * \brief Log a chunk-version-increment change to disk.
*/
int
MetaChangeChunkVersionInc::log(ofstream &file) const
{
	file << "chunkVersionInc/" << cvi << '\n';
	return file.fail() ? -EIO : 0;
}

/*!
 * \brief log change file replication
 */
int
MetaChangeFileReplication::log(ofstream &file) const
{
	file << "setrep/file/" << fid << "/replicas/" << numReplicas << '\n';
	return file.fail() ? -EIO : 0;
}

/*!
 * \brief log meta-make-chunks-stable sent by a client (nop)
 */
int
MetaMakeChunksStable::log(ofstream &file) const
{
	return 0;
}

/*!
 * \brief log retire chunkserver (nop)
 */
int
MetaRetireChunkserver::log(ofstream &file) const
{
	return 0;
}

/*!
 * \brief log toggling of chunkserver rebalancing state (nop)
 */
int
MetaToggleRebalancing::log(ofstream &file) const
{
	return 0;
}

/*!
 * \brief log toggling of metaserver WORM state (nop)
 */
int
MetaToggleWORM::log(ofstream &file) const
{
	return 0;
}

/*!
 * \brief log execution of rebalance plan (nop)
 */
int
MetaExecuteRebalancePlan::log(ofstream &file) const
{
	return 0;
}

/*!
 * \brief read the config file and update parameters (nop)
 */
int
MetaReadConfig::log(ofstream &file) const
{
	return 0;
}

/*!
 * \brief for a chunkserver hello, there is nothing to log
 */
int
MetaHello::log(ofstream &file) const
{
	return 0;
}

/*!
 * \brief for a chunkserver's death, there is nothing to log
 */
int
MetaBye::log(ofstream &file) const
{
	return 0;
}

/*!
 * \brief When asking a chunkserver for a chunk's size, there is
 * write out the estimate of the file's size.
 */
int
MetaChunkSize::log(ofstream &file) const
{
	if (filesize < 0)
		return 0;

	file << "size/file/" << fid << "/filesize/" << filesize << '\n';
	return file.fail() ? -EIO : 0;
}

/*!
 * \brief for a ping, there is nothing to log
 */
int
MetaPing::log(ofstream &file) const
{
	return 0;
}

/*!
 * \brief for a request of upserver, there is nothing to log
 */
int
MetaUpServers::log(ofstream &file) const
{
    return 0;
}

/*!
 * \brief for a stats request, there is nothing to log
 */
int
MetaStats::log(ofstream &file) const
{
	return 0;
}

/*!
 * \brief for a map dump request, there is nothing to log
 */
int
MetaDumpChunkToServerMap::log(ofstream &file) const
{
	return 0;
}

/*!
 * \brief for a fsck request, there is nothing to log
 */
int
MetaFsck::log(ofstream &file) const
{
	return 0;
}


/*!
 * \brief for a recompute dir size request, there is nothing to log
 */
int
MetaRecomputeDirsize::log(ofstream &file) const
{
	return 0;
}

/*!
 * \brief for a check all leases request, there is nothing to log
 */
int
MetaCheckLeases::log(ofstream &file) const
{
	return 0;
}

/*!
 * \brief for a dump chunk replication candidates request, there is nothing to log
 */
int
MetaDumpChunkReplicationCandidates::log(ofstream &file) const
{
	return 0;
}

/*!
 * \brief for an open files request, there is nothing to log
 */
int
MetaOpenFiles::log(ofstream &file) const
{
	return 0;
}

int
MetaSetChunkServersProperties::log(ofstream & /* file */) const
{
	return 0;
}

int
MetaGetChunkServersCounters::log(ofstream & /* file */) const
{
	return 0;
}

/*!
 * \brief for an open files request, there is nothing to log
 */
int
MetaChunkCorrupt::log(ofstream &file) const
{
	return 0;
}

/*!
 * \brief for a lease acquire request, there is nothing to log
 */
int
MetaLeaseAcquire::log(ofstream &file) const
{
	return 0;
}

/*!
 * \brief for a lease renew request, there is nothing to log
 */
int
MetaLeaseRenew::log(ofstream &file) const
{
	return 0;
}

/*!
 * \brief for a lease renew relinquish, there is nothing to log
 */
int
MetaLeaseRelinquish::log(ofstream &file) const
{
	return 0;
}

/*!
 * \brief for a lease cleanup request, there is nothing to log
 */
int
MetaLeaseCleanup::log(ofstream &file) const
{
	return 0;
}

/*!
 * \brief This is an internally generated op.  There is
 * nothing to log.
 */
int
MetaChunkReplicationCheck::log(ofstream &file) const
{
	return 0;
}

/*!
 * \brief This is an internally generated op. Log chunk id, size, and checksum.
 */
int
MetaLogMakeChunkStable::log(ofstream &file) const
{
	if (chunkVersion < 0) {
		KFS_LOG_STREAM_WARN << "invalid chunk version ignoring: " <<
			Show() <<
		KFS_LOG_EOM;
		return 0;
	}

	file << "mkstable"         <<
			(op == META_LOG_MAKE_CHUNK_STABLE ? "" : "done") <<
		"/fileId/"         << fid <<
		"/chunkId/"        << chunkId <<
		"/chunkVersion/"   << chunkVersion  <<
		"/size/"           << chunkSize <<
		"/checksum/"       << chunkChecksum <<
		"/hasChecksum/"    << (hasChunkChecksum ? 1 : 0) <<
	'\n';
	return file.fail() ? -EIO : 0;
}

/*!
 * \brief parse a command sent by a client
 *
 * Commands are of the form:
 * <COMMAND NAME> \r\n
 * {header: value \r\n}+\r\n
 *
 * The general model in parsing the client command:
 * 1. Each command has its own parser
 * 2. Extract out the command name and find the parser for that
 * command
 * 3. Dump the header/value pairs into a properties object, so that we
 * can extract the header/value fields in any order.
 * 4. Finally, call the parser for the command sent by the client.
 *
 * @param[in] cmdBuf: buffer containing the request sent by the client
 * @param[in] cmdLen: length of cmdBuf
 * @param[out] res: A piece of memory allocated by calling new that
 * contains the data for the request.  It is the caller's
 * responsibility to delete the memory returned in res.
 * @retval 0 on success;  -1 if there is an error
 */
int
ParseCommand(std::istream& is, MetaRequest **res)
{
	const char *delims = " \r\n";
	// header/value pairs are separated by a :
	const char separator = ':';
	string cmdStr;
	string::size_type cmdEnd;
	Properties prop;
	ParseHandlerMapIter entry;
	ParseHandler handler;

	*res = NULL;

	// get the first line and find the command name
	is >> cmdStr;
	// trim the command
	cmdEnd = cmdStr.find_first_of(delims);
	if (cmdEnd != cmdStr.npos) {
		cmdStr.erase(cmdEnd);
	}

	// find the parse handler and parse the thing
	entry = gParseHandlers.find(cmdStr);
	if (entry == gParseHandlers.end())
		return -1;
	handler = entry->second;

	prop.loadProperties(is, separator, false);

	return (*handler)(prop, res);
}

/*!
 * \brief Various parse handlers.  All of them follow the same model:
 * @param[in] prop: A properties table filled with values sent by the client
 * @param[out] r: If parse is successful, returns a dynamically
 * allocated meta request object. It is the callers responsibility to get rid
 * of this pointer.
 * @retval 0 if parse is successful; -1 otherwise.
 *
 * XXX: Need to make MetaRequest a smart pointer
 */

static int
parseHandlerLookup(Properties &prop, MetaRequest **r)
{
	fid_t dir;
	const char *name;
	seq_t seq;
	int protoVers;

	seq = prop.getValue("Cseq", (seq_t) -1);
	protoVers = prop.getValue("Client-Protocol-Version", (int) 0);
	dir = prop.getValue("Parent File-handle", (fid_t) -1);
	if (dir < 0)
		return -1;
	name = prop.getValue("Filename", (const char*) NULL);
	if (name == NULL)
		return -1;
	*r = new MetaLookup(seq, protoVers, dir, name);
	return 0;
}

static int
parseHandlerLookupPath(Properties &prop, MetaRequest **r)
{
	fid_t root;
	const char *path;
	seq_t seq;
	int protoVers;

	seq = prop.getValue("Cseq", (seq_t) -1);
	protoVers = prop.getValue("Client-Protocol-Version", (int) 0);
	root = prop.getValue("Root File-handle", (fid_t) -1);
	if (root < 0)
		return -1;
	path = prop.getValue("Pathname", (const char *) NULL);
	if (path == NULL)
		return -1;
	*r = new MetaLookupPath(seq, protoVers, root, path);
	return 0;
}

static int
parseHandlerCreate(Properties &prop, MetaRequest **r)
{
	fid_t dir;
	const char *name;
	seq_t seq;
	int16_t numReplicas;
	bool exclusive;
	int protoVers;

	seq = prop.getValue("Cseq", (seq_t) -1);
	dir = prop.getValue("Parent File-handle", (fid_t) -1);
	if (dir < 0)
		return -1;
	name = prop.getValue("Filename", (const char *) NULL);
	if (name == NULL)
		return -1;
	// cap replication
	numReplicas = min((int16_t) prop.getValue("Num-replicas", 1), gMaxReplicasPerFile);
	if (numReplicas <= 0)
		return -1;
	// by default, create overwrites the file; when it is turned off,
	// it is for supporting O_EXCL
	exclusive = (prop.getValue("Exclusive", 1)) == 1;
	protoVers = prop.getValue("Client-Protocol-Version", (int) 0);

	*r = new MetaCreate(seq, protoVers, dir, name, numReplicas, exclusive);
	return 0;
}

static int
parseHandlerRemove(Properties &prop, MetaRequest **r)
{
	fid_t dir;
	const char *name;
	seq_t seq;
	int protoVers;

	seq = prop.getValue("Cseq", (seq_t) -1);
	protoVers = prop.getValue("Client-Protocol-Version", (int) 0);
	dir = prop.getValue("Parent File-handle", (fid_t) -1);
	if (dir < 0)
		return -1;
	name = prop.getValue("Filename", (const char *) NULL);
	if (name == NULL)
		return -1;
	MetaRemove *rm = new MetaRemove(seq, protoVers, dir, name);
	rm->pathname = prop.getValue("Pathname", "");
	*r = rm;
	return 0;
}

static int
parseHandlerMkdir(Properties &prop, MetaRequest **r)
{
	fid_t dir;
	const char *name;
	seq_t seq;
	int protoVers;

	seq = prop.getValue("Cseq", (seq_t) -1);
	protoVers = prop.getValue("Client-Protocol-Version", (int) 0);
	dir = prop.getValue("Parent File-handle", (fid_t) -1);
	if (dir < 0)
		return -1;
	name = prop.getValue("Directory", (const char *) NULL);
	if (name == NULL)
		return -1;
	*r = new MetaMkdir(seq, protoVers, dir, name);
	return 0;
}

static int
parseHandlerRmdir(Properties &prop, MetaRequest **r)
{
	fid_t dir;
	const char *name;
	seq_t seq;
	int protoVers;

	seq = prop.getValue("Cseq", (seq_t) -1);
	protoVers = prop.getValue("Client-Protocol-Version", (int) 0);
	dir = prop.getValue("Parent File-handle", (fid_t) -1);
	if (dir < 0)
		return -1;
	name = prop.getValue("Directory", (const char *) NULL);
	if (name == NULL)
		return -1;
	protoVers = prop.getValue("Client-Protocol-Version", (int) 0);
	MetaRmdir *rm = new MetaRmdir(seq, protoVers, dir, name);
	rm->pathname = prop.getValue("Pathname", "");
	*r = rm;
	return 0;
}

static int
parseHandlerReaddir(Properties &prop, MetaRequest **r)
{
	fid_t dir;
	seq_t seq;
	int protoVers;

	seq = prop.getValue("Cseq", (seq_t) -1);
	protoVers = prop.getValue("Client-Protocol-Version", (int) 0);
	dir = prop.getValue("Directory File-handle", (fid_t) -1);
	if (dir < 0)
		return -1;
	*r = new MetaReaddir(seq, protoVers, dir);
	return 0;
}

static int
parseHandlerReaddirPlus(Properties &prop, MetaRequest **r)
{
	fid_t dir;
	seq_t seq;
	int protoVers;

	seq = prop.getValue("Cseq", (seq_t) -1);
	protoVers = prop.getValue("Client-Protocol-Version", (int) 0);
	dir = prop.getValue("Directory File-handle", (fid_t) -1);
	if (dir < 0)
		return -1;
	*r = new MetaReaddirPlus(seq, protoVers, dir);
	return 0;
}

static int
parseHandlerGetalloc(Properties &prop, MetaRequest **r)
{
	fid_t fid;
	seq_t seq;
	chunkOff_t offset;
	int protoVers;

	seq = prop.getValue("Cseq", (seq_t) -1);
	protoVers = prop.getValue("Client-Protocol-Version", (int) 0);
	fid = prop.getValue("File-handle", (fid_t) -1);
	offset = prop.getValue("Chunk-offset", (chunkOff_t) -1);
	if ((fid < 0) || (offset < 0))
		return -1;
	*r = new MetaGetalloc(seq, protoVers, fid, offset, prop.getValue("Pathname", string()));
	return 0;
}

static int
parseHandlerGetlayout(Properties &prop, MetaRequest **r)
{
	fid_t fid;
	seq_t seq;
	int protoVers;

	seq = prop.getValue("Cseq", (seq_t) -1);
	protoVers = prop.getValue("Client-Protocol-Version", (int) 0);
	fid = prop.getValue("File-handle", (fid_t) -1);
	if (fid < 0)
		return -1;
	*r = new MetaGetlayout(seq, protoVers, fid);
	return 0;
}

static int
parseHandlerAllocate(Properties &prop, MetaRequest **r)
{
	fid_t fid;
	seq_t seq;
	chunkOff_t offset = 0;
	short appendChunk = 0;
	int protoVers;

	seq = prop.getValue("Cseq", (seq_t) -1);
	protoVers = prop.getValue("Client-Protocol-Version", (int) 0);
	fid = prop.getValue("File-handle", (fid_t) -1);
	appendChunk = prop.getValue("Chunk-append", 0);
	if (!appendChunk)
		offset = prop.getValue("Chunk-offset", (chunkOff_t) -1);
	if ((fid < 0) || (offset < 0))
		return -1;
	MetaAllocate *m = new MetaAllocate(seq, protoVers, fid, offset);
	m->pathname = prop.getValue("Pathname", "");
	m->clientHost = prop.getValue("Client-host", "");
	m->appendChunk = (appendChunk == 1);
	m->spaceReservationSize = prop.getValue("Space-reserve", 1 << 20);
	m->maxAppendersPerChunk = prop.getValue("Max-appenders", 64);
	m->clientRack = prop.getValue("Client-rack", -1);
	if (m->appendChunk)
		// fill this value in when the allocation is processed: the
		// client is likely passing in a hint of where its last
		// allocation was and where metaserver should start looking for
		// a block.
		m->offset = prop.getValue("Chunk-offset", (chunkOff_t) -1);
	*r = m;
	return 0;
}

static int
parseHandlerTruncate(Properties &prop, MetaRequest **r)
{
	fid_t fid;
	seq_t seq;
	chunkOff_t offset;
	int protoVers;

	seq = prop.getValue("Cseq", (seq_t) -1);
	protoVers = prop.getValue("Client-Protocol-Version", (int) 0);
	fid = prop.getValue("File-handle", (fid_t) -1);
	offset = prop.getValue("Offset", (chunkOff_t) -1);
	if ((fid < 0) || (offset < 0))
		return -1;
	MetaTruncate *mt = new MetaTruncate(seq, protoVers, fid, offset);
	mt->pathname = prop.getValue("Pathname", "");
	mt->pruneBlksFromHead = prop.getValue("Prune-from-head", 0);
	*r = mt;
	return 0;
}

static int
parseHandlerRename(Properties &prop, MetaRequest **r)
{
	fid_t fid;
	seq_t seq;
	const char *oldname;
	const char *newpath;
	bool overwrite;
	int protoVers;

	seq = prop.getValue("Cseq", (seq_t) -1);
	protoVers = prop.getValue("Client-Protocol-Version", (int) 0);
	fid = prop.getValue("Parent File-handle", (fid_t) -1);
	oldname = prop.getValue("Old-name", (const char *) NULL);
	newpath = prop.getValue("New-path", (const char *) NULL);
	overwrite = (prop.getValue("Overwrite", 0)) == 1;
	if ((fid < 0) || (oldname == NULL) || (newpath == NULL))
		return -1;

	MetaRename *rn = new MetaRename(seq, protoVers, fid, oldname, newpath, overwrite);
	rn->oldpath = prop.getValue("Old-path", "");
	*r = rn;
	return 0;
}

/*
 * \brief Handler to parse out a meta-make-chunks-stable request.
*/
static int
parseHandlerMakeChunksStable(Properties &prop, MetaRequest **r)
{
	fid_t fid;
	seq_t seq;
	int protoVers;
	string pathname;

	seq = prop.getValue("Cseq", (seq_t) -1);
	protoVers = prop.getValue("Client-Protocol-Version", (int) 0);
	fid = prop.getValue("File-handle", (fid_t) -1);
	pathname = prop.getValue("Pathname", "");
	if (fid < 0)
		return -1;

	MetaMakeChunksStable *mmcs = new MetaMakeChunksStable(seq, protoVers, fid, pathname);
	*r = mmcs;
	return 0;
}

/*
 * \brief Handler to parse out a setmtime request.
*/

static int
parseHandlerSetMtime(Properties &prop, MetaRequest **r)
{
	string path;
	seq_t seq;
	struct timeval mtime;
	int protoVers;
	
	seq = prop.getValue("Cseq", (seq_t) -1);
	protoVers = prop.getValue("Client-Protocol-Version", (int) 0);
	path = prop.getValue("Pathname", "");
	mtime.tv_sec  = prop.getValue("Mtime-sec", 0);
	mtime.tv_usec = prop.getValue("Mtime-usec", 0);
	if (path == "")
		return -1;
	*r = new MetaSetMtime(seq, protoVers, path, mtime);
	return 0;
}

static int
parseHandlerChangeFileReplication(Properties &prop, MetaRequest **r)
{
	fid_t fid;
	seq_t seq;
	int16_t numReplicas;
	int protoVers;

	seq = prop.getValue("Cseq", (seq_t) -1);
	protoVers = prop.getValue("Client-Protocol-Version", (int) 0);
	fid = prop.getValue("File-handle", (fid_t) -1);
	numReplicas = min((int16_t) prop.getValue("Num-replicas", 1), gMaxReplicasPerFile);
	if (numReplicas <= 0)
		return -1;
	*r = new MetaChangeFileReplication(seq, protoVers, fid, numReplicas);
	return 0;
}

static int
parseHandlerCoalesceBlocks(Properties &prop, MetaRequest **r)
{
	seq_t seq;
	const char *srcPath, *dstPath;
	int protoVers;

	seq = prop.getValue("Cseq", (seq_t) -1);
	protoVers = prop.getValue("Client-Protocol-Version", (int) 0);
	srcPath = prop.getValue("Src-path", (const char *) NULL);
	dstPath = prop.getValue("Dest-path", (const char *) NULL);
	if ((srcPath == NULL) || (dstPath == NULL))
		return -1;

	*r = new MetaCoalesceBlocks(seq, protoVers, srcPath, dstPath);
	return 0;
}

/*!
 * \brief Message that initiates the retiring of a chunkserver.
*/
static int
parseHandlerRetireChunkserver(Properties &prop, MetaRequest **r)
{
	ServerLocation location;
	int downtime;
	int protoVers;

	seq_t seq = prop.getValue("Cseq", (seq_t) -1);
	protoVers = prop.getValue("Client-Protocol-Version", (int) 0);
	location.hostname = prop.getValue("Chunk-server-name", "");
	location.port = prop.getValue("Chunk-server-port", -1);
	if (!location.IsValid()) {
		return -1;
	}
	downtime = prop.getValue("Downtime", -1);
	*r = new MetaRetireChunkserver(seq, protoVers, location, downtime);
	return 0;
}

static int
parseHandlerToggleRebalancing(Properties &prop, MetaRequest **r)
{
	seq_t seq = prop.getValue("Cseq", (seq_t) -1);
	// 1 is enable; 0 is disable
	int value = prop.getValue("Toggle-rebalancing", 0);
	bool v = (value == 1);
	int protoVers = prop.getValue("Client-Protocol-Version", (int) 0);

	*r = new MetaToggleRebalancing(seq, protoVers, v);
	KFS_LOG_VA_INFO("Toggle rebalancing: %d", value);
	return 0;
}

/*!
 * \brief Message that initiates the execution of a rebalance plan.
*/
static int
parseHandlerExecuteRebalancePlan(Properties &prop, MetaRequest **r)
{
	seq_t seq = prop.getValue("Cseq", (seq_t) -1);
	int protoVers = prop.getValue("Client-Protocol-Version", (int) 0);
	string pathname = prop.getValue("Pathname", "");

	*r = new MetaExecuteRebalancePlan(seq, protoVers, pathname);
	return 0;
}

/*!
 * \brief Message that initiates the re-load of MetaServer.prp file
*/
static int
parseHandlerReadConfig(Properties &prop, MetaRequest **r)
{
	seq_t seq = prop.getValue("Cseq", (seq_t) -1);
	int protoVers = prop.getValue("Client-Protocol-Version", (int) 0);
	string pathname = prop.getValue("Pathname", "");

	*r = new MetaReadConfig(seq, protoVers, pathname);
	return 0;
}

/*!
 * \brief Validate that the md5 sent by a chunkserver matches one of the
 * acceptable md5's.
 */
static int
isValidMD5Sum(const string &md5sum)
{
	if (!file_exists(gMD5SumFn)) {
		KFS_LOG_VA_INFO("MD5Sum file %s doesn't exist; no admission control", gMD5SumFn.c_str());
		return 1;
	}
	ifstream ifs;
	const int MAXLINE = 512;
	char line[MAXLINE];

	ifs.open(gMD5SumFn.c_str());

	if (ifs.fail()) {
		KFS_LOG_VA_INFO("Unable to open MD5Sum file %s; no admission control", gMD5SumFn.c_str());
		return 1;
	}

	while (!ifs.eof()) {
		ifs.getline(line, MAXLINE);
		string key = line;
		// remove trailing white space
		string::size_type spc = key.find(' ');
		if (spc != string::npos)
			key.erase(spc);
		if (key == md5sum)
			return 1;
	}

	return 0;
}

/*!
 * \brief Parse out the headers from a HELLO message.  The message
 * body contains the id's of the chunks hosted on the server.
 */
static int
parseHandlerHello(Properties &prop, MetaRequest **r)
{
	seq_t seq = prop.getValue("Cseq", (seq_t) -1);
	MetaHello *hello;
	string key;

	hello = new MetaHello(seq);
	hello->location.hostname = prop.getValue("Chunk-server-name", "");
	hello->location.port = prop.getValue("Chunk-server-port", -1);
	if (!hello->location.IsValid()) {
		delete hello;
		return -1;
	}
	key = prop.getValue("Cluster-key", "");
	if (key != gClusterKey) {
		KFS_LOG_VA_INFO("cluster key mismatch: we have %s, chunkserver sent us %s",
				gClusterKey.c_str(), key.c_str());
		hello->status = -EBADCLUSTERKEY;
	}
	key = prop.getValue("MD5Sum", "");
	if (!isValidMD5Sum(key)) {
		KFS_LOG_VA_INFO("MD5sum mismatch from chunkserver %s:%d: it sent us %s",
				hello->location.hostname.c_str(), hello->location.port, key.c_str());
		hello->status = -EBADCLUSTERKEY;
	}
	hello->totalSpace = prop.getValue("Total-space", (long long) 0);
	hello->usedSpace = prop.getValue("Used-space", (long long) 0);
	hello->rackId = prop.getValue("Rack-id", (int) -1);
	// # of chunks hosted on this server
	hello->numChunks = prop.getValue("Num-chunks", 0);
        hello->numNotStableAppendChunks = prop.getValue("Num-not-stable-append-chunks", 0);
        hello->numNotStableChunks = prop.getValue("Num-not-stable-chunks", 0);
	hello->uptime = prop.getValue("Uptime", 0);
        hello->numAppendsWithWid = prop.getValue("Num-appends-with-wids", (long long)0);

	// The chunk names follow in the body.  This field tracks
	// the length of the message body
	hello->contentLength = prop.getValue("Content-length", 0);

	*r = hello;
	return 0;
}

/*!
 * \brief Parse out the headers from a LEASE_ACQUIRE message.
 */
int
parseHandlerLeaseAcquire(Properties &prop, MetaRequest **r)
{
	*r = new MetaLeaseAcquire(
		prop.getValue("Cseq", (seq_t) -1),
		prop.getValue("Client-Protocol-Version", (int) 0),
		prop.getValue("Chunk-handle", (chunkId_t) -1),
		prop.getValue("Pathname", string())
	);
	return 0;
}

/*!
 * \brief Parse out the headers from a LEASE_RENEW message.
 */
int
parseHandlerLeaseRenew(Properties &prop, MetaRequest **r)
{
	seq_t seq = prop.getValue("Cseq", (seq_t) -1);
	chunkId_t chunkId = prop.getValue("Chunk-handle", (chunkId_t) -1);
	int64_t leaseId = prop.getValue("Lease-id", (int64_t) -1);
	string leaseTypeStr = prop.getValue("Lease-type", "READ_LEASE");
	LeaseType leaseType;
	int protoVers = prop.getValue("Client-Protocol-Version", (int) 0);

	if (leaseTypeStr == "WRITE_LEASE")
		leaseType = WRITE_LEASE;
	else
		leaseType = READ_LEASE;

	*r = new MetaLeaseRenew(seq, protoVers, leaseType, chunkId, 
				leaseId, prop.getValue("Pathname", string()));
	return 0;
}

/*!
 * \brief Parse out the headers from a LEASE_RELINQUISH message.
 */
int
parseHandlerLeaseRelinquish(Properties &prop, MetaRequest **r)
{
	*r = new MetaLeaseRelinquish(
            prop.getValue("Cseq", (seq_t) -1),
	    prop.getValue("Client-Protocol-Version", (int) 0),
            strcmp(prop.getValue("Lease-type", "READ_LEASE"), "WRITE_LEASE") == 0 ?
                WRITE_LEASE : READ_LEASE,
            prop.getValue("Chunk-handle", (chunkId_t) -1),
            prop.getValue("Lease-id", (int64_t) -1),
            prop.getValue("Chunk-size", (int64_t)-1),
            ! prop.getValue("Chunk-checksum", std::string()).empty(),
            (uint32_t)prop.getValue("Chunk-checksum", (uint64_t)0)
        );
	return 0;
}

/*!
 * \brief Parse out the headers from a CORRUPT_CHUNK message.
 */
int
parseHandlerChunkCorrupt(Properties &prop, MetaRequest **r)
{
	seq_t seq = prop.getValue("Cseq", (seq_t) -1);
	fid_t fid = prop.getValue("File-handle", (chunkId_t) -1);
	chunkId_t chunkId = prop.getValue("Chunk-handle", (chunkId_t) -1);

	MetaChunkCorrupt *mcc = new MetaChunkCorrupt(seq, fid, chunkId);
	mcc->isChunkLost = prop.getValue("Is-chunk-lost", 0);
	*r = mcc;
	return 0;
}

/*!
 * \brief Parse out the headers from a PING message.
 */
int
parseHandlerPing(Properties &prop, MetaRequest **r)
{
	seq_t seq = prop.getValue("Cseq", (seq_t) -1);
	int protoVers = prop.getValue("Client-Protocol-Version", (int) 0);

	*r = new MetaPing(seq, protoVers);
	return 0;
}

/*!
 * \brief Parse out the headers for a UPSERVER message.
 */
int
parseHandlerUpServers(Properties &prop, MetaRequest **r)
{
	seq_t seq = prop.getValue("Cseq", (seq_t) -1);
	int protoVers = prop.getValue("Client-Protocol-Version", (int) 0);

	*r = new MetaUpServers(seq, protoVers);
	return 0;
}

/*!
 * \brief Parse out the headers from a TOGGLE_WORM message.
 */
int
parseHandlerToggleWORM(Properties &prop, MetaRequest **r)
{
	seq_t seq = prop.getValue("Cseq", (seq_t) -1);
	int protoVers = prop.getValue("Client-Protocol-Version", (int) 0);
	// 1 is enable; 0 is disable
	int value = prop.getValue("Toggle-WORM", 0);
	bool v = (value == 1);

	*r = new MetaToggleWORM(seq, protoVers, v);
	KFS_LOG_VA_INFO("Toggle WORM: %d", value);
	return 0;
}

/*!
 * \brief Parse out the headers from a STATS message.
 */
int
parseHandlerStats(Properties &prop, MetaRequest **r)
{
	seq_t seq = prop.getValue("Cseq", (seq_t) -1);
	int protoVers = prop.getValue("Client-Protocol-Version", (int) 0);

	*r = new MetaStats(seq, protoVers);
	return 0;
}

/*!
 * \brief Parse out a check leases request.
 */
int
parseHandlerCheckLeases(Properties &prop, MetaRequest **r)
{
	seq_t seq = prop.getValue("Cseq", (seq_t) -1);
	int protoVers = prop.getValue("Client-Protocol-Version", (int) 0);

	*r = new MetaCheckLeases(seq, protoVers);
	return 0;
}

/*!
 * \brief Parse out a dump server map request.
 */
int
parseHandlerDumpChunkToServerMap(Properties &prop, MetaRequest **r)
{
	seq_t seq = prop.getValue("Cseq", (seq_t) -1);
	int protoVers = prop.getValue("Client-Protocol-Version", (int) 0);

	*r = new MetaDumpChunkToServerMap(seq, protoVers);
	return 0;
}

/*!
 * \brief Parse out a dump server map request.
 */
int
parseHandlerRecomputeDirsize(Properties &prop, MetaRequest **r)
{
	seq_t seq = prop.getValue("Cseq", (seq_t) -1);
	int protoVers = prop.getValue("Client-Protocol-Version", (int) 0);

	*r = new MetaRecomputeDirsize(seq, protoVers);
	return 0;
}

/*!
 * \brief Parse out a dump chunk replication candidates request.
 */
int
parseHandlerDumpChunkReplicationCandidates(Properties &prop, MetaRequest **r)
{
	seq_t seq = prop.getValue("Cseq", (seq_t) -1);
	int protoVers = prop.getValue("Client-Protocol-Version", (int) 0);

	*r = new MetaDumpChunkReplicationCandidates(seq, protoVers);
	return 0;
}

/*!
 * \brief Parse out a dump chunk replication candidates request.
 */
int
parseHandlerFsck(Properties &prop, MetaRequest **r)
{
	seq_t seq = prop.getValue("Cseq", (seq_t) -1);
	int protoVers = prop.getValue("Client-Protocol-Version", (int) 0);

	*r = new MetaFsck(seq, protoVers);
	return 0;
}

/*!
 * \brief Parse out the headers from a STATS message.
 */
int
parseHandlerOpenFiles(Properties &prop, MetaRequest **r)
{
	seq_t seq = prop.getValue("Cseq", (seq_t) -1);
	int protoVers = prop.getValue("Client-Protocol-Version", (int) 0);

	*r = new MetaOpenFiles(seq, protoVers);
	return 0;
}

int
parseHandlerSetChunkServersProperties(Properties &prop, MetaRequest **r)
{
	seq_t seq = prop.getValue("Cseq", (seq_t) -1);
	int protoVers = prop.getValue("Client-Protocol-Version", (int) 0);

	MetaSetChunkServersProperties* const op =
		new MetaSetChunkServersProperties(seq, protoVers);
	prop.copyWithPrefix("chunkServer.", op->properties);
	*r = op;
	return 0;
}

int
parseHandlerGetChunkServerCounters(Properties &prop, MetaRequest **r)
{
	seq_t seq = prop.getValue("Cseq", (seq_t) -1);
	int protoVers = prop.getValue("Client-Protocol-Version", (int) 0);
	MetaGetChunkServersCounters* const op =
		new MetaGetChunkServersCounters(seq, protoVers);
	*r = op;
	return 0;
}

inline static bool
OkHeader(const MetaRequest* op, ostream &os, bool checkStatus = true)
{
    os << "OK\r\n";
    os << "Cseq: " << op->opSeqno << "\r\n";
    os << "Status: " << op->status << "\r\n";
    if (! op->statusMsg.empty()) {
        const size_t p = op->statusMsg.find('\r');
        assert(string::npos == p && op->statusMsg.find('\n') == string::npos);
        os << "Status-message: " <<
            (p == string::npos ? op->statusMsg : op->statusMsg.substr(0, p)) <<
        "\r\n";
    }
    if (checkStatus && op->status < 0) {
        os << "\r\n";
    }
    return (op->status >= 0);
}

inline static ostream&
PutHeader(const MetaRequest* op, ostream &os)
{
    OkHeader(op, os, false);
    return os;
}

/*!
 * \brief Generate response (a string) for various requests that
 * describes the result of the request execution.  The generated
 * response string is based on the KFS protocol.  All follow the same
 * model:
 * @param[out] os: A string stream that contains the response.
 */
void
MetaLookup::response(ostream &os)
{
	if (! OkHeader(this, os)) {
		return;
	}
        os << result << "\r\n";
}

void
MetaLookupPath::response(ostream &os)
{
	if (! OkHeader(this, os)) {
		return;
	}
        os << result << "\r\n";
}

void
MetaCreate::response(ostream &os)
{
	if (! OkHeader(this, os)) {
		return;
	}
	os << "File-handle: " << toString(fid) << "\r\n\r\n";
}

void
MetaRemove::response(ostream &os)
{
	PutHeader(this, os) << "\r\n";
}

void
MetaMkdir::response(ostream &os)
{
	if (! OkHeader(this, os)) {
		return;
	}
	os << "File-handle: " << toString(fid) << "\r\n\r\n";
}

void
MetaRmdir::response(ostream &os)
{
	PutHeader(this, os) << "\r\n";
}

void
MetaReaddir::response(ostream &os)
{
	vector<MetaDentry *>::iterator iter;
	ostringstream entries;
	int numEntries = 0;

	if (! OkHeader(this, os)) {
		return;
	}
	// Send over the names---1 name per line so it is easy to
	// extract it out
	// XXX This should include the file id also, and probably
	// the other NFS READDIR elements, namely a cookie and
	// eof indicator to support reading less than a whole
	// directory at a time.
	for (iter = v.begin(); iter != v.end(); ++iter) {
		MetaDentry *d = *iter;
		// "/" doesn't have "/" as an entry in it.
		if ((dir == ROOTFID) && (d->getName() == "/"))
			continue;

		entries << d->getName() << "\n";
		++numEntries;
	}
	os << "Num-Entries: " << numEntries << "\r\n";
	os << "Content-length: " << entries.str().length() << "\r\n\r\n";
	if (entries.str().length() > 0)
		os << entries.str();
}

void
MetaReaddirPlus::response(ostream &os)
{
	if (! OkHeader(this, os)) {
		return;
	}
	os << "Num-Entries: " << numEntries << "\r\n";
	os << "Content-length: " << v.str().length() << "\r\n\r\n";
	os << v.str();
}

void
MetaRename::response(ostream &os)
{
	PutHeader(this, os) << "\r\n";
}

void
MetaMakeChunksStable::response(ostream &os)
{
	PutHeader(this, os) << "\r\n";
}

void
MetaSetMtime::response(ostream &os)
{
	PutHeader(this, os) << "\r\n";
}

void
MetaGetalloc::response(ostream &os)
{
	PutHeader(this, os);
	os << "Chunk-handle: " << chunkId << "\r\n";
	os << "Chunk-version: " << chunkVersion << "\r\n";
	if (status < 0) {
		os << "\r\n";
		return;
	}
	os << "Num-replicas: " << locations.size() << "\r\n";

	assert(locations.size() > 0);

	os << "Replicas:";
	for_each(locations.begin(), locations.end(), ListServerLocations(os));
	os << "\r\n\r\n";
}

void
MetaGetlayout::response(ostream &os)
{
	vector<ChunkLayoutInfo>::iterator iter;
	ChunkLayoutInfo l;
	ostringstream entries;

	if (! OkHeader(this, os)) {
		return;
	}
	os << "Num-chunks: " << v.size() << "\r\n";
	// Send over the layout info
	for (iter = v.begin(); iter != v.end(); ++iter) {
		l = *iter;
		entries << l.toString();
	}
	os << "Content-length: " << entries.str().length() << "\r\n\r\n";

	if (entries.str().length() > 0)
		os << entries.str();
}

class PrintChunkServerLocations {
	ostream &os;
public:
	PrintChunkServerLocations(ostream &out): os(out) { }
	void operator () (ChunkServerPtr &s)
	{
		os << " " <<  s->ServerID();
	}
};

void
MetaAllocate::response(ostream &os)
{
	if (! OkHeader(this, os)) {
		return;
	}
	os << "Chunk-handle: " << chunkId << "\r\n";
	os << "Chunk-version: " << chunkVersion << "\r\n";
	if (appendChunk)
		os << "Chunk-offset: " << offset << "\r\n";

	os << "Master: " << master->ServerID() << "\r\n";
	os << "Num-replicas: " << servers.size() << "\r\n";

	assert(servers.size() > 0);
	os << "Replicas:";
	for_each(servers.begin(), servers.end(), PrintChunkServerLocations(os));
	os << "\r\n\r\n";
}

void
MetaLeaseAcquire::response(ostream &os)
{
	if (! OkHeader(this, os)) {
		return;
	}
	os << "Lease-id: " << leaseId << "\r\n\r\n";
}

void
MetaLeaseRenew::response(ostream &os)
{
	PutHeader(this, os) << "\r\n";
}

void
MetaLeaseRelinquish::response(ostream &os)
{
	PutHeader(this, os) << "\r\n";
}

void
MetaCoalesceBlocks::response(ostream &os)
{
	if (! OkHeader(this, os)) {
		return;
	}
	os << "Dst-start-offset: " << dstStartOffset << "\r\n\r\n";
}

void
MetaHello::response(ostream &os)
{
	PutHeader(this, os) << "\r\n";
}

void
MetaChunkCorrupt::response(ostream &os)
{
	PutHeader(this, os) << "\r\n";
}

void
MetaTruncate::response(ostream &os)
{
	PutHeader(this, os) << "\r\n";
}

void
MetaChangeFileReplication::response(ostream &os)
{
	PutHeader(this, os) <<
		"Num-replicas: " << numReplicas << "\r\n\r\n";
}

void
MetaRetireChunkserver::response(ostream &os)
{
	PutHeader(this, os) << "\r\n";
}

void
MetaToggleRebalancing::response(ostream &os)
{
	PutHeader(this, os) << "\r\n";
}

void
MetaToggleWORM::response(ostream &os)
{
	PutHeader(this, os) << "\r\n";
}

void
MetaExecuteRebalancePlan::response(ostream &os)
{
	PutHeader(this, os) << "\r\n";
}

void
MetaReadConfig::response(ostream &os)
{
	PutHeader(this, os) << "\r\n";
}

void
MetaPing::response(ostream &os)
{
	PutHeader(this, os);
	os << "Build-version: " << KFS::KFS_BUILD_VERSION_STRING << "\r\n";
	os << "Source-version: " << KFS::KFS_SOURCE_REVISION_STRING << "\r\n";
	if (gWormMode)
		os << "WORM: " << 1 << "\r\n";
	else
		os << "WORM: " << 0 << "\r\n";
	os << "System Info: " << systemInfo << "\r\n";
	os << "Servers: " << servers << "\r\n";
	os << "Retiring Servers: " << retiringServers << "\r\n";
	os << "Down Servers: " << downServers << "\r\n\r\n";
}

void
MetaUpServers::response(ostream &os)
{
	PutHeader(this, os);
	os << "Content-length: " << stringStream.str().length() << "\r\n\r\n";
	if (stringStream.str().length() > 0)
        	os << stringStream.str();
}

void
MetaStats::response(ostream &os)
{
	PutHeader(this, os) << stats << "\r\n";
}

void
MetaCheckLeases::response(ostream &os)
{
	PutHeader(this, os) << "\r\n";
}

void
MetaRecomputeDirsize::response(ostream &os)
{
	PutHeader(this, os) << "\r\n";
}

void
MetaDumpChunkToServerMap::response(ostream &os)
{
	PutHeader(this, os) << "Filename: " << chunkmapFile << "\r\n\r\n";
}

void
MetaDumpChunkReplicationCandidates::response(ostream &os)
{
	PutHeader(this, os) <<
		"Content-length: " << blocks.length() << "\r\n\r\n";
	if (blocks.length() > 0)
		os << blocks;
}

void
MetaFsck::response(ostream &os)
{
	PutHeader(this, os) <<
		"Content-length: " << fsckStatus.length() << "\r\n\r\n";
	if (fsckStatus.length() > 0)
		os << fsckStatus;
}

void
MetaOpenFiles::response(ostream &os)
{
	PutHeader(this, os);
	os << "Read: " << openForRead << "\r\n";
	os << "Write: " << openForWrite << "\r\n\r\n";
}

void
MetaSetChunkServersProperties::response(ostream &os)
{
	PutHeader(this, os) << "\r\n";
}

void
MetaGetChunkServersCounters::response(ostream &os)
{
	PutHeader(this, os) <<
		"Content-length: " << resp.length() << "\r\n\r\n" <<
		resp;
}

/*!
 * \brief Generate request (a string) that should be sent to the chunk
 * server.  The generated request string is based on the KFS
 * protocol.  All follow the same model:
 * @param[out] os: A string stream that contains the response.
 */
void
MetaChunkAllocate::request(ostream &os)
{
	assert(req != NULL);

	os << "ALLOCATE \r\n";
	os << "Cseq: " << opSeqno << "\r\n";
	os << "Version: KFS/1.0\r\n";
	os << "File-handle: " << req->fid << "\r\n";
	os << "Chunk-handle: " << req->chunkId << "\r\n";
	os << "Chunk-version: " << req->chunkVersion << "\r\n";
	if (leaseId >= 0) {
		os << "Lease-id: " << leaseId << "\r\n";
	}
        os << "Chunk-append: " << (req->appendChunk ? 1 : 0) << "\r\n";

	os << "Num-servers: " << req->servers.size() << "\r\n";
	assert(req->servers.size() > 0);

	os << "Servers:";
	for_each(req->servers.begin(), req->servers.end(),
			PrintChunkServerLocations(os));
	os << "\r\n\r\n";
}

void
MetaChunkDelete::request(ostream &os)
{
	os << "DELETE \r\n";
	os << "Cseq: " << opSeqno << "\r\n";
	os << "Version: KFS/1.0\r\n";
	os << "Chunk-handle: " << chunkId << "\r\n\r\n";
}

void
MetaChunkTruncate::request(ostream &os)
{
	os << "TRUNCATE \r\n";
	os << "Cseq: " << opSeqno << "\r\n";
	os << "Version: KFS/1.0\r\n";
	os << "Chunk-handle: " << chunkId << "\r\n";
	os << "Chunk-size: " << chunkSize << "\r\n\r\n";
}

void
MetaChunkHeartbeat::request(ostream &os)
{
	os << "HEARTBEAT \r\n";
	os << "Cseq: " << opSeqno << "\r\n";
	os << "Version: KFS/1.0\r\n\r\n";
}

void
MetaChunkStaleNotify::request(ostream &os)
{
	string s;
	vector<chunkId_t>::size_type i;

	os << "STALE_CHUNKS \r\n";
	os << "Cseq: " << opSeqno << "\r\n";
	os << "Version: KFS/1.0\r\n";
	os << "Num-chunks: " << staleChunkIds.size() << "\r\n";
	for (i = 0; i < staleChunkIds.size(); ++i) {
		s += toString(staleChunkIds[i]);
		s += " ";
	}
	os << "Content-length: " << s.length() << "\r\n\r\n";
	os << s;
}

void
MetaChunkRetire::request(ostream &os)
{
	os << "RETIRE \r\n";
	os << "Cseq: " << opSeqno << "\r\n";
	os << "Version: KFS/1.0\r\n\r\n";
}

void
MetaChunkVersChange::request(ostream &os)
{
	os << "CHUNK_VERS_CHANGE \r\n";
	os << "Cseq: " << opSeqno << "\r\n";
	os << "Version: KFS/1.0\r\n";
	os << "File-handle: " << fid << "\r\n";
	os << "Chunk-handle: " << chunkId << "\r\n";
	os << "Chunk-version: " << chunkVersion << "\r\n\r\n";
}

void
MetaBeginMakeChunkStable::request(ostream &os)
{
	os << "BEGIN_MAKE_CHUNK_STABLE\r\n"
		"Cseq: "          << opSeqno      << "\r\n"
		"Version: KFS/1.0\r\n"
		"File-handle: "   << fid          << "\r\n"
		"Chunk-handle: "  << chunkId      << "\r\n"
		"Chunk-version: " << chunkVersion << "\r\n"
        "\r\n";
}

void
MetaChunkMakeStable::request(ostream &os)
{
	os << "MAKE_CHUNK_STABLE \r\n";
	os << "Cseq: " << opSeqno << "\r\n";
	os << "Version: KFS/1.0\r\n";
	os << "File-handle: " << fid << "\r\n";
	os << "Chunk-handle: " << chunkId << "\r\n";
	os << "Chunk-version: " << chunkVersion << "\r\n";
        os << "Chunk-size: " << chunkSize << "\r\n";
        if (hasChunkChecksum) {
            os << "Chunk-checksum: " << chunkChecksum << "\r\n";
        }
        os << "\r\n";
}

void
MetaChunkReplicate::request(ostream &os)
{
	os << "REPLICATE \r\n";
	os << "Cseq: " << opSeqno << "\r\n";
	os << "Version: KFS/1.0\r\n";
	os << "File-handle: " << fid << "\r\n";
	os << "Chunk-handle: " << chunkId << "\r\n";
	os << "Chunk-version: " << chunkVersion << "\r\n";
	os << "Chunk-location: " << srcLocation.ToString() << "\r\n\r\n";
}

void
MetaChunkSize::request(ostream &os)
{
	os << "SIZE \r\n";
	os << "Cseq: " << opSeqno << "\r\n";
	os << "Version: KFS/1.0\r\n";
	os << "File-handle: " << fid << "\r\n";
	os << "Chunk-handle: " << chunkId << "\r\n\r\n";
}

void
MetaChunkSetProperties::request(ostream &os)
{
	os <<
	"CMD_SET_PROPERTIES\r\n"
	"Cseq: " << opSeqno << "\r\n"
	"Version: KFS/1.0\r\n"
	"Content-length: " << serverProps.length() << "\r\n\r\n" <<
	serverProps
	;
}

void
MetaChunkServerRestart::request(ostream &os)
{
	os <<
	"RESTART_CHUNK_SERVER\r\n"
	"Cseq: " << opSeqno << "\r\n"
	"Version: KFS/1.0\r\n"
	"\r\n"
        ;
}

} /* namespace KFS */
