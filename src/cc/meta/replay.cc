/*!
 * $Id: replay.cc 1552 2011-01-06 22:21:54Z sriramr $
 *
 * \file replay.cc
 * \brief log replay
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

#include <cassert>
#include <cstdlib>
#include <iostream>

#include "logger.h"
#include "replay.h"
#include "restore.h"
#include "util.h"
#include "entry.h"
#include "kfstree.h"
#include "LayoutManager.h"
#include "common/log.h"

using namespace KFS;

Replay KFS::replayer;

/*!
 * \brief open saved log file for replay
 * \param[in] p	a path in the form "<logdir>/log.<number>"
 */
void
Replay::openlog(const string &p)
{
	if (file.is_open())
		file.close();

	if (!file_exists(p)) {
		std::cerr << "log file " << p << " not found!\n";
		number = 0;
		path = oplog.logfile(0);
	} else {
		KFS_LOG_VA_INFO("Doing log replay from file: %s", p.c_str());
		string::size_type dot = p.rfind('.');
		assert(dot != string::npos);
		number = std::atoi(p.substr(dot + 1).c_str());
		assert(number >= 0);
		path = p;
		file.open(path.c_str());
		assert(!file.fail());
	}
}

/*!
 * \brief check log version
 * format: version/<number>
 */
static bool
replay_version(deque <string> &c)
{
	fid_t vers;
	bool ok = pop_fid(vers, "version", c, true);
	return (ok && vers == Logger::VERSION);
}

/*!
 * \brief handle common prefix for all log records
 */
static bool
pop_parent(fid_t &id, deque <string> &c)
{
	c.pop_front();		// get rid of record type
	return pop_fid(id, "dir", c, true);
}

/*!
 * \brief update the seed of a UniqueID with what is passed in.
 * Since this function is called in the context of log replay, it
 * better be the case that the seed passed in is higher than
 * the id's seed (which was set from a checkpoint file).
*/
static void
updateSeed(UniqueID &id, seqid_t seed)
{
	if (seed < id.getseed()) {
		KFS_LOG_VA_ERROR("Seed from log: %lld < id's seed: %lld", 
				seed, id.getseed());
		panic("Seed", false);
	}
	id.setseed(seed);
}

/*!
 * \brief replay a file create
 * format: create/dir/<parentID>/name/<name>/id/<myID>{/ctime/<time>}
 */
static bool
replay_create(deque <string> &c)
{
	fid_t parent, me;
	string myname;
	int status = 0;
	int16_t numReplicas;
	struct timeval ctime;

	bool ok = pop_parent(parent, c);
	ok = pop_name(myname, "name", c, ok);
	ok = pop_fid(me, "id", c, ok);
	ok = pop_short(numReplicas, "numReplicas", c, ok);
	// if the log has the ctime, pass it thru
	bool gottime = pop_time(ctime, "ctime", c, ok);
	if (ok) {
		// for all creates that were successful during normal operation,
		// when we replay it should work; so, exclusive = false
		status = metatree.create(parent, myname, &me, numReplicas, false);
		if (status == 0)
			updateSeed(fileID, me);
		if (gottime) {
			MetaFattr *fa = metatree.getFattr(me);
			if (fa != NULL)
				fa->mtime = fa->ctime = fa->crtime = ctime;
		}
	}
	KFS_LOG_VA_DEBUG("Replay create: name=%s, id=%lld", myname.c_str(), me);
	return (ok && status == 0);
}

/*!
 * \brief replay mkdir
 * format: mkdir/dir/<parentID>/name/<name>/id/<myID>{/ctime/<time>}
 */
static bool
replay_mkdir(deque <string> &c)
{
	fid_t parent, me;
	string myname;
	int status = 0;
	struct timeval ctime;

	bool ok = pop_parent(parent, c);
	ok = pop_name(myname, "name", c, ok);
	ok = pop_fid(me, "id", c, ok);
	// if the log has the ctime, pass it thru
	bool gottime = pop_time(ctime, "ctime", c, ok);
	if (ok) {
		status = metatree.mkdir(parent, myname, &me);
		if (status == 0)
			updateSeed(fileID, me);
		if (gottime) {
			MetaFattr *fa = metatree.getFattr(me);
			if (fa != NULL)
				fa->mtime = fa->ctime = fa->crtime = ctime;
		}
	}
	KFS_LOG_VA_DEBUG("Replay mkdir: name=%s, id=%lld", myname.c_str(), me);
	return (ok && status == 0);
}

/*!
 * \brief replay remove
 * format: remove/dir/<parentID>/name/<name>
 */
static bool
replay_remove(deque <string> &c)
{
	fid_t parent;
	string myname;
	int status = 0;
	bool ok = pop_parent(parent, c);
	ok = pop_name(myname, "name", c, ok);

	if (ok)
		status = metatree.remove(parent, myname, "");

	return (ok && status == 0);
}

/*!
 * \brief replay rmdir
 * format: rmdir/dir/<parentID>/name/<name>
 */
static bool
replay_rmdir(deque <string> &c)
{
	fid_t parent;
	string myname;
	int status = 0;
	bool ok = pop_parent(parent, c);
	ok = pop_name(myname, "name", c, ok);
	if (ok)
		status = metatree.rmdir(parent, myname, "");
	return (ok && status == 0);
}

/*!
 * \brief replay rename
 * format: rename/dir/<parentID>/old/<oldname>/new/<newpath>
 * NOTE: <oldname> is the name of file/dir in parent.  This
 * will never contain any slashes.
 * <newpath> is the full path of file/dir. This may contain slashes.
 * Since it is the last component, everything after new is <newpath>.
 * So, unlike <oldname> which just requires taking one element out,
 * we need to take everything after "new" for the <newpath>.
 * 
 */
static bool
replay_rename(deque <string> &c)
{
	fid_t parent;
	string oldname, newpath;
	int status = 0;
	bool ok = pop_parent(parent, c);
	ok = pop_name(oldname, "old", c, ok);
	ok = pop_path(newpath, "new", c, ok);
	if (ok) {
		MetaFattr *fa = metatree.lookup(parent, oldname);
		string oldpath = "";
		if (fa != NULL)
			oldpath = metatree.getPathname(fa->id());
		status = metatree.rename(parent, oldname, newpath, oldpath, true);
	}
	return (ok && status == 0);
}

/*!
 * \brief replay allocate
 * format: allocate/file/<fileID>/offset/<offset>/chunkId/<chunkID>/
 * chunkVersion/<chunkVersion>/{mtime/<time>}{/append/<1|0>}
 */
static bool
replay_allocate(deque <string> &c)
{
	fid_t fid;
	chunkId_t cid, logChunkId;
	chunkOff_t offset, tmp = 0;
	seq_t chunkVersion, logChunkVersion;
	int status = 0;
	MetaFattr *fa;
	struct timeval mtime;
        

	c.pop_front();
	bool ok = pop_fid(fid, "file", c, true);
	ok = pop_fid(offset, "offset", c, ok);
	ok = pop_fid(logChunkId, "chunkId", c, ok);
	ok = pop_fid(logChunkVersion, "chunkVersion", c, ok);
	// if the log has the mtime, pass it thru
	const bool gottime = pop_time(mtime, "mtime", c, ok);
	const bool append = pop_fid(tmp, "append", c, ok) && tmp != 0;

	// during normal operation, if a file that has a valid 
	// lease is removed, we move the file to the dumpster and log it.
	// a subsequent allocation on that file will succeed.
	// the remove/allocation is recorded in the logs in that order.
	// during replay, we do the remove first and then we try to
	// replay allocation; for the allocation, we won't find
	// the file attributes.  we move on...when the chunkservers
	// that has the associated chunks for the file contacts us, we won't
	// find the fid and so those chunks will get nuked as stale.
	fa = metatree.getFattr(fid);
	if (fa == NULL)
		return ok;

	if (ok) {
		// if the log has the mtime, set it up in the FA
		if (gottime) 
			fa->mtime = mtime;

		cid = logChunkId;
		status = metatree.allocateChunkId(fid, offset, &cid, 
						&chunkVersion, NULL);
		if (status == -EEXIST) {
			// allocates are particularly nasty: we can have
			// allocate requests that retrieve the info for an
			// existing chunk; since there is no tree mutation,
			// there is no way to turn off logging for the request
			// (the mutation field of a request is const).  so, if
			// we end up in a situation where what we get from the
			// log matches what is in the tree, ignore it and move
			// on
			if (chunkVersion == logChunkVersion)
				return ok;
			status = 0;
		}

		if (status == 0) {
			assert(cid == logChunkId);
			chunkVersion = logChunkVersion;
			status = metatree.assignChunkId(fid, offset,
							cid, chunkVersion);
			if (status == 0) {
				gLayoutManager.AddChunkToServerMapping(cid, fid, offset, NULL);
				// In case of append create begin make chunk stable entry,
				// if it doesn't already exist.
				if (append)
					gLayoutManager.ReplayPendingMakeStable(
						cid, chunkVersion, -1, false, 0, true);
				if (cid > chunkID.getseed()) {
					// chunkID are handled by a two-stage
					// allocation: the seed is updated in
					// the first part of the allocation and
					// the chunk is attached to the file
					// after the chunkservers have ack'ed
					// the allocation.  We can have a run
					// where: (1) the seed is updated, (2)
					// a checkpoint is taken, (3) allocation
					// is done and written to log file.  If
					// we crash, then the cid in log < seed in ckpt.
					updateSeed(chunkID, cid);
				}
			}
			// assign updates the mtime; so, set it to what is in
			// the log
			if (gottime) 
				fa->mtime = mtime;
		}
	}
	return (ok && status == 0);
}

/*!
 * \brief replay coalesce (do the cleanup/accounting actions)
 * format: coalesce/old/<srcFid>/new/<dstFid>/count/<# of blocks coalesced>
 */
static bool
replay_coalesce(deque <string> &c)
{
 	fid_t srcFid, dstFid;
	size_t count;

	c.pop_front();
	bool ok = pop_fid(srcFid, "old", c, true);
	ok = pop_fid(dstFid, "new", c, ok);
	ok = pop_size(count, "count", c, ok);
	vector<chunkId_t> srcChunks;
	fid_t             retSrcFid      = -1;
	fid_t             retDstFid      = -1;
	chunkOff_t        dstStartOffset = -1;
	ok = ok && metatree.coalesceBlocks(
		metatree.getFattr(srcFid), metatree.getFattr(dstFid),
		retSrcFid, srcChunks, retDstFid, dstStartOffset) == 0;
	if (ok) {
		gLayoutManager.CoalesceBlocks(srcChunks, retSrcFid, retDstFid,
						dstStartOffset);
	}
	return (
		ok &&
		retSrcFid == srcFid && retDstFid == dstFid &&
		srcChunks.size() == count
	);
}


/*!
 * \brief replay truncate
 * format: truncate/file/<fileID>/offset/<offset>{/mtime/<time>}
 */
static bool
replay_truncate(deque <string> &c)
{
	fid_t fid;
	chunkOff_t offset;
	int status = 0;
	struct timeval mtime;

	c.pop_front();
	bool ok = pop_fid(fid, "file", c, true);
	ok = pop_fid(offset, "offset", c, ok);
	// if the log has the mtime, pass it thru
	bool gottime = pop_time(mtime, "mtime", c, ok);
	if (ok) {
		chunkOff_t allocOffset;

		// an allocation should not occur during replay
		status = metatree.truncate(fid, offset, &allocOffset);

		if ((status == 0)  && gottime) {
			MetaFattr *fa = metatree.getFattr(fid);
			if (fa != NULL) 
				fa->mtime = mtime;
		}
	}
	return (ok && status == 0);
}

/*!
 * \brief replay prune blks from head of file
 * format: pruneFromHead/file/<fileID>/offset/<offset>{/mtime/<time>}
 */
static bool
replay_pruneFromHead(deque <string> &c)
{
	fid_t fid;
	chunkOff_t offset;
	int status = 0;
	struct timeval mtime;

	c.pop_front();
	bool ok = pop_fid(fid, "file", c, true);
	ok = pop_fid(offset, "offset", c, ok);
	// if the log has the mtime, pass it thru
	bool gottime = pop_time(mtime, "mtime", c, ok);
	if (ok) {
		status = metatree.pruneFromHead(fid, offset);

		if ((status == 0)  && gottime) {
			MetaFattr *fa = metatree.getFattr(fid);
			if (fa != NULL) 
				fa->mtime = mtime;
		}
	}
	return (ok && status == 0);
}

/*!
 * \brief replay size
 * format: size/file/<fileID>/filesize/<filesize>
 */
static bool
replay_size(deque <string> &c)
{
	fid_t fid;
	off_t filesize;

	c.pop_front();
	bool ok = pop_fid(fid, "file", c, true);
	ok = pop_offset(filesize, "filesize", c, ok);
	if (ok) {
		MetaFattr *fa = metatree.getFattr(fid);
		if (fa != NULL) {
			off_t delta = filesize;
			if (fa->filesize > 0) {
				// we are updating the size for a file.  if we
				// had a value for the file's size and the log
				// has a value, then the change in space usage
				// for the tree is the difference between the
				// two.
				delta -= fa->filesize;
			}

			if (delta > 0) {
				string pn = metatree.getPathname(fid);
				metatree.updateSpaceUsageForPath(pn, delta);
			}

			fa->filesize = filesize;
		}
	}
	return true;
}

/*!
 * Replay a change file replication RPC.
 * format: setrep/file/<fid>/replicas/<#>
 */

static bool
replay_setrep(deque <string> &c)
{
	fid_t fid;
	int16_t numReplicas;

	c.pop_front();
	bool ok = pop_fid(fid, "file", c, true);
	ok = pop_short(numReplicas, "replicas", c, ok);
	if (ok) {
		metatree.changePathReplication(fid, numReplicas);
	}
	return ok;
}

/*!
 * \brief replay setmtime
 * format: setmtime/file/<fileID>/mtime/<time>
 */
static bool
replay_setmtime(deque <string> &c)
{
	fid_t fid;
	struct timeval mtime;

	c.pop_front();
	bool ok = pop_fid(fid, "file", c, true);
	ok = pop_time(mtime, "mtime", c, ok);
	if (ok) {
		MetaFattr *fa = metatree.getFattr(fid);
		// If the fa isn't there that isn't fatal.
		if (fa != NULL)
			fa->mtime = mtime;
	}
	return ok;
}

/*!
 * \brief restore time
 * format: time/<time>
 */
static bool
restore_time(deque <string> &c)
{
	c.pop_front();
	std::cout << "Log time: " << c.front() << std::endl;
	return true;
}

/*!
 * \brief restore make chunk stable
 * format:
 * "mkstable{done}/fileId/" << fid <<
 * "/chunkId/"        << chunkId <<
 * "/chunkVersion/"   << chunkVersion  <<
 * "/size/"           << chunkSize <<
 * "/checksum/"       << chunkChecksum <<
 * "/hasChecksum/"    << (hasChunkChecksum ? 1 : 0)
 */
static bool
restore_makechunkstable(deque <string> &c, bool addFlag)
{
	fid_t     fid;
	chunkId_t chunkId;
	seq_t     chunkVersion;
	off_t     chunkSize;
        string    str;
	fid_t     tmp;
	uint32_t  checksum;
	bool      hasChecksum;

	c.pop_front();
	bool ok = pop_fid(fid, "fileId", c, true);
	ok = pop_fid(chunkId, "chunkId", c, ok);
	ok = pop_fid(chunkVersion, "chunkVersion", c, ok);
	ok = pop_name(str, "size", c, ok);
        chunkSize = toNumber(str);
	ok = pop_fid(tmp, "checksum", c, ok);
	checksum = (uint32_t)tmp;
	ok = pop_fid(tmp, "hasChecksum", c, ok);
	hasChecksum = tmp != 0;
	if (!ok) {
		std::cerr << "Ignore log line for mkstable <"
			<< fid << ',' << chunkId << ',' << chunkVersion
			<< ">" << std::endl;	
		return true;
	}
	if (ok) {
		gLayoutManager.ReplayPendingMakeStable(
			chunkId, chunkVersion, chunkSize,
			hasChecksum, checksum, addFlag);
	}
	return ok;
}

static bool
restore_mkstable(deque <string> &c)
{
	return restore_makechunkstable(c, true);
}

static bool
restore_mkstabledone(deque <string> &c)
{
	return restore_makechunkstable(c, false);
}

static void
init_map(DiskEntry &e)
{
	e.add_parser("version", replay_version);
	e.add_parser("create", replay_create);
	e.add_parser("mkdir", replay_mkdir);
	e.add_parser("remove", replay_remove);
	e.add_parser("rmdir", replay_rmdir);
	e.add_parser("rename", replay_rename);
	e.add_parser("allocate", replay_allocate);
	e.add_parser("truncate", replay_truncate);
	e.add_parser("coalesce", replay_coalesce);
	e.add_parser("pruneFromHead", replay_pruneFromHead);
	e.add_parser("setrep", replay_setrep);
	e.add_parser("size", replay_size);
	e.add_parser("setmtime", replay_setmtime);
	e.add_parser("chunkVersionInc", restore_chunkVersionInc);
	e.add_parser("time", restore_time);
	e.add_parser("mkstable", restore_mkstable);
	e.add_parser("mkstabledone", restore_mkstabledone);
}

/*!
 * \brief replay contents of log file
 * \return	zero if replay successful, negative otherwise
 */
int
Replay::playlog()
{
	if (!file.is_open()) {
		//!< no log...so, reset the # to 0.
		number = 0;
		return 0;
	}

	const int MAXLINE = 400;
	char line[MAXLINE];
	int lineno = 0;

	DiskEntry entrymap;
	init_map(entrymap);

	bool is_ok = true;
	seq_t opcount = oplog.checkpointed();
	while (is_ok && !file.eof()) {
		++lineno;
		file.getline(line, MAXLINE);
		is_ok = entrymap.parse(line);
		if (!is_ok)
			std::cerr << "Error at line " << lineno << ": "
					<< line << '\n';
	}
	opcount += lineno;
	oplog.set_seqno(opcount);

	file.close();
	return is_ok ? 0 : -EIO;
}

/*!
 * \brief replay contents of all log files since CP
 * \return	zero if replay successful, negative otherwise
 */
int
Replay::playAllLogs()
{
	int status = 0;

	if (logno() < 0) {
		//!< no log...so, reset the # to 0.
		number = 0;
		return 0;
	}

	for (int i = logno(); ; i++) {
		string logfn = oplog.logfile(i);

		if (!file_exists(logfn))
			break;
		openlog(logfn);
		status = playlog();
		if (status < 0)
			break;
	}
	return status;

}
