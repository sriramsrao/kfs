/*
 * $Id: restore.cc 1552 2011-01-06 22:21:54Z sriramr $
 *
 * \file restore.cc
 * \brief rebuild metatree from saved checkpoint
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

#include <fcntl.h>
#include <map>
#include <cerrno>
#include <cstring>
#include "restore.h"
#include "util.h"
#include "logger.h"
#include "meta.h"
#include "kfstree.h"
#include "replay.h"
#include "restore.h"
#include "entry.h"
#include "checkpoint.h"
#include "LayoutManager.h"

using namespace KFS;
int16_t minReplicasPerFile;

// The chunks of a file are stored next to each other in the tree and are
// written out contigously.  Use this property when restoring the chunkinfo:
// stash the fileattr for the the file we are currently working on; as long as
// this doesn't change, we avoid tree lookups.
MetaFattr *gCurrFa = NULL;

static bool
checkpoint_seq(deque <string> &c)
{
	c.pop_front();
	if (c.empty())
		return false;
	seq_t highest = toNumber(c.front());
	oplog.set_seqno(highest);
	return (highest >= 0);
}

static bool
checkpoint_fid(deque <string> &c)
{
	c.pop_front();
	if (c.empty())
		return false;
	fid_t highest = toNumber(c.front());
	fileID.setseed(highest);
	return (highest > 0);
}

static bool
checkpoint_chunkId(deque <string> &c)
{
	c.pop_front();
	if (c.empty())
		return false;
	fid_t highest = toNumber(c.front());
	chunkID.setseed(highest);
	return (highest > 0);
}

static bool
checkpoint_version(deque <string> &c)
{
	c.pop_front();
	if (c.empty())
		return false;
	int version = toNumber(c.front());
	return version == Checkpoint::VERSION;
}

static bool
checkpoint_time(deque <string> &c)
{
	c.pop_front();
	std::cout << "restoring from checkpoint of " << c.front() << '\n';
	return true;
}

static bool
checkpoint_log(deque <string> &c)
{
	c.pop_front();
	string s;
	while (!c.empty()) {
		s += c.front();
		c.pop_front();
		if (!c.empty())
			s += "/";
	}
	std::cout << "log file is " << s << '\n';
	replayer.openlog(s);
	return true;
}

bool
KFS::restore_chunkVersionInc(deque <string> &c)
{
	c.pop_front();
	if (c.empty())
		return false;
	chunkVersionInc = toNumber(c.front());
	return (chunkVersionInc >= 1);
}

static bool
restore_dentry(deque <string> &c)
{
	string name;
	fid_t id, parent;
	c.pop_front();
	bool ok = pop_name(name, "name", c, true);
	ok = pop_fid(id, "id", c, ok);
	ok = pop_fid(parent, "parent", c, ok);
	if (!ok)
		return false;

	MetaDentry *d = new MetaDentry(parent, name, id);
	return (metatree.insert(d) == 0);
}

static bool
restore_fattr(deque <string> &c)
{
	FileType type;
	fid_t fid;
	long long chunkcount;
	off_t filesize = -1;
	struct timeval mtime, ctime, crtime;
	int16_t numReplicas;

	bool ok = pop_type(type, "fattr", c, true);
	ok = pop_fid(fid, "id", c, ok);
	ok = pop_fid(chunkcount, "chunkcount", c, ok);
	ok = pop_short(numReplicas, "numReplicas", c, ok);
	ok = pop_time(mtime, "mtime", c, ok);
	ok = pop_time(ctime, "ctime", c, ok);
	ok = pop_time(crtime, "crtime", c, ok);
	if (!ok)
		return false;
	// filesize is optional; if it isn't there, we can re-compute
	// by asking the chunkservers
	bool gotfilesize = pop_offset(filesize, "filesize", c, true);

	if (numReplicas < minReplicasPerFile)
		numReplicas = minReplicasPerFile;

	// chunkcount is an estimate; recompute it as we add chunks to the file.
	// reason for it being estimate: if a CP is in progress while the
	// metatree is updated, we have cases where the chunkcount is off by 1
	// and the checkpoint contains the newly added chunk.
	MetaFattr *f = new MetaFattr(type, fid, mtime, ctime, crtime, 
					0, numReplicas);
	if (gotfilesize)
		f->filesize = filesize;

	if (type == KFS_DIR)
		UpdateNumDirs(1);
	else {
		UpdateNumFiles(1);
		UpdateNumChunks(chunkcount);
	}
	return (metatree.insert(f) == 0);
}

static bool
restore_chunkinfo(deque <string> &c)
{
	fid_t fid;
	chunkId_t cid;
	off_t offset;
	seq_t chunkVersion;

	c.pop_front();
	bool ok = pop_fid(fid, "fid", c, true);
	ok = pop_fid(cid, "chunkid", c, ok);
	ok = pop_offset(offset, "offset", c, ok);
	ok = pop_fid(chunkVersion, "chunkVersion", c, ok);
	if (!ok)
		return false;

	MetaChunkInfo *ch = new MetaChunkInfo(fid, offset, cid, chunkVersion);
	if (metatree.insert(ch) == 0) {
		MetaFattr *fa = gCurrFa;

		if ((fa == NULL) || (fa->id() != fid)) {
			fa = metatree.getFattr(fid);
			gCurrFa = fa;
		}

		assert(fa != NULL);
                const chunkOff_t boundary = chunkStartOffset(offset);
	        if (boundary >= fa->nextChunkOffset) {
		        fa->nextChunkOffset = boundary + CHUNKSIZE;
	        }
		fa->chunkcount++;
		gLayoutManager.AddChunkToServerMapping(cid, fid, offset, NULL);
		return true;
	}
	return false;
}

static bool
restore_makestable(deque <string> &c)
{
	chunkId_t chunkId;
	seq_t     chunkVersion;
	off_t     chunkSize;
        string    str;
	fid_t     tmp;
	uint32_t  checksum;
	bool      hasChecksum;

	c.pop_front();
	bool ok = pop_fid(chunkId, "chunkId", c, true);
	ok = pop_fid(chunkVersion, "chunkVersion", c, ok);
	ok = pop_name(str, "size", c, ok);
        chunkSize = toNumber(str);
	ok = pop_fid(tmp, "checksum", c, ok);
	checksum = (uint32_t)tmp;
	ok = pop_fid(tmp, "hasChecksum", c, ok);
	hasChecksum = tmp != 0;
	if (ok) {
		gLayoutManager.ReplayPendingMakeStable(
			chunkId, chunkVersion, chunkSize,
			hasChecksum, checksum, true);
	}
	return ok;
}

static void
init_map(DiskEntry &e)
{
	e.add_parser("checkpoint", checkpoint_seq);
	e.add_parser("version", checkpoint_version);
	e.add_parser("fid", checkpoint_fid);
	e.add_parser("chunkId", checkpoint_chunkId);
	e.add_parser("time", checkpoint_time);
	e.add_parser("log", checkpoint_log);
	e.add_parser("chunkVersionInc", restore_chunkVersionInc);
	e.add_parser("dentry", restore_dentry);
	e.add_parser("fattr", restore_fattr);
	e.add_parser("chunkinfo", restore_chunkinfo);
	e.add_parser("mkstable", restore_makestable);
}

/*!
 * \brief rebuild metadata tree from CP file cpname
 * \param[in] cpname	the CP file
 * \param[in] minReplicas  the desired # of replicas for each chunk of a file; 
 *   if the values in the checkpoint file are below this threshold, then
 *   bump replication.
 * \return		true if successful
 */
bool
Restorer::rebuild(const string cpname, int16_t minReplicas)
{
	const int MAXLINE = 400;
	char line[MAXLINE];
	int lineno = 0;

	DiskEntry entrymap;
	init_map(entrymap);

	file.open(cpname.c_str());
	bool is_ok = !file.fail();

	minReplicasPerFile = minReplicas;

	while (is_ok && !file.eof()) {
		++lineno;
		file.getline(line, MAXLINE);
		is_ok = entrymap.parse(line);
		if (!is_ok)
			std::cerr << "Error at line " << lineno << ": "
					<< line << '\n';
	}

	file.close();
	return is_ok;
}

void
KFS::acquire_lockfile(const string &lockfn, int ntries)
{
	struct flock fl;
	int fd;

	memset(&fl, 0, sizeof(struct flock));
	fl.l_type = F_WRLCK;
	fl.l_whence = SEEK_SET;

	fd = open(lockfn.c_str(), O_APPEND|O_CREAT|O_RDWR, 0644);
	if (fd < 0) {
		std::cerr << "Unable to open lock file: " << lockfn << " exiting...\n";
		exit(-1);
	}
	for (int i = 0; i < ntries; i++) {
		int ret = fcntl(fd, F_SETLK, &fl);
		if (ret == 0) {
			std::cerr << "Acquired lock file: " << lockfn << " yipeee...\n";
			return;
		}
		std::cerr << "Lock file: " << lockfn << " is busy; waiting...\n";
		sleep(60);
	}
	std::cerr << "Unable to open lock file: " << lockfn << " after " 
		<< ntries << " so, exiting...\n";
	exit(-1);
}
