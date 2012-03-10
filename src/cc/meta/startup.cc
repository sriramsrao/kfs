/*!
 * $Id: startup.cc 1552 2011-01-06 22:21:54Z sriramr $
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
 *
 *
 * \file startup.cc
 * \brief code for starting up the metadata server
 *
 */
#include "startup.h"
#include "logger.h"
#include "checkpoint.h"
#include "kfstree.h"
#include "request.h"
#include "restore.h"
#include "replay.h"
#include "util.h"
#include "LayoutManager.h"

#include <cassert>

extern "C" {
#include <sys/resource.h>
#include <signal.h>
}

using namespace KFS;

/*!
 * \brief rebuild the metatree at startup
 *
 * If the latest CP file exists, use it to restore the contents
 * of the metatree, otherwise create a fresh tree with just "/"
 * and its associated "." and ".." links.
 *
 * Eventually, we should include an option here to restore from
 * a specified CP file instead of just the latest.
 *
 * After restoring from the checkpoint file, apply any log
 * records that are from after the CP.
 */
static int
setup_initial_tree(uint32_t minNumReplicasPerFile)
{
	string logfile;
	int status;
	if (file_exists(LASTCP)) {
		Restorer r;
		status = r.rebuild(LASTCP, minNumReplicasPerFile) ? 0 : -EIO;
		gLayoutManager.InitRecoveryStartTime();
	} else {
		status = metatree.new_tree();
	}
	return status;
}

/*!
 * \brief call init functions and start threads
 *
 * Before starting any threads, block SIGALRM so that it is caught
 * only by the checkpoint timer thread; since the start_CP code
 * acquires locks, we have to be careful not to call it asynchronously
 * in other thread contexts.  Afterwards, initialize metadata request
 * handlers and start the various helper threads going.
 *
 * XXX Eventually, we will want more options here, for instance,
 * specifying a checkpoint file instead of just using "latest".
 */
void
KFS::kfs_startup(const string &logdir, const string &cpdir, 
		uint32_t minChunkServers, uint32_t numReplicasPerFile,
		bool enablePathToFidCache)
{
	struct rlimit rlim;
	int status = getrlimit(RLIMIT_NOFILE, &rlim);
	if (status == 0) {
		// bump up the # of open fds to as much as possible
		rlim.rlim_cur = rlim.rlim_max;
		setrlimit(RLIMIT_NOFILE, &rlim);
	}
	std::cout << "Setting the # of open files to: " << rlim.rlim_cur << std::endl;

	sigset_t sset;
	sigemptyset(&sset);
	sigaddset(&sset, SIGALRM);
	status = sigprocmask(SIG_BLOCK, &sset, NULL);
	if (status != 0)
		panic("kfs_startup: sigprocmask", true);
	metatree.disableFidToPathname();
	RegisterCounters();
	// get the paths setup before we get going
	logger_setup_paths(logdir);
	checkpointer_setup_paths(cpdir);

	if (enablePathToFidCache)
		metatree.enablePathToFidCache();

	status = setup_initial_tree(numReplicasPerFile);
	if (status != 0)
		panic("setup_initial_tree failed", false);
	status = replayer.playAllLogs();
	if (status != 0)
		panic("log replay failed", false);
	metatree.enableFidToPathname();
	// get the sizes of all dirs up-to-date
	metatree.recomputeDirSize();
	ChangeIncarnationNumber(NULL);
	gLayoutManager.SetMinChunkserversToExitRecovery(minChunkServers);
	// empty the dumpster dir on startup; if it doesn't exist, create it
	// whatever is in the dumpster needs to be nuked anyway; if we
	// remove all the file entries from that dir, the space for the
	// chunks of the file will get reclaimed: chunkservers will tell us
	// about chunks we don't know and those will nuked due to staleness
	emptyDumpsterDir();
	initialize_request_handlers();
	logger_init();
	checkpointer_init();
}
