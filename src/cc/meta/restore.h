/*!
 * $Id: restore.h 1552 2011-01-06 22:21:54Z sriramr $
 *
 * \file restore.h
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
#if !defined(KFS_RESTORE_H)
#define KFS_RESTORE_H

#include <fstream>
#include <string>
#include <deque>
#include <map>
#include "util.h"

using std::ifstream;
using std::string;
using std::deque;
using std::map;

namespace KFS {

/*!
 * \brief state for restoring from a checkpoint file
 */
class Restorer {
	ifstream file;			//!< the CP file
public:
	/* 
	 * process the CP file.  also, if the # of replicas of a file is below
	 * the specified value, bump up replication.  this allows us to change
	 * the filesystem wide degree of replication in a simple manner.
	 */
	bool rebuild(string cpname, int16_t minNumReplicasPerFile = 1);	
};

extern bool restore_chunkVersionInc(deque <string> &c);

/* 
 * Whenever a checkpoint file is loaded in, it takes up a ton of memory.
 * To prevent logcompactor from stomping over a filechecker or filelister or
 * vice-versa, all of these tools acquire a lock file before loading the
 * checkpoint.
 */
extern void acquire_lockfile(const string &lockfn, int ntries);

}
#endif // !defined(KFS_RESTORE_H)
