/*
 * $Id: checkpoint.h 1552 2011-01-06 22:21:54Z sriramr $
 *
 * \file checkpoint.h
 * \brief KFS checkpointer
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
 */
#if !defined(KFS_CHECKPOINT_H)
#define KFS_CHECKPOINT_H

#include <fstream>
#include <sstream>
#include <string>

#include "thread.h"
#include "kfstypes.h"
#include "queue.h"
#include "meta.h"
#include "kfstree.h"
#include "util.h"

using std::string;
using std::ofstream;

namespace KFS {

/*!
 * \brief keeps track of checkpoint status
 *
 * This class records the current state of the metadata server
 * with respect to checkpoints---the name of the checkpoint file,
 * whether the CP is running, whether the server has any recent
 * updates that would make a new CP worthwhile, etc.
 *
 * Writing out a checkpoint involves walking the leaves of the
 * metatree and saving them to disk.  The on-disk checkpoint file stores 
 * the name of the log that contains all the operations after the checkpoint
 * was taken.  For failure recovery, there is a notion of a "LATEST" checkpoint
 * file (created via a hardlink) that identifies the checkpoint that should be
 * used for restore purposes.
 *
 */
class Checkpoint {
	string cpdir;		//!< dir for CP files
	string cpname;		//!< name of CP file
	ofstream file;		//!< current CP file
	int mutations;		//!< changes since last CP
	int cpcount;		//!< number of CP's since startup
	Node *activeNode;	//!< level-1 node currently being written
	void save_active(Node *n); //!< save new activeNode
	string cpfile(seq_t highest)	//!< generate the next file name
	{
		return makename(cpdir, "chkpt", highest);
	}
	int write_leaves();
public:
	static const int VERSION = 1;
	Checkpoint(string d): cpdir(d), cpcount(0) { } 
	~Checkpoint() { }
	void setCPDir(const string &d) 
	{
		cpdir = d;
	}
	const string name() const { return cpname; }
	//!< return true if a CP will be taken
	bool isCPNeeded() { return mutations != 0; }
	void initial_CP();	//!< schedule a checkpoint on startup if needed
	int do_CP();		//!< do the actual work
	void note_mutation() { ++mutations; }
	void resetMutationCount() { mutations = 0; }
};

extern string CPDIR;		//!< directory for CP files
extern string LASTCP;		//!< most recent CP file (link)

extern Checkpoint cp;
extern void checkpointer_setup_paths(const string &cpdir);
extern void checkpointer_init();

}

#endif // !defined(KFS_CHECKPOINT_H)
