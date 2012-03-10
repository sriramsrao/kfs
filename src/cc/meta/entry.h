/*!
 * $Id: entry.h 1552 2011-01-06 22:21:54Z sriramr $
 *
 * \file entry.h
 * \brief process entries from the checkpoint and log files
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
#if !defined(KFS_ENTRY_H)
#define KFS_ENTRY_H

#include <string>
#include <deque>
#include <map>

#include "kfstypes.h"

using std::string;
using std::deque;
using std::map;

namespace KFS {

/*!
 * \brief a checkpoint or log entry read back from disk
 *
 * This class represents lines that have been read back from either
 * the checkpoint or log file during KFS startup.  Each entry in
 * these files is in the form
 *
 * 	<keyword>/<data1>/<data2>/...
 *
 * where <keyword> represents a type of metatree node in the case
 * of checkpoint, or an update request, in a log file.  The basic
 * processing is to split the line into ts component parts, then
 * use the keyword to look up a function that validates the remaining
 * data and performs whatever action is appropriate.  In the case
 * of checkpoints, this will be to insert the specified node into
 * the tree, while for log entries, we redo the update (taking care
 * to specify any new file ID's so that they remain the same as
 * they were before the restart).
 */
class DiskEntry {
public:
	typedef bool (*parser)(deque <string> &c); //!< a parsing function
	static const char SEPARATOR = '/';
private:
	typedef map <string, parser> parsetab;	//!< map type to parser
	parsetab table;
public:
	void add_parser(string k, parser f) { table[k] = f; }
	bool parse(char *line);		//!< look up parser and call it
};

/*!
 * \brief parser helper routines
 * These functions remove items of the specified kind from the deque
 * of components.  The item will be preceded by an identifying keyword,
 * which is passed in as "tag".
 */
extern bool pop_name(
	string &name, const string tag, deque <string> &c, bool ok);
extern bool pop_path(
	string &path, const string tag, deque <string> &c, bool ok);
extern bool pop_fid(fid_t &fid, const string tag, deque <string> &c, bool ok);
extern bool pop_size(size_t &sz, const string tag, deque <string> &c, bool ok);
extern bool pop_offset(off_t &o, const string tag, deque <string> &c, bool ok);
extern bool pop_short(int16_t &n, const string tag, deque <string> &c, bool ok);
extern bool pop_type(
	FileType &t, const string tag, deque <string> &c, bool ok);
extern bool pop_time(
	struct timeval &tv, const string tag, deque <string> &c, bool ok);

}
#endif // !defined(KFS_ENTRY_H)
