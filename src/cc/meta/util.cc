/*!
 * $Id: util.cc 1552 2011-01-06 22:21:54Z sriramr $
 *
 * \file util.cc
 * \brief miscellaneous metadata server code
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

extern "C" {
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
}

#include <iostream>
#include <sstream>
#include <cstdio>
#include <cstdlib>
#include <cerrno>
#include "util.h"

using namespace KFS;

/*!
 * \brief Find the chunk that contains the specified file offset.
 * \param[in] offset	offset in the file for which we need the chunk
 * \return		offset in the file that corresponds to the
 * start of the chunk.
 */
chunkOff_t 
KFS::chunkStartOffset(chunkOff_t offset)
{
	return (offset / CHUNKSIZE) * CHUNKSIZE;
}

/*!
 * \brief link to the latest version of a file
 *
 * \param[in] realname	name of the target file
 * \param[in] alias	name to link to target
 * \return		error code from link command
 *
 * Make a hard link with the name "alias" to the
 * given file "realname"; we use this to make it
 * easy to find the latest log and checkpoint files.
 */
int
KFS::link_latest(const string realname, const string alias)
{
	int status = 0;
	(void) unlink(alias.c_str());	// remove any previous link
	if (link(realname.c_str(), alias.c_str()) != 0)
		status = -errno;
	return status;
}

/*!
 * \brief convert a number to a string
 * \param[in] n	the number
 * \return	the string
 */
const string
KFS::toString(long long n)
{
	std::ostringstream s(std::ostringstream::out);

	s << n;
	return s.str();
}

/*!
 * \brief convert a string to a number
 * \param[in] s	the string
 * \return	the number
 */
long long
KFS::toNumber(string s)
{
	char *endptr;
	long long n = strtoll(s.c_str(), &endptr, 10);
	if (*endptr != '\0')
		n = -1;
	return n;
}

/*!
 * \brief paste together a pathname from its constituent parts
 * \param[in] dir	directory path
 * \param[in] prefix	beginning part of file name
 * \param[in] number	numeric suffix
 * \return		string "<dir>/<prefix>.<number>"
 */
string
KFS::makename(const string dir, const string prefix, int number)
{
	return dir + "/" + prefix + "." + toString(number);
}

/*!
 * \brief split a path name into its component parts
 * \param[out]	component	the list of components
 * \param[in]	path		the path name
 * \param[in]	sep		the component separator (e.g., '/')
 */
void
KFS::split(deque <string> &component, const string path, char sep)
{
	string::size_type start = 0;
	string::size_type slash = 0;

	while (slash != string::npos) {
		assert(slash == 0 || path[slash] == sep);
		slash = path.find(sep, start);
		string nextc = path.substr(start, slash - start);
		start = slash + 1;
		component.push_back(nextc);
	}
}

/*!
 * \brief check whether a file exists
 * \param[in]	name	path name of the file
 * \return		true if stat says it is a plain file
 */
bool
KFS::file_exists(string name)
{
	struct stat s;
	if (stat(name.c_str(), &s) == -1)
		return false;

	return S_ISREG(s.st_mode);
}

///
/// Return true if there is a sequence of "\r\n\r\n".
/// @param[in] iobuf: Buffer with data 
/// @param[out] msgLen: string length of the command in the buffer
/// @retval true if a command is present; false otherwise.
///
bool
KFS::IsMsgAvail(IOBuffer *iobuf,
                int *msgLen)
{
    const int idx = iobuf->IndexOf(0, "\r\n\r\n");
    if (idx < 0) {
        return false;
    }
    *msgLen = idx + 4; // including terminating seq. length.
    return true;
}

/*!
 * A helper function to print out a timeval into a string buffer with
 * a prefix/suffix string around the time values.
 */
void
KFS::sendtime(ostream &os, const string &prefix, 
	      const struct timeval &t, 
	      const string &suffix)
{
	os << prefix << " " << t.tv_sec << " " << t.tv_usec << suffix;
}

/*!
 * A helper function that converts a time_t to string.
 */
string
KFS::timeToStr(time_t val)
{
	char timebuf[64];

	ctime_r(&val, timebuf);
	if (timebuf[24] == '\n')
		timebuf[24] = '\0';
	return timebuf;
}

/*!
 * \brief print warning message on syscall failure
 * \param[in] msg	message text
 * \param[in] use_perror pass text to perror() if true
 */
void
KFS::warn(const string msg, bool use_perror)
{
	if (use_perror)
		perror(msg.c_str());
	else
		std::cerr << msg << '\n';
}

/*!
 * \brief bomb out on "impossible" error
 * \param[in] msg	panic text
 * \param[in] use_perror pass text to perror() if true
 */
void
KFS::panic(const string msg, bool use_perror)
{
	warn(msg, use_perror);
	abort();
}

float KFS::ComputeTimeDiff(const struct timeval &startTime, 
			const struct timeval &endTime)
{
	float timeSpent;
	
	timeSpent = (endTime.tv_sec * 1e6 + endTime.tv_usec) - 
		(startTime.tv_sec * 1e6 + startTime.tv_usec);
	return timeSpent / 1e6;
}
