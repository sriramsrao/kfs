/*!
 * $Id: util.h 1552 2011-01-06 22:21:54Z sriramr $
 *
 * \file util.h
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
#if !defined(KFS_UTIL_H)
#define KFS_UTIL_H

#include <string>
#include <deque>
#include <sstream>
#include "kfstypes.h"
#include "libkfsIO/IOBuffer.h"

using std::string;
using std::deque;
using std::ostream;

namespace KFS {

extern chunkOff_t chunkStartOffset(chunkOff_t offset);
extern int link_latest(const string real, const string alias);
extern const string toString(long long n);
extern long long toNumber(const string s);
extern string makename(const string dir, const string prefix, int number);
extern void split(deque <string> &component, const string path, char sep);
extern bool file_exists(string s);
extern void warn(const string s, bool use_perror);
extern void panic(const string s, bool use_perror);

extern void sendtime(ostream &os, const string &prefix, 
		     const struct timeval &t, const string &suffix);

extern string timeToStr(time_t val);

/// Is a message that ends with "\r\n\r\n" available in the
/// buffer.
/// @param[in] iobuf  An IO buffer stream with message
/// received from the chunk server.
/// @param[out] msgLen If a valid message is
/// available, length (in bytes) of the message.
/// @retval true if a message is available; false otherwise
///
extern bool IsMsgAvail(IOBuffer *iobuf, int *msgLen);

extern float ComputeTimeDiff(const struct timeval &start, const struct timeval &end);

}
#endif // !defined(KFS_UTIL_H)
