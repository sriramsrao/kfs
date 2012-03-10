//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: Utils.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2006/09/27
//
// Copyright 2008 Quantcast Corp.
// Copyright 2006-2008 Kosmix Corp.
//
// This file is part of Kosmos File System (KFS).
//
// Licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// 
//----------------------------------------------------------------------------

#ifndef CHUNKSERVER_UTILS_H
#define CHUNKSERVER_UTILS_H

#include "libkfsIO/IOBuffer.h"
#include <string>
#include <vector>

namespace KFS
{

///
/// Given some data in a buffer, determine if we have a received a
/// valid op---one that ends with "\r\n\r\n".  
/// @param[in]  iobuf : buffer containing data
/// @param[out] msgLen : if we do have a valid command, return the length of
/// the command
/// @retval True if we have a valid command; False otherwise.
///
extern bool IsMsgAvail(IOBuffer *iobuf, int *msgLen);

///
/// \brief bomb out on "impossible" error
/// \param[in] msg       panic text
///
extern void die(const std::string &msg);

///
/// Split a path into components as defined by a separator.  For instance,
/// "a.b.c" when split on "." will result into a vector of the form ["a", "b", "c"]
///
extern void split(std::vector<std::string> &component, const std::string &path, char separator);

///
/// \brief compute the time difference in seconds between start and
/// end times
/// \param[in] startTime, endTime  require: endTime >= startTime
/// \retval  The time difference in seconds.
///
extern float ComputeTimeDiff(const struct timeval &startTime, const struct timeval &endTime);
}

#endif // CHUNKSERVER_UTILS_H
