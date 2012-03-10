//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: util.cc $
//
// Created 2011/02/08
//
// Copyright 2011 Yahoo Corporation.  All rights reserved.
// This file is part of the Sailfish project.
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

#include "util.h"
using std::string;
using namespace KFS;

MsgLogger::LogLevel
KFS::str2LogLevel(string logLevel)
{
    if (logLevel == "DEBUG")
        return MsgLogger::kLogLevelDEBUG;
    if (logLevel == "INFO")
        return MsgLogger::kLogLevelINFO;
    if (logLevel == "WARN")
        return MsgLogger::kLogLevelWARN;
    if (logLevel == "FATAL")
        return MsgLogger::kLogLevelFATAL;

    return MsgLogger::kLogLevelINFO;
}

bool KFS::IsMessageAvail(IOBuffer *iobuf, int &msgLen)
{
    const int idx = iobuf->IndexOf(0, "\r\n\r\n");
    if (idx < 0) {
        return false;
    }
    msgLen = idx + 4; // including terminating seq. length.
    return true;
}

int KFS::MemAlignedMalloc(void **ptr, size_t alignment, size_t size)
{
    if (alignment < sizeof(void *)) {
        alignment = sizeof(void *);
    }

    // check for power of 2
    bool isPowerOf2 = ((alignment != 0) && !(alignment & (alignment - 1)));

    assert(isPowerOf2);

    free(*ptr);
    *ptr = NULL;
    return posix_memalign(ptr, alignment, size);
}
