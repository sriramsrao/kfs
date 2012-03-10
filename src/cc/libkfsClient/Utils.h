//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: Utils.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2006/08/31
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
// \brief Utils.h: Utilities for manipulating paths and other misc. support.
// 
//----------------------------------------------------------------------------

#ifndef LIBKFSCLIENT_UTILS_H
#define LIBKFSCLIENT_UTILS_H

#include <string>
extern "C" {
#include <sys/time.h>
}

namespace KFS {

// we call this function by creating temporaries on the stack. to let
// that thru, dont' stick in "&"
extern std::string strip_dots(std::string path);
extern std::string build_path(std::string &cwd, const char *input);

extern void GetTimeval(std::string &s, struct timeval &tv);
// Introduce a delay for nsecs...i.e., sleep
extern void Sleep(int nsecs);

}

#endif // LIBKFSCLIENT_UTILS_H
