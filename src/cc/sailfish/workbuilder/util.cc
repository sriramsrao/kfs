//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: util.cc 3637 2012-01-25 19:28:06Z sriramr $
//
// Created 2011/01/04
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
#include <cassert>
using namespace workbuilder;

using std::string;
using std::vector;

void
workbuilder::split(const string &str, vector<string> &parts, char sep)
{
    string::size_type start = 0;
    string::size_type slash = 0;

    while (slash != string::npos) {
        assert(slash == 0 || str[slash] == sep);
        slash = str.find(sep, start);
        string nextc = str.substr(start, slash - start);
        start = slash + 1;
        parts.push_back(nextc);
    }
}

void
workbuilder::splitPair(const string &str, vector<string> &pair, char sep)
{
    string::size_type slash = str.rfind(sep);

    if (slash == string::npos)
        return;
    string s = str.substr(0, slash);
    pair.push_back(s);
    s = str.substr(slash+1);
    pair.push_back(s);
}

const char *
workbuilder::now(void)
{
    static char buf[128];
    static time_t tt = 0;
    time_t t = time(0);
    if (t != tt) {
        tt = t;
        struct tm *tm = localtime(&t);
        strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", tm);
        buf[sizeof(buf)-1]=0;
    }
    return buf;
}

int 
workbuilder::MemAlignedMalloc(void **ptr, size_t alignment, size_t size)
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
