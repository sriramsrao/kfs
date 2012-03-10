//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: Utils.cc 1552 2011-01-06 22:21:54Z sriramr $
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
// \brief Utilities for manipulating paths and other misc support.
//
//----------------------------------------------------------------------------

#include "Utils.h"

#include <cassert>
#include <cerrno>
#include <vector>
#include <sstream>
using std::vector;
using std::string;
using std::istringstream;

extern "C" {
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
}

using namespace KFS;

string
KFS::strip_dots(string path)
{
	vector <string> component;
	string result;
	string::size_type start = 0;

	while (start != string::npos) {
		assert(path[start] == '/');
		string::size_type slash = path.find('/', start + 1);
		string nextc(path, start, slash - start);
		start = slash;
		if (nextc.compare("/..") == 0) {
			if (!component.empty())
				component.pop_back();
		} else if (nextc.compare("/.") != 0)
			component.push_back(nextc);
	}

	if (component.empty())
		component.push_back(string("/"));

	for (vector <string>::iterator c = component.begin();
			c != component.end(); c++) {
		result += *c;
	}
	return result;
}

/*
 * Take a path name that was supplied as an argument for a KFS operation.
 * If it is not absolute, add the current directory to the front of it and
 * in either case, call strip_dots to strip out any "." and ".." components.
 */
string
KFS::build_path(string &cwd, const char *input)
{
	string tail(input);
	if (input[0] == '/')
		return strip_dots(tail);

	const char *c = cwd.c_str();
	bool is_root = (c[0] == '/' && c[1] == '\0');
	string head(c);
	if (!is_root)
		head.append("/");
	return strip_dots(head + tail);
}

void
KFS::Sleep(int nsecs)
{
        int res;
        struct timeval start;

        gettimeofday(&start, NULL);
        while (1) {
            struct timeval timeout, now;

            gettimeofday(&now, NULL);
            if (now.tv_sec - start.tv_sec >= nsecs)
                break;
            
            timeout.tv_sec = nsecs - (now.tv_sec - start.tv_sec);
            timeout.tv_usec = 0;

            if (timeout.tv_sec < 0)
                break;

            res = select(0, NULL, NULL, NULL, &timeout);
            if (res == 0)
                break;
        }
}

void
KFS::GetTimeval(string &s, struct timeval &tv)
{
    if (s != "") {
	istringstream ist(s);

	ist >> tv.tv_sec;
	ist >> tv.tv_usec;
    } else {
	tv.tv_sec = tv.tv_usec = 0;
    }
}
