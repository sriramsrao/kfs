//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: utils.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2007/09/20
//
// Copyright 2008 Quantcast Corp.
// Copyright 2007-2008 Kosmix Corp.
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
// \brief Common utility functions for KfsShell
// 
//----------------------------------------------------------------------------

#include <iostream>
#include <cerrno>

#include "libkfsClient/KfsClient.h"
#include "tools/KfsShell.h"

using std::cout;
using std::endl;
using namespace KFS;

// Make the directory hierarchy in KFS defined by path.

int
KFS::tools::doMkdirs(const char *path)
{
    int res;
    KfsClientPtr kfsClient = getKfsClientFactory()->GetClient();

    res = kfsClient->Mkdirs((char *) path);
    if ((res < 0) && (res != -EEXIST)) {
        cout << "Mkdir failed: " << ErrorCodeToStr(res) << endl;
        return res;
    }
    return res;
}

// remove a single directory in kfs

int
KFS::tools::doRmdir(const char *dirname)
{
    int res;
    KfsClientPtr kfsClient = getKfsClientFactory()->GetClient();

    res = kfsClient->Rmdir(dirname);
    if (res < 0) {
        cout << "unable to rmdir: " << dirname <<  ':' << ErrorCodeToStr(res) << endl;
        return res;
    }
    return 0;
}

void
KFS::tools::GetPathComponents(const string &path, string &parent, string &name)
{
    string::size_type slash = path.rfind('/');

    // get everything after the last slash
    if (slash != string::npos) {
        parent.assign(path, 0, slash);
	name.assign(path, slash+1, string::npos);
    } else {
        name = path;
        parent = "/";
    }
}
