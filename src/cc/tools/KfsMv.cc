//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: KfsMv.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2007/09/24
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
// \brief Tool that renames a file/directory from a KFS path to another
// KFS path.  This does the analogous of "mv".
//
//----------------------------------------------------------------------------

#include <iostream>    
#include <fstream>
#include <cerrno>

extern "C" {
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
}

#include "libkfsClient/KfsClient.h"
#include "tools/KfsShell.h"

using std::cout;
using std::endl;
using std::ifstream;

using namespace KFS;
using namespace KFS::tools;


int
KFS::tools::handleMv(const vector<string> &args)
{
    int res;

    if ((args.size() < 2) || (args[0] == "--help") || (args[0] == "") || (args[1] == "")) {
        cout << "Usage: mv <source path> <dst path>" << endl;
        return 0;
    }

    KfsClientPtr kfsClient = getKfsClientFactory()->GetClient();

    if (!kfsClient->Exists(args[0].c_str())) {
	cout << "Source path: " << args[0] << " is non-existent!" << endl;
        return -ENOENT;
    }

    string target = args[1].c_str();

    if (kfsClient->IsFile(args[0].c_str()) &&
        kfsClient->IsDirectory(args[1].c_str())) {
        string parent, filename;

        GetPathComponents(args[0], parent, filename);
        target += "/";
        target += filename;
    }
    
    if ((res = kfsClient->Rename(args[0].c_str(), target.c_str())) < 0) {
        cout << "Rename failed: " << ErrorCodeToStr(res) << endl;
        return res;
    }
    return 0;
}
