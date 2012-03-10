//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: KfsFstat.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2008/12/15
//
// Copyright 2008 Quantcast Corp.
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
// \brief Tool for fstat'ing a file.
// 
//----------------------------------------------------------------------------

#include <iostream>    
#include <fstream>
#include <cerrno>

extern "C" {
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
}

#include "libkfsClient/KfsClient.h"
#include "tools/KfsShell.h"

using std::cout;
using std::endl;
using std::ofstream;
using std::vector;

using namespace KFS;

int
KFS::tools::handleFstat(const vector<string> &args)
{
    if ((args.size() >= 1) && (args[0] == "--help")) {
        cout << "Usage: stat {<path>} " << endl;
        return 0;
    }

    KfsClientPtr kfsClient = getKfsClientFactory()->GetClient();

    struct stat statInfo;
    kfsClient->Stat(args[0].c_str(), statInfo);

    cout << "File: " << args[0] << endl;
    cout << "ctime: " << statInfo.st_ctime << endl;
    cout << "mtime: " << statInfo.st_mtime << endl;
    cout << "Size: " << statInfo.st_size << endl;

    return 0;
}
