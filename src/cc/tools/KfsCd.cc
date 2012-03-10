//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: KfsCd.cc 1552 2011-01-06 22:21:54Z sriramr $
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
// \brief Tool that changes the cwd.
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

#include <boost/lexical_cast.hpp>

using std::cout;
using std::endl;
using std::vector;
using std::string;

using namespace KFS;

int
KFS::tools::handleCd(const vector<string> &args)
{
    if ((args.size() < 1) || (args[0] == "--help")) {
        cout << "Usage: cd <path> " << endl;
        return 0;
    }

    KfsClientPtr kfsClient = getKfsClientFactory()->GetClient();

    int res;

    if ((res = kfsClient->Cd(args[0].c_str())) < 0) {
	cout << "cd failed: " << ErrorCodeToStr(res) << endl;
        return res;
    }
    return 0;
}

