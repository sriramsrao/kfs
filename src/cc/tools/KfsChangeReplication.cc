//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: KfsChangeReplication.cc 1552 2011-01-06 22:21:54Z sriramr $
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
// \brief Tool that changes the degree of replication for a file.
// This tool asks the metaserver to change the degree of replication;
// the change, presuming resources exist, will be done by the
// metaserver asynchronously.
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
KFS::tools::handleChangeReplication(const vector<string> &args)
{
    if ((args.size() < 2) || (args[0] == "--help")) {
        cout << "Usage: changeReplication <filename> <replication factor> " << endl;
        return 0;
    }

    KfsClientPtr kfsClient = getKfsClientFactory()->GetClient();

    int16_t numReplicas = 1;
    int res;

    try {
        numReplicas = (int16_t) boost::lexical_cast<int> (args[1]);
    } catch (...) {
        cout << "Print : " << args[1] << " is not a valid replication factor" << endl;
        return -EINVAL;
    }

    if ((res = kfsClient->SetReplicationFactor(args[0].c_str(), numReplicas)) < 0) {
	cout << "Set replication failed: " << ErrorCodeToStr(res) << endl;
        return res;
    } 
    return 0;
}

