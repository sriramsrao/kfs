//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kfsls_main.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2009/04/18
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
//----------------------------------------------------------------------------

#include <iostream>    
#include <fstream>
#include <cerrno>

extern "C" {
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
#include <sys/stat.h>
}

#include "libkfsClient/KfsClient.h"
#include "common/log.h"
#include "KfsToolsCommon.h"

using std::cout;
using std::endl;
using std::ofstream;

using namespace KFS;
using namespace KFS::tools;

void
doLs(const string & progName, const string & arg, bool longMode, bool humanReadable, bool timeInSecs)
{
    string serverHost = "";
    string filePath = "";
    int serverPort = -1;
    bool isRemote = false;
    
    if (arg.length() < 1) return;
    
    if(!parsePath(arg, serverHost, serverPort, filePath, isRemote))
    {
	cout << progName << ": Error: Parsing '" << arg << "' failed.\n";
	exit(-1);
    }
    
    assert (filePath.length() > 0);
    
    if (!isRemote)
    {
	// Path provided can be classified as local one if it doesn't
	// contain protocol information (like kfs:// or file://), or
	// if it contains 'file://'. In the first case we want to try to use this
	// path anyway. Only if user specified 'file://' explicitely we fail.
	
	if (arg.length() >= 7 && arg.substr(0,7) == "file://")
	{
	    cout << progName << ": Error: Could not list '" << arg << "' - Only remote locations can be listed.\n";
	    exit(-1);
	}
	
	// If the path doesn't contain 'file://' we try to treat it as remote path
	// on default server - let's try to get that default server!
	 getEnvServer(serverHost, serverPort);
    }
    
    if (serverHost.length() < 1 || serverPort < 0)
    {
	cout << progName << ": Error listing '" << arg << "' - no server address could be found.\n";
	exit(-1);
    }
    
    KfsClientPtr kfsClient = getKfsClientFactory()->GetClient(serverHost, serverPort);
    
    if (!kfsClient)
    {
	cout << progName << ": Error initializing KFS client for " << getRemotePath(serverHost, serverPort, filePath) << "\n";
	exit(-1);
    }

    getKfsClientFactory()->SetDefaultClient(kfsClient);
    
    cout << "Listing " << getRemotePath(serverHost, serverPort, filePath) << ":\n";
    
    DirList(kfsClient, filePath, longMode, humanReadable, timeInSecs);
}

int
main(int argc, char **argv)
{
    bool longMode = false, humanReadable = false, timeInSecs = false;
    bool listedSomething = false;
    
    KFS::MsgLogger::Init(NULL);
    
    for (int i = 1; i < argc; ++i)
    {
	string arg = argv[i];
	
	if (arg.length() < 1) continue;
	
	if (arg.length() > 1 && arg[0] == '-')
	{
	    for (size_t j = 1; j < arg.length(); ++j)
	    {
		switch(arg[j])
		{
		    case 'l':
			    longMode = true;
			break;
		    case 'h':
			    humanReadable = true;
			break;
		    case 't':
			    timeInSecs = true;
			break;
		    default:
			cout << "Unrecognized option: '" << arg[j] << "'\n";
			cout << "Usage: " << argv[0] << " [-lht] [kfs_path_1] [kfs_path_2] ...\n";
			return EXIT_FAILURE;
			break;
		}
	    }
	}
	else
	{
	    doLs(argv[0], arg, longMode, humanReadable, timeInSecs);
	    listedSomething = true;
	}
    }
 
    if (!listedSomething) 
    {
	doLs(argv[0], "/", longMode, humanReadable, timeInSecs);
    }
    
    return 0;
}
