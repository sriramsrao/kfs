//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kfscp_main.cc 1552 2011-01-06 22:21:54Z sriramr $
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

int
main(int argc, char **argv)
{
    vector<string> sources;
    string dest;
    bool verboseLogging = false;
    
    KFS::MsgLogger::Init(NULL);
    
    for (int i = 1; i < argc; ++i)
    {
	string arg = argv[i];
	
	if (arg == "-v")
	{
	    verboseLogging = true;
	}
	else
	{
	    sources.push_back(argv[i]);
	}
    }
    
    if (sources.size() < 2)
    {
	cout << "Usage: " << argv[0] << " [-v] source [source2] [source3] ... destination\n";
	return EXIT_FAILURE;
    }
    
    if (verboseLogging) 
    {
        KFS::MsgLogger::SetLevel(MsgLogger::kLogLevelDEBUG);
    }
    else
    {
        KFS::MsgLogger::SetLevel(MsgLogger::kLogLevelWARN);
    } 
    
    dest = sources.back();
    sources.pop_back();
    
    string destHost = "";
    int destPort;
    string destPath = "";
    bool destIsRemote = false;
    DIR *destDir = 0;
    
    KfsClientPtr destClient;
    
    if (!parsePath(dest, destHost, destPort, destPath, destIsRemote))
    {
	cout << argv[0] << ": Error parsing destination path '" << dest << "'\n";
	exit(-1);
    }
    
    assert (destPath.length() > 0);
    
    if (destIsRemote)
    {
	destClient = getKfsClientFactory()->GetClient(destHost, destPort);
	
	if (!destClient)
	{
	    cout << argv[0] << ": Error initializing KFS client for " << getRemotePath(destHost, destPort, destPath) << "\n";
	    exit(-1);
	}
	
	assert (destClient != 0);
	getKfsClientFactory()->SetDefaultClient(destClient);
    }
    
    // We have multiple sources, which means that 'dest' has to exist and be a directory!
    if (sources.size() > 1)
    {
	if (destIsRemote)
	{
	    assert (destClient != 0);
	    
	    if (!destClient->Exists(destPath.c_str()))
	    {
		cout << argv[0] << ": Remote path " << getRemotePath(destHost, destPort, destPath)
		    << " could not be found (when multiple source locations are used, destination must be existing directory).\n";
		exit(-1);
	    }

	    if (!destClient->IsDirectory(destPath.c_str()))
	    {
		cout << argv[0] << ": Remote path " << getRemotePath(destHost, destPort, destPath) 
		    << " is not a directory (when multiple source locations are used, destination must be existing directory).\n";
		exit(-1);
	    }
	}
	else
	{
	    struct stat statInfo;
	    
	    statInfo.st_mode = S_IFREG;
	     
	    if (stat(destPath.c_str(), &statInfo) < 0)
	    {
		cout << argv[0] << ": Local path '" << destPath << "' could not be found "
		    << "(when multiple source locations are used, destination must be existing directory).\n";
		exit(-1);
	    }

	    if (!S_ISDIR(statInfo.st_mode))
	    {
		cout << argv[0] << ": Local path '" << destPath << "' is not a directory "
		    << "(when multiple source locations are used, destination must be existing directory).\n";
		exit(-1);
	    }
	    
	    destDir = opendir(destPath.c_str());

	    if (!destDir)
	    {
		cout << argv[0] << ": Error opening '" << destPath << "' directory.\n";
		perror("opendir: ");
		exit(-1);
	    }
	}
    }
    
    for (size_t i = 0; i < sources.size(); ++i)
    {
	string srcHost = "";
	int srcPort = -1;
	string srcPath = "";
	bool srcIsRemote = false;
	DIR *srcDir = 0;
	
	if (!parsePath(sources[i], srcHost, srcPort, srcPath, srcIsRemote))
	{
	    cout << argv[0] << ": Error parsing source path '" << sources[i] << "'\n";
	    exit(-1);
	}
	
	assert (srcPath.length() > 0);
	
	if (srcIsRemote && destIsRemote)
	{
	    assert (srcPath.length() > 0);
	    assert (destPath.length() > 0);
	    assert (srcPath[0] == '/');
	    assert (destPath[0] == '/');
	    
	    if (srcHost != destHost || srcPort != destPort)
	    {
		cout << argv[0] << ": Error copying " << getRemotePath(srcHost, srcPort, srcPath)
		    << " to " << getRemotePath(destHost, destPort, destPath)
		    << " - copying between different remote KFS servers is not supported (yet?).\n";
		exit(-1);
	    }
	    
	    assert (destClient != 0);
	    
	    if (!destClient->Exists(srcPath.c_str()))
	    {
		cout << argv[0] << ": Remote path " << getRemotePath(srcHost, srcPort, srcPath)
		    << " does not exist.\n";
		exit(-1);
	    }
	    
	    if (destClient->IsFile(srcPath.c_str()))
	    {
		cout << argv[0] << ": Copying file " << getRemotePath(destHost, destPort, srcPath)
		    << " to " << getRemotePath(destHost, destPort, destPath) << ": ";
		cout.flush();
		
		if (!CopyFile(destClient, srcPath, destPath))
		{
		    cout << "OK\n";
		}
		else
		{
		    cout << "FAILED\n";
		    exit(-1);
		}
	    }
	    else
	    {
		cout << argv[0] << ": Copying directory " << getRemotePath(destHost, destPort, srcPath)
		    << " to " << getRemotePath(destHost, destPort, destPath) << ": ";
		cout.flush();
		
		if (!CopyDir(destClient, srcPath, destPath))
		{
		    cout << "OK\n";
		}
		else
		{
		    cout << "FAILED\n";
		    exit(-1);
		}
	    }
	}
	else if (!srcIsRemote && destIsRemote)
	{
	    assert (destClient != 0);
	    assert (srcPath.length() > 0);
	    assert (destPath.length() > 0);
	    assert (destPath[0] == '/');
	    
	    struct stat statInfo;
	
	    statInfo.st_mode = S_IFREG;
	    
	    if (stat(srcPath.c_str(), &statInfo) < 0)
	    {
		cout << argv[0] << ": Local path '" << srcPath << "' does not exist.\n";
		exit(-1);
	    }

	    if (!S_ISDIR(statInfo.st_mode))
	    {
		cout << argv[0] << ": Copying local file '" << srcPath << "' to "
		    << getRemotePath(destHost, destPort, destPath) << ": ";
		cout.flush();
		
		if(!BackupFile(destClient, srcPath, destPath))
		{
		    cout << "OK\n";
		}
		else
		{
		    cout << "FAILED\n";
		    exit(-1);
		}
	    }
	    else
	    {
		if (!(srcDir = opendir(srcPath.c_str())))
		{
		    cout << argv[0] << ": Could not open local directory " << srcPath << "'.\n";
		    perror("opendir: ");
		    exit(-1);
		}
		
		cout << argv[0] << ": Copying local directory '" << srcPath << "' to "
		    << getRemotePath(destHost, destPort, destPath) << ": ";
		cout.flush();
		
		if (destClient->Exists(destPath.c_str()))
		{
		    if (!destClient->IsDirectory(destPath.c_str()))
		    {
			cout << argv[0] << ": Destination path " << getRemotePath(destHost, destPort, destPath) << " exists and is not a directory.\n";
			exit(-1);
		    }
		}
		    
		MakeKfsLeafDir(destClient, srcPath, destPath);
		
		if (!BackupDir(destClient, srcPath, destPath))
		{
		    cout << "OK\n";
		}
		else
		{
		    cout << "FAILED\n";
		    exit(-1);
		}
		
		closedir(srcDir);
	    }
	}
	else if (srcIsRemote && !destIsRemote)
	{
	    assert (srcPath.length() > 0);
	    assert (destPath.length() > 0);
	    assert (srcPath[0] == '/');
	    
	    KfsClientPtr srcClient = getKfsClientFactory()->GetClient(srcHost, srcPort);
	
	    if (!srcClient)
	    {
		cout << argv[0] << ": Error initializing KFS client for " << getRemotePath(srcHost, srcPort, srcPath) << "\n";
		exit(-1);
	    }
	    getKfsClientFactory()->SetDefaultClient(srcClient);
	    
	    if (!srcClient->Exists(srcPath.c_str()))
	    {
		cout << argv[0] << ": " << getRemotePath(srcHost, srcPort, srcPath) << " does not exist.\n";
		exit(-1);
	    }
	    
	    if (!srcClient->IsDirectory(srcPath.c_str()))
	    {
		cout << argv[0] << ": Copying file " << getRemotePath(srcHost, srcPort, srcPath) << " to local '"
		    << destPath << "': ";
		cout.flush();
		
		if (!RestoreFile(srcClient, srcPath, destPath))
		{
		    cout << "OK\n";
		}
		else
		{
		    cout << "FAILED\n";
		    exit(-1);
		}
	    }
	    else
	    {
		cout << argv[0] << ": Copying directory " << getRemotePath(srcHost, srcPort, srcPath) << " to local '"
		    << destPath << "': ";
		cout.flush();
		
		if (!RestoreDir(srcClient, srcPath, destPath))
		{
		    cout << "OK\n";
		}
		else
		{
		    cout << "FAILED\n";
		    exit(-1);
		}
	    }
	}
	else
	{
	    cout << argv[0] << ": Error copying '" << srcPath << "' to '" << destPath << "' - local->local copying is not supported by this tool.\n";
	    exit(-1);
	}
    }
    
    if(destDir != 0) closedir(destDir);
    
    return 0;
}
