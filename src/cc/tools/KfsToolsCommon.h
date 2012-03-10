//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: KfsToolsCommon.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2009/04/17
//
//
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

#ifndef TOOLS_KFSCOMMON_H
#define TOOLS_KFSCOMMON_H

#include <string>
#include "libkfsClient/KfsClient.h"

namespace KFS
{
    namespace tools
    {
	///
	/// Tries to interpret given string as integer value. If it can't
	/// be properly convert (or contains garbage at the end) it returns false
	/// and doesn't modify 'toInt' value.
	/// @param[in] fromString  String to convert
	/// @param[out] toInt  Result of conversion will be put here (only if it returns true)
	/// @retval true on success; false on failure
	///
	bool getInt(const std::string & fromString, int &toInt);
	
	///
	/// Tries to convert given server description as server_host:port
	/// If serverDesc doesn't contain ':' it will be all put in serverHost and serverPort is not modified.
	/// If it contains ':', first part will be put in serverHost. Second part will be put in serverPort
	/// only if it can be properly converted to integer.
	/// @param[in] serverDesc  Server description
	/// @param[out] serverHost  Hostname of the server
	/// @param[out] serverPort  Port of the server
	///
	void parseServer(const std::string & serverDesc, std::string & serverHost, int & serverPort);
	
	///
	/// Tries to get server host and port from environment variables:
	/// KFS_SERVER_HOST and KFS_SERVER_PORT.
	/// If KFS_SERVER_HOST contains "hostname:port", port will be set as well
	/// (but only if it's correct)
	/// If KFS_SERVER_HOST contains "hostname:port" and KFS_SERVER_PORT
	/// is specified, the value from KFS_SERVER_PORT is used.	
	/// @param[out] serverHost  Hostname of the server
	/// @param[out] serverPort  Port of the server
	///
	void getEnvServer(std::string & serverHost, int & serverPort);
	
	bool parsePath(const std::string & pathDesc, std::string & serverHost,
			int & serverPort, std::string & filePath, bool & isRemote);
	
	std::string getRemotePath(const std::string & host, const int & port, const std::string & path);
	
	// Parts moved from other .cc files:

	//
	// For the purpose of the cp -r, take the leaf from sourcePath and
	// make that directory in kfsPath. 
	//
	void MakeKfsLeafDir(KfsClientPtr kfsClient, const std::string &sourcePath, std::string &kfsPath);

	//
	// Given a file defined by sourcePath, copy it to KFS as defined by
	// kfsPath
	//
	int BackupFile(KfsClientPtr kfsClient, const std::string &sourcePath, const std::string &kfsPath);

	// Given a dirname, backit up it to dirname.  Dirname will be created
	// if it doesn't exist.  
	int BackupDir(KfsClientPtr kfsClient, const std::string &dirname, std::string &kfsdirname);

	// Given a kfsdirname, restore it to dirname.  Dirname will be created
	// if it doesn't exist. 
	int RestoreDir(KfsClientPtr kfsClient, std::string &dirname, std::string &kfsdirname);

	// Given a kfsdirname/filename, restore it to dirname/filename.  The
	// operation here is simple: read the file from KFS and dump it to filename.
	//
	int RestoreFile(KfsClientPtr kfsClient, std::string &kfspath, std::string &localpath);
	
	//
	// Given a file defined by a KFS srcPath, copy it to KFS as defined by
	// dstPath
	//
	int CopyFile(KfsClientPtr kfsClient, const std::string &srcPath, const std::string &dstPath);

	// Given a srcDirname, copy it to dirname.  Dirname will be created
	// if it doesn't exist.  
	int CopyDir(KfsClientPtr kfsClient, const std::string &srcDirname, std::string dstDirname);
	
	int DirList(KfsClientPtr kfsClient, std::string kfsdirname, bool longMode, bool humanReadable, bool timeInSecs);
    }
}

#endif // TOOLS_KFSCOMMON_H
