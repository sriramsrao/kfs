//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: metaserver_main.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2006/03/22
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
// \file Main.cc
// \brief Driver code that starts up the meta server
//
//----------------------------------------------------------------------------

#include <signal.h>

#include "common/properties.h"
#include "libkfsIO/NetManager.h"
#include "libkfsIO/Globals.h"
#include "libkfsIO/IOBuffer.h"

#include "NetDispatch.h"
#include "startup.h"
#include "ChunkServer.h"
#include "LayoutManager.h"
#include "common/log.h"
#include "qcdio/qcutils.h"
#include "qcdio/qciobufferpool.h"

using namespace KFS;

// NetDispatch KFS::gNetDispatch;

// Port at which KFS clients connect and send RPCs
int gClientPort;

// Port at which Chunk servers connect
int gChunkServerPort;

// paths for logs and checkpoints
string gLogDir, gCPDir;

// min # of chunk servers to exit recovery mode
uint32_t gMinChunkservers;

int16_t gMinReplicasPerFile;
bool gIsPathToFidCacheEnabled = false;

Properties gProp;

int ReadMetaServerProperties(char *fileName);

using std::cout;
using std::endl;

int
main(int argc, char **argv)
{
	if (argc > 2) {
		KFS::MsgLogger::Init(argv[2]);
	} else {
		KFS::MsgLogger::Init(NULL);
	}

        if (argc < 2) {
                cout << "Usage: " << argv[0] << " <properties file> {<msg log file>}" << endl;
                exit(0);
        }

        if (ReadMetaServerProperties(argv[1]) != 0) {
                cout << "Bad properties file: " << argv[1] << " aborting..." << endl;
                exit(-1);
        }

	KFS_LOG_INFO("Starting metaserver...");

	libkfsio::InitGlobals();

        kfs_startup(gLogDir, gCPDir, gMinChunkservers, gMinReplicasPerFile,
			gIsPathToFidCacheEnabled);

        // Ignore SIGPIPE's that generated when clients break TCP
        // connection.
        //
        signal(SIGPIPE, SIG_IGN);
	
	// this never returns...
        gNetDispatch.Start(gClientPort, gChunkServerPort);

        return 0;
}

class BufferAllocator : public libkfsio::IOBufferAllocator
{
public:
    BufferAllocator()
        : mBufferPool()
        {}
    virtual size_t GetBufferSize() const
        { return mBufferPool.GetBufferSize(); }
    virtual char* Allocate()
    {
        char* const buf = mBufferPool.Get();
        if (! buf) {
            QCUtils::FatalError("out of io buffers", 0);
        }
        return buf;
    }
    virtual void Deallocate(
        char* inBufferPtr)
        { mBufferPool.Put(inBufferPtr); }
    QCIoBufferPool& GetBufferPool()
        { return mBufferPool; }
private:
    QCIoBufferPool mBufferPool;

private:
    BufferAllocator(
        const BufferAllocator& inAllocator);
    BufferAllocator& operator=(
        const BufferAllocator& inAllocator);
};

static const BufferAllocator* sAllocatorForGdbToFind = 0;

static BufferAllocator& GetIoBufAllocator()
{
    static BufferAllocator sAllocator;
    if (! sAllocatorForGdbToFind) {
        sAllocatorForGdbToFind = &sAllocator;
    }
    return sAllocator;
}

///
/// Read and validate the configuration settings for the meta
/// server. The configuration file is assumed to contain lines of the
/// form: xxx.yyy.zzz = <value>
/// @result 0 on success; -1 on failure
/// @param[in] fileName File that contains configuration information
/// for the chunk server.
///

int
ReadMetaServerProperties(char *fileName)
{
	int16_t maxReplicasPerFile;

        if (gProp.loadProperties(fileName, '=', true) != 0) {
                return -1;
        }
        MsgLogger::Init(0);
	MsgLogger::GetLogger()->SetLogLevel(
		gProp.getValue("metaServer.loglevel",
		MsgLogger::GetLogLevelNamePtr(
			MsgLogger::GetLogger()->GetLogLevel())));
	MsgLogger::GetLogger()->SetMaxLogWaitTime(0);
	MsgLogger::GetLogger()->SetParameters(gProp, "metaServer.msgLogWriter.");
        const int err = GetIoBufAllocator().GetBufferPool().Create(
		gProp.getValue("metaServer.bufferPool.partitions", 1),
		gProp.getValue("metaServer.bufferPool.partionBuffers",
			(sizeof(long) < 8 ? 32 : 64) << 10),
		gProp.getValue("metaServer.bufferPool.bufferSize", 4 << 10),
		gProp.getValue("metaServer.bufferPool.lockMemory",0) != 0
        );
	if (err != 0) {
		cout << QCUtils::SysError(err, "io buffer pool create: ") << endl;
		return -1;
	}
	if (! libkfsio::SetIOBufferAllocator(&GetIoBufAllocator())) {
		cout << "failed to set io buffer allocatior" << endl;
		return -1;
	}
        gClientPort = gProp.getValue("metaServer.clientPort", -1);
        if (gClientPort < 0) {
                cout << "Aborting...bad client port: " << gClientPort << endl;
                return -1;
        }
        cout << "Using meta server client port: " << gClientPort << endl;

        gChunkServerPort = gProp.getValue("metaServer.chunkServerPort", -1);
        if (gChunkServerPort < 0) {
                cout << "Aborting...bad chunk server port: " << gChunkServerPort << endl;
                return -1;
        }
        cout << "Using meta server chunk server port: " << gChunkServerPort << endl;
	gLogDir = gProp.getValue("metaServer.logDir","");
	gCPDir = gProp.getValue("metaServer.cpDir","");
	bool wormMode = gProp.getValue("metaServer.wormMode", false);

	const char *clusterKey = gProp.getValue("metaServer.clusterKey", "");
	cout << "Setting cluster key to: " << clusterKey << endl;
	setClusterKey(clusterKey);

	const char *md5sumFn = gProp.getValue("metaServer.md5sumFilename", "");
	cout << "Setting md5sumFilename to: " << md5sumFn << endl;
	setMD5SumFn(md5sumFn);

	// min # of chunkservers that should connect to exit recovery mode
	gMinChunkservers = gProp.getValue("metaServer.minChunkservers", 1);
	cout << "min. # of chunkserver that should connect: " << gMinChunkservers << endl;

	// desired min. # of replicas per file
	gMinReplicasPerFile = gProp.getValue("metaServer.minReplicasPerFile", 1);
	maxReplicasPerFile = gProp.getValue("metaServer.maxReplicasPerFile", MAX_REPLICAS_PER_FILE);
	setMaxReplicasPerFile(maxReplicasPerFile);

	cout << "min. # of replicas per file: " << gMinReplicasPerFile << endl;
	cout << "max. # of replicas per file: " << maxReplicasPerFile << endl;

	if (wormMode) {
		cout << "Enabling WORM mode" << endl;
		setWORMMode(wormMode);
	}

	// By default, path->fid cache is disabled.
	gIsPathToFidCacheEnabled = (gProp.getValue("metaServer.enablePathToFidCache", 0)) != 0;
	if (gIsPathToFidCacheEnabled) {
		cout << "Enabling path->fid cache" << endl;
	}

	string chunkmapDumpDir = gProp.getValue("metaServer.chunkmapDumpDir", ".");
	setChunkmapDumpDir(chunkmapDumpDir);

	ChunkServer::SetParameters(gProp);
        gLayoutManager.SetParameters(gProp);

        return 0;
}
