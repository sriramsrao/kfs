//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: ChunkServer.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2006/03/16
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
// 
//----------------------------------------------------------------------------

#ifndef _CHUNKSERVER_H
#define _CHUNKSERVER_H

#include "libkfsIO/TelemetryClient.h"

#include "ChunkManager.h"
#include "ClientManager.h"
#include "ClientSM.h"
#include "MetaServerSM.h"
#include "RemoteSyncSM.h"

namespace KFS
{
class ChunkServer {
public:
    ChunkServer() : mOpCount(0), mKickNetThread(false) { };
    
    void Init();

    void MainLoop(int clientAcceptPort, const std::string & serverHostname);

    bool IsLocalServer(const ServerLocation &location) const {
        return mLocation == location;
    }
    RemoteSyncSMPtr FindServer(const ServerLocation &location,
                               bool connect = true);
    void RemoveServer(RemoteSyncSM *target);

    std::string GetMyLocation() const {
        return mLocation.ToString();
    }

    void ToggleNetThreadKicking (bool v) {
        mKickNetThread = v;
    }

    bool NeedToKickNetThread() {
        return mKickNetThread;
    }
    
    void OpInserted() {
        mOpCount++;
    }

    void OpFinished() {
        mOpCount--;
        if (mOpCount < 0)
            mOpCount = 0;
    }
    int GetNumOps() const {
        return mOpCount;
    }

    void SendTelemetryReport(KfsOp_t op, double timeSpent);

private:
    int mClientAcceptPort;
    // # of ops in the system
    int mOpCount;
    bool mKickNetThread;
    ServerLocation mLocation;
    std::list<RemoteSyncSMPtr> mRemoteSyncers;
    // telemetry reporter...used for notifying slow writes thru this node
    TelemetryClient mTelemetryReporter;
};

extern void verifyExecutingOnNetProcessor();
extern void verifyExecutingOnEventProcessor();
extern void StopNetProcessor(int status);

extern ChunkServer gChunkServer;
}

#endif // _CHUNKSERVER_H
