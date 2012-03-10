//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: ChunkServer.cc 1857 2011-02-13 07:20:40Z sriramr $
//
// Created 2006/03/23
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

// order of #includes here is critical.  don't change it
#include "libkfsIO/Counter.h"
#include "libkfsIO/Globals.h"

#include "ChunkServer.h"
#include "ChunkSortHelper.h"
#include "Logger.h"
#include "Utils.h"

#include <netdb.h>
#include <arpa/inet.h>

using std::list;
using std::string;

using namespace KFS;
using namespace KFS::libkfsio;

// single network thread that manages connections and net I/O
static MetaThread netProcessor;

ChunkServer KFS::gChunkServer;

static void *
netWorker(void *dummy)
{
    globalNetManager().MainLoop();
    return NULL;
}

static void
StartNetProcessor()
{
    netProcessor.start(netWorker, NULL);
}

void
ChunkServer::Init()
{
    InitParseHandlers();
    // Register the counters
    RegisterCounters();

    // setup the telemetry stuff...
    struct ip_mreq imreq;
    string srvIp = "10.2.0.10";
    int srvPort = 12000;
    int multicastPort = 13000;

    imreq.imr_multiaddr.s_addr = inet_addr("226.0.0.1");
    imreq.imr_interface.s_addr = INADDR_ANY; // use DEFAULT interface

    mTelemetryReporter.Init(imreq, multicastPort, srvIp, srvPort);
}

void
ChunkServer::SendTelemetryReport(KfsOp_t op, double timeSpent)
{
    if (op != CMD_WRITE)
        return;
    mTelemetryReporter.publish(timeSpent, "WRITE");
}

void
ChunkServer::MainLoop(int clientAcceptPort, const string & serverHostname)
{
    struct hostent *hent = 0;

    mClientAcceptPort = clientAcceptPort;
    
    if (serverHostname.size() < 1) {
#if !defined (__sun__)
        static const int MAXHOSTNAMELEN = 256;
#endif
        char hostname[MAXHOSTNAMELEN];
        
        if (gethostname(hostname, MAXHOSTNAMELEN)) {
            perror("gethostname: ");
            exit(-1);
        }
        KFS_LOG_VA_INFO("gethostname returned: %s", hostname);
        
        // convert to IP address
        hent = gethostbyname(hostname);
    }
    else {
        // convert to IP address
        hent = gethostbyname(serverHostname.c_str());
    }

    if (hent) {
        in_addr ipaddr;

        memcpy(&ipaddr, hent->h_addr, hent->h_length);

        // Warn user if resolved address is the local loopback address which may
        // cause duplicate chunk server issues.
        if ( (ipaddr.s_addr >> 0 & 0xFF) == 127) {
            KFS_LOG_VA_INFO("hostname resolved to: %s", inet_ntoa(ipaddr));
            KFS_LOG_WARN("WARNING: IP resolved to 127.x.x.x address - "
                "check 'hosts' line in /etc/nsswitch.conf to make sure that 'dns'"
                " is before 'files'");
        }

        mLocation.Reset(inet_ntoa(ipaddr), clientAcceptPort);
    } else {
        mLocation.Reset(serverHostname.c_str(), clientAcceptPort);
    }
    gClientManager.StartAcceptor(clientAcceptPort);
    gLogger.Start();
    gChunkManager.Start();
    // gMetaServerSM.SendHello(clientAcceptPort);
    gMetaServerSM.Init(clientAcceptPort, serverHostname);

    gChunkSortHelperManager.StartAcceptor();

    // gChunkManager.DumpChunkMap();
    StartNetProcessor();

    netProcessor.join();
    gChunkManager.Shutdown();
}

void
KFS::verifyExecutingOnNetProcessor()
{
    assert(netProcessor.isEqual(pthread_self()));
    if (!netProcessor.isEqual(pthread_self())) {
        die("FATAL: Not executing on net processor");
    }
}

void
KFS::StopNetProcessor(int status)
{
    netProcessor.exit(status);
}

RemoteSyncSMPtr ChunkServer::FindServer(const ServerLocation &location, bool connect)
{
    return KFS::FindServer(mRemoteSyncers, location, connect);
}

void ChunkServer::RemoveServer(RemoteSyncSM *target)
{
    KFS::RemoveServer(mRemoteSyncers, target);
}
