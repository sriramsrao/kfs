//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kfsping_main.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2006/07/20
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
// \brief Ping the meta/chunk server for liveness
//----------------------------------------------------------------------------

extern "C" {
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
};

#include <iostream>
#include <string>

#include "libkfsIO/TcpSocket.h"
#include "common/log.h"

#include "MonUtils.h"

using std::string;
using std::cout;
using std::endl;
using std::vector;
using namespace KFS;
using namespace KFS_MON;

static void
PingMetaServer(const ServerLocation &location);

static void
PingChunkServer(const ServerLocation &location);

float convertToMB(long bytes)
{
    return bytes / (1024.0 * 1024.0);
}

int main(int argc, char **argv)
{
    char optchar;
    bool help = false, meta = false, chunk = false;
    const char *server = NULL;
    int port = -1;
    bool verboseLogging = false;

    KFS::MsgLogger::Init(NULL);

    while ((optchar = getopt(argc, argv, "hmcs:p:v")) != -1) {
        switch (optchar) {
            case 'm': 
                meta = true;
                break;
            case 'c':
                chunk = true;
                break;
            case 's':
                server = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 'h':
                help = true;
                break;
            case 'v':
                verboseLogging = true;
                break;
            default:
                KFS_LOG_VA_ERROR("Unrecognized flag %c", optchar);
                help = true;
                break;
        }
    }

    if (verboseLogging) {
        KFS::MsgLogger::SetLevel(KFS::MsgLogger::kLogLevelDEBUG);
    } else {
        KFS::MsgLogger::SetLevel(KFS::MsgLogger::kLogLevelINFO);
    } 

    help = help || (!meta && !chunk);

    if (help || (server == NULL) || (port < 0)) {
        cout << "Usage: " << argv[0] << " [-m|-c] -s <server name> -p <port> {-v}" 
             << endl;
        exit(-1);
    }

    ServerLocation loc(server, port);

    if (meta)
        PingMetaServer(loc);
    else if (chunk)
        PingChunkServer(loc);
}

void
PingMetaServer(const ServerLocation &location)
{
    int numIO;
    vector<string>::size_type i;
    TcpSocket metaServerSock;
    MetaPingOp *op;

    if (metaServerSock.Connect(location) < 0) {
        KFS_LOG_VA_ERROR("Unable to connect to %s",
                         location.ToString().c_str());
        exit(0);
    }
    op = new MetaPingOp(1);
    numIO = DoOpCommon(op, &metaServerSock);
    if (numIO < 0) {
        KFS_LOG_VA_ERROR("Server (%s) isn't responding to ping",
                         location.ToString().c_str());
        exit(0);
    }
    if (op->upServers.size() == 0) {
        cout << "No chunkservers are connected" << endl;
    } else {
        cout << "Up servers: " << op->upServers.size() << endl;
        for (i = 0; i < op->upServers.size(); ++i) {
            cout << op->upServers[i] << endl;
        }
    }

    if (op->downServers.size() > 0) {
        cout << "Down servers: " << op->downServers.size() << endl;
        for (i = 0; i < op->downServers.size(); ++i) {
            cout << op->downServers[i] << endl;
        }
    }

    delete op;
    metaServerSock.Close();
}

void
PingChunkServer(const ServerLocation &location)
{
    int numIO;
    TcpSocket chunkServerSock;
    ChunkPingOp *op;

    if (chunkServerSock.Connect(location) < 0) {
        KFS_LOG_VA_ERROR("Unable to connect to %s",
                         location.ToString().c_str());
        exit(0);
    }
    op = new ChunkPingOp(1);
    numIO = DoOpCommon(op, &chunkServerSock);
    if (numIO < 0) {
        KFS_LOG_VA_ERROR("Server %s isn't responding to ping",
                         location.ToString().c_str());
        exit(0);
    }
    cout << "Meta-server: " << op->location.ToString().c_str() << endl;
    cout << "Total-space: " << convertToMB(op->totalSpace) << " (MB) " << endl;
    cout << "Used-space: " << convertToMB(op->usedSpace) << " (MB) " << endl;
    delete op;
    chunkServerSock.Close();
}
