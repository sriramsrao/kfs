//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kfsrebalance_main.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2010/05/13
//
// Copyright 2010 Quantcast Corp.
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
// \brief Notify the metaserver to rebalance chunkservers.  The
// rebalance plan is built offline; this tool sends an RPC to the
// metaserver asking it to load the plan and run it.
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
ExecRebalance(const ServerLocation &location, string &planfile);

int main(int argc, char **argv)
{
    char optchar;
    bool help = false;
    const char *server = NULL;
    int port = -1;
    bool verboseLogging = false;
    string planfile;

    KFS::MsgLogger::Init(NULL);

    while ((optchar = getopt(argc, argv, "hs:p:f:v")) != -1) {
        switch (optchar) {
            case 's':
                server = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 'h':
                help = true;
                break;
            case 'f':
                planfile = optarg;
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

    if (help || (server == NULL) || (port < 0)) {
        cout << "Usage: " << argv[0] << " -s <server name> -p <port> -f <plan-file> {-v}\n" << endl;
        exit(-1);
    }

    ServerLocation loc(server, port);

    ExecRebalance(loc, planfile);
}

void
ExecRebalance(const ServerLocation &location, string &planfile)
{
    int numIO;
    TcpSocket metaServerSock;
    MetaExecRebalancePlanOp *op;

    if (metaServerSock.Connect(location) < 0) {
        KFS_LOG_VA_ERROR("Unable to connect to %s",
                         location.ToString().c_str());
        exit(-1);
    }
    op = new MetaExecRebalancePlanOp(1, planfile);
    numIO = DoOpCommon(op, &metaServerSock);
    if (numIO < 0) {
        KFS_LOG_VA_ERROR("Server (%s) isn't responding to exec rebalance plan",
                         location.ToString().c_str());
        exit(-1);
    }

    delete op;
    metaServerSock.Close();
}
