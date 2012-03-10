//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kfstoggleworm_main.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2008/10/06
//
// Copyright 2008 Quantcast Corp.
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
// \brief Toggle WORM mode of KFS
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
ToggleWORM(const ServerLocation &location, int toggle);

int main(int argc, char **argv)
{
    char optchar;
    bool help = false;
    const char *server = NULL;
    int port = -1;
    bool verboseLogging = false;
	int toggle = -1;

    KFS::MsgLogger::Init(NULL);

    while ((optchar = getopt(argc, argv, "hs:p:t:v")) != -1) {
        switch (optchar) {
            case 's':
                server = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
			case 't':
				toggle = atoi(optarg);
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

    if (help || (server == NULL) || (port < 0) || 
		((toggle != 0) && (toggle != 1))) {
        cout << "Usage: " << argv[0] << " -s <server name> -p <port> -t [1|0] {-v}\n\t-s : meta server\n\t-p : meta server port\n\t-t : toggle value\n\t-v : verbose" << endl;
        exit(-1);
    }

    ServerLocation loc(server, port);

    ToggleWORM(loc, toggle);
}

void
ToggleWORM(const ServerLocation &location, int toggle)
{
    int numIO;
    TcpSocket metaServerSock;
    MetaToggleWORMOp *op;

    if (metaServerSock.Connect(location) < 0) {
        KFS_LOG_VA_ERROR("Unable to connect to %s",
                         location.ToString().c_str());
        exit(0);
    }
    op = new MetaToggleWORMOp(1, toggle);
    numIO = DoOpCommon(op, &metaServerSock);
    if (numIO < 0) {
        KFS_LOG_VA_ERROR("Server (%s) isn't responding to toggleworm",
                         location.ToString().c_str());
        exit(0);
    } else {
		cout << "MetaServer " << location.ToString().c_str() << " WORM mode ";
		if (toggle == 1) 
			cout << "ON" << endl;
		else 
			cout << "OFF" << endl;
	}

    delete op;
    metaServerSock.Close();
}
