//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: telemetry_repeater_main.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2008/09/13
//
// Copyright 2008 Quantcast Corporation.  All rights reserved.
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
// \brief Telemetry service repeater: it gets a UDP packet from the
// telemetry server and then sends that packet out as multicast.  
//
//----------------------------------------------------------------------------


#include "packet.h"

using namespace KFS;

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include <sys/poll.h>
#include <cerrno>
#include <algorithm>
#include <vector>
#include <fstream>
#include <iostream>

#include "common/log.h"

using std::for_each;
using std::mem_fun_ref;
using std::cout;
using std::endl;
using std::string;
using std::vector;
using std::ifstream;

int main(int argc, char **argv)
{
    int sock, status;
    struct sockaddr_in saddr;
    struct in_addr iaddr;
    unsigned char ttl = 8;
    unsigned int one = 1;
    bool initedLogger = false, help = false;
    char optchar;
    int port = -1;

    while ((optchar = getopt(argc, argv, "hl:p:")) != -1) {
        switch (optchar) {
            case 'l':
                initedLogger = true;
                KFS::MsgLogger::Init(optarg, MsgLogger::kLogLevelINFO);
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 'h':
                help = true;
                break;
        }
    }

    if (!initedLogger) {
        KFS::MsgLogger::Init(NULL, MsgLogger::kLogLevelINFO);
    }

    if (help || (port < 0)) {
        cout << "Usage: " << argv[0] << " -l <log fn> -p <port #>" << endl;
        exit(0);
    }

    memset(&saddr, 0, sizeof(struct sockaddr_in));
    memset(&iaddr, 0, sizeof(struct in_addr));

    // open a UDP socket
    sock = socket(PF_INET, SOCK_DGRAM, 0);
    if ( sock < 0 ) {
        perror("Error creating socket");
        exit(-1);
    }

    if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, 
                   (char *) &one, sizeof(one)) < 0) {
        perror("Setsockopt: ");
    }

    // get the unicast 
    saddr.sin_family = PF_INET;
    saddr.sin_port = htons(port);
    saddr.sin_addr.s_addr = htonl(INADDR_ANY); // bind socket to any interface
    status = bind(sock, (struct sockaddr *)&saddr, sizeof(struct sockaddr_in));

    if ( status < 0 )
        perror("Error binding socket to interface"), exit(0);

    iaddr.s_addr = INADDR_ANY; // use DEFAULT interface

    // Set the outgoing interface to DEFAULT
    status = setsockopt(sock, IPPROTO_IP, IP_MULTICAST_IF, &iaddr,
                        sizeof(struct in_addr));
    if (status < 0)
        perror("Multicast-if:");

    // Set multicast packet TTL to 20; default TTL is 1
    status = setsockopt(sock, IPPROTO_IP, IP_MULTICAST_TTL, &ttl,
                        sizeof(unsigned char));

    if (status < 0)
        perror("Multicast-ttl:");

    setsockopt(sock, IPPROTO_IP, IP_MULTICAST_LOOP, (char *) &one, sizeof(one));

    saddr.sin_family = PF_INET;
    saddr.sin_addr.s_addr = inet_addr("226.0.0.1");
    saddr.sin_port = htons(13000);

    TelemetryServerPacket_t srvPkt;
    socklen_t socklen = sizeof(struct sockaddr_in);

    while (1) {
        status = recv(sock, &srvPkt, sizeof(TelemetryServerPacket_t), 0);
        if (status == sizeof(TelemetryServerPacket_t))  {
            status = sendto(sock, &srvPkt, sizeof(TelemetryServerPacket_t), 0,
                            (struct sockaddr *) &saddr, socklen);
        }
    }
    // shutdown socket
    shutdown(sock, 2);
    // close socket
    close(sock);

    return 0;
}
