//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: telemetry_server_main.cc 2130 2011-03-08 21:13:43Z sriramr $
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
// \brief Telemetry server for KFS.  This periodically sends out a
// multicast packet identifying slow nodes in the system.
//
//----------------------------------------------------------------------------

#include "telemetry_server.h"

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
#include <ostream>
#include <sstream>
#include <iostream>

#include "common/log.h"

using std::for_each;
using std::mem_fun_ref;
using std::cout;
using std::endl;
using std::string;
using std::vector;
using std::ifstream;
using std::ostringstream;

NodeMap gNodeMap;

// set of targets to which the telemetry report has to be sent
vector<struct sockaddr_in> targets;

static void readTelemetryTargets(const char *machinesFn)
{
    // format of the file: [IP addr] [port]
    ifstream ifs;
    string hostIp;
    int hostPort;

    ifs.open(machinesFn);
    if (!ifs)
        return;
    while (1) {
        if (ifs.eof())
            break;
        ifs >> hostIp;
        ifs >> hostPort;
        struct sockaddr_in saddr;

        saddr.sin_addr.s_addr = inet_addr(hostIp.c_str());
        saddr.sin_port = htons(hostPort);
        saddr.sin_family = PF_INET;
        targets.push_back(saddr);
    }
}

int main(int argc, char **argv)
{
    int sock, status;
    struct sockaddr_in saddr;
    struct in_addr iaddr;
    unsigned char ttl = 8;
    unsigned int one = 1;
    bool initedLogger = false, help = false;
    char *machinesFn = NULL;
    char optchar;

    while ((optchar = getopt(argc, argv, "hl:m:")) != -1) {
        switch (optchar) {
            case 'm':
                machinesFn = optarg;
                break;
            case 'l':
                initedLogger = true;
                KFS::MsgLogger::Init(optarg);
                break;
            case 'h':
                help = true;
                break;
        }
    }

    if (!initedLogger) {
        KFS::MsgLogger::Init(NULL);
    }

    KFS::MsgLogger::SetLevel(MsgLogger::kLogLevelINFO);
    if (help) {
        cout << "Usage: " << argv[0] << " -m <machines file> " << " -l <log fn>" << endl;
        exit(0);
    }

    readTelemetryTargets(machinesFn);

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

    saddr.sin_family = PF_INET;
    saddr.sin_port = htons(12000);
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

    if (targets.size() == 0) {
        // set destination multicast address
        saddr.sin_family = PF_INET;
        saddr.sin_addr.s_addr = inet_addr("226.0.0.1");
        saddr.sin_port = htons(13000);
        targets.push_back(saddr);
    }

    time_t roundStart, now;
    roundStart = time(0);

    while (1) {
        // publish a report once every 10 secs
        gatherState(sock);
        now = time(0);
        if (now - roundStart > 60) {
            updateState();
            roundStart = time(0);
        }
        distributeState(sock);
    }

    // shutdown socket
    shutdown(sock, 2);
    // close socket
    close(sock);

    return 0;
}

void
KFS::gatherState(int sock)
{
    struct pollfd pollfds;
    TelemetryClntPacket_t tpkt;
    int res, status;
    time_t startTime, now;
    char *buf;
    const uint32_t bufsz = sizeof(TelemetryClntPacket_t);
    
    startTime = time(0);
    buf = new char[bufsz];
    while (1) {
        // poll for 10 secs
        int pollTimeout = 10;

        now = time(0);
        if (now - startTime >= pollTimeout) {
            break;
        }
        pollfds.fd = sock;
        pollfds.events = POLLIN;
        pollfds.revents = 0;

        pollTimeout -= (now - startTime);
        if (pollTimeout < 1)
            pollTimeout = 1;
        
        res = poll(&pollfds, 1, pollTimeout * 1000);
        if (pollfds.revents & POLLIN) {
            // status = recv(sock, &tpkt, sizeof(TelemetryClntPacket_t), 0);
            status = recv(sock, buf, bufsz, 0);
            KFS_LOG_VA_DEBUG("Received: %d bytes", status);
            if (status < (int) (sizeof(TelemetryClntPacket_t) - 4)) {
                // dont know what this packet is
                continue;
            }
            if (status == sizeof(TelemetryClntPacket_t)) {
                memcpy(&tpkt, buf, bufsz);
            } else {
                // packet-len == sizeof TelemetryClntPackt_t - 4...so, fix up
                int offset = 0;
                memcpy(&tpkt.source, buf, sizeof(in_addr));
                offset += sizeof(in_addr);
                memcpy(&tpkt.target, buf + offset, sizeof(in_addr));
                offset += sizeof(in_addr);
                memcpy(&tpkt.timetaken, buf + offset, sizeof(double));
                offset += sizeof(double);
                memcpy(&tpkt.count, buf + offset, sizeof(uint32_t));
                offset += sizeof(uint32_t);
                memcpy(tpkt.diskIOTime, buf + offset, sizeof(double) * MAX_IO_INFO_PER_PKT);
                offset += (sizeof(double) * MAX_IO_INFO_PER_PKT);
                memcpy(tpkt.elapsedTime, buf + offset, sizeof(double) * MAX_IO_INFO_PER_PKT);
                offset += (sizeof(double) * MAX_IO_INFO_PER_PKT);
                memcpy(tpkt.opname, buf + offset, sizeof(char) * OPNAME_LEN);
                offset += OPNAME_LEN;
                memcpy(tpkt.msg, buf + offset, sizeof(char) * MSG_LEN);

            }

            updateNodeState(tpkt);

        }
    }
    delete [] buf;
}

void
KFS::updateNodeState(TelemetryClntPacket_t &tpkt)
{
    char buffer[INET_ADDRSTRLEN];

    inet_ntop(AF_INET, (struct in_addr *) &(tpkt.target), buffer, INET_ADDRSTRLEN);
    
    string s(buffer);
    NodeMapIter iter = gNodeMap.find(s);

    inet_ntop(AF_INET, (struct in_addr *) &(tpkt.source), buffer, INET_ADDRSTRLEN);

    if (tpkt.timetaken < 1e-6) {
        // packets with negative #'s imply a status report about something gone bad
        KFS_LOG_VA_INFO("%s reports from %s:  %s",
                        buffer, s.c_str(), tpkt.opname);
        return;
        
    }


    if (strncmp(tpkt.opname,"READ", 4) == 0) {
        KFS_LOG_VA_INFO("%s reports that %s takes %.3f for %s with msg: %s",
                        buffer, s.c_str(), tpkt.timetaken, tpkt.opname, tpkt.msg);

        // dump out individual times for READ
        ostringstream os;
        for (uint32_t i = 0; i < tpkt.count; i++) {
            if ((i > 0) && (tpkt.elapsedTime[i] < tpkt.elapsedTime[i - 1]))
                // we are getting some bogus times...
                tpkt.elapsedTime[i] = tpkt.elapsedTime[i - 1];
            os << "(" << tpkt.diskIOTime[i] << " , " << tpkt.elapsedTime[i] << " ) ";
        }
        KFS_LOG_VA_INFO("Breakdown: %s", os.str().c_str());
    } else {
        KFS_LOG_VA_INFO("%s reports that %s takes %.3f for %s",
                        buffer, s.c_str(), tpkt.timetaken, tpkt.opname);
    }
    
    if (iter != gNodeMap.end()) {
        iter->second.sampleData(tpkt.timetaken);
        return;
    }
    NodeState_t ns;

    ns.sampleData(tpkt.timetaken);
    gNodeMap[s] = ns;

}

void
KFS::updateState()
{
    for (NodeMapIter iter = gNodeMap.begin(); iter != gNodeMap.end(); iter++) {
        iter->second.endPeriod();
    }
}

void
KFS::distributeState(int sock)
{
    TelemetryServerPacket_t srvPkt;
    socklen_t socklen;
    int status;
    struct in_addr addr;

    for (NodeMapIter iter = gNodeMap.begin(); iter != gNodeMap.end(); iter++) {
        if (iter->second.movingAvg > 10.0) {
            KFS_LOG_VA_INFO("%s seems slow (moving avg=%.3f)",
                            iter->first.c_str(), iter->second.movingAvg);
            inet_pton(AF_INET, iter->first.c_str(), &addr);
            srvPkt.addNode(addr);
        }
    }

    // send it to all the targets
    socklen = sizeof(struct sockaddr_in);
    for (uint32_t i = 0; i < targets.size(); i++) {
        status = sendto(sock, &srvPkt, sizeof(TelemetryServerPacket_t), 0,
                        (struct sockaddr *) &targets[i], socklen);
        if (status < 0)
            perror("sendto: ");
    }
}
