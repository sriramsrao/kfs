//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: packet.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2008/09/13
//
// Copyright 2008 Quantcast Corporation.  All rights reserved.
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
// \file Telemetry_srv.cc
// \brief Format of the packets that are sent/recevied by the telemetry service.
// 
//----------------------------------------------------------------------------

#ifndef TELEMETRY_PACKET_H
#define TELEMETRY_PACKET_H

#include <netinet/in.h>
#include <string.h>

namespace KFS
{
    static const int OPNAME_LEN = 32;
    static const int MAX_NODES_PER_PKT = 32;
    static const uint32_t MAX_IO_INFO_PER_PKT = 32;
    // optional message
    static const int MSG_LEN = 32;

    // Packet sent to the telemetry service by the clients: each
    // client identifies itself and the target that is "slow".  This
    // state is accumulated by the service and a list of slow nodes is
    // periodically published.
    struct TelemetryClntPacket_t {
        TelemetryClntPacket_t() { }
        TelemetryClntPacket_t(const struct in_addr &s, const struct in_addr &t,
                              double tt, const char *n) :
            source(s), target(t), timetaken(tt), count(0) {
            strncpy(opname, n, OPNAME_LEN);
        }

        struct in_addr source;
        struct in_addr target;
        double timetaken;
        // for each individual request, send the time it took
        // how many IOs are we reporting per packet
        // XXX: Packet alignment :-(  Count should be moved up and a
        // padding should be added; otherwise, if we have components
        // compiled in 32 and 64-bit running in the same environment,
        // packets wont' line up.  need to move this field around and
        // setup a padding or claim it to be uint64
        uint32_t count;
        double diskIOTime[MAX_IO_INFO_PER_PKT];
        double elapsedTime[MAX_IO_INFO_PER_PKT];
        char opname[OPNAME_LEN];
        char msg[MSG_LEN];
    };

    // set of nodes that are slow for a given op-type
    struct TelemetryServerPacket_t {
        TelemetryServerPacket_t() : numNodes(0) { }
        void addNode(const struct in_addr &t) {
            if (numNodes >= MAX_NODES_PER_PKT)
                return;
            slowNodes[numNodes] = t;
            numNodes++;
        }
        int numNodes;
        struct in_addr slowNodes[MAX_NODES_PER_PKT];
    };

}

#endif // TELEMETRY_PACKET_H
