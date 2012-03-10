//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: telemetry_server.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2008/09/13
//
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
// \file Telemetry_srv.h
// \brief Telemetry server for KFS.  This periodically sends out a
// multicast packet identifying slow nodes in the system.
//
//----------------------------------------------------------------------------


#ifndef TELEMETRY_TELEMETRY_SRV_H
#define TELEMETRY_TELEMETRY_SRV_H

#include <netinet/in.h>
#include <tr1/unordered_map>
#include <string>

#include "common/cxxutil.h"
#include "packet.h"

namespace KFS
{
    struct NodeState_t {
        // # of packets we have received for this node in the last
        // period.
        int numSamplesInPeriod;
        double totalTimeInPeriod;
        double movingAvg;
        NodeState_t() : numSamplesInPeriod(0), totalTimeInPeriod(0.0), movingAvg(0.0) { }
        void startPeriod() {
            numSamplesInPeriod = 0;
            totalTimeInPeriod = 0.0;
        }
        void sampleData(double timespent) {
            numSamplesInPeriod++;
            totalTimeInPeriod += timespent;
        }
        void endPeriod() {
            movingAvg /= 2.0;
            if (numSamplesInPeriod > 0) 
                movingAvg += (totalTimeInPeriod / numSamplesInPeriod);
            startPeriod();
        }
    };
    
    typedef std::tr1::unordered_map<std::string, NodeState_t> NodeMap;
    typedef std::tr1::unordered_map<std::string, NodeState_t>::iterator NodeMapIter;

    // update state of nodes based on the packets we get from the socket.
    void gatherState(int sock);

    // update state of all nodes in system
    void updateState();
    // update state of a specific node
    void updateNodeState(TelemetryClntPacket_t &tpkt);

    // distribute the list of slow nodes
    void distributeState(int sock);
        
}

#endif // TELEMETRY_TELEMETRY_SRV_H
