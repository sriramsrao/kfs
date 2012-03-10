//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: TelemetryClient.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2008/09/14
//
//
// Copyright 2008 Quantcast Corporation.
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
// \brief Telemetry client for KFS.  This sends out a packet to the
// server whenever an operation is slow; it also receives packets from
// the server that identifies the current set of slow nodes.
// 
//----------------------------------------------------------------------------

#ifndef _LIBKFSIO_TELEMETRYCLIENT_H
#define _LIBKFSIO_TELEMETRYCLIENT_H

extern "C" {
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <fcntl.h>
}

#include <vector>
#include <string>

#include "telemetry/packet.h"

namespace KFS
{
    class TelemetryClient {
    public:
        TelemetryClient() : mSock(-1) { }
        ~TelemetryClient();
        // @param[in] multicastPort: The port to listen on for
        // multicast traffic.
        // @param[in] srvIp/srvPort: The ip/port at which telemetry
        // packets should  be sent to the server.
        //
        void Init(const struct ip_mreq &imreq, int multicastPort,
                  const std::string &srvIp, int srvPort);

        // send out a message to the server identifying that we are a slow node
        void publish(double timetaken, std::string opname);
        // send out a message to the server identifying a slow node
        void publish(struct in_addr &target, double timetaken, std::string opname);
        void publish(struct in_addr &target, double timetaken, std::string opname,
                     uint32_t count, double *diskIOTime, double *elapsedTime);

        void publish(struct in_addr &target, double timetaken, std::string opname,
                     std::string message,
                     uint32_t count, double *diskIOTime, double *elapsedTime);
        
        // get notification from the server of slow nodes in the system.
        // @param[in/out] slowNodes --- the set of slow nodes as
        // published by the server; this method is non-blocking; if we
        // have received a notification about slow nodes, that
        // information is passed back; if no message has been received
        // in the last two minutes, the list of slow nodes is cleared.
        // @retval: -EAGAIN if there is no message from the server; 0 otherwise
        int getNotification(std::vector<struct in_addr> &slowNodes);
    private:
        // where the server is
        std::string mServerIp;
        int mServerPort;
        // the socket for sending/receiving packets
        int mSock;
        // our address that we fill in the packet
        struct in_addr mAddr;
        // when we last got a notification
        time_t mLastNotification;
        // helper methods
        void publish(TelemetryClntPacket_t &tpkt);
    };
}

#endif // _LIBKFSIO_TELEMETRYCLIENT_H
