//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: MonUtils.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2006/07/18
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

#ifndef TOOLS_MONUTILS_H
#define TOOLS_MONUTILS_H

#include <vector>
#include <string>
#include <sstream>

#include "libkfsIO/TcpSocket.h"
#include "common/properties.h"

namespace KFS_MON
{
    enum KfsMonOp_t {
        CMD_METAPING,
        CMD_CHUNKPING,
        CMD_METASTATS,
        CMD_METATOGGLE_WORM,
        CMD_CHUNKSTATS,
        CMD_RETIRE_CHUNKSERVER
    };

    struct KfsMonOp {
        KfsMonOp_t op;
        int32_t  seq;
        ssize_t  status;
        KfsMonOp(KfsMonOp_t o, int32_t s) :
            op(o), seq(s) { };
        virtual ~KfsMonOp() { };
        virtual void Request(std::ostringstream &os) = 0;
        virtual void ParseResponse(const char *resp, int len) = 0;
        void ParseResponseCommon(std::string &resp, KFS::Properties &prop);
    };

    struct MetaPingOp : public KfsMonOp {
        std::vector<std::string> upServers; /// result
        std::vector<std::string> downServers; /// result
        MetaPingOp(int32_t s) :
            KfsMonOp(CMD_METAPING, s) { };
        void Request(std::ostringstream &os);
        void ParseResponse(const char *resp, int len);
    };

    struct MetaToggleWORMOp : public KfsMonOp {
        int value;
        MetaToggleWORMOp(int32_t s, int v) :
            KfsMonOp(CMD_METATOGGLE_WORM, s), value(v) { };
        void Request(std::ostringstream &os);
        void ParseResponse(const char *resp, int len);
    };


    struct ChunkPingOp : public KfsMonOp {
        KFS::ServerLocation location;
        int64_t totalSpace;
        int64_t usedSpace;
        ChunkPingOp(int32_t s) :
            KfsMonOp(CMD_CHUNKPING, s) { };
        void Request(std::ostringstream &os);
        void ParseResponse(const char *resp, int len);
    };

    struct MetaStatsOp : public KfsMonOp {
        KFS::Properties stats; // result
        MetaStatsOp(int32_t s) :
            KfsMonOp(CMD_METAPING, s) { };
        void Request(std::ostringstream &os);
        void ParseResponse(const char *resp, int len);
    };

    struct ChunkStatsOp : public KfsMonOp {
        KFS::Properties stats; // result
        ChunkStatsOp(int32_t s) :
            KfsMonOp(CMD_CHUNKPING, s) { };
        void Request(std::ostringstream &os);
        void ParseResponse(const char *resp, int len);
    };

    struct RetireChunkserverOp : public KfsMonOp {
        KFS::ServerLocation chunkLoc;
        int downtime; // # of seconds of downtime
        RetireChunkserverOp(int32_t s, const KFS::ServerLocation &c, int d) :
            KfsMonOp(CMD_RETIRE_CHUNKSERVER, s), chunkLoc(c), downtime(d) { };
        void Request(std::ostringstream &os);
        void ParseResponse(const char *resp, int len);
    };

    extern int DoOpCommon(KfsMonOp *op, KFS::TcpSocket *sock);
    extern int GetResponse(char *buf, int bufSize,
                           int *delims,
                           KFS::TcpSocket *sock);

}

#endif // TOOLS_MONUTILS_H
