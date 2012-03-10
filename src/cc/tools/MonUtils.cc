//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: MonUtils.cc 1552 2011-01-06 22:21:54Z sriramr $
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
// \brief Utilities for monitoring/stat extraction from meta/chunk servers.
//----------------------------------------------------------------------------

#include "MonUtils.h"
#include "common/kfstypes.h"

#include <cassert>
#include <cerrno>
#include <sstream>
#include <boost/scoped_array.hpp>

using std::istringstream;
using std::ostringstream;
using std::string;

using namespace KFS;
using namespace KFS_MON;

static const char *KFS_VERSION_STR = "KFS/1.0";
// use a big buffer so we don't have issues about server responses not fitting in
static const int CMD_BUF_SIZE = 1 << 20;

void
KfsMonOp::ParseResponseCommon(string &resp, Properties &prop)
{
    istringstream ist(resp);
    kfsSeq_t resSeq;
    const char separator = ':';

    prop.loadProperties(ist, separator, false);
    resSeq = prop.getValue("Cseq", -1);
    this->status = prop.getValue("Status", -1);
}

void
MetaPingOp::Request(ostringstream &os)
{
    os << "PING\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Cseq: " << seq << "\r\n\r\n";
}

void
MetaToggleWORMOp::Request(ostringstream &os)
{
    os << "TOGGLE_WORM\r\n";
    os << "Toggle-WORM: " << value << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Cseq: " << seq << "\r\n\r\n";
}

void
MetaToggleWORMOp::ParseResponse(const char *resp, int len)
{
    string respStr(resp, len);
}
void
MetaPingOp::ParseResponse(const char *resp, int len)
{
    string respStr(resp, len);
    Properties prop;
    string serv;
    const char delim = '\t';
    string::size_type start, end;

    ParseResponseCommon(respStr, prop);
    serv = prop.getValue("Servers", "");
    start = serv.find_first_of("s=");
    if (start == string::npos) {
        return;
    }

    string serverInfo;

    while (start != string::npos) {
        end = serv.find_first_of(delim, start);

        if (end != string::npos)
            serverInfo.assign(serv, start, end - start);
        else
            serverInfo.assign(serv, start, serv.size() - start);

        this->upServers.push_back(serverInfo);
        start = serv.find_first_of("s=", end);
    }

    serv = prop.getValue("Down Servers", "");
    start = serv.find_first_of("s=");
    if (start == string::npos) {
        return;
    }

    while (start != string::npos) {
        end = serv.find_first_of(delim, start);

        if (end != string::npos)
            serverInfo.assign(serv, start, end - start);
        else
            serverInfo.assign(serv, start, serv.size() - start);

        this->downServers.push_back(serverInfo);
        start = serv.find_first_of("s=", end);
    }

}

void
ChunkPingOp::Request(ostringstream &os)
{
    os << "PING\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Cseq: " << seq << "\r\n\r\n";
}

void
ChunkPingOp::ParseResponse(const char *resp, int len)
{
    string respStr(resp, len);
    Properties prop;

    ParseResponseCommon(respStr, prop);
    location.hostname = prop.getValue("Meta-server-host", "");
    location.port = prop.getValue("Meta-server-port", 0);
    totalSpace = prop.getValue("Total-space", (long long) 0);
    usedSpace = prop.getValue("Used-space", (long long) 0);
}

void
MetaStatsOp::Request(ostringstream &os)
{
    os << "STATS\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Cseq: " << seq << "\r\n\r\n";
}

void
ChunkStatsOp::Request(ostringstream &os)
{
    os << "STATS\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Cseq: " << seq << "\r\n\r\n";
}

void
MetaStatsOp::ParseResponse(const char *resp, int len)
{
    string respStr(resp, len);

    ParseResponseCommon(respStr, stats);
}

void
ChunkStatsOp::ParseResponse(const char *resp, int len)
{
    string respStr(resp, len);

    ParseResponseCommon(respStr, stats);
}

void
RetireChunkserverOp::Request(ostringstream &os)
{
    os << "RETIRE_CHUNKSERVER\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Downtime: " << downtime << "\r\n";
    os << "Chunk-server-name: " << chunkLoc.hostname << "\r\n";
    os << "Chunk-server-port: " << chunkLoc.port << "\r\n\r\n";
}

void
RetireChunkserverOp::ParseResponse(const char *resp, int len)
{
    string respStr(resp, len);
    Properties prop;

    ParseResponseCommon(respStr, prop);
}

int
KFS_MON::DoOpCommon(KfsMonOp *op, TcpSocket *sock)
{
    int numIO;
    boost::scoped_array<char> buf;
    int len;
    ostringstream os;

    op->Request(os);
    numIO = sock->DoSynchSend(os.str().c_str(), os.str().length());
    if (numIO <= 0) {
        op->status = -1;
        return -1;
    }

    buf.reset(new char[CMD_BUF_SIZE]);
    numIO = GetResponse(buf.get(), CMD_BUF_SIZE, &len, sock);

    if (numIO <= 0) {
        op->status = -1;
        return -1;
    }

    assert(len > 0);

    op->ParseResponse(buf.get(), len);

    return numIO;
}

///
/// Get a response from the server.  The response is assumed to
/// terminate with "\r\n\r\n".
/// @param[in/out] buf that should be filled with data from server
/// @param[in] bufSize size of the buffer
///
/// @param[out] delims the position in the buffer where "\r\n\r\n"
/// occurs; in particular, the length of the response string that ends
/// with last "\n" character.  If the buffer got full and we couldn't
/// find "\r\n\r\n", delims is set to -1.
///
/// @param[in] sock the socket from which data should be read
/// @retval # of bytes that were read; 0/-1 if there was an error
///
int
KFS_MON::GetResponse(char *buf, int bufSize,
                     int *delims,
                     TcpSocket *sock)
{
    int nread;
    int i;

    *delims = -1;
    while (1) {
        struct timeval timeout;

        timeout.tv_sec = 300;
        timeout.tv_usec = 0;

        nread = sock->DoSynchPeek(buf, bufSize, timeout);
        if (nread <= 0)
            return nread;
        for (i = 4; i <= nread; i++) {
            if (i < 4)
                break;
            if ((buf[i - 3] == '\r') &&
                (buf[i - 2] == '\n') &&
                (buf[i - 1] == '\r') &&
                (buf[i] == '\n')) {
                // valid stuff is from 0..i; so, length of resulting
                // string is i+1.
                memset(buf, '\0', bufSize);
                *delims = (i + 1);
                nread = sock->Recv(buf, *delims);
                return nread;
            }
        }
    }
    return -1;
}
