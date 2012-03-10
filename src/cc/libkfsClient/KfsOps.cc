//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: KfsOps.cc 3491 2011-12-15 02:00:43Z sriramr $
//
// Created 2006/05/24
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

#include "KfsOps.h"
#include <cassert>

extern "C" {
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <string.h>
}
#include "libkfsIO/Checksum.h"
#include "Utils.h"

using std::istringstream;
using std::ostream;
using std::string;

#include <iostream>
using std::cout;
using std::endl;

static const char *KFS_VERSION_STR = "KFS/1.0";

using namespace KFS;

///
/// All Request() methods build a request RPC based on the KFS
/// protocol and output the request into a ostream.
/// @param[out] os which contains the request RPC.
///
void
CreateOp::Request(ostream &os)
{
    int e = exclusive ? 1 : 0;

    os << "CREATE " << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Parent File-handle: " << parentFid << "\r\n";
    os << "Filename: " << filename << "\r\n";
    os << "Num-replicas: " << numReplicas << "\r\n";
    os << "Exclusive: " << e << "\r\n\r\n";
}

void
MkdirOp::Request(ostream &os)
{
    os << "MKDIR " << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Parent File-handle: " << parentFid << "\r\n";
    os << "Directory: " << dirname << "\r\n\r\n";
}

void
RmdirOp::Request(ostream &os)
{
    os << "RMDIR " << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Parent File-handle: " << parentFid << "\r\n";
    os << "Pathname: " << pathname << "\r\n";
    os << "Directory: " << dirname << "\r\n\r\n";
}

void
RenameOp::Request(ostream &os)
{
    int o = overwrite ? 1 : 0;

    os << "RENAME " << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Parent File-handle: " << parentFid << "\r\n";
    os << "Old-name: " << oldname << "\r\n";
    os << "New-path: " << newpath << "\r\n";
    os << "Old-path: " << oldpath << "\r\n";
    os << "Overwrite: " << o << "\r\n\r\n";
}

void
MakeChunksStableOp::Request(ostream &os)
{
    os << "MAKE_CHUNKS_STABLE " << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "File-handle: " << fid << "\r\n";
    os << "Pathname: " << pathname << "\r\n\r\n";
}

void
ReaddirOp::Request(ostream &os)
{
    os << "READDIR " << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Directory File-handle: " << fid << "\r\n\r\n";
}

void
SetMtimeOp::Request(ostream &os)
{
    os << "SET_MTIME" << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Pathname: " << pathname << "\r\n";
    os << "Mtime-sec: " << mtime.tv_sec << "\r\n";
    os << "Mtime-usec: " << mtime.tv_usec << "\r\n\r\n";
}

void
DumpChunkServerMapOp::Request(ostream &os)
{
    os << "DUMP_CHUNKTOSERVERMAP" << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n\r\n";
}

void
DumpChunkMapOp::Request(ostream &os)
{
    os << "DUMP_CHUNKMAP" << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n\r\n";
}

void
UpServersOp::Request(ostream &os)
{
    os << "UPSERVERS" << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n\r\n";
}

void
ReaddirPlusOp::Request(ostream &os)
{
    os << "READDIRPLUS " << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Directory File-handle: " << fid << "\r\n\r\n";
}

void
GetDirSummaryOp::Request(ostream &os)
{
    os << "GETDIRSUMMARY " << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Directory File-handle: " << fid << "\r\n\r\n";
}

void
RemoveOp::Request(ostream &os)
{
    os << "REMOVE " << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Pathname: " << pathname << "\r\n";
    os << "Parent File-handle: " << parentFid << "\r\n";
    os << "Filename: " << filename << "\r\n\r\n";
}

void
LookupOp::Request(ostream &os)
{
    os << "LOOKUP\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Parent File-handle: " << parentFid << "\r\n";
    os << "Filename: " << filename << "\r\n\r\n";
}

void
LookupPathOp::Request(ostream &os)
{
    os << "LOOKUP_PATH\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Root File-handle: " << rootFid << "\r\n";
    os << "Pathname: " << filename << "\r\n\r\n";
}

void
GetAllocOp::Request(ostream &os)
{
    assert(fileOffset >= 0);

    os << "GETALLOC\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Pathname: " << filename << "\r\n";
    os << "File-handle: " << fid << "\r\n";
    os << "Chunk-offset: " << fileOffset << "\r\n\r\n";
}


void
GetLayoutOp::Request(ostream &os)
{
    os << "GETLAYOUT\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "File-handle: " << fid << "\r\n\r\n";
}

void
CoalesceBlocksOp::Request(ostream &os)
{
    os << "COALESCE_BLOCKS\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Src-path: " << srcPath << "\r\n";
    os << "Dest-path: " << dstPath << "\r\n\r\n";
}

void
GetChunkMetadataOp::Request(ostream &os)
{
    os << "GET_CHUNK_METADATA\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n\r\n";
}

void
GetChunkIndexOp::Request(ostream &os)
{
    os << "GET_CHUNK_INDEX\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n\r\n";
}

void
AllocateOp::Request(ostream &os)
{
    static const int MAXHOSTNAMELEN = 256;
    static char *myIp = NULL;

    if (! myIp) {
        char hostname[MAXHOSTNAMELEN];
        gethostname(hostname, MAXHOSTNAMELEN);
        // Convert to IP: chunkserver location is tracked using IP not hostname;
        // Providing IP helps metaserver to do locality
        struct hostent *hent = gethostbyname(hostname);
        if (hent) {
            in_addr ipaddr;
            memcpy(&ipaddr, hent->h_addr, hent->h_length);
            myIp = inet_ntoa(ipaddr);
        }
    }

    os << "ALLOCATE\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    if (myIp)
        os << "Client-host: " << myIp << "\r\n";
    os << "Client-rack: " << rackId << "\r\n";
    os << "Pathname: " << pathname << "\r\n";
    os << "File-handle: " << fid << "\r\n";
    os << "Chunk-offset: " << fileOffset << "\r\n";
    if (append) {
        os << "Chunk-append: 1\r\n";
        os << "Space-reserve: " << spaceReservationSize << "\r\n";
        os << "Max-appenders: " << maxAppendersPerChunk << "\r\n";
    }
    os << "\r\n";
}

void
TruncateOp::Request(ostream &os)
{
    os << "TRUNCATE\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Pathname: " << pathname << "\r\n";
    if (pruneBlksFromHead)
        os << "Prune-from-head: 1\r\n";
    os << "File-handle: " << fid << "\r\n";
    os << "Offset: " << fileOffset << "\r\n\r\n";
}

void
OpenOp::Request(ostream &os)
{
    const char *modeStr;

    os << "OPEN\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n";
    if (openFlags == O_RDONLY)
	modeStr = "READ";
    else {
	assert(openFlags == O_WRONLY || openFlags == O_RDWR);
	modeStr = "WRITE";
    }
    os << "Intent: " << modeStr << "\r\n\r\n";
}

void
CloseOp::Request(ostream &os)
{
    os <<
        "CLOSE\r\n"
        "Cseq: "         << seq             << "\r\n"
        "Version: "      << KFS_VERSION_STR << "\r\n"
        "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n"
        "Chunk-handle: " << chunkId         << "\r\n"
    ;
    if (! writeInfo.empty()) {
        os <<
            "Has-write-id: 1\r\n"
            "Num-servers: "  << writeInfo.size() << "\r\n"
            "Servers:"
        ;
        for (vector<WriteInfo>::const_iterator i = writeInfo.begin();
                i < writeInfo.end(); ++i) {
	    os << " " << i->serverLoc.ToString() << " " << i->writeId;
        }
        os << "\r\n";
    } else if (chunkServerLoc.size() > 1) {
        os <<
            "Num-servers: " << chunkServerLoc.size() << "\r\n"
            "Servers:"
        ;
        for (vector<ServerLocation>::const_iterator i = chunkServerLoc.begin();
                i != chunkServerLoc.end(); ++i) {
            os << " " << i->ToString();
        }
        os << "\r\n";
    }
    os << "\r\n";
}

void
ReadOp::Request(ostream &os)
{
    os << "READ\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n";
    os << "Chunk-version: " << chunkVersion << "\r\n";
    os << "Offset: " << offset << "\r\n";
    os << "Num-bytes: " << numBytes << "\r\n\r\n";
}

void
WriteIdAllocOp::Request(ostream &os)
{
    os << "WRITE_ID_ALLOC\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n";
    os << "Chunk-version: " << chunkVersion << "\r\n";
    os << "Offset: " << offset << "\r\n";
    os << "Num-bytes: " << numBytes << "\r\n";
    if (isForRecordAppend)
        os << "For-record-append: " << 1 << "\r\n";
    else
        os << "For-record-append: " << 0 << "\r\n";
    os << "Num-servers: " << chunkServerLoc.size() << "\r\n";
    os << "Servers:";
    for (vector<ServerLocation>::size_type i = 0; i < chunkServerLoc.size(); ++i) {
        os << chunkServerLoc[i].ToString().c_str() << ' ';
    }
    os << "\r\n\r\n";
}

void
ChunkSpaceReserveOp::Request(ostream &os)
{
    os << "CHUNK_SPACE_RESERVE\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n";
    os << "Chunk-version: " << chunkVersion << "\r\n";
    os << "Num-bytes: " << numBytes << "\r\n";
    os << "Num-servers: " << writeInfo.size() << "\r\n";
    os << "Servers:";
    for (vector<WriteInfo>::size_type i = 0; i < writeInfo.size(); ++i) {
	os << writeInfo[i].serverLoc.ToString().c_str();
	os << ' ' << writeInfo[i].writeId << ' ';
    }
    os << "\r\n\r\n";
}

void
ChunkSpaceReleaseOp::Request(ostream &os)
{
    os << "CHUNK_SPACE_RELEASE\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n";
    os << "Chunk-version: " << chunkVersion << "\r\n";
    os << "Num-bytes: " << numBytes << "\r\n";
    os << "Num-servers: " << writeInfo.size() << "\r\n";
    os << "Servers:";
    for (vector<WriteInfo>::size_type i = 0; i < writeInfo.size(); ++i) {
	os << writeInfo[i].serverLoc.ToString().c_str();
	os << ' ' << writeInfo[i].writeId << ' ';
    }
    os << "\r\n\r\n";
}

void
WritePrepareOp::Request(ostream &os)
{
    os << "WRITE_PREPARE\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n";
    os << "Chunk-version: " << chunkVersion << "\r\n";
    os << "Offset: " << offset << "\r\n";
    os << "Num-bytes: " << numBytes << "\r\n";
    // one checksum over the whole data
    os << "Checksum: " << checksum << "\r\n";
    // one checksum per 64K block
    os << "Checksum-entries: " << checksums.size() << "\r\n";
    if (checksums.size() > 0) {
        os << "Checksums: ";
        for (uint32_t i = 0; i < checksums.size(); i++) {
            os << checksums[i] << ' ';
        }
        os << "\r\n";
    }
    os << "Num-servers: " << writeInfo.size() << "\r\n";
    os << "Servers:";
    for (vector<WriteInfo>::size_type i = 0; i < writeInfo.size(); ++i) {
	os << writeInfo[i].serverLoc.ToString().c_str();
	os << ' ' << writeInfo[i].writeId << ' ';
    }
    os << "\r\n\r\n";
}

void
WriteSyncOp::Request(ostream &os)
{
    os << "WRITE_SYNC\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n";
    os << "Chunk-version: " << chunkVersion << "\r\n";
    os << "Offset: " << offset << "\r\n";
    os << "Num-bytes: " << numBytes << "\r\n";
    os << "Checksum-entries: " << checksums.size() << "\r\n";
    if (checksums.size() > 0) {
        os << "Checksums: ";
        for (uint32_t i = 0; i < checksums.size(); i++) {
            os << checksums[i] << ' ';
        }
        os << "\r\n";
    }
    os << "Num-servers: " << writeInfo.size() << "\r\n";
    os << "Servers:";
    for (vector<WriteInfo>::size_type i = 0; i < writeInfo.size(); ++i) {
	os << writeInfo[i].serverLoc.ToString().c_str();
	os << ' ' << writeInfo[i].writeId << ' ';
    }
    os << "\r\n\r\n";
}

void
SizeOp::Request(ostream &os)
{
    os << "SIZE\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n";
    os << "Chunk-version: " << chunkVersion << "\r\n\r\n";
}

void
LeaseAcquireOp::Request(ostream &os)
{
    os << "LEASE_ACQUIRE\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Pathname: " << pathname << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n\r\n";
}

void
LeaseRenewOp::Request(ostream &os)
{
    os << "LEASE_RENEW\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Pathname: " << pathname << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n";
    os << "Lease-id: " << leaseId << "\r\n";
    os << "Lease-type: READ_LEASE" << "\r\n\r\n";
}

void
LeaseRelinquishOp::Request(ostream &os)
{
    os << "LEASE_RELINQUISH\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Chunk-handle:" << chunkId << "\r\n";
    os << "Lease-id: " << leaseId << "\r\n";
    os << "Lease-type: READ_LEASE" << "\r\n\r\n";
}

void
RecordAppendOp::Request(ostream &os)
{
    os << "RECORD_APPEND\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n";
    os << "Chunk-version: " << chunkVersion << "\r\n";
    os << "Num-bytes: " << contentLength + keyLength << "\r\n";
    os << "Key-length: " << keyLength << "\r\n";
    os << "Checksum: " << checksum << "\r\n";
    os << "Offset: " << offset << "\r\n";
    os << "File-offset: -1\r\n";
    os << "Num-servers: " << writeInfo.size() << "\r\n";
    os << "Servers:";
    for (vector<WriteInfo>::size_type i = 0; i < writeInfo.size(); ++i) {
	os << writeInfo[i].serverLoc.ToString().c_str();
	os << ' ' << writeInfo[i].writeId << ' ';
    }
    os << "\r\n\r\n";
}

void
GetRecordAppendOpStatus::Request(ostream &os)
{
    os <<
        "GET_RECORD_APPEND_OP_STATUS\r\n"
        "Cseq: "          << seq     << "\r\n"
        "Chunk-handle: "  << chunkId << "\r\n"
        "Version: " << KFS_VERSION_STR << "\r\n"
        "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n"
        "Write-id: "      << writeId << "\r\n"
    "\r\n";
}

void
GetRecordAppendOpStatus::ParseResponseHeaderSelf(const Properties &prop)
{
    chunkVersion        = prop.getValue("Chunk-version",               (int64_t)-1);
    opSeq               = prop.getValue("Op-seq",                      (int64_t)-1);
    opStatus            = prop.getValue("Op-status",                   -1);
    opOffset            = prop.getValue("Op-offset",                   (int64_t)-1);
    opLength            = (size_t)prop.getValue("Op-length",           (uint64_t)0);     
    widAppendCount      = (size_t)prop.getValue("Wid-append-count",    (uint64_t)0);  
    widBytesReserved    = (size_t)prop.getValue("Wid-bytes-reserved",  (uint64_t)0); 
    chunkBytesReserved  = (size_t)prop.getValue("Chunk-bytes-reserved",(uint64_t)0);
    remainingLeaseTime  = prop.getValue("Remaining-lease-time",        (int64_t)-1);
    widWasReadOnlyFlag  = prop.getValue("Wid-was-read-only",            0) != 0;
    masterFlag          = prop.getValue("Chunk-master",                 0) != 0;
    stableFlag          = prop.getValue("Stable-flag",                  0) != 0;
    openForAppendFlag   = prop.getValue("Open-for-append-flag",         0) != 0;
    appenderState       = prop.getValue("Appender-state",               -1);
    appenderStateStr    = prop.getValue("Appender-state-string",        "");
    masterCommitOffset  = prop.getValue("Master-commit-offset",         (int64_t)-1);
    nextCommitOffset    = prop.getValue("Next-commit-offset",           (int64_t)-1);
    widReadOnlyFlag     = prop.getValue("Wid-read-only",                 0) != 0;
}

void
ChangeFileReplicationOp::Request(ostream &os)
{
    os << "CHANGE_FILE_REPLICATION\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Client-Protocol-Version: " << KFS_CLIENT_PROTO_VERS << "\r\n";
    os << "File-handle: " << fid << "\r\n";
    os << "Num-replicas: " << numReplicas << "\r\n\r\n";
}


///
/// Handlers to parse a response sent by the server.  The model is
/// similar to what is done by ChunkServer/metaserver: put the
/// response into a properties object and then extract out the values.
///
/// \brief Parse the response common to all RPC requests.
/// @param[in] resp: a string consisting of header/value pairs in
/// which header/value is separated by a ':' character.
/// @param[out] prop: a properties object that contains the result of
/// parsing.
///
void
KfsOp::ParseResponseHeader(std::istream& is)
{
    const char separator = ':';
    Properties prop;
    prop.loadProperties(is, separator, false);
    ParseResponseHeader(prop);
}

void
KfsOp::ParseResponseHeader(const Properties &prop)
{
    // kfsSeq_t resSeq = prop.getValue("Cseq", (kfsSeq_t) -1);
    status = prop.getValue("Status", -1);
    contentLength = prop.getValue("Content-length", 0);
    statusMsg = prop.getValue("Status-message", string());
    ParseResponseHeaderSelf(prop);
}

///
/// Default parse response handler.
/// @param[in] buf: buffer containing the response
/// @param[in] len: str-len of the buffer.
void
KfsOp::ParseResponseHeaderSelf(const Properties &prop)
{
}

///
/// Specific response parsing handlers.
///
void
CreateOp::ParseResponseHeaderSelf(const Properties &prop)
{
    fileId = prop.getValue("File-handle", (kfsFileId_t) -1);
}

void
ReaddirOp::ParseResponseHeaderSelf(const Properties &prop)
{
    numEntries = prop.getValue("Num-Entries", 0);
}

void
GetDirSummaryOp::ParseResponseHeaderSelf(const Properties &prop)
{
    numFiles = prop.getValue("Num-files", 0);
    numBytes = prop.getValue("Num-bytes", 0);
}

void
DumpChunkServerMapOp::ParseResponseHeaderSelf(const Properties &prop)
{
}

void
DumpChunkMapOp::ParseResponseHeaderSelf(const Properties &prop)
{
}

void
UpServersOp::ParseResponseHeaderSelf(const Properties &prop)
{
}

void
ReaddirPlusOp::ParseResponseHeaderSelf(const Properties &prop)
{
    numEntries = prop.getValue("Num-Entries", 0);
}

void
MkdirOp::ParseResponseHeaderSelf(const Properties &prop)
{
    fileId = prop.getValue("File-handle", (kfsFileId_t) -1);
}

void
LookupOp::ParseResponseHeaderSelf(const Properties &prop)
{
    string s;

    fattr.fileId = prop.getValue("File-handle", (kfsFileId_t) -1);
    s = prop.getValue("Type", "");
    fattr.isDirectory = (s == "dir");
    fattr.chunkCount = prop.getValue("Chunk-count", 0);
    fattr.fileSize = prop.getValue("File-size", (off_t) -1);
    fattr.numReplicas = prop.getValue("Replication", 1);
    s = prop.getValue("M-Time", "");
    GetTimeval(s, fattr.mtime);

    s = prop.getValue("C-Time", "");
    GetTimeval(s, fattr.ctime);

    s = prop.getValue("CR-Time", "");
    GetTimeval(s, fattr.crtime);
}

void
LookupPathOp::ParseResponseHeaderSelf(const Properties &prop)
{
    string s;
    istringstream ist;

    fattr.fileId = prop.getValue("File-handle", (kfsFileId_t) -1);
    s = prop.getValue("Type", "");
    fattr.isDirectory = (s == "dir");
    fattr.fileSize = prop.getValue("File-size", (off_t) -1);
    fattr.chunkCount = prop.getValue("Chunk-count", 0);
    fattr.numReplicas = prop.getValue("Replication", 1);

    s = prop.getValue("M-Time", "");
    GetTimeval(s, fattr.mtime);

    s = prop.getValue("C-Time", "");
    GetTimeval(s, fattr.ctime);

    s = prop.getValue("CR-Time", "");
    GetTimeval(s, fattr.crtime);
}

void
AllocateOp::ParseResponseHeaderSelf(const Properties &prop)
{
    chunkId = prop.getValue("Chunk-handle", (kfsFileId_t) -1);
    chunkVersion = prop.getValue("Chunk-version", (int64_t) -1);
    if (append)
        fileOffset = prop.getValue("Chunk-offset", (off_t) 0);

    string master = prop.getValue("Master", "");
    if (master != "") {
	istringstream ist(master);

	ist >> masterServer.hostname;
	ist >> masterServer.port;
	// put the master the first in the list
	chunkServers.push_back(masterServer);
    }

    int numReplicas = prop.getValue("Num-replicas", 0);
    string replicas = prop.getValue("Replicas", "");

    if (replicas != "") {
	istringstream ser(replicas);
	ServerLocation loc;

	for (int i = 0; i < numReplicas; ++i) {
	    ser >> loc.hostname;
	    ser >> loc.port;
	    if (loc != masterServer)
		chunkServers.push_back(loc);
	}
    }
}

void
GetAllocOp::ParseResponseHeaderSelf(const Properties &prop)
{
    chunkId = prop.getValue("Chunk-handle", (kfsFileId_t) -1);
    chunkVersion = prop.getValue("Chunk-version", (int64_t) -1);

    int numReplicas = prop.getValue("Num-replicas", 0);
    string replicas = prop.getValue("Replicas", "");
    if (replicas != "") {
	istringstream ser(replicas);
	ServerLocation loc;

	for (int i = 0; i < numReplicas; ++i) {
	    ser >> loc.hostname;
	    ser >> loc.port;
	    chunkServers.push_back(loc);
	}
    }
}

void
CoalesceBlocksOp::ParseResponseHeaderSelf(const Properties &prop)
{
    dstStartOffset = prop.getValue("Dst-start-offset", (off_t) 0);
}

void
GetLayoutOp::ParseResponseHeaderSelf(const Properties &prop)
{
    numChunks = prop.getValue("Num-chunks", 0);
}

int
GetLayoutOp::ParseLayoutInfo()
{
    if (numChunks == 0 || contentBuf == NULL)
	return 0;

    istringstream ist(contentBuf);
    for (int i = 0; i < numChunks; ++i) {
	ChunkLayoutInfo l;
	ServerLocation s;
	int numServers;

	ist >> l.fileOffset;
	ist >> l.chunkId;
	ist >> l.chunkVersion;
	ist >> numServers;
	for (int j = 0; j < numServers; j++) {
	    ist >> s.hostname;
	    ist >> s.port;
	    l.chunkServers.push_back(s);
	}
	chunks.push_back(l);
    }
    return 0;
}

void
SizeOp::ParseResponseHeaderSelf(const Properties &prop)
{
    size = prop.getValue("Size", (long long) 0);
}

void
GetChunkMetadataOp::ParseResponseHeaderSelf(const Properties &prop)
{
    chunkSize = prop.getValue("Size", (int) 0);
    indexSize = prop.getValue("Index-size", (int) 0);
}

void
GetChunkIndexOp::ParseResponseHeaderSelf(const Properties &prop)
{
    indexSize = prop.getValue("Content-length", (int) 0);
}

void
ReadOp::ParseResponseHeaderSelf(const Properties &prop)
{
    string checksumStr;
    uint32_t nentries;

    nentries = prop.getValue("Checksum-entries", 0);
    checksumStr = prop.getValue("Checksums", "");
    diskIOTime = prop.getValue("DiskIOtime", 0.0);
    drivename = prop.getValue("Drivename", "");
    istringstream ist(checksumStr);
    checksums.clear();
    for (uint32_t i = 0; i < nentries; i++) {
        uint32_t cksum;
        ist >> cksum;
        checksums.push_back(cksum);
    }
}

void
WriteIdAllocOp::ParseResponseHeaderSelf(const Properties &prop)
{
    writeIdStr = prop.getValue("Write-id", "");
}

void
LeaseAcquireOp::ParseResponseHeaderSelf(const Properties &prop)
{
    leaseId = prop.getValue("Lease-id", (long long) -1);
}

void
ChangeFileReplicationOp::ParseResponseHeaderSelf(const Properties &prop)
{
    numReplicas = prop.getValue("Num-replicas", 1);
}

