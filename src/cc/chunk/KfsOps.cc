//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: KfsOps.cc 3369 2011-11-28 20:05:28Z sriramr $
//
// Created 2006/05/26
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
// Code for parsing commands sent to the Chunkserver and generating
// responses that summarize the result of their execution.
//
// 
//----------------------------------------------------------------------------

#include "KfsOps.h"
#include "common/Version.h"
#include "common/kfstypes.h"
#include "libkfsIO/Globals.h"
#include "meta/thread.h"
#include "meta/queue.h"
#include "libkfsIO/Checksum.h"

#include "ChunkManager.h"
#include "Logger.h"
#include "ChunkServer.h"
#include "LeaseClerk.h"
#include "Replicator.h"
#include "AtomicRecordAppender.h"
#include "Utils.h"

#include <algorithm>
#include <boost/lexical_cast.hpp>
#include <stdlib.h>
#include <sys/resource.h>

using std::map;
using std::string;
using std::ofstream;
using std::ifstream;
using std::istringstream;
using std::ostringstream;
using std::istream;
using std::ostream;
using std::for_each;
using std::vector;
using std::min;

using namespace KFS;
using namespace KFS::libkfsio;

typedef int (*ParseHandler)(Properties &, KfsOp **);

/// command -> parsehandler map
typedef map<string, ParseHandler> ParseHandlerMap;
typedef map<string, ParseHandler>::iterator ParseHandlerMapIter;

// handlers for parsing
ParseHandlerMap	gParseHandlers;

// Counters for the various ops
static struct OpCounterMap : public map<KfsOp_t, Counter *>
{
    OpCounterMap()
        : map<KfsOp_t, Counter *>(),
          mWriteMaster("Write Master"),
          mWriteDuration("Write Duration")
      {}
    ~OpCounterMap()
    {
        for (iterator i = begin(); i != end(); ++i) {
            globals().counterManager.RemoveCounter(i->second);
            delete i->second;
        }
        globals().counterManager.RemoveCounter(&mWriteMaster);
        globals().counterManager.RemoveCounter(&mWriteDuration);
    }
    Counter mWriteMaster;
    Counter mWriteDuration;
} gCounters;
typedef OpCounterMap::iterator OpCounterMapIter;


const char *KFS_VERSION_STR = "KFS/1.0";

// various parse handlers
int parseHandlerOpen(Properties &prop, KfsOp **c);
int parseHandlerClose(Properties &prop, KfsOp **c);
int parseHandlerRead(Properties &prop, KfsOp **c);
int parseHandlerWriteIdAlloc(Properties &prop, KfsOp **c);
int parseHandlerWritePrepare(Properties &prop, KfsOp **c);
int parseHandlerWriteSync(Properties &prop, KfsOp **c);
int parseHandlerSize(Properties &prop, KfsOp **c);
int parseHandlerRecordAppend(Properties &prop, KfsOp **c);
int parseHandlerGetRecordAppendStatus(Properties &prop, KfsOp **c);
int parseHandlerChunkSpaceReserve(Properties &prop, KfsOp **c);
int parseHandlerChunkSpaceRelease(Properties &prop, KfsOp **c);
int parseHandlerGetChunkMetadata(Properties &prop, KfsOp **c);
int parseHandlerGetChunkIndex(Properties &prop, KfsOp **c);
int parseHandlerAllocChunk(Properties &prop, KfsOp **c);
int parseHandlerDeleteChunk(Properties &prop, KfsOp **c);
int parseHandlerTruncateChunk(Properties &prop, KfsOp **c);
int parseHandlerReplicateChunk(Properties &prop, KfsOp **c);
int parseHandlerBeginMakeChunkStableOp(Properties &prop, KfsOp **c);
int parseHandlerMakeChunkStable(Properties &prop, KfsOp **c);
int parseHandlerCoalesceBlock(Properties &prop, KfsOp **c);
int parseHandlerHeartbeat(Properties &prop, KfsOp **c);
int parseHandlerChangeChunkVers(Properties &prop, KfsOp **c);
int parseHandlerStaleChunks(Properties &prop, KfsOp **c);
int parseHandlerRetire(Properties &prop, KfsOp **c);
int parseHandlerPing(Properties &prop, KfsOp **c);
int parseHandlerDumpChunkMap(Properties &prop, KfsOp **c);
int parseHandlerStats(Properties &prop, KfsOp **c);
int parseHandlerSetProperties(Properties &prop, KfsOp **c);
int parseRestartChunkServer(Properties &prop, KfsOp **c);
int parseHandlerSortFile(Properties &prop, KfsOp **c);
int parseHandlerSorterFlush(Properties &prop, KfsOp **c);

static bool
needToForwardToPeer(string &serverInfo, uint32_t numServers, int &myPos,
                    ServerLocation &peerLoc, 
                    bool isWriteIdPresent, int64_t &writeId);

static TimeoutOp timeoutOp(0);

void
KFS::SubmitOp(KfsOp *op)
{
    op->type = OP_REQUEST;
    op->Execute();
}

void
KFS::SubmitOpResponse(KfsOp *op)
{
    op->type = OP_RESPONSE;
    op->HandleEvent(EVENT_CMD_DONE, op);
}

void
KFS::verifyExecutingOnEventProcessor()
{
    return;
}


void
KFS::InitParseHandlers()
{
    gParseHandlers["OPEN"] = parseHandlerOpen;
    gParseHandlers["CLOSE"] = parseHandlerClose;
    gParseHandlers["READ"] = parseHandlerRead;
    gParseHandlers["WRITE_ID_ALLOC"] = parseHandlerWriteIdAlloc;
    gParseHandlers["WRITE_PREPARE"] = parseHandlerWritePrepare;
    gParseHandlers["WRITE_SYNC"] = parseHandlerWriteSync;
    gParseHandlers["SIZE"] = parseHandlerSize;
    gParseHandlers["RECORD_APPEND"] = parseHandlerRecordAppend;
    gParseHandlers["GET_RECORD_APPEND_OP_STATUS"] = parseHandlerGetRecordAppendStatus;
    gParseHandlers["CHUNK_SPACE_RESERVE"] = parseHandlerChunkSpaceReserve;
    gParseHandlers["CHUNK_SPACE_RELEASE"] = parseHandlerChunkSpaceRelease;
    gParseHandlers["GET_CHUNK_METADATA"] = parseHandlerGetChunkMetadata;
    gParseHandlers["GET_CHUNK_INDEX"] = parseHandlerGetChunkIndex;
    gParseHandlers["ALLOCATE"] = parseHandlerAllocChunk;
    gParseHandlers["DELETE"] = parseHandlerDeleteChunk;
    gParseHandlers["TRUNCATE"] = parseHandlerTruncateChunk;
    gParseHandlers["REPLICATE"] = parseHandlerReplicateChunk;
    gParseHandlers["HEARTBEAT"] = parseHandlerHeartbeat;
    gParseHandlers["STALE_CHUNKS"] = parseHandlerStaleChunks;
    gParseHandlers["CHUNK_VERS_CHANGE"] = parseHandlerChangeChunkVers;
    gParseHandlers["BEGIN_MAKE_CHUNK_STABLE"] = parseHandlerBeginMakeChunkStableOp;
    gParseHandlers["MAKE_CHUNK_STABLE"] = parseHandlerMakeChunkStable;
    gParseHandlers["COALESCE_BLOCK"] = parseHandlerCoalesceBlock;
    gParseHandlers["RETIRE"] = parseHandlerRetire;
    gParseHandlers["PING"] = parseHandlerPing;
    gParseHandlers["DUMP_CHUNKMAP"] = parseHandlerDumpChunkMap;
    gParseHandlers["STATS"] = parseHandlerStats;
    gParseHandlers["CMD_SET_PROPERTIES"] = &parseHandlerSetProperties;
    gParseHandlers["RESTART_CHUNK_SERVER"] = &parseRestartChunkServer;
    gParseHandlers["SORTER_FLUSH"] = &parseHandlerSorterFlush;
    gParseHandlers["SORT_FILE"] = &parseHandlerSortFile;
}

static void
AddCounter(const char *name, KfsOp_t opName)
{
    Counter *c;
    
    c = new Counter(name);
    globals().counterManager.AddCounter(c);
    gCounters[opName] = c;
}

void
KFS::RegisterCounters()
{
    static int calledOnce = 0;
    if (calledOnce)
        return;

    calledOnce = 1;
    AddCounter("Open", CMD_OPEN);
    AddCounter("Read", CMD_READ);
    AddCounter("Write Prepare", CMD_WRITE_PREPARE);
    AddCounter("Write Sync", CMD_WRITE_SYNC);
    AddCounter("Write (AIO)", CMD_WRITE);
    AddCounter("Size", CMD_SIZE);
    AddCounter("Record append", CMD_RECORD_APPEND);
    AddCounter("Space reserve", CMD_SPC_RESERVE);
    AddCounter("Space release", CMD_SPC_RELEASE);
    AddCounter("Get Chunk Metadata", CMD_GET_CHUNK_METADATA);
    AddCounter("Get Chunk Index", CMD_GET_CHUNK_INDEX);
    AddCounter("Alloc", CMD_ALLOC_CHUNK);
    AddCounter("Delete", CMD_DELETE_CHUNK);
    AddCounter("Truncate", CMD_TRUNCATE_CHUNK);
    AddCounter("Replicate", CMD_REPLICATE_CHUNK);
    AddCounter("Heartbeat", CMD_HEARTBEAT);
    AddCounter("Change Chunk Vers", CMD_CHANGE_CHUNK_VERS);
    AddCounter("Make Chunk Stable", CMD_MAKE_CHUNK_STABLE);
    AddCounter("Coalesce Block", CMD_COALESCE_BLOCK);

    globals().counterManager.AddCounter(&gCounters.mWriteMaster);
    globals().counterManager.AddCounter(&gCounters.mWriteDuration);
}

static void
UpdateCounter(KfsOp_t opName)
{
    Counter *c;
    OpCounterMapIter iter;
    
    iter = gCounters.find(opName);
    if (iter == gCounters.end())
        return;
    c = iter->second;
    c->Update(1);
}

#if 0
static void
DecrementCounter(KfsOp_t opName)
{
    Counter *c;
    OpCounterMapIter iter;
    
    iter = gCounters.find(opName);
    if (iter == gCounters.end())
        return;
    c = iter->second;
    c->Update(-1);
}
#endif


static void
UpdateMsgProcessingTime(const KfsOp *op) 
{
    struct timeval timeNow;
    float timeSpent;

    gettimeofday(&timeNow, NULL);

    timeSpent = ComputeTimeDiff(op->startTime, timeNow);

    OpCounterMapIter iter = gCounters.find(op->op);
    if (iter == gCounters.end())
        return;
    Counter *c = iter->second;
    c->Update(timeSpent);
}

KfsOp::~KfsOp()
{
    UpdateMsgProcessingTime(this);
}

///
/// Given a command in a buffer, parse it out and build a "Command"
/// structure which can then be executed.  For parsing, we take the
/// string representation of a command and build a Properties object
/// out of it; we can then pull the various headers in whatever order
/// we choose.
/// Commands are of the form:
/// <COMMAND NAME> \r\n
/// {header: value \r\n}+\r\n
///
///  The general model in parsing the client command:
/// 1. Each command has its own parser
/// 2. Extract out the command name and find the parser for that
/// command
/// 3. Dump the header/value pairs into a properties object, so that we
/// can extract the header/value fields in any order.
/// 4. Finally, call the parser for the command sent by the client.
///
/// @param[in] cmdBuf: buffer containing the request sent by the client
/// @param[in] cmdLen: length of cmdBuf
/// @param[out] res: A piece of memory allocated by calling new that
/// contains the data for the request.  It is the caller's
/// responsibility to delete the memory returned in res.
/// @retval 0 on success;  -1 if there is an error
/// 
int
KFS::ParseCommand(std::istream& is, KfsOp **res)
{
    const char *delims = " \r\n";
    // header/value pairs are separated by a :
    const char separator = ':';
    string cmdStr;
    string::size_type cmdEnd;
    Properties prop;
    ParseHandlerMapIter entry;
    ParseHandler handler;
    
    // get the first line and find the command name
    is >> cmdStr;
    // trim the command
    cmdEnd = cmdStr.find_first_of(delims);
    if (cmdEnd != string::npos) {
        cmdStr.erase(cmdEnd);
    }
    
    // find the parse handler and parse the thing
    entry = gParseHandlers.find(cmdStr);
    if (entry == gParseHandlers.end())
        return -1;
    handler = entry->second;

    prop.loadProperties(is, separator, false);

    return (*handler)(prop, res);
}

void
parseCommon(Properties &prop, kfsSeq_t &seq)
{
    seq = prop.getValue("Cseq", (kfsSeq_t) -1);
}

int
parseHandlerOpen(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    OpenOp *oc;
    string openMode;

    parseCommon(prop, seq);

    oc = new OpenOp(seq);
    oc->chunkId = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    openMode = prop.getValue("Intent", "");
    // XXX: need to do a string compare
    oc->openFlags = O_RDWR;
    *c = oc;

    return 0;
}

int
parseHandlerClose(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    CloseOp *cc;

    parseCommon(prop, seq);

    cc = new CloseOp(seq);
    cc->chunkId         = prop.getValue("Chunk-handle", (kfsChunkId_t)-1);
    cc->numServers      = prop.getValue("Num-servers", 0);
    cc->servers         = prop.getValue("Servers", "");
    cc->needAck         = prop.getValue("Need-ack", 1) != 0;
    cc->hasWriteId      = prop.getValue("Has-write-id", 0) != 0;
    cc->masterCommitted = prop.getValue("Master-committed", (int64_t)-1);

    *c = cc;

    return 0;
}

int
parseHandlerRead(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    ReadOp *rc;

    parseCommon(prop, seq);

    rc = new ReadOp(seq);
    rc->chunkId = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    rc->chunkVersion = prop.getValue("Chunk-version", (int64_t) -1);
    rc->offset = prop.getValue("Offset", (off_t) 0);
    rc->numBytes = prop.getValue("Num-bytes", (long long) 0);
    if (rc->numBytes > CHUNKSIZE)
        rc->numBytes = 131072;
    *c = rc;

    return 0;
}

int
parseHandlerWriteIdAlloc(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    WriteIdAllocOp *wi;

    parseCommon(prop, seq);

    wi = new WriteIdAllocOp(seq);
    wi->chunkId = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    wi->chunkVersion = prop.getValue("Chunk-version", (int64_t) -1);
    wi->offset = prop.getValue("Offset", (off_t) 0);
    wi->numBytes = prop.getValue("Num-bytes", (long long) 0);
    wi->numServers = prop.getValue("Num-servers", 0);
    wi->servers = prop.getValue("Servers", "");
    // is the write-id allocation for a record append that is going to follow
    wi->isForRecordAppend = (prop.getValue("For-record-append", 0) != 0);
    wi->clientSeq = prop.getValue("Client-cseq", wi->seq);

    *c = wi;

    return 0;
}

int
parseHandlerWritePrepare(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    WritePrepareOp *wp;

    parseCommon(prop, seq);

    wp = new WritePrepareOp(seq);
    wp->chunkId = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    wp->chunkVersion = prop.getValue("Chunk-version", (int64_t) -1);
    wp->offset = prop.getValue("Offset", (off_t) 0);
    wp->numBytes = prop.getValue("Num-bytes", (long long) 0);
    wp->numServers = prop.getValue("Num-servers", 0);
    wp->servers = prop.getValue("Servers", "");
    wp->checksum = (uint32_t) prop.getValue("Checksum", (off_t) 0);

    uint32_t nentries = prop.getValue("Checksum-entries", 0);
    if (nentries > 0) {
        string checksumStr = prop.getValue("Checksums", "");
        istringstream ist(checksumStr);
        for (uint32_t i = 0; i < nentries; i++) {
            uint32_t cksum;
            ist >> cksum;
            wp->checksums.push_back(cksum);
        }
    }

    *c = wp;

    return 0;
}

int
parseHandlerRecordAppend(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    RecordAppendOp *ra;

    parseCommon(prop, seq);

    ra = new RecordAppendOp(seq);
    ra->chunkId               = prop.getValue("Chunk-handle",  (kfsChunkId_t)-1);
    ra->chunkVersion          = prop.getValue("Chunk-version", (int64_t)     -1);
    ra->offset                = prop.getValue("Offset",        (off_t)       -1);
    ra->fileOffset            = prop.getValue("File-offset",   (off_t)       -1);
    ra->numBytes              = prop.getValue("Num-bytes",     (long long)    0);
    ra->numServers            = prop.getValue("Num-servers",                  0);
    ra->servers               = prop.getValue("Servers",                     "");
    ra->checksum              = (uint32_t)prop.getValue("Checksum",   (off_t) 0);
    ra->clientSeq             = prop.getValue("Client-cseq",            ra->seq);
    ra->masterCommittedOffset = prop.getValue("Master-committed",     (off_t)-1);
    *c = ra;

    return 0;
}

int
parseHandlerGetRecordAppendStatus(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    GetRecordAppendOpStatus* op;

    parseCommon(prop, seq);
    op = new GetRecordAppendOpStatus(seq);
    op->chunkId      = prop.getValue("Chunk-handle",  (kfsChunkId_t) -1);
    op->chunkVersion = prop.getValue("Chunk-version", (int64_t) -1);
    op->writeId      = prop.getValue("Write-id",      (int64_t) -1);
    *c = op;
    return 0;
}

int
parseHandlerWriteSync(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    WriteSyncOp *ws;
    kfsChunkId_t cid;
    int64_t chunkVers;
    off_t offset;
    size_t numBytes;

    parseCommon(prop, seq);
    cid = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    chunkVers = prop.getValue("Chunk-version", (int64_t) -1);
    // if the user doesn't provide any value, pass
    offset = prop.getValue("Offset", (off_t) 0);
    numBytes = (size_t)prop.getValue("Num-bytes", (int64_t)0);

    ws = new WriteSyncOp(seq, cid, chunkVers, offset, numBytes);
    ws->numServers = prop.getValue("Num-servers", 0);
    ws->servers = prop.getValue("Servers", "");

    uint32_t nentries = prop.getValue("Checksum-entries", 0);
    if (nentries > 0) {
        string checksumStr = prop.getValue("Checksums", "");
        istringstream ist(checksumStr);
        for (uint32_t i = 0; i < nentries; i++) {
            uint32_t cksum;
            ist >> cksum;
            ws->checksums.push_back(cksum);
        }
    }

    *c = ws;

    return 0;
}

int
parseHandlerSize(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    SizeOp *sc;

    parseCommon(prop, seq);

    sc = new SizeOp(seq);
    sc->fileId = prop.getValue("File-handle", (kfsFileId_t) -1);
    sc->chunkId = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    sc->chunkVersion = prop.getValue("Chunk-version", (int64_t) -1);
    *c = sc;

    return 0;
}

int
parseHandlerChunkSpaceReserve(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    ChunkSpaceReserveOp *csr;

    parseCommon(prop, seq);

    csr = new ChunkSpaceReserveOp(seq);
    csr->chunkId = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    csr->nbytes = prop.getValue("Num-bytes", (int) 0);
    csr->numServers = prop.getValue("Num-servers", 0);
    csr->servers = prop.getValue("Servers", "");
    *c = csr;

    return 0;
}

int
parseHandlerChunkSpaceRelease(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    ChunkSpaceReleaseOp *csr;

    parseCommon(prop, seq);

    csr = new ChunkSpaceReleaseOp(seq);
    csr->chunkId = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    csr->nbytes = prop.getValue("Num-bytes", (int) 0);
    csr->numServers = prop.getValue("Num-servers", 0);
    csr->servers = prop.getValue("Servers", "");
    *c = csr;

    return 0;
}

int
parseHandlerGetChunkMetadata(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    GetChunkMetadataOp *gcm;

    parseCommon(prop, seq);

    gcm = new GetChunkMetadataOp(seq);
    gcm->chunkId = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    *c = gcm;

    return 0;
}

int
parseHandlerGetChunkIndex(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    GetChunkIndexOp *gci;

    parseCommon(prop, seq);

    gci = new GetChunkIndexOp(seq);
    gci->chunkId = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    *c = gci;

    return 0;
}

int
parseHandlerAllocChunk(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    AllocChunkOp *cc;

    parseCommon(prop, seq);

    cc = new AllocChunkOp(seq);
    cc->fileId = prop.getValue("File-handle", (kfsFileId_t) -1);
    cc->chunkId = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    cc->chunkVersion = prop.getValue("Chunk-version", (int64_t) -1);
    // if the leaseId is a positive value, then this server has the
    // lease on this chunk.
    cc->leaseId = prop.getValue("Lease-id", (int64_t) -1);
    cc->appendFlag = prop.getValue("Chunk-append", 0) != 0;
    cc->numServers = prop.getValue("Num-servers", 0);
    cc->servers    = prop.getValue("Servers", "");
    *c = cc;

    return 0;
}

int
parseHandlerDeleteChunk(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    DeleteChunkOp *cc;

    parseCommon(prop, seq);

    cc = new DeleteChunkOp(seq);
    cc->chunkId = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    *c = cc;

    return 0;
}

int
parseHandlerTruncateChunk(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    TruncateChunkOp *tc;

    parseCommon(prop, seq);

    tc = new TruncateChunkOp(seq);
    tc->chunkId = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    tc->chunkSize = prop.getValue("Chunk-size", (long long) 0);

    *c = tc;

    return 0;
}

int
parseHandlerReplicateChunk(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    ReplicateChunkOp *rc;
    string s;

    parseCommon(prop, seq);

    rc = new ReplicateChunkOp(seq);
    rc->fid = prop.getValue("File-handle", (kfsFileId_t) -1);
    rc->chunkId = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    rc->chunkVersion = prop.getValue("Chunk-version", (int64_t) -1);
    s = prop.getValue("Chunk-location", "");
    if (s != "") {
        rc->location.FromString(s);
    }

    *c = rc;

    return 0;
}

int
parseHandlerChangeChunkVers(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    ChangeChunkVersOp *rc;

    parseCommon(prop, seq);

    rc = new ChangeChunkVersOp(seq);
    rc->fileId = prop.getValue("File-handle", (kfsFileId_t) -1);
    rc->chunkId = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    rc->chunkVersion = prop.getValue("Chunk-version", (int64_t) -1);

    *c = rc;

    return 0;
}

int
parseHandlerBeginMakeChunkStableOp(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    parseCommon(prop, seq);

    BeginMakeChunkStableOp* const op = new BeginMakeChunkStableOp(seq);
    op->fileId       = prop.getValue("File-handle",   (kfsFileId_t)  -1);
    op->chunkId      = prop.getValue("Chunk-handle",  (kfsChunkId_t) -1);
    op->chunkVersion = prop.getValue("Chunk-version", (int64_t)      -1);

    *c = op;
    return 0;
}

int
parseHandlerMakeChunkStable(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    MakeChunkStableOp *mc;

    parseCommon(prop, seq);

    mc = new MakeChunkStableOp(seq);
    mc->fileId        = prop.getValue("File-handle",   (kfsFileId_t) -1);
    mc->chunkId       = prop.getValue("Chunk-handle",  (kfsChunkId_t)-1);
    mc->chunkVersion  = prop.getValue("Chunk-version", (int64_t)     -1);
    mc->chunkSize     = prop.getValue("Chunk-size",    (int64_t)     -1);
    mc->chunkChecksum = (uint32_t)prop.getValue("Chunk-checksum", (uint64_t)0);
    mc->hasChecksum   = ! prop.getValue("Chunk-checksum", std::string()).empty();

    *c = mc;

    return 0;
}

int
parseHandlerCoalesceBlock(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    CoalesceBlockOp *cb;

    parseCommon(prop, seq);

    cb = new CoalesceBlockOp(seq);
    cb->srcFileId = prop.getValue("Src-File-handle", (kfsFileId_t) -1);
    cb->srcChunkId = prop.getValue("Src-Chunk-handle", (kfsChunkId_t) -1);
    cb->dstFileId = prop.getValue("Dest-File-handle", (kfsFileId_t) -1);
    cb->dstChunkId = prop.getValue("Dest-Chunk-handle", (kfsChunkId_t) -1);

    *c = cb;

    return 0;
}

int
parseHandlerHeartbeat(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    HeartbeatOp *hb;

    parseCommon(prop, seq);

    hb = new HeartbeatOp(seq);
    *c = hb;

    return 0;
}

int
parseHandlerRetire(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;

    parseCommon(prop, seq);

    *c = new RetireOp(seq);

    return 0;
}

int
parseHandlerStaleChunks(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    StaleChunksOp *sc;

    parseCommon(prop, seq);

    sc = new StaleChunksOp(seq);
    sc->contentLength = prop.getValue("Content-length", 0);
    sc->numStaleChunks = prop.getValue("Num-chunks", 0);
    *c = sc;
    return 0;
}

int
parseHandlerPing(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    PingOp *po;

    parseCommon(prop, seq);

    po = new PingOp(seq);
    *c = po;
    return 0;
}

int
parseHandlerDumpChunkMap(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    DumpChunkMapOp *po;

    parseCommon(prop, seq);

    po = new DumpChunkMapOp(seq);
    *c = po;
    return 0;
}

int
parseHandlerStats(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    StatsOp *so;

    parseCommon(prop, seq);

    so = new StatsOp(seq);
    *c = so;
    return 0;
}

int
parseHandlerSetProperties(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    parseCommon(prop, seq);

    SetProperties* const op = new SetProperties(seq);
    op->contentLength = prop.getValue("Content-length", 0);
    *c = op;
    return 0;
}

int parseRestartChunkServer(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    parseCommon(prop, seq);

    *c = new RestartChunkServerOp(seq);
    return 0;
}

int
parseHandlerSorterFlush(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    SorterFlushOp *sf;

    parseCommon(prop, seq);

    sf = new SorterFlushOp(seq);
    sf->fid = prop.getValue("File-handle", (kfsFileId_t) -1);
    sf->chunkId = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    sf->chunkSize = prop.getValue("Chunk-size", (int64_t) -1);
    sf->outputFn = prop.getValue("Output-name", "");
    *c = sf;

    return 0;
}

int
parseHandlerSortFile(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    SortFileOp *sf;

    parseCommon(prop, seq);

    sf = new SortFileOp(seq);
    sf->fid = prop.getValue("File-handle", (kfsFileId_t) -1);
    sf->chunkId = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    sf->chunkSize = prop.getValue("Chunk-size", (int64_t) -1);
    sf->inputFn = prop.getValue("Input-name", "");
    sf->outputFn = prop.getValue("Output-name", "");
    *c = sf;

    return 0;
}

///
/// Generic event handler for tracking completion of an event
/// execution.  Push the op to the logger and the net thread will pick
/// it up and dispatch it.
///
int
KfsOp::HandleDone(int code, void *data)
{
    gLogger.Submit(this);
    return 0;
}

///
/// A read op finished.  Set the status and the # of bytes read
/// alongwith the data and notify the client.
///
int
ReadOp::HandleDone(int code, void *data)
{
    IOBuffer *b;
    off_t chunkSize = 0;

    // DecrementCounter(CMD_READ);

    verifyExecutingOnEventProcessor();

    if (code == EVENT_DISK_ERROR) {
        status = -1;
        if (data != NULL) {
            status = *(int *) data;
            KFS_LOG_STREAM_INFO <<
                "Disk error: errno: " << status << " chunkid: " << chunkId <<
            KFS_LOG_EOM;
        }
        gChunkManager.ChunkIOFailed(chunkId, status);
    }
    else if (code == EVENT_DISK_READ) {
        if (dataBuf == NULL) {
            dataBuf = new IOBuffer();
        }
        b = (IOBuffer *) data;
        // Order matters...when we append b, we take the data from b
        // and put it into our buffer.
        dataBuf->Append(b);
        // verify checksum
        gChunkManager.ReadChunkDone(this);
        numBytesIO = dataBuf->BytesConsumable();
        if (status == 0)
            // checksum verified
            status = numBytesIO;
    }

    if (status >= 0) {
        assert(numBytesIO >= 0);
        if (offset % CHECKSUM_BLOCKSIZE != 0 ||
                numBytesIO % CHECKSUM_BLOCKSIZE != 0) {
            checksum = ComputeChecksums(dataBuf, numBytesIO);
        }
        assert(size_t((numBytesIO + CHECKSUM_BLOCKSIZE - 1) / CHECKSUM_BLOCKSIZE) ==
            checksum.size());
        // send the disk IO time back to client for telemetry reporting
        struct timeval timeNow;

        gettimeofday(&timeNow, NULL);
        diskIOTime = ComputeTimeDiff(startTime, timeNow);
    }
    
    kfsFileId_t dummy;
    gChunkManager.ChunkSize(chunkId, dummy, &chunkSize);

    if (wop != NULL) {
        // if the read was triggered by a write, then resume execution of write
        wop->Execute();
        return 0;
    }

    if ((chunkSize > 0 && (offset + numBytesIO >= (off_t) chunkSize)) &&
        (!gLeaseClerk.IsLeaseValid(chunkId))) {
        // If we have read the full chunk, close out the fd.  The
        // observation is that reads are sequential and when we
        // finished a chunk, the client will move to the next one.
        //
        // Release disk io first for CloseChunk to have effect: normally
        // this method is invoked from io completion routine, and diskIo has a
        // reference to file dataFH.
        // DiskIo completion path doesn't expect diskIo pointer to remain valid
        // upon return.
        diskIo.reset();
        KFS_LOG_STREAM_INFO << "Closing chunk " <<
            chunkId << " and might give up lease" <<
        KFS_LOG_EOM;
        gChunkManager.CloseChunk(chunkId);
    }

    gLogger.Submit(this);

    return 0;
}

int
ReadOp::HandleReplicatorDone(int code, void *data)
{
    if ((status >= 0) && (checksum.size() > 0)) {
        const vector<uint32_t> datacksums = ComputeChecksums(dataBuf, numBytesIO);
        if (datacksums.size() > checksum.size()) {
                    KFS_LOG_STREAM_INFO <<
                        "Checksum number of entries mismatch in re-replication: "
                        " expect: " << datacksums.size() <<
                        " got: " << checksum.size() <<
                    KFS_LOG_EOM;
                    status = -EBADCKSUM;
        } else {
            for (uint32_t i = 0; i < datacksums.size(); i++) {
                if (datacksums[i] != checksum[i]) {
                    KFS_LOG_STREAM_INFO <<
                        "Checksum mismatch in re-replication: "
                        " expect: " << datacksums[i] <<
                        " got: " << checksum[i] <<
                    KFS_LOG_EOM;
                    status = -EBADCKSUM;
                    break;
                }
            }
        }
    }
    // notify the replicator object that the read it had submitted to
    // the peer has finished. 
    return clnt->HandleEvent(code, data);
}

int
WriteOp::HandleRecordAppendDone(int code, void *data)
{
    if (code == EVENT_DISK_ERROR) {
        // eat up everything that was sent
        dataBuf->Consume(numBytes);
        status = -1;
        if (data) {
            status = *(int *) data;
            KFS_LOG_STREAM_INFO <<
                "Disk error: errno: " << status << " chunkid: " << chunkId <<
            KFS_LOG_EOM;
        }
    } else if (code == EVENT_DISK_WROTE) {
        status = *(int *) data;
        numBytesIO = status;
        dataBuf->Consume(numBytesIO);
    } else {
        assert(! "unexpected event code");
        abort();
    }
    return clnt->HandleEvent(EVENT_CMD_DONE, this);
}

int
WriteOp::HandleWriteDone(int code, void *data)
{
    // DecrementCounter(CMD_WRITE);

    if (isFromReReplication) {
        if (code == EVENT_DISK_WROTE) {
            status = std::min(*(int *) data, int(numBytes));
            numBytesIO = status;
        }
        else {
            status = -1;
        }
        return clnt->HandleEvent(code, this);
    }
    assert(wpop != NULL);

    if (code == EVENT_DISK_ERROR) {
        // eat up everything that was sent
        dataBuf->Consume(std::max(int(numBytesIO), int(numBytes)));
        status = -1;
        if (data != NULL) {
            status = *(int *) data;
            KFS_LOG_STREAM_INFO <<
                "Disk error: errno: " << status << " chunkid: " << chunkId <<
            KFS_LOG_EOM;
        }
        gChunkManager.ChunkIOFailed(chunkId, status);

        wpop->HandleEvent(EVENT_CMD_DONE, this);
        return 0;
    }
    else if (code == EVENT_DISK_WROTE) {
        status = *(int *) data;
        SET_HANDLER(this, &WriteOp::HandleSyncDone);
        if (numBytesIO != status || status < (int)numBytes) {
            // write didn't do everything that was asked; we need to retry
            KFS_LOG_STREAM_INFO <<
                "Write on chunk did less: asked: " << numBytes << "/" << numBytesIO <<
                " did: " << status << "; asking clnt to retry" <<
            KFS_LOG_EOM;
            status = -EAGAIN;
        } else {
            status = numBytes; // reply back the same # of bytes as in request.
        }
        if (numBytesIO > ssize_t(numBytes) && dataBuf) {
            const int off(offset % IOBufferData::GetDefaultBufferSize());
            KFS_LOG_STREAM_DEBUG <<
                "chunk write: asked " << numBytes << "/" << numBytesIO <<
                " actual, buf offset: " << off <<
            KFS_LOG_EOM;
            // restore original data in the buffer.
            assert(ssize_t(numBytes) <= numBytesIO - off);
            dataBuf->Consume(off);
            dataBuf->Trim(int(numBytes));
        }
        numBytesIO = numBytes;
        // queue the sync op only if we are all done with writing to
        // this chunk:
        waitForSyncDone = false;

        if (offset + numBytesIO >= (off_t) KFS::CHUNKSIZE) {
            // If we have written till the end of the chunk, close out the
            // fd.  The observation is that writes are sequential and when
            // we finished a chunk, the client will move to the next
            // one.
#if 0
            if (gChunkManager.Sync(this) < 0) {
                KFS_LOG_STREAM_DEBUG << "Sync failed..." << KFS_LOG_EOM;
                // eat up everything that was sent
                dataBuf->Consume(numBytes);
                // Sync failed
                status = -1;
                // clnt->HandleEvent(EVENT_CMD_DONE, this);
                return wsop->HandleEvent(EVENT_CMD_DONE, this);
            }
#endif
        }

        if (!waitForSyncDone) {
            // KFS_LOG_DEBUG("Queued sync; not waiting for sync to finish...");
            // sync is queued; no need to wait for it to finish
            return HandleSyncDone(EVENT_SYNC_DONE, 0);
        }
    }
    return 0;
}

///
/// A write op finished.  Set the status and the # of bytes written
/// and notify the owning write commit op.
///
int
WriteOp::HandleSyncDone(int code, void *data)
{
    // eat up everything that was sent
    dataBuf->Consume(numBytes);

    if (code != EVENT_SYNC_DONE) {
        status = -1;
    }

    if (status >= 0) {
        kfsFileId_t dummy;
        gChunkManager.ChunkSize(chunkId, dummy, &chunkSize);
        SET_HANDLER(this, &WriteOp::HandleLoggingDone);
        gLogger.Submit(this);
    }
    else {
        wpop->HandleEvent(EVENT_CMD_DONE, this);
    }
    
    return 0;
}

int
WriteOp::HandleLoggingDone(int code, void *data)
{
    assert(wpop != NULL);
    return wpop->HandleEvent(EVENT_CMD_DONE, this);
}

///
/// Handlers for ops that need logging.  This method is invoked by the
/// logger thread.  So, don't access any globals here---otherwise, we
/// need to add locking.
///
void
AllocChunkOp::Log(ofstream &ofs)
{
    ofs << "ALLOCATE " << chunkId << ' ' << fileId << ' ';
    ofs << chunkVersion << "\n";
    assert(!ofs.fail());
}

/// Resetting a chunk's version # is equivalent to doing an allocation
/// of an existing chunk.
void
ChangeChunkVersOp::Log(ofstream &ofs)
{
    ofs << "CHANGE_CHUNK_VERS " << chunkId << ' ' << fileId << ' ';
    ofs << chunkVersion << "\n";
    assert(!ofs.fail());
}

void
DeleteChunkOp::Log(ofstream &ofs)
{
    ofs << "DELETE " << chunkId << "\n";
    assert(!ofs.fail());
}

void
MakeChunkStableOp::Log(ofstream &ofs)
{
    ofs << "MAKE_CHUNK_STABLE " << chunkId << "\n";
    assert(!ofs.fail());
}

void
CoalesceBlockOp::Log(ofstream &ofs)
{
    ofs << "COALESCE_BLOCK " << srcFileId << ' ' << srcChunkId << ' ' 
        << dstFileId << ' ' << dstChunkId << "\n";
    assert(!ofs.fail());
}

void
WriteOp::Log(ofstream &ofs)
{
    ofs << "WRITE " << chunkId << ' ' << chunkSize << ' ';
    ofs << offset << ' ';
    ofs << checksums.size();
    for (vector<uint32_t>::size_type i = 0; i < checksums.size(); i++) {
        ofs << ' ' << checksums[i];
    }
    ofs << "\n";
    assert(!ofs.fail());
}

void
TruncateChunkOp::Log(ofstream &ofs)
{
    ofs << "TRUNCATE " << chunkId << ' ' << chunkSize << "\n";
    assert(!ofs.fail());
}

// For replicating a chunk, we log nothing.  We don't write out info
// about the chunk in checkpoint files until replication is complete.
// This way, if we ever crash during chunk-replication, we'll simply
// nuke out the chunk on startup.
void
ReplicateChunkOp::Log(ofstream &ofs)
{

}

///
/// Handlers for executing the various ops.  If the op execution is
/// "in-line", that is the op doesn't block, then when the execution
/// is finished, the op is handed off to the logger; the net thread
/// will drain the logger and then notify the client.  Otherwise, the op is queued
/// for execution and the client gets notified whenever the op
/// finishes execution.
///
void
OpenOp::Execute()
{
    status = gChunkManager.OpenChunk(chunkId, openFlags);

    UpdateCounter(CMD_OPEN);

    //clnt->HandleEvent(EVENT_CMD_DONE, this);
    gLogger.Submit(this);
}

void
CloseOp::Execute()
{
    UpdateCounter(CMD_CLOSE);

    KFS_LOG_STREAM_DEBUG <<
        "Close chunk: " << chunkId << " and might give up lease" <<
    KFS_LOG_EOM;

    int            myPos;
    int64_t        writeId = -1;
    ServerLocation peerLoc;
    bool needToForward = needToForwardToPeer(servers, numServers, myPos, peerLoc, hasWriteId, writeId);
    if (! gAtomicRecordAppendManager.CloseChunk(this, writeId, needToForward)) {
        // forward the close only if it was accepted by the chunk
        // manager.  the chunk manager can reject a close if the
        // chunk is being written to by multiple record appenders
        needToForward = gChunkManager.CloseChunk(chunkId) == 0 && needToForward;
        status = 0;
    }
    if (needToForward) {
        ForwardToPeer(peerLoc);
    }
    gLogger.Submit(this);
}

void
CloseOp::ForwardToPeer(const ServerLocation &loc)
{
    RemoteSyncSMPtr const peer = gChunkServer.FindServer(loc);
    if (! peer) {
        KFS_LOG_STREAM_DEBUG <<
            "unable to forward to peer: " << loc.ToString() <<
            " cmd: " << Show() <<
        KFS_LOG_EOM;
        return;
    }
    CloseOp * const fwdedOp = new CloseOp(peer->NextSeqnum(), this);
    // don't need an ack back
    fwdedOp->needAck = false;
    // this op goes to the remote-sync SM and after it is sent, comes right back to be nuked
    // when this op comes, just nuke it
    fwdedOp->clnt = fwdedOp;

    KFS_LOG_STREAM_INFO <<
        "forwarding close to peer " << loc.ToString() << " " << fwdedOp->Show() <<
    KFS_LOG_EOM;
    SET_HANDLER(fwdedOp, &CloseOp::HandlePeerReply);
    peer->Enqueue(fwdedOp);
}

int
CloseOp::HandlePeerReply(int code, void *data)
{
    delete this;
    return 0;
}

void
AllocChunkOp::Execute()
{
    UpdateCounter(CMD_ALLOC_CHUNK);
    int res;

    // page in the chunk meta-data if needed
    if (!gChunkManager.NeedToReadChunkMetadata(chunkId)) {
        HandleChunkMetaReadDone(0, NULL);
        return;
    }
    
    SET_HANDLER(this, &AllocChunkOp::HandleChunkMetaReadDone);
    if ((res = gChunkManager.ReadChunkMetadata(chunkId, this)) < 0) {
        KFS_LOG_STREAM_INFO <<
            "Unable read chunk metadata for chunk: " << chunkId << "; error: " << res <<
        KFS_LOG_EOM;
        status = -EINVAL;
        gLogger.Submit(this);
        return;
    }
}

int
AllocChunkOp::HandleChunkMetaReadDone(int code, void *data)
{
    if (leaseId >= 0) {
        gCounters.mWriteMaster.Update(1);
        gLeaseClerk.RegisterLease(chunkId, leaseId);
    }
    if (appendFlag) {
        int            myPos   = -1;
        int64_t        writeId = -1;
        ServerLocation peerLoc;
        needToForwardToPeer(servers, numServers, myPos, peerLoc, false, writeId);
        gChunkManager.AllocChunkForAppend(this, myPos, peerLoc);
    } else {
        status = gChunkManager.AllocChunk(fileId, chunkId, chunkVersion);
        if (status >= 0) {
            status = gChunkManager.ScheduleWriteChunkMetadata(chunkId);
        }
    }
    gLogger.Submit(this);
    return 0;
}

void
DeleteChunkOp::Execute()
{
    UpdateCounter(CMD_DELETE_CHUNK);

    status = gChunkManager.DeleteChunk(chunkId);
    //     if (status < 0)
    //         // failed; nothing to log
    //         clnt->HandleEvent(EVENT_CMD_DONE, this);
    //     else
    gLogger.Submit(this);
}

void
TruncateChunkOp::Execute()
{
    UpdateCounter(CMD_TRUNCATE_CHUNK);

    SET_HANDLER(this, &TruncateChunkOp::HandleChunkMetaReadDone);
    if (gChunkManager.ReadChunkMetadata(chunkId, this) < 0) {
        status = -EINVAL;
        gLogger.Submit(this);
    }
}

int
TruncateChunkOp::HandleChunkMetaReadDone(int code, void *data)
{
    status = *(int *) data;
    if (status < 0) {
        gLogger.Submit(this);
        return 0;
    }

    status = gChunkManager.TruncateChunk(chunkId, chunkSize);
    if (status < 0) {
        gLogger.Submit(this);
        return 0;
    }
    status = gChunkManager.ScheduleWriteChunkMetadata(chunkId);
    gLogger.Submit(this);
    return 0;
}

void
ReplicateChunkOp::Execute()
{
    RemoteSyncSMPtr peer = gChunkServer.FindServer(location);

    UpdateCounter(CMD_REPLICATE_CHUNK);

    KFS_LOG_STREAM_DEBUG
        << "Replicating chunk: " << chunkId << " from " << location.ToString() <<
    KFS_LOG_EOM;

    if (!peer) {
        KFS_LOG_STREAM_INFO <<
            "Unable to find peer: " << location.ToString() <<
        KFS_LOG_EOM;
        status = -1;
        gLogger.Submit(this);
        return;
    }

    replicator.reset(new Replicator(this));
    // Get the animation going...
    SET_HANDLER(this, &ReplicateChunkOp::HandleDone);

    replicator->Start(peer);
}

void
BeginMakeChunkStableOp::Execute()
{
    status = 0;
    if (gAtomicRecordAppendManager.BeginMakeChunkStable(this)) {
        return;
    }
    gLogger.Submit(this);
}

void
MakeChunkStableOp::Execute()
{
    UpdateCounter(CMD_MAKE_CHUNK_STABLE);

    status = 0;
    if (! hasChecksum && gChunkManager.IsChunkStable(chunkId)) {
        gLogger.Submit(this);
        return;
    }
    SET_HANDLER(this, &MakeChunkStableOp::HandleARAFinalizeDone);
    if (gAtomicRecordAppendManager.MakeChunkStable(this)) {
        return;
    }
/*
    Meta data should be up to date at this point: record appender, and normal
    write path schedule meta data updates when needed.
    MakeChunkStable() doesn't cancel scheduled meta data write.
    In the case of meta data write [delayed] failure the meta server gets
    "corrupted chunk" notification.
    AllocChunkOp schedules meta data update, thus even if no writes were issued
    empty chunks should have meta data written.

    const int ret = gChunkManager.ScheduleWriteChunkMetadata(chunkId);
    assert(ret == 0 || ! gChunkManager.IsChunkMetadataLoaded(chunkId));
    KFS_LOG_STREAM_DEBUG <<
        "MakeChunkStable: writing chunk metadata for chunk: " << chunkId <<
        " status: " << ret <<
    KFS_LOG_EOM;
*/
    HandleARAFinalizeDone(EVENT_CMD_DONE, this);
}

int
MakeChunkStableOp::HandleARAFinalizeDone(int code, void *data)
{
    if (status == 0) {
        status = gChunkManager.MakeChunkStable(chunkId);
    }
    gLogger.Submit(this);
    return 0;
}

void
CoalesceBlockOp::Execute()
{
    UpdateCounter(CMD_COALESCE_BLOCK);

    status = gChunkManager.CoalesceBlock(srcFileId, srcChunkId, dstFileId, dstChunkId);
    gLogger.Submit(this);
}

void
ChangeChunkVersOp::Execute()
{
    UpdateCounter(CMD_CHANGE_CHUNK_VERS);

    SET_HANDLER(this, &ChangeChunkVersOp::HandleChunkMetaReadDone);
    if (gChunkManager.ReadChunkMetadata(chunkId, this) < 0) {
        status = -EINVAL;
        gLogger.Submit(this);
    }
}

int
ChangeChunkVersOp::HandleChunkMetaReadDone(int code, void *data)
{
    status = *(int *) data;
    if (status < 0) {
        gLogger.Submit(this);
        return 0;
    }

    status = gChunkManager.ChangeChunkVers(fileId, chunkId, 
                                           chunkVersion);
    if (status < 0) {
        gLogger.Submit(this);
        return 0;
    }
    status = gChunkManager.ScheduleWriteChunkMetadata(chunkId);
    gLogger.Submit(this);
    return 0;
}

template<typename T> void
HeartbeatOp::Append(const char* key1, const char* key2, T val)
{
    if (key1 && *key1) {
        response << key1 << ": " << val << "\r\n";
    }
    if (key2 && *key2) {
        cmdShow  << " " << key2 << ": " << val;
    }
}
inline static int64_t MicorSecs(const struct timeval& tv)
{
    return (tv.tv_sec * 1000000 + tv.tv_usec);
}

// This is the heartbeat sent by the meta server
void
HeartbeatOp::Execute()
{
    UpdateCounter(CMD_HEARTBEAT);

    double lavg1                = -1;
    int    kernelExecutingCount = -1;
    int    kernelThreadCount    = -1;
#ifdef KFS_OS_NAME_LINUX
    ifstream rawavg("/proc/loadavg");
    if (rawavg) {
        double lavg5 = -1, lavg10 = -1;
        char   slash;
        rawavg >> lavg1 >> lavg5 >> lavg10 >>
            kernelExecutingCount >> slash >> kernelThreadCount;
        rawavg.close();
    }
#endif
    const int64_t writeCount       = gChunkManager.GetNumWritableChunks();
    const int64_t writeAppendCount = gAtomicRecordAppendManager.GetOpenAppendersCount();
    const int64_t replicationCount = Replicator::GetNumReplications();
    struct rusage ru;
    if (getrusage(RUSAGE_SELF, &ru)) {
	    ru.ru_utime.tv_sec  = -1;
	    ru.ru_utime.tv_usec = -1;
	    ru.ru_stime.tv_sec  = -1;
	    ru.ru_stime.tv_usec = -1;
    }

    cmdShow << " space:";
    Append("Total-space",  "total",  gChunkManager.GetTotalSpace());
    Append("Used-space",   "used",   gChunkManager.GetUsedSpace());
    Append("Num-drives",   "drives", gChunkManager.GetUsableChunkDirs());
    Append("Num-chunks",   "chunks", gChunkManager.GetNumChunks());
    Append("Num-writable-chunks", "wrchunks",
        writeCount + writeAppendCount + replicationCount
    );
    Append("Num-random-writes",     "rwr",  writeCount);
    Append("Num-appends",           "awr",  writeAppendCount);
    Append("Num-re-replications",   "rep",  replicationCount);
    Append("Num-appends-with-wids", "awid",
        gAtomicRecordAppendManager.GetAppendersWithWidCount());
    Append("Uptime", "up", libkfsio::globalNetManager().UpTime());

    Append("CPU-user", "ucpu", MicorSecs(ru.ru_utime));
    Append("CPU-sys",  "scpu", MicorSecs(ru.ru_utime));
    Append("CPU-load-avg",             "load", lavg1);
    Append("Kernel-executing-threads", "ex",   kernelExecutingCount);
    Append("Kernel-total-threads",     "th",   kernelThreadCount);

    ChunkManager::Counters cm;    
    gChunkManager.GetCounters(cm);
    cmdShow << " chunk: err:";
    Append("Chunk-corrupted",     "cnt",  cm.mCorruptedChunksCount);
    Append("Chunk-header-errors", "hdr",  cm.mBadChunkHeaderErrorCount);
    Append("Chunk-chksum-errors", "csum", cm.mReadChecksumErrorCount);
    Append("Chunk-read-errors",   "rd",   cm.mReadErrorCount);
    Append("Chunk-write-errors",  "wr",   cm.mWriteErrorCount);
    Append("Chunk-open-errors",   "open", cm.mOpenErrorCount);
    Append("Dir-chunk-lost",      "dce",  cm.mDirLostChunkCount);
    Append("Chunk-dir-lost",      "cdl",  cm.mChunkDirLostCount);

    MetaServerSM::Counters mc;
    gMetaServerSM.GetCounters(mc);
    cmdShow << " meta:";
    Append("Meta-connect",     "conn", mc.mConnectCount);
    cmdShow << " hello:";
    Append("Meta-hello-count",  "cnt", mc.mHelloCount);
    Append("Meta-hello-errors", "err", mc.mHelloErrorCount);
    cmdShow << " alloc:";
    Append("Meta-alloc-count",  "cnt", mc.mAllocCount);
    Append("Meta-alloc-errors", "err", mc.mAllocErrorCount); 

    ClientManager::Counters cli;
    gClientManager.GetCounters(cli);
    cmdShow << " cli:";
    Append("Client-accept",  "accept", cli.mAcceptCount);
    Append("Client-active",  "cur",    cli.mClientCount);
    cmdShow << " req: err:";
    Append("Client-req-invalid",        "inval", cli.mBadRequestCount);
    Append("Client-req-invalid-header", "hdr",   cli.mBadRequestHeaderCount);
    Append("Client-req-invalid-length", "len",
        cli.mRequestLengthExceededCount);
    cmdShow << " read:";
    Append("Client-read-count",     "cnt",   cli.mReadRequestCount);
    Append("Client-read-bytes",     "bytes", cli.mReadRequestBytes);
    Append("Client-read-micro-sec", "tm",    cli.mReadRequestTimeMicroSecs);
    Append("Client-read-errors",    "err",   cli.mReadRequestErrors);
    cmdShow << " write:";
    Append("Client-write-count",     "cnt",   cli.mWriteRequestCount);
    Append("Client-write-bytes",     "bytes", cli.mWriteRequestBytes);
    Append("Client-write-micro-sec", "tm",    cli.mWriteRequestTimeMicroSecs);
    Append("Client-write-errors",    "err",   cli.mWriteRequestErrors);
    cmdShow << " append:";
    Append("Client-append-count",     "cnt",   cli.mAppendRequestCount);
    Append("Client-append-bytes",     "bytes", cli.mAppendRequestBytes);
    Append("Client-append-micro-sec", "tm",    cli.mAppendRequestTimeMicroSecs);
    Append("Client-append-errors",    "err",   cli.mAppendRequestErrors);
    cmdShow << " other:";
    Append("Client-other-count",     "cnt",   cli.mOtherRequestCount);
    Append("Client-other-micro-sec", "tm",    cli.mOtherRequestTimeMicroSecs);
    Append("Client-other-errors",    "err",   cli.mOtherRequestErrors);

    cmdShow << " timer: ovr:";
    Append("Timer-overrun-count", "cnt",
        libkfsio::globalNetManager().GetTimerOverrunCount());
    Append("Timer-overrun-sec",   "sec",
        libkfsio::globalNetManager().GetTimerOverrunSec());

    cmdShow << " wappend:";
    Append("Write-appenders", "cur",
        gAtomicRecordAppendManager.GetAppendersCount());
    AtomicRecordAppendManager::Counters wa;
    gAtomicRecordAppendManager.GetCounters(wa);
    Append("WAppend-count", "cnt",   wa.mAppendCount);
    Append("WAppend-bytes", "bytes", wa.mAppendByteCount);
    Append("WAppend-errors","err",   wa.mAppendErrorCount);
    cmdShow << " repl:";
    Append("WAppend-replication-errors",   "err", wa.mReplicationErrorCount);
    Append("WAppend-replication-tiemouts", "tmo", wa.mReplicationTimeoutCount);
    cmdShow << " alloc:";
    Append("WAppend-alloc-count",        "cnt", wa.mAppenderAllocCount);
    Append("WAppend-alloc-master-count", "mas", wa.mAppenderAllocMasterCount);
    Append("WAppend-alloc-errors",       "err", wa.mAppenderAllocErrorCount);
    cmdShow << " wid:";
    Append("WAppend-wid-alloc-count",      "cnt", wa.mWriteIdAllocCount);
    Append("WAppend-wid-alloc-errors",     "err", wa.mWriteIdAllocErrorCount);
    Append("WAppend-wid-alloc-no-appender","nae", wa.mWriteIdAllocNoAppenderCount);
    cmdShow << " srsrv:";
    Append("WAppend-sreserve-count",  "cnt",   wa.mSpaceReserveCount);
    Append("WAppend-sreserve-bytes",  "bytes", wa.mSpaceReserveByteCount);
    Append("WAppend-sreserve-errors", "err",   wa.mSpaceReserveErrorCount);
    Append("WAppend-sreserve-denied", "den",   wa.mSpaceReserveDeniedCount);
    cmdShow << " bmcs:";
    Append("WAppend-bmcs-count",  "cnt", wa.mBeginMakeStableCount);
    Append("WAppend-bmcs-errors", "err", wa.mBeginMakeStableErrorCount);
    cmdShow << " mcs:";
    Append("WAppend-mcs-count",         "cnt", wa.mMakeStableCount);
    Append("WAppend-mcs-errors",        "err", wa.mMakeStableErrorCount);
    Append("WAppend-mcs-length-errors", "eln", wa.mMakeStableLengthErrorCount);
    Append("WAppend-mcs-chksum-errors", "ecs", wa.mMakeStableChecksumErrorCount);
    cmdShow << " gos:";
    Append("WAppend-get-op-status-count", "cnt", wa.mGetOpStatusCount);
    Append("WAppend-get-op-status-errors","err", wa.mGetOpStatusErrorCount);
    Append("WAppend-get-op-status-known", "knw", wa.mGetOpStatusKnownCount);
    cmdShow << " err:";
    Append("WAppend-chksum-erros",    "csum",  wa.mChecksumErrorCount);
    Append("WAppend-read-erros",      "rd",    wa.mReadErrorCount);
    Append("WAppend-write-errors",    "wr",    wa.mWriteErrorCount);
    Append("WAppend-lease-ex-errors", "lease", wa.mLeaseExpiredCount);
    cmdShow << " lost:";
    Append("WAppend-lost-timeouts", "tm",   wa.mTimeoutLostCount);
    Append("WAppend-lost-chunks",   "csum", wa.mLostChunkCount);

    const BufferManager&  bufMgr = DiskIo::GetBufferManager();
    cmdShow <<  " buffers: bytes:";
    Append("Buffer-bytes-total", "total", bufMgr.GetTotalByteCount());
    Append("Buffer-bytes-wait",  "wait",  bufMgr.GetWaitingByteCount());
    cmdShow << " cnt:";
    Append("Buffer-total-count", "total", bufMgr.GetTotalBufferCount());
    Append("Buffer-min-count",   "min",   bufMgr.GetMinBufferCount());
    Append("Buffer-free-count",  "free",  bufMgr.GetFreeBufferCount());
    cmdShow << " req:";
    Append("Buffer-clients",      "cbuf",  bufMgr.GetClientsWihtBuffersCount());
    Append("Buffer-clients-wait", "cwait", bufMgr.GetWaitingCount());
    BufferManager::Counters bmCnts;
    bufMgr.GetCounters(bmCnts);
    Append("Buffer-req-total",         "cnt",   bmCnts.mRequestCount);
    Append("Buffer-req-bytes",         "bytes", bmCnts.mRequestByteCount);
    Append("Buffer-req-denied-total",  "den",   bmCnts.mRequestDeniedCount);
    Append("Buffer-req-denied-bytes",  "denb",  bmCnts.mRequestDeniedByteCount);
    Append("Buffer-req-granted-total", "grn",   bmCnts.mRequestGrantedCount);
    Append("Buffer-req-granted-bytes", "grnb",  bmCnts.mRequestGrantedByteCount);

    DiskIo::Counters dio;
    DiskIo::GetCounters(dio);
    cmdShow <<  " disk: read:";
    Append("Disk-read-count", "cnt",   dio.mReadCount);
    Append("Disk-read-bytes", "bytes", dio.mReadByteCount);
    Append("Disk-read-errors","err",   dio.mReadErrorCount);
    cmdShow <<  " write:";
    Append("Disk-write-count", "cnt",   dio.mWriteCount);
    Append("Disk-write-bytes", "bytes", dio.mWriteByteCount);
    Append("Disk-write-errors","err",   dio.mWriteErrorCount);
    cmdShow <<  " sync:";
    Append("Disk-sync-count", "cnt",   dio.mSyncCount);
    Append("Disk-sync-errors","err",   dio.mSyncErrorCount);

    cmdShow <<  " msglog:";
    MsgLogger::Counters msgLogCntrs;
    MsgLogger::GetLogger()->GetCounters(msgLogCntrs);
    Append("Msg-log-level",            "level",  MsgLogger::GetLogger()->GetLogLevel());
    Append("Msg-log-count",            "cnt",    msgLogCntrs.mAppendCount);
    Append("Msg-log-drop",             "drop",   msgLogCntrs.mDroppedCount);
    Append("Msg-log-write-errors",     "werr",   msgLogCntrs.mWriteErrorCount);
    Append("Msg-log-wait",             "wait",   msgLogCntrs.mAppendWaitCount);
    Append("Msg-log-waited-micro-sec", "waittm", msgLogCntrs.mAppendWaitMicroSecs);

    status = 0;
    // clnt->HandleEvent(EVENT_CMD_DONE, this);
    gLogger.Submit(this);
}

void
RetireOp::Execute()
{
    // we are told to retire...so, bow out
    KFS_LOG_INFO("We have been asked to retire...bye");
    StopNetProcessor(0);
}

bool
StaleChunksOp::ParseContent(istream& is)
{
    for(int i = 0; i < numStaleChunks; ++i) {
        kfsChunkId_t c;
        if (! (is >> c)) {
            ostringstream os;
            os <<
                "failed to parse stale chunks request:"
                " expected: "   << numStaleChunks <<
                " got: "        << i <<
                " last chunk: " <<
                    (staleChunkIds.empty() ? kfsChunkId_t(-1) :
                        staleChunkIds.back())
            ;
            statusMsg = os.str();
            status = -EINVAL;
            return false;
        }
        staleChunkIds.push_back(c);
    }
    return true;
}

void
StaleChunksOp::Execute()
{
    vector<kfsChunkId_t>::size_type i;

    for (i = 0; i < staleChunkIds.size(); ++i) {
        gChunkManager.StaleChunk(staleChunkIds[i], true);
    }
    status = 0;
    // clnt->HandleEvent(EVENT_CMD_DONE, this);
    gLogger.Submit(this);
}

void
ReadOp::Execute()
{
    UpdateCounter(CMD_READ);

    if (numBytes > CHUNKSIZE) {
        KFS_LOG_STREAM_DEBUG <<
            "read request size exceeds chunk size: " << numBytes <<
        KFS_LOG_EOM;
        status = -EINVAL;
        gLogger.Submit(this);
        return;
    }

    gChunkManager.GetDriveName(this);
   
    SET_HANDLER(this, &ReadOp::HandleChunkMetaReadDone);
    const int res = gChunkManager.ReadChunkMetadata(chunkId, this);
    if (res < 0) {
        KFS_LOG_STREAM_ERROR <<
            "failed read chunk meta data, status: " << res <<
        KFS_LOG_EOM;
        status = res;
        gLogger.Submit(this);
    }
}

int
ReadOp::HandleChunkMetaReadDone(int code, void *data)
{
    status = *(int *) data;
    if (status < 0) {
        gLogger.Submit(this);
        return 0;
    }

    SET_HANDLER(this, &ReadOp::HandleDone);    
    status = gChunkManager.ReadChunk(this);

    if (status < 0) {
        // clnt->HandleEvent(EVENT_CMD_DONE, this);
        if (wop == NULL) {
            // we are done with this op; this needs draining
            gLogger.Submit(this);
        }
        else {
            // resume execution of write
            wop->Execute();
        }
    }
    return 0;
}

//
// Handling of writes is done in multiple steps:
// 1. The client allocates a chunk from the metaserver; the metaserver
// picks a set of hosting chunkservers and nominates one of the
// server's as the "master" for the transaction.
// 2. The client pushes data for a write via a WritePrepareOp to each
// of the hosting chunkservers (in any order).
// 3. The chunkserver in turn enqueues the write with the ChunkManager
// object.  The ChunkManager assigns an id to the write.   NOTE:
// nothing is written out to disk at this point.
// 4. After the client has pushed out data to replica chunk-servers
// and gotten write-id's, the client does a WriteSync to the master.
// 5. The master retrieves the write corresponding to the write-id and
// commits the write to disk.
// 6. The master then sends out a WriteCommit to each of the replica
// chunkservers asking them to commit the write; this commit message
// is sent concurrently to all the replicas.
// 7. After the replicas reply, the master replies to the client with
// status from individual servers and how much got written on each.
//

static bool
needToForwardToPeer(string &serverInfo, uint32_t numServers, int &myPos,
                    ServerLocation &peerLoc, 
                    bool isWriteIdPresent, int64_t &writeId)
{
    istringstream ist(serverInfo);
    ServerLocation loc;
    bool foundLocal = false;
    int64_t id;
    bool needToForward = false;

    // the list of servers is ordered: we forward to the next one
    // in the list.
    for (uint32_t i = 0; i < numServers; i++) {
        ist >> loc.hostname;
        ist >> loc.port;
        if (isWriteIdPresent)
            ist >> id;
            
        if (gChunkServer.IsLocalServer(loc)) {
            // return the position of where this server is present in the list
            myPos = i; 
            foundLocal = true;
            if (isWriteIdPresent)
                writeId = id;
            continue;
        }
        // forward if we are not the last in the list
        if (foundLocal) {
            needToForward = true;
            break;
        }
    }
    peerLoc = loc;
    return needToForward;
}

void
WriteIdAllocOp::Execute()
{
    // check if we need to forward anywhere
    int64_t        dummyWriteId  = -1;
    int            myPos         = -1;
    ServerLocation peerLoc;
    const bool     needToForward = needToForwardToPeer(
        servers, numServers, myPos, peerLoc, false, dummyWriteId);
    const bool     writeMaster   = myPos == 0;
    const int      res           = myPos < 0 ? -EINVAL :
        gChunkManager.AllocateWriteId(this, myPos, peerLoc);
    if (res != 0 && status == 0) {
        status = res < 0 ? res : -res;
    }
    if (status != 0) {
        if (myPos < 0) {
            statusMsg = "invalid request bad or missing Servers: field";
        }
        Done(EVENT_CMD_DONE, &status);
        return;
    }
    if (writeMaster) {
        // Notify the lease clerk that we are doing write.  This is to
        // signal the lease clerk to renew the lease for the chunk when appropriate.
        gLeaseClerk.DoingWrite(chunkId);
    }
    writeIdStr = gChunkServer.GetMyLocation() + " " + 
        boost::lexical_cast<string>(writeId);
    if (needToForward) {
        ForwardToPeer(peerLoc);
    } else {
        ReadChunkMetadata();
    }
}

int
WriteIdAllocOp::ForwardToPeer(const ServerLocation &loc)
{
    assert(! fwdedOp && status == 0 && (clnt || isForRecordAppend));

    RemoteSyncSMPtr const peer = isForRecordAppend ?
        gChunkServer.FindServer(loc) :
        static_cast<ClientSM *>(clnt)->FindServer(loc);
    if (! peer) {
        status    = -EHOSTUNREACH;
        statusMsg = "unable to find peer " + loc.ToString();
        return Done(EVENT_CMD_DONE, &status);
    }
    fwdedOp = new WriteIdAllocOp(peer->NextSeqnum(), *this);
    // When forwarded op completes, call this op HandlePeerReply.
    fwdedOp->clnt = this;
    SET_HANDLER(this, &WriteIdAllocOp::HandlePeerReply);

    KFS_LOG_STREAM_DEBUG <<
        "forwarding to " << loc.ToString() << " " << fwdedOp->Show() <<
    KFS_LOG_EOM;
    peer->Enqueue(fwdedOp);
    return 0;
}

int
WriteIdAllocOp::HandlePeerReply(int code, void *data)
{
    assert(code == EVENT_CMD_DONE && data == fwdedOp);

    if (status == 0 && fwdedOp->status < 0) {
        status    = fwdedOp->status;
        statusMsg = fwdedOp->statusMsg.empty() ?
            string("forwarding failed") : fwdedOp->statusMsg;
    }
    if (status != 0) {
        return Done(EVENT_CMD_DONE, &status);
    }
    writeIdStr += " " + fwdedOp->writeIdStr;
    ReadChunkMetadata();
    return 0;
}

void
WriteIdAllocOp::ReadChunkMetadata()
{
    assert(status == 0);
    // Now, we are all done pending metadata read
    // page in the chunk meta-data if needed
    // if the read was successful, the call to read will callback handle-done
    SET_HANDLER(this, &WriteIdAllocOp::Done);
    int res = 0;
    if (! gChunkManager.NeedToReadChunkMetadata(chunkId) ||
            (res = gChunkManager.ReadChunkMetadata(chunkId, this)) < 0) {
        Done(EVENT_CMD_DONE, &res);
    }
}

int
WriteIdAllocOp::Done(int code, void *data)
{
    if (status == 0) {
        status = (code == EVENT_CMD_DONE && data) ?
            *reinterpret_cast<const int*>(data) : -1;
        if (status != 0) {
            statusMsg = "chunk meta data read failed";
        }
    }
    if (status != 0) {
        if (isForRecordAppend) {
            if (! writeIdStr.empty()) {
                gAtomicRecordAppendManager.InvalidateWriteIdDeclareFailure(chunkId, writeId);
            }
        } else {
            if (gLeaseClerk.IsLeaseValid(chunkId)) {
                // The write id alloc has failed; we don't want to renew the lease.
                // Now, when the client forces a re-allocation, the
                // metaserver will do a version bump; when the node that
                // was dead comes back, we can detect it has missed a write
                gLeaseClerk.RelinquishLease(chunkId);
            }
        }
    }
    KFS_LOG_STREAM(
        status == 0 ? MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelERROR) <<
        (status == 0 ? "done: " : "failed: ") << Show() <<
    KFS_LOG_EOM;
    gLogger.Submit(this);
    return 0;
}

void 
WritePrepareOp::Execute()
{
    ServerLocation peerLoc;
    int myPos;

    UpdateCounter(CMD_WRITE_PREPARE);

    SET_HANDLER(this, &WritePrepareOp::Done);

    // check if we need to forward anywhere
    bool needToForward = false, writeMaster;

    needToForward = needToForwardToPeer(servers, numServers, myPos, peerLoc, true, writeId);
    writeMaster = (myPos == 0);

    if (!gChunkManager.IsValidWriteId(writeId)) {
        statusMsg = "invalid write id";
        status = -EINVAL;
        gLogger.Submit(this);
        return;
    }

    if (!gChunkManager.IsChunkMetadataLoaded(chunkId)) {
        statusMsg = "checksums are not loaded";
        status = -KFS::ELEASEEXPIRED;
        Done(EVENT_CMD_DONE, this);
        return;
    }

    if (writeMaster) {
        // if we are the master, check the lease...
        if (!gLeaseClerk.IsLeaseValid(chunkId)) {
            KFS_LOG_STREAM_ERROR <<
                "Write prepare failed, lease expired for " << chunkId <<
            KFS_LOG_EOM;
            statusMsg = "lease expired";
            gLeaseClerk.RelinquishLease(chunkId);
            status = -KFS::ELEASEEXPIRED;
            gLogger.Submit(this);
            return;
        }
        // Notify the lease clerk that we are doing write.  This is to
        // signal the lease clerk to renew the lease for the chunk when appropriate.
        gLeaseClerk.DoingWrite(chunkId);
    }

    if ((checksum != 0) && (checksums.size() == 0)) {
        const uint32_t val = ComputeBlockChecksum(dataBuf, numBytes);
        if (val != checksum) {
            statusMsg = "checksum mismatch";
            KFS_LOG_STREAM_ERROR <<
                "Checksum mismatch: sent: " << checksum <<
                ", computed: " << val << "for " << Show() <<
            KFS_LOG_EOM;
            status = -EBADCKSUM;
            // so that the error goes out on a sync
            gChunkManager.SetWriteStatus(writeId, status);
            gLogger.Submit(this);
            return;
        }
    }

    // will clone only when the op is good
    writeOp = gChunkManager.CloneWriteOp(writeId);
    UpdateCounter(CMD_WRITE);

    if (writeOp == NULL) {
        // the write has previously failed; so fail this op and move on
        status = gChunkManager.GetWriteStatus(writeId);
        if (status >= 0) {
            status = -EINVAL;
        }
        Done(EVENT_CMD_DONE, this);
        return;
    }

    if (needToForward) {
        IOBuffer * const clonedData = dataBuf->Clone();
        status = ForwardToPeer(peerLoc, clonedData);
        if (status < 0) {
            delete clonedData;
            // can't forward to peer...so fail the write
            Done(EVENT_CMD_DONE, this);
            return;
        }
    }

    writeOp->offset = offset;
    writeOp->numBytes = numBytes;
    writeOp->dataBuf = dataBuf;
    writeOp->wpop = this;
    if (checksums.size() > 0)
        writeOp->checksums = checksums;
    else
        writeOp->checksums.push_back(checksum);
    dataBuf = NULL;

    writeOp->enqueueTime = time(NULL);

    KFS_LOG_STREAM_DEBUG <<
        "Writing to chunk: " << chunkId <<
        " @offset: " << offset <<
        " nbytes: " << numBytes <<
        " checksum: " << checksum <<
    KFS_LOG_EOM;

    status = gChunkManager.WriteChunk(writeOp);    
    if (status < 0) {
        Done(EVENT_CMD_DONE, this);
    }
}

int
WritePrepareOp::ForwardToPeer(const ServerLocation &loc, IOBuffer *dataBuf)
{
    assert(clnt);
    RemoteSyncSMPtr peer = static_cast<ClientSM *>(clnt)->FindServer(loc);
    if (!peer) {
        statusMsg = "no such peer " + loc.ToString();
        return -EHOSTUNREACH;
    }
    writeFwdOp = new WritePrepareFwdOp(peer->NextSeqnum(), this, dataBuf);
    writeFwdOp->clnt = this;
    KFS_LOG_STREAM_DEBUG <<
        "forwarding to " << loc.ToString() << " " << writeFwdOp->Show() <<
    KFS_LOG_EOM;
    peer->Enqueue(writeFwdOp);
    return 0;
}

int
WritePrepareOp::Done(int code, void *data)
{
    verifyExecutingOnEventProcessor();    

    if (status >= 0 && writeFwdOp && writeFwdOp->status < 0) {
        status    = writeFwdOp->status;
        statusMsg = writeFwdOp->statusMsg;
    }
    if (status < 0) {
        // so that the error goes out on a sync
        gChunkManager.SetWriteStatus(writeId, status);
        if (gLeaseClerk.IsLeaseValid(chunkId)) {
            // The write has failed; we don't want to renew the lease.
            // Now, when the client forces a re-allocation, the
            // metaserver will do a version bump; when the node that
            // was dead comes back, we can detect it has missed a write
            gLeaseClerk.RelinquishLease(chunkId);
        }
    }
    numDone++;
    if (writeFwdOp && numDone < 2) {
        return 0;
    }
    KFS_LOG_STREAM(
        status >= 0 ? MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelERROR) <<
        (status >= 0 ? "done: " : "failed: ") << Show() <<
        " status: " << status <<
        (statusMsg.empty() ? "" : " msg: ") << statusMsg <<
    KFS_LOG_EOM;
    gLogger.Submit(this);
    return 0;
}

void 
WriteSyncOp::Execute()
{
    ServerLocation peerLoc;
    int            myPos               = 0;
    bool           needToWriteMetadata = true;

    UpdateCounter(CMD_WRITE_SYNC);

    KFS_LOG_STREAM_DEBUG << "executing: " << Show() << KFS_LOG_EOM;
    // check if we need to forward anywhere
    const bool needToForward = needToForwardToPeer(servers, numServers, myPos, peerLoc, true, writeId);
    writeMaster = myPos == 0;

    writeOp = gChunkManager.CloneWriteOp(writeId);
    if (writeOp == NULL) {
        status    = -EINVAL;
        statusMsg = "no such write id";
        KFS_LOG_STREAM_ERROR <<
            "failed: " << statusMsg << " " << Show() <<
        KFS_LOG_EOM;
        gLogger.Submit(this);
        return;
    }

    writeOp->enqueueTime = time(NULL);

    if (writeOp->status < 0) {
        // due to failures with data forwarding/checksum errors and such
        status    = writeOp->status;
        statusMsg = "write error";
        gLogger.Submit(this);
        return;
    }

    if (!gChunkManager.IsChunkMetadataLoaded(chunkId)) {
        off_t csize;
        kfsFileId_t dummy;

        gChunkManager.ChunkSize(chunkId, dummy, &csize);
        if (csize > 0 && (csize >= (off_t) KFS::CHUNKSIZE)) {
            // the metadata block could be paged out by a previous sync
            needToWriteMetadata = false;
        } else {
            status    = -KFS::ELEASEEXPIRED;
            statusMsg = "meta data unloaded";
            KFS_LOG_STREAM_ERROR <<
                "failed: " << statusMsg << " " << Show() <<
            KFS_LOG_EOM;
            gChunkManager.SetWriteStatus(writeId, status);
            gLogger.Submit(this);
            return;
        }
    }

    if (writeMaster) {
        // if we are the master, check the lease...
        if (!gLeaseClerk.IsLeaseValid(chunkId)) {
            statusMsg = "lease expired";
            status    = -KFS::ELEASEEXPIRED;
            KFS_LOG_STREAM_ERROR <<
                "failed: " << statusMsg << " " << Show() <<
            KFS_LOG_EOM;
            gChunkManager.SetWriteStatus(writeId, status);
            gLogger.Submit(this);
            return;
        }

        // Notify the lease clerk that we are doing write.  This is to
        // signal the lease clerk to renew the lease for the chunk when appropriate.
        gLeaseClerk.DoingWrite(chunkId);
    }
    if (needToForward) {
        status = ForwardToPeer(peerLoc);
        if (status < 0) {
            // can't forward to peer...so fail the write
            Done(EVENT_CMD_DONE, this);
            return;
        }
    }

    SET_HANDLER(this, &WriteSyncOp::Done);

    // when things aren't aligned, we can't validate the checksums
    // handed by the client.  In such cases, make sure that the
    // chunkservers agree on the checksum
    bool validateChecksums = true;
    bool mismatch = false;
    if (writeMaster &&
        (((offset % CHECKSUM_BLOCKSIZE) != 0) || ((numBytes % CHECKSUM_BLOCKSIZE) != 0))) {
            validateChecksums = false;
    }
    // in the non-writemaster case, our checksums should match what
    // the write master sent us.

    vector<uint32_t> myChecksums = gChunkManager.GetChecksums(chunkId, offset, numBytes);
    if ((!validateChecksums) || (checksums.size() == 0)) {
        // Either we can't validate checksums due to alignment OR the
        // client didn't give us checksums.  In either case:
        // The sync covers a certain region for which the client
        // sent data.  The value for that region should be non-zero
        for (uint32_t i = 0; (i < myChecksums.size()) && !mismatch; i++) {
            if (myChecksums[i] == 0) {
                KFS_LOG_STREAM_ERROR << 
                    "Sync failed due to checksum mismatch: we have 0 in the range " <<
                    offset << "->" << offset+numBytes << " ; but should be non-zero" << KFS_LOG_EOM;
                mismatch = true;
            }
        }
        if (!mismatch)
            KFS_LOG_STREAM_DEBUG << "Validated checksums are non-zero for chunk = " << chunkId 
                                 << " offset = " << offset << " numbytes = " << numBytes << KFS_LOG_EOM;
    } else {
        if (myChecksums.size() != checksums.size()) {
            KFS_LOG_STREAM_ERROR <<
                "Checksum mismatch: # of entries we have: " << myChecksums.size() <<
                " # of entries client sent: " << checksums.size() << KFS_LOG_EOM;
            mismatch = true;
        }
        for (uint32_t i = 0; (i < myChecksums.size()) && !mismatch; i++) {
            if (myChecksums[i] != checksums[i]) {
                KFS_LOG_STREAM_ERROR << 
                    "Sync failed due to checksum mismatch: we have = " <<
                    myChecksums[i] << " but the value should be: " << checksums[i] << 
                    KFS_LOG_EOM;
                mismatch = true;
                break;
            }
            // KFS_LOG_STREAM_DEBUG << "Got = " << checksums[i] << " and ours: " << myChecksums[i] << KFS_LOG_EOM;
        }
        // bit of testing code
        // if ((rand() % 20) == 0) {
        // if ((offset == 33554432) && (chunkVersion == 1)) {
        // if ((2097152 <= offset) && (offset <= 4194304) && (chunkVersion == 1)) {
        // KFS_LOG_STREAM_DEBUG << "Intentionally failing verify for chunk = " << chunkId << " offset = " << offset
        // << KFS_LOG_EOM;
        // mismatch = true;
        //}

        if (!mismatch)
            KFS_LOG_STREAM_DEBUG << "Checksum verified for chunk = " << chunkId << " offset = " << offset 
                                 << ": " << myChecksums.size() << " and got: " << checksums.size() << KFS_LOG_EOM;
    }
    if (mismatch) {
        status = -EAGAIN;
        statusMsg = "checksum mismatch";
        Done(EVENT_CMD_DONE, this);
        return;
    }
    // commit writes on local/remote servers
    status = needToWriteMetadata ?
        gChunkManager.ScheduleWriteChunkMetadata(chunkId) : 0;
    assert(status >= 0);
    Done(EVENT_CMD_DONE, this);
}

int
WriteSyncOp::ForwardToPeer(const ServerLocation &loc)
{
    assert(clnt != NULL);
    RemoteSyncSMPtr const peer = static_cast<ClientSM *>(clnt)->FindServer(loc);
    if (! peer) {
        statusMsg = "no such peer " + loc.ToString();
        return -EHOSTUNREACH;
    }
    fwdedOp = new WriteSyncOp(peer->NextSeqnum(), chunkId, chunkVersion, offset, numBytes);
    fwdedOp->numServers = numServers;
    fwdedOp->servers = servers;
    fwdedOp->clnt = this;
    SET_HANDLER(fwdedOp, &KfsOp::HandleDone);

    if (writeMaster) {
        fwdedOp->checksums = gChunkManager.GetChecksums(chunkId, offset, numBytes);
    } else {
        fwdedOp->checksums = this->checksums;
    }
    KFS_LOG_STREAM_DEBUG <<
        "forwarding write-sync to peer: " << fwdedOp->Show() <<
    KFS_LOG_EOM;
    peer->Enqueue(fwdedOp);
    return 0;
}

int
WriteSyncOp::Done(int code, void *data)
{
    verifyExecutingOnEventProcessor();    

    if (status >= 0 && fwdedOp && fwdedOp->status < 0) {
        status    = fwdedOp->status;
        statusMsg = fwdedOp->statusMsg;
        KFS_LOG_STREAM_ERROR <<
            "Peer: " << fwdedOp->Show() << " returned: " << fwdedOp->status <<
        KFS_LOG_EOM;
    }
    if (status < 0 && gLeaseClerk.IsLeaseValid(chunkId)) {
        // The write has failed; we don't want to renew the lease.
        // Now, when the client forces a re-allocation, the
        // metaserver will do a version bump; when the node that
        // was dead comes back, we can detect it has missed a write
        gLeaseClerk.RelinquishLease(chunkId);
    }
    numDone++;
    if (fwdedOp && numDone < 2) {
        return 0;
    }
    KFS_LOG_STREAM(
        status >= 0 ? MsgLogger::kLogLevelINFO : MsgLogger::kLogLevelERROR) <<
        (status >= 0 ? "done: " : "failed: ") << Show() <<
        " status: " << status <<
        (statusMsg.empty() ? "" : " msg: ") << statusMsg <<
    KFS_LOG_EOM;
    gLogger.Submit(this);
    return 0;
}

void 
WriteOp::Execute()
{
    UpdateCounter(CMD_WRITE);

    status = gChunkManager.WriteChunk(this);

    if (status < 0) {
        if (isFromRecordAppend) {
            HandleEvent(EVENT_CMD_DONE, this);
            return;
        } else {
            assert(wpop != NULL);
            wpop->HandleEvent(EVENT_CMD_DONE, this);
        }
    }
}

void
RecordAppendOp::Execute()
{
    ServerLocation peerLoc;
    int myPos;

    UpdateCounter(CMD_RECORD_APPEND);

    needToForwardToPeer(servers, numServers, myPos, peerLoc, true, writeId);
    gAtomicRecordAppendManager.AppendBegin(this, myPos, peerLoc);
}

void
GetRecordAppendOpStatus::Execute()
{
    gAtomicRecordAppendManager.GetOpStatus(this);
    gLogger.Submit(this);
}

void
SizeOp::Execute()
{
    UpdateCounter(CMD_SIZE);
    kfsFileId_t fid;

    // XXX: Sriram---we will need to schedule a read of the chunk metadata
    // For chunks that have an index, we don't know the size of the
    // data.  The stat() we did on startup will provide a size that is
    // larger than CHUNKSIZE.  We'll need to read the chunk metadata
    // to determine the length of the data in the chunk.

    size = -1;
    bool araStableFlag = true;
    status = gChunkManager.ChunkSize(chunkId, fid, &size, &araStableFlag);
    if (status >= 0 && ! araStableFlag) {
        statusMsg = "write append in progress, returning max chunk size";
        size      = KFS::CHUNKSIZE;
        KFS_LOG_STREAM_DEBUG <<
            statusMsg <<
            " chunk: " << chunkId <<
            " file: "  << fileId  <<
            " szie: "  << size    <<
        KFS_LOG_EOM;
    }
    // clnt->HandleEvent(EVENT_CMD_DONE, this);
    gLogger.Submit(this);
}

void
ChunkSpaceReserveOp::Execute()
{
    ServerLocation peerLoc;
    int myPos;

    UpdateCounter(CMD_SPC_RESERVE);

    needToForwardToPeer(servers, numServers, myPos, peerLoc, true, writeId);
    status = gAtomicRecordAppendManager.ChunkSpaceReserve(
            chunkId, writeId, nbytes, &statusMsg);
    if (status == 0) {
        // Only master keeps track of space reservations.
        assert(myPos == 0);
        ClientSM * const client = dynamic_cast<ClientSM *>(clnt);
        assert((client != 0) == (clnt != 0));
        if (client) {
            client->ChunkSpaceReserve(chunkId, writeId, nbytes);
        }
    }
    KFS_LOG_STREAM(status >= 0 ?
            MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
        "space reserve: "
        " chunk: "   << chunkId <<
        " writeId: " << writeId <<
        " bytes: "   << nbytes  <<
        " status: "  << status  <<
    KFS_LOG_EOM;
    gLogger.Submit(this);
}

void
ChunkSpaceReleaseOp::Execute()
{
    ServerLocation peerLoc;
    int myPos;

    UpdateCounter(CMD_SPC_RELEASE);
    needToForwardToPeer(servers, numServers, myPos, peerLoc, true, writeId);

    ClientSM * const client = dynamic_cast<ClientSM *>(clnt);
    assert((client != 0) == (clnt != 0));
    const size_t rsvd = client ?
        std::min(client->GetReservedSpace(chunkId, writeId), nbytes) : nbytes;
    status = gAtomicRecordAppendManager.ChunkSpaceRelease(
        chunkId, writeId, rsvd, &statusMsg);
    if (status == 0) {
        assert(myPos == 0);
        if (client) {
            client->UseReservedSpace(chunkId, writeId, rsvd);
        }
    }
    KFS_LOG_STREAM(status >= 0 ?
            MsgLogger::kLogLevelDEBUG : MsgLogger::kLogLevelERROR) <<
        "space release: "
        " chunk: "     << chunkId <<
        " writeId: "   << writeId <<
        " requested: " << nbytes  <<
        " reserved: "  << rsvd    <<
        " status: "    << status  <<
    KFS_LOG_EOM;
    gLogger.Submit(this);
}

void
GetChunkMetadataOp::Execute()
{
    ChunkInfo_t ci;

    UpdateCounter(CMD_GET_CHUNK_METADATA);

    SET_HANDLER(this, &GetChunkMetadataOp::HandleChunkMetaReadDone);
    if (gChunkManager.ReadChunkMetadata(chunkId, this) < 0) {
        status = -EINVAL;
        gLogger.Submit(this);
    }
}

int
GetChunkMetadataOp::HandleChunkMetaReadDone(int code, void *data)
{
    status = *(int *) data;
    if (status < 0) {
        gLogger.Submit(this);
        return 0;
    }
    const ChunkInfo_t * const info = gChunkManager.GetChunkInfo(chunkId);
    if (info) {
        if (info->chunkBlockChecksum || info->chunkSize == 0) {
            chunkVersion = info->chunkVersion;
            chunkSize    = info->chunkSize;
            indexSize    = info->chunkIndexSize;
            if (info->chunkBlockChecksum) {
                dataBuf = new IOBuffer();
                dataBuf->CopyIn((const char *)info->chunkBlockChecksum,
                    MAX_CHUNK_CHECKSUM_BLOCKS * sizeof(uint32_t));
                numBytesIO = dataBuf->BytesConsumable();
            }
        } else {
            assert(! "no checksums");
            status = -EIO;
        }
    } else {
        status = -EBADF;
    }
    gLogger.Submit(this);
    return 0;
}

void
GetChunkIndexOp::Execute()
{
    UpdateCounter(CMD_GET_CHUNK_INDEX);
    int res;

    // page in the chunk meta-data if needed
    if (!gChunkManager.NeedToReadChunkMetadata(chunkId)) {
        res = 0;
        HandleChunkMetaReadDone(0, &res);
        return;
    }
    
    SET_HANDLER(this, &GetChunkIndexOp::HandleChunkMetaReadDone);
    if ((res = gChunkManager.ReadChunkMetadata(chunkId, this)) < 0) {
        KFS_LOG_STREAM_INFO <<
            "Unable read chunk metadata for chunk: " << chunkId << "; error: " << res <<
        KFS_LOG_EOM;
        status = -EINVAL;
        gLogger.Submit(this);
        return;
    }
}

int
GetChunkIndexOp::HandleChunkMetaReadDone(int code, void *data)
{
    status = *(int *) data;
    if (status < 0) {
        gLogger.Submit(this);
        return 0;
    }
    const ChunkInfo_t * const info = gChunkManager.GetChunkInfo(chunkId);
    if (!info) {
        status = -EBADF;
        gLogger.Submit(this);
        return 0;
    }
    SET_HANDLER(this, &GetChunkIndexOp::HandleChunkIndexReadDone);
    gChunkManager.ReadChunkIndex(chunkId, this);
    return 0;
}

int
GetChunkIndexOp::HandleChunkIndexReadDone(int code, void *data)
{
    if (code == EVENT_DISK_ERROR) {
        status = data ? *(int *) data : -EIO;
        KFS_LOG_STREAM_INFO <<
            "Disk error: errno: " << status << " chunkid: " << chunkId <<
            KFS_LOG_EOM;
        gChunkManager.ChunkIOFailed(chunkId, status);        
    } else if (code == EVENT_DISK_READ) {
        // XXX: no checksumming on the index for now
        IOBuffer *b = (IOBuffer *) data;
        if (b) {
            dataBuf = new IOBuffer();
            dataBuf->Append(b);

            const ChunkInfo_t * const info = gChunkManager.GetChunkInfo(chunkId);

            if (info == NULL) {
                status = -EIO;
                KFS_LOG_STREAM_FATAL << "handle chunk index read possibly corrupt chunk: " <<
                    chunkId << KFS_LOG_EOM;
                gLogger.Submit(this);
                return 0;
            }
            // indexSize = dataBuf->BytesConsumable();
            indexSize = std::min((int) info->chunkIndexSize, dataBuf->BytesConsumable());
        } else {
            indexSize = 0;
        }
        status = indexSize;
        KFS_LOG_STREAM_INFO << "For chunk = " << chunkId <<
            " read chunk index of size: " << indexSize << KFS_LOG_EOM;
    } else {
        status = -EINVAL;
        KFS_LOG_STREAM_FATAL << "handle chunk index read unexpected event: " 
            " code: " << code << " data: " << data << KFS_LOG_EOM;
    }
    gLogger.Submit(this);
    return 0;
}


void
PingOp::Execute()
{
    totalSpace = gChunkManager.GetTotalSpace();
    usedSpace = gChunkManager.GetUsedSpace();
    if (usedSpace < 0)
        usedSpace = 0;
    status = 0;
    // clnt->HandleEvent(EVENT_CMD_DONE, this);
    gLogger.Submit(this);
}

void
DumpChunkMapOp::Execute()
{
   // Dump chunk map
   gChunkManager.DumpChunkMap();
   status = 0;
   gLogger.Submit(this);
}

void
StatsOp::Execute()
{
    ostringstream os;

    os << "Num aios: " << 0 << "\r\n";
    os << "Num ops: " << gChunkServer.GetNumOps() << "\r\n";
    globals().counterManager.Show(os);
    stats = os.str();
    status = 0;
    // clnt->HandleEvent(EVENT_CMD_DONE, this);
    gLogger.Submit(this);
}

inline static bool
OkHeader(const KfsOp* op, ostream &os, bool checkStatus = true)
{
    os << "OK\r\n";
    os << "Cseq: " << op->seq << "\r\n";
    os << "Status: " << op->status << "\r\n";
    if (! op->statusMsg.empty()) {
        const size_t p = op->statusMsg.find('\r');
        assert(string::npos == p && op->statusMsg.find('\n') == string::npos);
        os << "Status-message: " <<
            (p == string::npos ? op->statusMsg : op->statusMsg.substr(0, p)) <<
        "\r\n";
    }
    if (checkStatus && op->status < 0) {
        os << "\r\n";
    }
    return (op->status >= 0);
}

inline static ostream&
PutHeader(const KfsOp* op, ostream &os)
{
    OkHeader(op, os, false);
    return os;
}

///
/// Generate response for an op based on the KFS protocol.
///
void
KfsOp::Response(ostream &os)
{
    PutHeader(this, os) << "\r\n";
}

void
SizeOp::Response(ostream &os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os << "Size: " << size << "\r\n\r\n";
}

void
GetChunkMetadataOp::Response(ostream &os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os << "Chunk-handle: " << chunkId << "\r\n";
    os << "Chunk-version: " << chunkVersion << "\r\n";
    os << "Size: " << chunkSize << "\r\n";
    os << "Index-size: " << indexSize << "\r\n";
    os << "Content-length: " << numBytesIO << "\r\n\r\n";
}

void
GetChunkIndexOp::Response(ostream &os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os << "Content-length: " << indexSize << "\r\n\r\n";
}

void
ReadOp::Response(ostream &os)
{
    // if (! OkHeader(this, os, false)) {
    // return;
    // }
    OkHeader(this, os, false);
    os << "Drivename: " << driveName << "\r\n";
    if (status < 0) {
        os << "\r\n";
        return;
    }

    os << "DiskIOtime: " << diskIOTime << "\r\n";
    os << "Checksum-entries: " << checksum.size() << "\r\n";
    if (checksum.size() == 0) {
        os << "Checksums: " << 0 << "\r\n";
    } else {
        os << "Checksums: ";
        for (uint32_t i = 0; i < checksum.size(); i++)
            os << checksum[i] << ' ';
        os << "\r\n";
    }
    os << "Content-length: " << numBytesIO << "\r\n\r\n";
}

void
WriteIdAllocOp::Response(ostream &os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os << "Write-id: " << writeIdStr << "\r\n\r\n";
}

void
WritePrepareOp::Response(ostream &os)
{
    // no reply for a prepare...the reply is covered by sync
}

void
RecordAppendOp::Response(ostream &os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os << "File-offset: " << fileOffset << "\r\n\r\n";
}

void
RecordAppendOp::Request(ostream &os)
{
    os <<
        "RECORD_APPEND \r\n"
        "Cseq: "             << seq                   << "\r\n"
        "Version: "          << KFS_VERSION_STR       << "\r\n"
        "Chunk-handle: "     << chunkId               << "\r\n"
        "Chunk-version: "    << chunkVersion          << "\r\n"
        "Offset: "           << offset                << "\r\n"
        "File-offset: "      << fileOffset            << "\r\n"
        "Num-bytes: "        << numBytes              << "\r\n"
        "Checksum: "         << checksum              << "\r\n"
        "Num-servers: "      << numServers            << "\r\n"
        "Client-cseq: "      << clientSeq             << "\r\n"
        "Servers: "          << servers               << "\r\n"
        "Master-committed: " << masterCommittedOffset << "\r\n"
    "\r\n";
}

void
GetRecordAppendOpStatus::Request(std::ostream &os)
{
    os <<
        "GET_RECORD_APPEND_OP_STATUS \r\n"
        "Cseq: "          << seq     << "\r\n"
        "Chunk-handle: "  << chunkId << "\r\n"
        "Write-id: "      << writeId << "\r\n"
    "\r\n";
}

void
GetRecordAppendOpStatus::Response(std::ostream &os)
{
    PutHeader(this, os);
    os <<
        "Chunk-version: "         << chunkVersion       << "\r\n"
        "Op-seq: "                << opSeq              << "\r\n"
        "Op-status: "             << opStatus           << "\r\n"
        "Op-offset: "             << opOffset           << "\r\n"
        "Op-length: "             << opLength           << "\r\n"
        "Wid-append-count: "      << widAppendCount     << "\r\n"
        "Wid-bytes-reserved: "    << widBytesReserved   << "\r\n"
        "Chunk-bytes-reserved: "  << chunkBytesReserved << "\r\n"
        "Remaining-lease-time: "  << remainingLeaseTime << "\r\n"
        "Master-commit-offset: "  << masterCommitOffset << "\r\n"
        "Next-commit-offset: "    << nextCommitOffset   << "\r\n"
        "Wid-read-only: "         << (widReadOnlyFlag    ? 1 : 0) << "\r\n"
        "Wid-was-read-only: "     << (widWasReadOnlyFlag ? 1 : 0) << "\r\n"
        "Chunk-master: "          << (masterFlag         ? 1 : 0) << "\r\n"
        "Stable-flag: "           << (stableFlag         ? 1 : 0) << "\r\n"
        "Open-for-append-flag: "  << (openForAppendFlag  ? 1 : 0) << "\r\n"
        "Appender-state: "        << appenderState      << "\r\n"
        "Appender-state-string: " << appenderStateStr   << "\r\n"
    "\r\n";        
}

void
CloseOp::Request(ostream &os)
{
    os <<
        "CLOSE \r\n"
        "Cseq: "     << seq               << "\r\n"
        "Version: "  << KFS_VERSION_STR   << "\r\n"
        "Need-ack: " << (needAck ? 1 : 0) << "\r\n"
    ;
    if (numServers > 0) {
        os <<
            "Num-servers: " << numServers << "\r\n"
            "Servers: "     << servers    << "\r\n"
        ;
    }
    os << "Chunk-handle: " << chunkId << "\r\n";
    if (hasWriteId) {
        os << "Has-write-id: " << 1 << "\r\n";
    }
    if (masterCommitted >= 0) {
        os  << "Master-committed: " << masterCommitted << "\r\n";
    }
    os << "\r\n";
}

void
SizeOp::Request(ostream &os)
{
    os << "SIZE \r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n";
    os << "Chunk-version: " << chunkVersion << "\r\n\r\n";
}

void
GetChunkMetadataOp::Request(ostream &os)
{
    os << "GET_CHUNK_METADATA \r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n\r\n";
}

void
GetChunkIndexOp::Request(ostream &os)
{
    os << "GET_CHUNK_INDEX\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n\r\n";
}

void
ReadOp::Request(ostream &os)
{
    os << "READ \r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n";
    os << "Chunk-version: " << chunkVersion << "\r\n";
    os << "Offset: " << offset << "\r\n";
    os << "Num-bytes: " << numBytes << "\r\n\r\n";
}

void
WriteIdAllocOp::Request(ostream &os)
{
    os <<
        "WRITE_ID_ALLOC\r\n"
        "Version: "           << KFS_VERSION_STR             << "\r\n"
        "Cseq: "              << seq                         << "\r\n"
        "Chunk-handle: "      << chunkId                     << "\r\n"
        "Chunk-version: "     << chunkVersion                << "\r\n"
        "Offset: "            << offset                      << "\r\n"
        "Num-bytes: "         << numBytes                    << "\r\n"
        "For-record-append: " << (isForRecordAppend ? 1 : 0) << "\r\n"
        "Client-cseq: "       << clientSeq                   << "\r\n"
        "Num-servers: "       << numServers                  << "\r\n"
        "Servers: "           << servers                     << "\r\n"
    "\r\n";
}

void
WritePrepareFwdOp::Request(ostream &os)
{
    os << "WRITE_PREPARE\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Chunk-handle: " << owner->chunkId << "\r\n";
    os << "Chunk-version: " << owner->chunkVersion << "\r\n";
    os << "Offset: " << owner->offset << "\r\n";
    os << "Num-bytes: " << owner->numBytes << "\r\n";
    os << "Checksum: " << owner->checksum << "\r\n";
    os << "Num-servers: " << owner->numServers << "\r\n";
    os << "Servers: " << owner->servers << "\r\n\r\n";
}

void
WriteSyncOp::Request(ostream &os)
{
    os << "WRITE_SYNC\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n";
    os << "Chunk-version: " << chunkVersion << "\r\n";
    os << "Offset: " << offset << "\r\n";
    os << "Num-bytes: " << numBytes << "\r\n";
    os << "Checksum-entries: " << checksums.size() << "\r\n";
    if (checksums.size() == 0) {
        os << "Checksums: " << 0 << "\r\n";
    } else {
        os << "Checksums: ";
        for (uint32_t i = 0; i < checksums.size(); i++)
            os << checksums[i] << ' ';
        os << "\r\n";
    }
    os << "Num-servers: " << numServers << "\r\n";
    os << "Servers: " << servers << "\r\n\r\n";
}

void
HeartbeatOp::Response(ostream &os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os << response.str() << "\r\n";
}

void
ReplicateChunkOp::Response(ostream &os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os << "File-handle: " << fid << "\r\n";
    os << "Chunk-version: " << chunkVersion << "\r\n\r\n";
}

void
PingOp::Response(ostream &os)
{
    ServerLocation loc = gMetaServerSM.GetLocation();

    PutHeader(this, os);
    os << "Meta-server-host: " << loc.hostname << "\r\n";
    os << "Meta-server-port: " << loc.port << "\r\n";
    os << "Total-space: " << totalSpace << "\r\n";
    os << "Used-space: " << usedSpace << "\r\n\r\n";
}

void
BeginMakeChunkStableOp::Response(std::ostream& os)
{
    if (! OkHeader(this, os)) {
        return;
    }
    os <<
        "Chunk-size: "     << chunkSize     << "\r\n"
        "Chunk-checksum: " << chunkChecksum << "\r\n" 
    "\r\n";
}

void
DumpChunkMapOp::Response(ostream &os)
{
    ostringstream v;
    gChunkManager.DumpChunkMap(v);
    PutHeader(this, os) <<
        "Content-length: " << v.str().length() << "\r\n\r\n";
    if (v.str().length() > 0) {
       os << v.str();
    }
}

void
StatsOp::Response(ostream &os)
{
    PutHeader(this, os) << stats << "\r\n";
}

////////////////////////////////////////////////
// Now the handle done's....
////////////////////////////////////////////////

int
SizeOp::HandleDone(int code, void *data)
{
    // notify the owning object that the op finished
    clnt->HandleEvent(EVENT_CMD_DONE, this);
    return 0;
}

int
GetChunkMetadataOp::HandleDone(int code, void *data)
{
    // notify the owning object that the op finished
    clnt->HandleEvent(EVENT_CMD_DONE, this);
    return 0;
}

int
ReplicateChunkOp::HandleDone(int code, void *data)
{
    if (data != NULL)
        chunkVersion = * (kfsSeq_t *) data;
    else
        chunkVersion = -1;
    gLogger.Submit(this);
    return 0;
}

class ReadChunkMetaNotifier {
    int res;
public:
    ReadChunkMetaNotifier(int r) : res(r) { }
    void operator()(KfsOp *op) {
        op->HandleEvent(EVENT_CMD_DONE, &res);
    }
};

int
ReadChunkMetaOp::HandleMetaReadDone(int code, void *data)
{
    if (code == EVENT_DISK_ERROR) {
        status = data ? *(int*)data : -EIO;
        KFS_LOG_STREAM_INFO <<
            "Disk error: errno: " << status << " chunkid: " << chunkId <<
        KFS_LOG_EOM;
        gChunkManager.ChunkIOFailed(chunkId, status);
    } else if (code == EVENT_DISK_READ) {
        IOBuffer *dataBuf = (IOBuffer *) data;
        if (dataBuf->BytesConsumable() >= (int) sizeof(DiskChunkInfo_t)) {            
            DiskChunkInfo_t dci;
            dataBuf->CopyOut((char *) &dci, sizeof(DiskChunkInfo_t));
            gChunkManager.SetChunkMetadata(dci, chunkId);
        } else {
            // Force validation to fail:
            KFS_LOG_STREAM_ERROR << "read chunk meta data short read: " <<
                dataBuf->BytesConsumable() <<
            KFS_LOG_EOM;
            gChunkManager.ChunkIOFailed(chunkId, status);
            status = -EIO;
        }
    } else {
        status = -EINVAL;
        KFS_LOG_STREAM_FATAL << "read chunk meta data unexpected event: "
            " code: " <<  code << " data: " << data <<
        KFS_LOG_EOM;
        abort();
    }
    int res = status;
    gChunkManager.ReadChunkMetadataDone(chunkId);
    clnt->HandleEvent(EVENT_CMD_DONE, &res);

    for_each(waiters.begin(), waiters.end(), ReadChunkMetaNotifier(res));

    delete this;
    return 0;
}

int
ReadChunkMetaOp::HandleIndexReadDone(int code, void *data)
{
    if (code == EVENT_DISK_ERROR) {
        status = data ? *(int*)data : -EIO;
        KFS_LOG_STREAM_INFO <<
            "Disk error: errno: " << status << " chunkid: " << chunkId <<
        KFS_LOG_EOM;
        gChunkManager.ChunkIOFailed(chunkId, status);
    } else if (code != EVENT_DISK_READ) {
        status = -EINVAL;
        KFS_LOG_STREAM_FATAL << "read chunk index unexpected event: "
            " code: " <<  code << " data: " << data <<
        KFS_LOG_EOM;
        abort();
    }
    clnt->HandleEvent(code, data);

    delete this;
    return 0;
}

WriteOp::~WriteOp()
{
    if (isWriteIdHolder) {
        // track how long it took for the write to finish up:
        // enqueueTime tracks when the last write was done to this
        // writeid
        struct timeval lastWriteTime;

        lastWriteTime.tv_sec = enqueueTime;
        lastWriteTime.tv_usec = 0;
        float timeSpent = ComputeTimeDiff(startTime, lastWriteTime);
        if (timeSpent < 1e-6)
            timeSpent = 0.0;

        if (timeSpent > 5.0) {
            gChunkServer.SendTelemetryReport(CMD_WRITE, timeSpent);
        }
        
        // we don't want write id's to pollute stats
        gettimeofday(&startTime, NULL);
        gCounters.mWriteDuration.Update(1);
        gCounters.mWriteDuration.Update(timeSpent);
    }

    delete dataBuf;
    if (rop != NULL) {
        rop->wop = NULL;
        // rop->dataBuf can be non null when read completes but WriteChunk
        // fails, and returns before using this buff.
        // Read op destructor deletes dataBuf.
        delete rop;
    }
}

WriteIdAllocOp::~WriteIdAllocOp()
{
    delete fwdedOp;
}

WritePrepareOp::~WritePrepareOp()
{
    // on a successful prepare, dataBuf should be moved to a write op.
    assert((status != 0) || (dataBuf == NULL));

    delete dataBuf;
    delete writeFwdOp;
    delete writeOp;
}

WriteSyncOp::~WriteSyncOp()
{
    delete fwdedOp;
    delete writeOp;
}

void
LeaseRenewOp::Request(ostream &os)
{
    os << "LEASE_RENEW\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n";
    os << "Lease-id: " << leaseId << "\r\n";
    os << "Lease-type: " << leaseType << "\r\n\r\n";
}

int
LeaseRenewOp::HandleDone(int code, void *data)
{
    KfsOp *op = (KfsOp *) data;

    assert(op == this);
    return op->clnt->HandleEvent(EVENT_CMD_DONE, data);
}

void
LeaseRelinquishOp::Request(ostream &os)
{
    os << "LEASE_RELINQUISH\r\n"
        "Version: "        << KFS_VERSION_STR << "\r\n"
        "Cseq: "           << seq             << "\r\n"
        "Chunk-handle: "   << chunkId         << "\r\n"
        "Lease-id: "       << leaseId         << "\r\n"
        "Lease-type: "     << leaseType       << "\r\n"
    ;
    if (chunkSize >= 0) {
        os << "Chunk-size: " << chunkSize << "\r\n";
    }
    if (hasChecksum) {
        os << "Chunk-checksum: " << chunkChecksum << "\r\n";
    }
    os << "\r\n";
}

int
LeaseRelinquishOp::HandleDone(int code, void *data)
{
    assert((KfsOp *)data == this);
    delete this;
    return 0;
}

void
CorruptChunkOp::Request(ostream &os)
{
    os << "CORRUPT_CHUNK\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "File-handle: " << fid << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n";
    os << "Is-chunk-lost: " << isChunkLost << "\r\n\r\n";
}

int
CorruptChunkOp::HandleDone(int code, void *data)
{
    // Thank you metaserver for replying :-)
    delete this;
    return 0;
}

// Messages we send to sort-helper
void
AllocChunkOp::Request(ostream &os)
{
    os << "ALLOCATE\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "File-handle: " << fileId << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n\r\n";
}

void
SorterFlushOp::Request(ostream &os)
{
    os << "SORTER_FLUSH\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "File-handle: " << fid << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n";
    os << "Chunk-size: " << chunkSize << "\r\n";
    os << "Output-name: " << outputFn << "\r\n\r\n";
}

void
SortFileOp::Request(ostream &os)
{
    os << "SORT_FILE\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "File-handle: " << fid << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n";
    os << "Chunk-size: " << chunkSize << "\r\n";
    os << "Input-name: " << inputFn << "\r\n";
    os << "Output-name: " << outputFn << "\r\n\r\n";
}

void
SorterFlushOp::Response(ostream &os)
{
    OkHeader(this, os, false);
    os << "Chunk-handle: " << chunkId << "\r\n";
    os << "Index-size: " << indexSize << "\r\n";
    os << "Output-name: " << outputFn << "\r\n\r\n";
}

void
SortFileOp::Response(ostream &os)
{
    OkHeader(this, os, false);
    os << "Chunk-handle: " << chunkId << "\r\n";
    os << "Index-size: " << indexSize << "\r\n";
    os << "Input-name: " << inputFn << "\r\n";
    os << "Output-name: " << outputFn << "\r\n\r\n";
}

class PrintChunkInfo {
    ostringstream &os;
public:
    PrintChunkInfo(ostringstream &o) : os(o) { }
    void operator() (ChunkInfo_t &c) {
        os << c.fileId << ' ';
        os << c.chunkId << ' ';
        os << c.chunkVersion << ' ';
    }
};

void
HelloMetaOp::Request(ostream &os)
{
    ostringstream chunkInfo;

    os << "HELLO \r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Chunk-server-name: " << myLocation.hostname << "\r\n";
    os << "Chunk-server-port: " << myLocation.port << "\r\n";
    os << "Cluster-key: " << clusterKey << "\r\n";
    os << "MD5Sum: " << md5sum << "\r\n";
    os << "Rack-id: " << rackId << "\r\n";
    os << "Total-space: " << totalSpace << "\r\n";
    os << "Used-space: " << usedSpace << "\r\n";
    os << "Uptime: " << libkfsio::globalNetManager().UpTime() << "\r\n";

    // now put in the chunk information
    os << "Num-chunks: " << chunks.size() << "\r\n";
    os << "Num-not-stable-append-chunks: " << notStableAppendChunks.size() << "\r\n";
    os << "Num-not-stable-chunks: " << notStableChunks.size() << "\r\n";
    os << "Num-appends-with-wids: " <<
        gAtomicRecordAppendManager.GetAppendersWithWidCount() << "\r\n";
    os << "Num-re-replications: " << Replicator::GetNumReplications() << "\r\n";
    // figure out the content-length first...
    for_each(chunks.begin(), chunks.end(), PrintChunkInfo(chunkInfo));
    for_each(notStableAppendChunks.begin(), notStableAppendChunks.end(), PrintChunkInfo(chunkInfo));
    for_each(notStableChunks.begin(), notStableChunks.end(), PrintChunkInfo(chunkInfo));

    os << "Content-length: " << chunkInfo.str().length() << "\r\n\r\n";
    os << chunkInfo.str().c_str();
}

void
SetProperties::Request(std::ostream &os)
{
    string content;
    properties.getList(content, "");
    contentLength = content.length();
    os << "CMD_SET_PROPERTIES \r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Content-length: " << contentLength << "\r\n\r\n";
    os << content;
}

bool
SetProperties::ParseContent(istream& is)
{
    properties.clear();
    status = min(0, properties.loadProperties(is, '=', false));
    if (status != 0) {
        statusMsg = "failed to parse properties";
    }
    return (status == 0);
}

void
SetProperties::Execute()
{
    if (status == 0) {
        if (! MsgLogger::GetLogger()) {
            status    = -ENOENT;
            statusMsg = "no logger";
        } else {
            MsgLogger::GetLogger()->SetParameters(
                properties, "chunkServer.msgLogWriter.");
        }
    }
    gLogger.Submit(this);
}

// string RestartChunkServer();

void
RestartChunkServerOp::Execute()
{
    // This is silly...the code in the libary depends on something in main
    // statusMsg = RestartChunkServer();
    statusMsg = "";
    status = statusMsg.empty() ? 0 : -1;
    gLogger.Submit(this);
}

// timeout op to the event processor going
void
TimeoutOp::Execute()
{
    gChunkManager.Timeout();
    gLeaseClerk.Timeout();
    gAtomicRecordAppendManager.Timeout();
    // do not delete "this" since it is either a member variable of
    // the ChunkManagerTimeoutImpl or a static object.  
    // bump the seq # so we know how many times it got executed
    seq++;
}

void
KillRemoteSyncOp::Execute()
{
    RemoteSyncSM *remoteSyncSM = static_cast<RemoteSyncSM *>(clnt);
    assert(remoteSyncSM != NULL);

    remoteSyncSM->Finish();
}

void
HelloMetaOp::Execute()
{
    totalSpace = gChunkManager.GetTotalSpace();
    usedSpace = gChunkManager.GetUsedSpace();
    gChunkManager.GetHostedChunks(chunks, notStableChunks, notStableAppendChunks);
    status = 0;
    gLogger.Submit(this);
}

