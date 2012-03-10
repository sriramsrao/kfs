//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: chunksort_test_main.cc $
//
// Created 2011/02/12
//
// Copyright 2011 Yahoo Corporation.  All rights reserved.
// This file is part of the Sailfish project.
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
// \brief Driver program that can be used to test chunksorter.
//----------------------------------------------------------------------------

#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <cerrno>
#include <vector>
#include <string>
#include <sstream>
#include <algorithm>
#include <boost/program_options.hpp>
namespace po = boost::program_options;

#include "chunk/KfsOps.h"
#include "chunk/Chunk.h"
#include "chunk/ChunkManager.h"
#include "sailfish/mrtools/ifile_base.h"

using namespace KFS;
using std::string;
using std::vector;
using std::ostringstream;
using std::find_if;

char *chunkBuffer;
int chunkDataLen;
DiskChunkInfo_t *dci;
// buffer to hold one record
char *recordBuffer;
uint nextCseq = 1;
bool doParsing = true;

uint
nextSeq()
{
    return nextCseq++;
}

int
scanMatch(const char *buffer, int bufsz, const string &key)
{
    const char *curP = buffer;
    const char *endP = buffer + bufsz;
    IFileRecord_t record;
    int numHits = 0;

    while (curP < endP) {
        bool match = false;
        int cval = -1;
        // Format: <pkt hdr><4 byte key-len><key><4-byte val-len><value>
        curP += sizeof(IFilePktHdr_t);
        memcpy(&record.keyLen, curP, sizeof(uint32_t));
        curP += sizeof(uint32_t);
        record.keyLen = ntohl(record.keyLen);
        if ((record.keyLen - 1 == key.size()) &&
            (cval = memcmp(key.c_str(), curP + 1, record.keyLen - 1)))
            match = true;
        curP += record.keyLen;
        memcpy(&record.valLen, curP, sizeof(uint32_t));
        curP += sizeof(uint32_t);
        record.valLen = ntohl(record.valLen);
        if (match) {
            record.value.reset(new unsigned char[record.valLen]);
            memcpy(record.value.get(), curP, record.valLen);
            numHits++;
        }
        curP += record.valLen;
        record.recLen = record.keyLen + record.valLen;
    }
    return numHits;
}

void
parseChunk(const char *buffer, int bufsz, vector<IFileRecord_t> &records)
{
    const char *curP = buffer;
    const char *endP = buffer + bufsz;

    while (curP < endP) {
        IFileRecord_t record;

        // Format: <pkt hdr><4 byte key-len><key><4-byte val-len><value>
        memcpy(&record.pktHdr, curP, sizeof(IFilePktHdr_t));
        curP += sizeof(IFilePktHdr_t);
        memcpy(&record.keyLen, curP, sizeof(uint32_t));
        curP += sizeof(uint32_t);
        record.keyLen = ntohl(record.keyLen);
        record.key.reset(new unsigned char[record.keyLen]);
        memcpy(record.key.get(), curP, record.keyLen);
        curP += record.keyLen;
        memcpy(&record.valLen, curP, sizeof(uint32_t));
        curP += sizeof(uint32_t);
        record.valLen = ntohl(record.valLen);
        record.value.reset(new unsigned char[record.valLen]);
        memcpy(record.value.get(), curP, record.valLen);
        curP += record.valLen;
        record.recLen = record.keyLen + record.valLen;
        records.push_back(record);
    }

}

int
readFromChunk(const string &inputFn, vector<IFileRecord_t> &records)
{
    struct stat statRes;
    int fd, res, nread;

    res = stat(inputFn.c_str(), &statRes);
    if (res < 0) {
        KFS_LOG_STREAM_INFO << "Unable to stat: " << inputFn
            << " bailing..." << KFS_LOG_EOM;
        return res;
    }
    fd = open(inputFn.c_str(), O_RDONLY);
    if (fd < 0) {
        KFS_LOG_STREAM_INFO << "Unable to open: " << inputFn
            << " bailing..." << KFS_LOG_EOM;
        return res;
    }
    // don't worry about any index that exists in the file
    if (statRes.st_size > (int) (KFS_CHUNK_HEADER_SIZE + KFS::CHUNKSIZE))
        statRes.st_size = KFS_CHUNK_HEADER_SIZE + KFS::CHUNKSIZE;

    chunkBuffer = new char[statRes.st_size];

    nread = read(fd, chunkBuffer, statRes.st_size);
    if (nread != (int) statRes.st_size) {
        KFS_LOG_STREAM_INFO << "Short read: expect = " << statRes.st_size
            << " ; however, read = " << nread << " err = " << errno << KFS_LOG_EOM;
        close(fd);
        return -1;
    }
    close(fd);

    dci = new DiskChunkInfo_t;
    memcpy(dci, chunkBuffer, sizeof(DiskChunkInfo_t));
    chunkDataLen = dci->chunkSize;

    recordBuffer = new char[32768];

    if (doParsing)
        parseChunk(chunkBuffer + KFS_CHUNK_HEADER_SIZE, chunkDataLen, records);
    return 0;
}

void
sendAllocateChunk(int outputFd, int chunkId)
{
    AllocChunkOp ac(nextSeq());
    ostringstream os;

    ac.fileId = 10;
    ac.chunkId = chunkId;
    ac.chunkVersion = 1;
    ac.Request(os);
    write(outputFd, os.str().c_str(), os.str().size());
}

void
sendRecordAppend(int outputFd, int chunkId, IFileRecord_t &record)
{
    RecordAppendOp ra(nextSeq());
    ostringstream os;

    ra.chunkId = chunkId;
    ra.chunkVersion = 1;
    ra.numBytes = record.serialize(recordBuffer);
    ra.Request(os);
    write(outputFd, os.str().c_str(), os.str().size());
    write(outputFd, recordBuffer, ra.numBytes);
}

void
sendFlush(int outputFd, int chunkId, const string &outputFn)
{
    SorterFlushOp shf(nextSeq());
    ostringstream os;

    shf.chunkId = chunkId;
    shf.chunkVersion = 1;
    shf.outputFn = outputFn;
    shf.Request(os);
    write(outputFd, os.str().c_str(), os.str().size());
}

void
sortChunk(const string &inputFn, const string &outputFn)
{
    vector<IFileRecord_t> records;
    int outputFd = 1;
    int chunkId = 64;

    if (readFromChunk(inputFn, records)) {
        KFS_LOG_STREAM_INFO << "Unable to read: " << inputFn << KFS_LOG_EOM;
        return;
    }

    sendAllocateChunk(outputFd, chunkId);

    for (uint32_t i = 0; i < records.size(); i++) {
        sendRecordAppend(outputFd, chunkId, records[i]);
    }

    sendFlush(outputFd, chunkId, outputFn);

    KFS_LOG_STREAM_INFO << "Done with sending records and asking for flush" << KFS_LOG_EOM;

}

class RecordMatcherPred {
    string key;
public:
    RecordMatcherPred(const string &k) : key(k) { }
    bool operator() (const IFileRecord_t &entry) const {
        // dealing with Java serialized keys :(
        if (entry.keyLen - 1 != key.size())
            return false;
        const unsigned char *data = entry.key.get() + 1;
        int cval = memcmp(key.c_str(), data, entry.keyLen - 1);
        return cval == 0;
    }
};

void
scanChunk1(const string &inputFn, const string &scanKey)
{
    vector<IFileRecord_t> records;
    vector<IFileRecord_t>::iterator entry;

    KFS_LOG_STREAM_INFO << "Starting read..." << KFS_LOG_EOM;

    if (readFromChunk(inputFn, records)) {
        KFS_LOG_STREAM_INFO << "Unable to read: " << inputFn << KFS_LOG_EOM;
        return;
    }

    KFS_LOG_STREAM_INFO << "Read done...starting scan" << KFS_LOG_EOM;

    entry = find_if(records.begin(), records.end(), RecordMatcherPred(scanKey));

    if (entry != records.end()) {
        KFS_LOG_STREAM_INFO << "Scan found key" << KFS_LOG_EOM;
    } else {
        KFS_LOG_STREAM_INFO << "Hmm...Done with scan" << KFS_LOG_EOM;
    }
}

void
scanChunk(const string &inputFn, const string &scanKey)
{
    vector<IFileRecord_t> records;

    doParsing = false;
    KFS_LOG_STREAM_INFO << "Starting read..." << KFS_LOG_EOM;

    if (readFromChunk(inputFn, records)) {
        KFS_LOG_STREAM_INFO << "Unable to read: " << inputFn << KFS_LOG_EOM;
        return;
    }

    KFS_LOG_STREAM_INFO << "Read done...starting scan" << KFS_LOG_EOM;

    int numHits;

    numHits = scanMatch(chunkBuffer + KFS_CHUNK_HEADER_SIZE, chunkDataLen, scanKey);

    if (numHits > 0) {
        KFS_LOG_STREAM_INFO << "Scan found key" << KFS_LOG_EOM;
    } else {
        KFS_LOG_STREAM_INFO << "Hmm...Done with scan" << KFS_LOG_EOM;
    }
}

int main(int argc, char **argv)
{
    string inputFn;
    string outputFn;
    string scanKey;

    // Declare the supported options.
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("input,I", po::value<string>(&inputFn)->default_value(""), "<testmode> input chunk to sort")
        ("scankey,S", po::value<string>(&scanKey)->default_value(""), "<testmode> key to search for the chunk")
        ("output,O", po::value<string>(&outputFn)->default_value(""), "<testmode> output file to be written to")
        ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    MsgLogger::Init(NULL, MsgLogger::kLogLevelDEBUG);

    if (scanKey != "")
        scanChunk(inputFn, scanKey);
    else
        sortChunk(inputFn, outputFn);

    MsgLogger::Stop();
}
