//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: ifilereader_main.cc $
//
// Created 2011/02/07
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
// \brief Tool to serialize the contents of an I-file
//----------------------------------------------------------------------------

#include "common/log.h"
#include "ifile_base.h"
#include "chunk/Chunk.h"
#include "chunk/ChunkManager.h"
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

#include <cerrno>
#include <arpa/inet.h>
#include <iostream>
#include <sstream>
#include <boost/program_options.hpp>
#include <boost/scoped_array.hpp>
#include <algorithm>
#include <string>
#include <vector>
namespace po = boost::program_options;

using namespace KFS;
using std::string;
using std::ostringstream;
using std::sort;
using std::vector;

static
void parseChunk(const char *bufferStart, int bufsz)
{
    const char *curP = bufferStart;
    const char *endP = bufferStart + bufsz;
    int pos;

    while (curP < endP) {
        IFileRecord_t record;

        pos = curP - bufferStart;
        memcpy(&record.pktHdr, curP, sizeof(IFilePktHdr_t));
        curP += sizeof(IFilePktHdr_t);
        memcpy(&record.keyLen, curP, sizeof(uint32_t));
        curP += sizeof(uint32_t);
        record.keyLen = ntohl(record.keyLen);
        record.key.reset(new unsigned char[record.keyLen + 1]);
        record.key.get()[record.keyLen] = '\0';
        memcpy(record.key.get(), curP, record.keyLen);
        curP += record.keyLen;
        memcpy(&record.valLen, curP, sizeof(uint32_t));
        curP += sizeof(uint32_t);
        record.valLen = ntohl(record.valLen);
        record.value.reset(new unsigned char[record.valLen]);
        memcpy(record.value.get(), curP, record.valLen);
        curP += record.valLen;
        record.recLen = record.keyLen + record.valLen;
        std::cout << pos << ' ' << record.key.get() << std::endl;
    }
}

static
void parseKeys(const string &filename)
{
    struct stat statRes;
    int fd, res, nread;
    boost::scoped_array<char> bufferPtr;

    res = stat(filename.c_str(), &statRes);
    if (res < 0) {
        KFS_LOG_STREAM_INFO << "Unable to stat: " << filename
            << " exiting..." << KFS_LOG_EOM;
        exit(-1);
    }
    fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        KFS_LOG_STREAM_INFO << "Unable to open: " << filename
            << " exiting..." << KFS_LOG_EOM;
        exit(-1);
    }
    bufferPtr.reset(new char[statRes.st_size]);
    nread = read(fd, bufferPtr.get(), statRes.st_size);
    if (nread != (int) statRes.st_size) {
        KFS_LOG_STREAM_INFO << "Short read: expect = " << statRes.st_size
            << " ; however, read = " << nread << " err = " << errno << KFS_LOG_EOM;
        exit(-1);
    }
    close(fd);

    DiskChunkInfo_t *dci = new DiskChunkInfo_t;
    memcpy(dci, bufferPtr.get(), sizeof(DiskChunkInfo_t));

    parseChunk(bufferPtr.get() + KFS_CHUNK_HEADER_SIZE, dci->chunkSize);

}

int main(int argc, char **argv)
{
    string logLevel;
    string filename;

    // Declare the supported options.
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("input,I", po::value<string>(&filename)->default_value(""), "input chunk to read")
        ("loglevel,L", po::value<string>(&logLevel)->default_value("INFO"), "Log level (INFO or DEBUG)")
        ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if ((vm.count("help") || (filename == ""))) {
        std::cout << desc << std::endl;
        return 0;
    }

    MsgLogger::Init(NULL, MsgLogger::kLogLevelINFO);
    parseKeys(filename);

}


