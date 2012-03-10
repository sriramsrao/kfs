//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: icomparer_main.cc $
//
// Created 2011/10/21
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
// Validate that the output of a merge is ordered.
//----------------------------------------------------------------------------

#include "common/log.h"
#include "imerger.h"
#include "kappender.h"

#include <iostream>
#include <sstream>
#include <boost/program_options.hpp>
#include <string>
#include <sys/types.h>
#include <unistd.h>

#if defined(KFS_OS_NAME_LINUX)
#include <sys/prctl.h>
#include <signal.h>
#endif

namespace po = boost::program_options;

using namespace KFS;
using std::string;
using std::ostringstream;

void validateMergeOutput(const string &inputFile);
int readKey(int fd, IFileKey_t &key);

int main(int argc, char **argv)
{
    string inputFile;

    // Declare the supported options.
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("inputFile,i", po::value<string>(&inputFile)->default_value(""), "Input file")
        ;
    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    // Initialize the msg-logger
    MsgLogger::Init(NULL, MsgLogger::kLogLevelINFO);

    std::cerr << "Our pid is: " << getpid() << std::endl;

    validateMergeOutput(inputFile);

    MsgLogger::Stop();
}

void
validateMergeOutput(const string &inputFile)
{
    int fd = open(inputFile.c_str(), O_RDONLY);

    if (fd < 0) {
        std::cerr << "Unable to open: " << inputFile << std::endl;
        return;
    }

    IFileKey_t key;
    IFileKey_t prevKey;
    int retval;
    off_t fdPos;
    retval = readKey(fd, prevKey);
    while (1) {
        fdPos = lseek(fd, 0, SEEK_CUR);
        retval = readKey(fd, key);
        if (retval < 0) {
            KFS_LOG_STREAM_INFO << "Stopping because we ran out of keys in: " << inputFile
                << KFS_LOG_EOM;
            break;
        }
        if ((prevKey < key) || (prevKey == key)) {
            prevKey = key;
            continue;
        }
        assert(!"Possible: mismatch");
    }
    close(fd);
}

int
readKey(int fd, IFileKey_t &key)
{
    int retval;
    int valLen;

    retval = read(fd, &key.keyLen, sizeof(int));
    if (retval < 0)
        return -1;
    key.keyLen = ntohl(key.keyLen);
    key.key.reset(new unsigned char[key.keyLen]);
    retval = read(fd, key.key.get(), key.keyLen);
    if (retval != (int) key.keyLen)
        return -1;

    // skip over the value
    retval = read(fd, &valLen, sizeof(int));
    if (retval < 0)
        return 0;
    valLen = ntohl(valLen);
    lseek(fd, valLen, SEEK_CUR);
    return 0;
}
