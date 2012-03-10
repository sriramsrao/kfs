//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: chunksorter_main.cc $
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
// \brief Code that sorts a chunk and writes it back to disk: The chunk contains
// <key, value> pairs and sorting is useful to allow reading keys by range.
//----------------------------------------------------------------------------

#include "common/log.h"
#include "chunksorter.h"

#include <unistd.h>
#include <fcntl.h>

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

SortWorkerManager gSortWorkerManager;

static MsgLogger::LogLevel
str2LogLevel(string logLevel)
{
    if (logLevel == "DEBUG")
        return MsgLogger::kLogLevelDEBUG;
    if (logLevel == "INFO")
        return MsgLogger::kLogLevelINFO;
    if (logLevel == "WARN")
        return MsgLogger::kLogLevelWARN;
    if (logLevel == "FATAL")
        return MsgLogger::kLogLevelFATAL;

    return MsgLogger::kLogLevelINFO;
}

int main(int argc, char **argv)
{
    string logLevel;
    string logFn;
    int chunkserverPort;

    // Declare the supported options.
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("chunkserverport,C", po::value<int>(&chunkserverPort)->default_value(21000), "KFS chunkserver sorter port or 0 to read from stdin")
        ("loglevel,L", po::value<string>(&logLevel)->default_value("INFO"), "Log level (INFO or DEBUG)")
        ("logFn,l", po::value<string>(&logFn)->default_value(""), "Path to a log file")
        ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    if (logFn == "")
        MsgLogger::Init(NULL, str2LogLevel(logLevel));
    else {
        MsgLogger::Init(logFn.c_str(), str2LogLevel(logLevel));
    }

    gSortWorkerManager.mainLoop(chunkserverPort);
    MsgLogger::Stop();
}
