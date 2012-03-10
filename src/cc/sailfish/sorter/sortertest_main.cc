//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: sortertest_main.cc $
//
// Created 2011/09/21
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
// \brief Uses the sorter code for testing: if compression is involved,
// that code path is tested too.
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

int main(int argc, char **argv)
{
    SortFileOp sfo(0);
    KFS::SortWorker worker(0, 0, 0);

    // Declare the supported options.
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("input,i", po::value<string>(&sfo.inputFn)->default_value(""), "Input file to be sorted")
        ("output,o", po::value<string>(&sfo.outputFn)->default_value(""), "Output file")
        ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    MsgLogger::Init(NULL, MsgLogger::kLogLevelINFO);

    worker.sortFile(&sfo);

    MsgLogger::Stop();
}
