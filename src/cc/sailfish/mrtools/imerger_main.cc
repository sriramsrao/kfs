//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: imerger_main.cc 3372 2011-11-28 22:13:52Z sriramr $
//
// Created 2010/11/14
//
// Copyright 2010 Yahoo Corporation.  All rights reserved.
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
// \brief imerger: merges records from chunks of an I-file and write to stdout.
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

int main(int argc, char **argv)
{
    ServerLocation metaLoc;
    ServerLocation workbuilderLoc;
    string basedir;
    int partition;
    string logLevel;
    string jobId;
    // set it at 256M
    uint64_t mergeHeapSize = 256 * 1024 * 1024;

    // pass in the memory size for each sorted run file...DWIM

    // Declare the supported options.
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("metahost,M", po::value<string>(&metaLoc.hostname)->default_value("usualfall.greatamerica.corp.yahoo.com"), "KFS metaserver hostname")
        ("metaport,P", po::value<int>(&metaLoc.port)->default_value(20000), "KFS metaserver port")
        ("wbhost,w", po::value<string>(&workbuilderLoc.hostname)->default_value("localhost"), "Sailfish workbuilder hostname")
        ("wbport,x", po::value<int>(&workbuilderLoc.port)->default_value(12345), "Sailfish workbuilder port")
        ("jobid,J", po::value<string>(&jobId)->default_value(""), "Hadoop Job id")
        ("basedir,B", po::value<string>(&basedir)->default_value("/jobs"), "Base dir for storing I-files in KFS")
        ("loglevel,L", po::value<string>(&logLevel)->default_value("INFO"), "Log level (INFO or DEBUG)")
        ("partition,i",po::value<int>(&partition)->default_value(1), "partition")
        ("mergeHeapSize,m",po::value<uint64_t>(&mergeHeapSize)->default_value(256 * 1024 * 1024), "Heap size for merge (default = 256MB)")
        ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

#if defined(KFS_OS_NAME_LINUX)
    // if we are created by a fork, we exit when parent dies
    prctl(PR_SET_PDEATHSIG, SIGHUP);
#endif

    // assume you got the args right

    // Initialize the msg-logger
    MsgLogger::Init(NULL, str2LogLevel(logLevel));

    std::cerr << "Our pid is: " << getpid() << std::endl;

    KfsClientPtr kfsClient = getKfsClientFactory()->GetClient(metaLoc.hostname, metaLoc.port);
    if (!kfsClient) {
        std::cerr << "Unable to initialize kfsClient.  Exiting!" << std::endl;
        exit(-1);
    }

    string wbHostPort = workbuilderLoc.hostname + ":" + boost::lexical_cast<string>(workbuilderLoc.port);

    IMerger imerger(kfsClient, partition, jobId, wbHostPort,
        basedir.c_str());

    // kgroupBy.LoadData(1, basedir + "/deadmappers");

    // send output to stdout
    imerger.Start(1, basedir + "/deadmappers", mergeHeapSize);

    MsgLogger::Stop();

    std::cerr << "IMerger returned...we are all done" << std::endl;
}
