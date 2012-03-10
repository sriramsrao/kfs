//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kappender_main.cc 3373 2011-11-28 22:17:21Z sriramr $
//
// Created 2010/11/10
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
// \brief Driver code that starts up the kappender.
//----------------------------------------------------------------------------

#include "util.h"
#include "kappender.h"
#include "libkfsClient/KfsClient.h"

#include <stdlib.h>
#include <sys/resource.h>
#include <iostream>
#include <sstream>
#include <boost/program_options.hpp>
#include <string>
namespace po = boost::program_options;

using namespace KFS;
using std::string;
using std::ostringstream;

static void
InvalidateDeadMappers(KfsClientPtr &kfsClient, const string &basedir,
    int mapperId, int attemptNum);

int main(int argc, char **argv)
{
    int bufferLimit;
    ServerLocation metaLoc;
    ServerLocation workbuilderLoc;
    string jobId;
    string basedir;
    string logLevel;
    int numPartitions;
    int mapperId;
    int attemptNum;
    bool enableCompression = false;

    // Declare the supported options.
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("compress,z", "Enable compression in appender (default=false)")
        ("bufferlimit,l", po::value<int>(&bufferLimit)->default_value(0), "Num bytes appender buffers before flushing to disk")
        ("metahost,M", po::value<string>(&metaLoc.hostname)->default_value("localhost"), "KFS metaserver hostname")
        ("metaport,P", po::value<int>(&metaLoc.port)->default_value(20000), "KFS metaserver port")
        ("basedir,B", po::value<string>(&basedir)->default_value("/jobs"), "Base dir for storing I-files in KFS")
        ("loglevel,L", po::value<string>(&logLevel)->default_value("INFO"), "Log level (INFO or DEBUG)")
        ("numparts,n",po::value<int>(&numPartitions)->default_value(1), "# of partitions (i.e., # of I-files to create")
        ("mapperId,i",po::value<int>(&mapperId)->default_value(1), "mapper id")
        ("attemptNum,a",po::value<int>(&attemptNum)->default_value(1), "mapper attempt #")
        ("wbhost,w", po::value<string>(&workbuilderLoc.hostname)->default_value("localhost"), "Sailfish workbuilder hostname")
        ("wbport,x", po::value<int>(&workbuilderLoc.port)->default_value(12345), "Sailfish workbuilder port")
        ("jobid,J", po::value<string>(&jobId)->default_value(""), "Hadoop Job id")
        ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }
    if ((bufferLimit > 0) && (vm.count("compress"))) {
        // if there is no buffering in the appender, then we can't
        // compress.  CPU overheads become too high, particularly, if
        // the records are small.
        enableCompression = true;
    }

    // assume you got the args right

    std::cerr << "Input args: id = " << mapperId << " attempt: " << attemptNum
        << " compression: " << (enableCompression ? "enabled" : "disabled")
        << " with pid: " << getpid() << std::endl;

    // Initialize the msg-logger
    MsgLogger::Init(NULL, str2LogLevel(logLevel));

    if (attemptNum != 0) {
        KfsClientPtr kfsClient = getKfsClientFactory()->GetClient(metaLoc.hostname, metaLoc.port);
        if (!kfsClient) {
            std::cerr << "Unable to initialize kfsClient.  Exiting!" << std::endl;
            exit(-1);
        }
        InvalidateDeadMappers(kfsClient, basedir, mapperId, attemptNum);
    }

#ifdef USE_INTEL_IPP
    IppStatus retCode = ippStaticInit();
    if (retCode != ippStsNoErr) {
        KFS_LOG_STREAM_ERROR << "ipp failed to init" << KFS_LOG_EOM;
        enableCompression = false;
    }
#else
    if (lzo_init() != LZO_E_OK) {
        KFS_LOG_STREAM_ERROR << "lzo failed to init" << KFS_LOG_EOM;
        enableCompression = false;
    }
#endif

    struct rlimit rlp;
    int errCode;
    getrlimit(RLIMIT_DATA, &rlp);
    // 1.25G
    rlp.rlim_max = (1 << 30) + (256 * 1024 * 1024);
    rlp.rlim_cur = rlp.rlim_max;
    errCode = setrlimit(RLIMIT_DATA, &rlp);
    if (errCode) {
        std::cerr << "Error in setting limits: " << errno << std::endl;
    }

    srand(getpid());

    InputProcessor inputProcessor(metaLoc.hostname, metaLoc.port, basedir,
        numPartitions, mapperId, attemptNum);
    // read from stdin and write to stdout
    inputProcessor.Start(0, 1, bufferLimit, enableCompression);
    inputProcessor.Close();
    std::cerr << "Input processor returned...we are nearing all done" << std::endl;
    inputProcessor.NotifyChunksToWorkbuilder(jobId, workbuilderLoc);
    MsgLogger::Stop();
    std::cerr << "Notified workbuilder about the chunks we appened...we are all done"
        << std::endl;
}

//
// For each of the previous attempts, create a file with the
// <mapperId, deadAttemptNum>.  The records from these mappers can
// then be filtered out in kGroupBy.
//
static void
InvalidateDeadMappers(KfsClientPtr &kfsClient, const string &basedir,
    int mapperId, int attemptNum)
{
    string deadMapsDir = basedir + "/deadmappers";
    kfsClient->Mkdirs(deadMapsDir.c_str());
    for (int prevAttempt = 0; prevAttempt < attemptNum; prevAttempt++) {
        uint64_t prevId = ((uint64_t) mapperId) << 32 | prevAttempt;
        ostringstream fn;

        fn << deadMapsDir << "/" << prevId;
        KFS_LOG_STREAM_INFO << "Invalidating dead mapper: " << mapperId << '_' <<
            prevId << KFS_LOG_EOM;
        // create with "exclusive" flag; if the file already exists, leave alone
        kfsClient->Create(fn.str().c_str(), 1, true);
    }
}
