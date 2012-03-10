//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: workbuilder_main.cc 3637 2012-01-25 19:28:06Z sriramr $
//
// Created 2011/01/04
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
//
//----------------------------------------------------------------------------

#include <iostream>
#include <sstream>
#include <boost/program_options.hpp>
#include <string>
namespace po = boost::program_options;

#include "util.h"
#include "workbuilder.h"
#include "common/kfsdecls.h"
#include "common/log.h"

extern "C" {
#include <sys/resource.h>
}
using namespace KFS;
using namespace workbuilder;
using std::string;

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
    ServerLocation metaLoc;
    int clientAcceptPort;
    int ifileId;
    string basedir;
    uint64_t taskWorkLimitBytes;
    string jobid;
    bool retry;
    bool isIndexReadDisabled;
    string logLevel;
    string logDir;
    string indexDir;

    // Declare the supported options.
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("metahost,M", po::value<string>(&metaLoc.hostname)->default_value("localhost"), "KFS metaserver hostname")
        ("metaport,P", po::value<int>(&metaLoc.port)->default_value(20000), "KFS metaserver port")
        ("clientport,C", po::value<int>(&clientAcceptPort)->default_value(30000), "Client connection port")
        ("basedir,B", po::value<string>(&basedir)->default_value("/jobs"), "Base dir for storing I-files in KFS")
        ("taskworklimit,T", po::value<uint64_t>(&taskWorkLimitBytes)->default_value(1073741824), "Reduce task input data limit (bytes)")
        ("jobid,J", po::value<string>(&jobid)->default_value(""), "Job id as <job tracker>-<jobid>")
        ("retry,r", po::value<bool>(&retry)->default_value(false), "Always send retry message when requested for work (debug mode)")
        ("disableindexread,D", po::value<bool>(&isIndexReadDisabled)->default_value(false), "Disable read of per-chunk index (debug mode)")
        ("loglevel,L", po::value<string>(&logLevel)->default_value("INFO"), "Log level (INFO or DEBUG)")
        ("indexdir,I", po::value<string>(&indexDir)->default_value(""), "Dir on local disk containing per-chunk indexes (test mode)")
        ("logdir,l", po::value<string>(&logDir)->default_value(""), "Base dir for storing workbuilder log files")
        ("ifileId,p", po::value<int>(&ifileId)->default_value(0), "(DEBUG mode) Ifile # for merge")
        ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }
    if (logDir == "")
        MsgLogger::Init(NULL, str2LogLevel(logLevel));
    else {
        string logFn = logDir + "/wblog.txt";
        MsgLogger::Init(logFn.c_str(), str2LogLevel(logLevel));
    }

    struct rlimit rlim;
    int status = getrlimit(RLIMIT_NOFILE, &rlim);
    if (status == 0) {
        // bump up the # of open fds to as much as possible
        rlim.rlim_cur = rlim.rlim_max;
        setrlimit(RLIMIT_NOFILE, &rlim);
    }

    std::cout << "jobid " << jobid << std::endl;
    std::cout << "metaLocHost " << metaLoc.hostname << std::endl;
    std::cout << "metaLocPort " << metaLoc.port << std::endl;
    std::cout << "index reading: "
        << (isIndexReadDisabled ? "disabled" : "enabled")
        << std::endl;

    initIndexReader(metaLoc.hostname.c_str(), metaLoc.port, isIndexReadDisabled);
    initWorkbuilderOpMap();

    //if jobid parameter set, fake the mapper start signal
    if (jobid != "") {
        gWorkbuilder.dummyMapperStart(basedir, logDir, jobid, ifileId);
    }

#ifdef USE_INTEL_IPP
    IppStatus retCode = ippStaticInit();
    if (retCode != ippStsNoErr) {
        KFS_LOG_STREAM_ERROR << "ipp failed to init" << KFS_LOG_EOM;
    }
#else
    if (lzo_init() != LZO_E_OK) {
        KFS_LOG_STREAM_ERROR << "lzo failed to init" << KFS_LOG_EOM;
    }
#endif

    gWorkbuilder.setAlwaysReplyWithRetry(retry);

    gWorkbuilder.setJobParams(taskWorkLimitBytes);

    gWorkbuilder.mainLoop(indexDir, basedir, logDir, clientAcceptPort);
}
