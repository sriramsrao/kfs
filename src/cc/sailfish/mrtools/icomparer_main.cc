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
// Compare the output from two mergers and find what records they differ.
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

void diffFiles(const string &file1, const string &file2, off_t startOffset1, off_t startOffset2);
int readKey(int fd, IFileKey_t &key);

int main(int argc, char **argv)
{
    string file1, file2;
    off_t startOffset1, startOffset2;

    // Declare the supported options.
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("file1,L", po::value<string>(&file1)->default_value(""), "Input file1")
        ("file2,R", po::value<string>(&file2)->default_value(""), "Input file2")
        ("skip1,a", po::value<off_t>(&startOffset1)->default_value(0), "Start offset1")
        ("skip2,b", po::value<off_t>(&startOffset2)->default_value(0), "Start offset2")
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
    MsgLogger::Init(NULL, MsgLogger::kLogLevelINFO);

    std::cerr << "Our pid is: " << getpid() << std::endl;

    diffFiles(file1, file2, startOffset1, startOffset2);

    MsgLogger::Stop();
}

void
diffFiles(const string &file1, const string &file2,
    off_t startOffset1, off_t startOffset2)
{
    int fd1 = open(file1.c_str(), O_RDONLY);
    int fd2 = open(file2.c_str(), O_RDONLY);

    if ((fd1 < 0) || (fd2 < 0)) {
        std::cerr << "Unable to open one of: " << file1 << " or " << file2 << std::endl;
        return;
    }

    lseek(fd1, startOffset1, SEEK_CUR);
    lseek(fd2, startOffset2, SEEK_CUR);

    string file1Out = file1 + ".out_keys";
    string file2Out = file2 + ".out_keys";
    int fd3 = open(file1Out.c_str(), O_WRONLY|O_CREAT);
    int fd4 = open(file2Out.c_str(), O_WRONLY|O_CREAT);

    IFileKey_t k1, k2;
    IFileKey_t prevk1, prevk2;
    int retval;
    off_t fdPos1, fdPos2;
    while (1) {
        fdPos1 = lseek(fd1, 0, SEEK_CUR);
        fdPos2 = lseek(fd2, 0, SEEK_CUR);
        retval = readKey(fd1, k1);
        if (retval < 0) {
            KFS_LOG_STREAM_INFO << "Stopping because we ran out of keys in: " << file1
                << KFS_LOG_EOM;
            break;
        }
        retval = readKey(fd2, k2);
        if (retval < 0) {
            KFS_LOG_STREAM_INFO << "Stopping because we ran out of keys in: " << file2
                << KFS_LOG_EOM;
            break;
        }
        if (k1 == k2) {
            prevk1 = k1;
            prevk2 = k2;
            continue;
        }
        KFS_LOG_STREAM_WARN << "Differing keys at pos: f1 = " << fdPos1 << " and f2 = " << fdPos2
            << KFS_LOG_EOM;
        write(fd3, &(k1.keyLen), sizeof(int));
        write(fd3, k1.key.get(), k1.keyLen);

        write(fd4, &(k2.keyLen), sizeof(int));
        write(fd4, k2.key.get(), k2.keyLen);
    }
    close(fd1);
    close(fd2);
    close(fd3);
    close(fd4);
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
