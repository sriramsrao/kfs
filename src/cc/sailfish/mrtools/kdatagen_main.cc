//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdatagen_main.cc 3365 2011-11-23 22:40:49Z sriramr $
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
// \brief Code to test kappender: write out records
//----------------------------------------------------------------------------

#include "kappender.h"
#include <boost/program_options.hpp>
#include <string>
namespace po = boost::program_options;

using namespace KFS;

int main(int argc, char **argv)
{
    ProtoHdr_t protoHdr;
    int fd = 1; // write to stdout
    const int kKeyLen = 8;
    char keyBuffer[kKeyLen];
    const int kBufLen = 128;
    char buffer[kBufLen];
    int numPartitions;
    int numRecords;
    bool sendKeys = false;

    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("key,k", "send a key with each record")
        ("numparts,n",po::value<int>(&numPartitions)->default_value(1), "# of partitions (i.e., # of I-files to create")
        ("numrecs,r",po::value<int>(&numRecords)->default_value(1), "# of records (i.e., # of records to send")
        ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    if (vm.count("key"))
        sendKeys = true;

    int rv;

    std::cerr << "Size of proto-hdr: " << sizeof(ProtoHdr_t) << std::endl;

    for (int32_t i = 0; i < numRecords; i++) {
        for (int32_t j = 0; j < kKeyLen; j++) {
            keyBuffer[j] = 'a' + (i % 26);
        }
        int partition = rand() % numPartitions;
        protoHdr.partition = htonl(partition);
        protoHdr.keyLength = sendKeys ? htonl(kKeyLen) : 0;
        protoHdr.dataLength = htonl(kBufLen + sizeof(int) + kKeyLen + sizeof(int));
        rv = write(fd, &protoHdr, sizeof(ProtoHdr_t));
        assert(rv == sizeof(ProtoHdr_t));
        if (protoHdr.keyLength > 0) {
            rv = write(fd, keyBuffer, kKeyLen);
            assert(rv == kKeyLen);
        }
        int v = htonl(kKeyLen);
        rv = write(fd, &v, sizeof(int));
        rv = write(fd, keyBuffer, kKeyLen);
        v = htonl(kBufLen);
        rv = write(fd, &v, sizeof(int));
        rv = write(fd, buffer, kBufLen);
        assert(rv == kBufLen);
    }
    // all done...send a 4-byte record to signal end
    protoHdr.partition = htonl(-1);
    protoHdr.keyLength = 0;
    protoHdr.dataLength = htonl(4);
    rv = write(fd, &protoHdr, sizeof(ProtoHdr_t));
    int dummy = 0xDEADDEAD;
    rv = write(fd, &dummy, sizeof(int));
    int timeLeft = 120;
    while (timeLeft > 0) {
        sleep(10);
        timeLeft -= 10;
    }
}

