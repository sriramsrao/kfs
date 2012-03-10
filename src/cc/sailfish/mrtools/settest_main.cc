//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kgroupby_main.cc 1565 2011-01-08 01:23:20Z sriramr $
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
// \brief kgroupby: read the records from an I-file and write to stdout.
//----------------------------------------------------------------------------

#include "common/log.h"
#include "kgroupby.h"
#include "kappender.h"

#include <iostream>
#include <sstream>
#include <boost/program_options.hpp>
#include <string>
#include <set>
#include <vector>

using namespace KFS;

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
    /*
    std::set<IFileKey_t,IFileKeyCompare>keyset;
    std::string key1 = "key1";
    IFileKey_t k1((const unsigned char*)key1.c_str(),4);
    keyset.insert(k1);
    std::cout << "keyset size: " << keyset.size() << std::endl;
    std::string key2 = "key2";
    IFileKey_t k2((const unsigned char*)key2.c_str(),4);
    keyset.insert(k2);
    std::cout << "keyset size: " << keyset.size() << std::endl;
    std::string key3 = "key3";
    IFileKey_t k3;
    k3.key=(const unsigned char*)key3.c_str();
    k3.keyLen=4;
    keyset.insert(k3);
    std::cout << "keyset size: " << keyset.size() << std::endl;

    IFileKey_t reuseKey;
    std::string key4 = "key4";
    reuseKey.key = (const unsigned char*)key4.c_str();
    reuseKey.keyLen = 4;
    keyset.insert(reuseKey);
    std::cout << "keyset size: " << keyset.size() << std::endl;
    std::string key5 = "key5";
    reuseKey.key = (const unsigned char*)key5.c_str();
    reuseKey.keyLen = 4;
    keyset.insert(reuseKey);
    std::cout << "keyset size: " << keyset.size() << std::endl;
    vector<string> keys;
    keys.push_back("key6");
    keys.push_back("key7");
    keys.push_back("key8");
    std::cout << "done populating vector" << std::endl;
    for (int i=6;i<9;i++) {
        //IFileKey_t loopKey;
        char* key = new char[20];
        sprintf(key,"key%i",i);
        //loopKey.key = (const unsigned char*)key;
        //loopKey.keyLen=4;
        IFileKey_t loopKey((const unsigned char*)key, 4);
        keyset.insert(loopKey);
        std::cout << "keyset size: " << keyset.size() << std::endl;
    }
    std::set<IFileKey_t,IFileKeyCompare>::iterator keyIter;
    for (keyIter = keyset.begin(); keyIter != keyset.end(); keyIter++) {
        IFileKey_t currKey = *keyIter;
        std::cout << "currKey: " << currKey.key << std::endl;
    }
    */
}

