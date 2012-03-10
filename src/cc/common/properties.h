//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: properties.h 1552 2011-01-06 22:21:54Z sriramr $
//
// \brief Properties file similar to java.util.Properties
//
// Created 2004/05/05
//
// Copyright 2008 Quantcast Corp.
// Copyright 2006-2008 Kosmix Corp.
//
// This file is part of Kosmos File System (KFS).
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
//----------------------------------------------------------------------------

#ifndef COMMON_PROPERTIES_H
#define COMMON_PROPERTIES_H
  
#include <stdint.h>
#include <istream>
#include <string>
#include <map>
#include <boost/pool/pool_alloc.hpp> 

namespace KFS
{

class Properties {

  private :
    //Map that holds the (key,value) pairs
    typedef std::map<std::string, std::string,
        std::less<std::string>,
        boost::fast_pool_allocator<std::pair<const std::string, std::string> >
    > PropMap;
    PropMap propmap; 

  public:
    typedef PropMap::const_iterator iterator;
    iterator begin() const { return propmap.begin(); }
    iterator end() const { return propmap.end(); }
    // load the properties from a file
    int loadProperties(const char* fileName, char delimiter, bool verbose, bool multiline = false);
    // load the properties from an in-core buffer
    int loadProperties(std::istream &ist, char delimiter, bool verbose, bool multiline = false);
    std::string getValue(std::string key, std::string def) const;
    const char* getValue(std::string key, const char* def) const;
    int getValue(std::string key, int def) const;
    unsigned int getValue(std::string key, unsigned int def) const;
    long getValue(std::string key, long def) const;
    unsigned long getValue(std::string key, unsigned long def) const;
    long long getValue(std::string key, long long def) const;
    unsigned long long getValue(std::string key, unsigned long long def) const;
    double getValue(std::string key, double def) const;
    void setValue(const std::string key, const std::string value);
    void getList(std::string &outBuf, std::string linePrefix, std::string lineSuffix = std::string("\n")) const;
    void clear() { propmap.clear(); }
    bool empty() const { return propmap.empty(); }
    size_t size() const { return propmap.size(); }
    void copyWithPrefix(std::string prefix, Properties& props) const;
    void swap(Properties& props)
        { propmap.swap(props.propmap); }
    Properties();
    Properties(const Properties &p);
    ~Properties();

};

}

#endif // COMMON_PROPERTIES_H
