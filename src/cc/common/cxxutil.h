//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: cxxutil.h 1552 2011-01-06 22:21:54Z sriramr $
//
// \brief Hash declarations to get the code to compile on Mac
//
// Created 2007/10/01
//
// Copyright 2008 Quantcast Corp.
// Copyright 2007-2008 Kosmix Corp.
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

#ifndef COMMON_CXXUTIL_H
#define COMMON_CXXUTIL_H

#if defined (__APPLE__) || defined (__i386__)
#include <tr1/functional>
#if ((__GNUC__ == 4) && (__GNUC_MINOR__ < 2))
namespace std
{
namespace tr1
  {
    template <> struct hash<long long> {
      std::size_t operator()(long long val) const { return static_cast<std::size_t>(val); }
    };
  }
}
#endif
#endif

#endif
