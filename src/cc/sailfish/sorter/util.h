//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: util.h $
//
// Created 2011/02/08
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

#ifndef KAPPENDER_UTIL_H
#define KAPPENDER_UTIL_H

#include "common/log.h"
#include <string>
#include <stdlib.h>

namespace KFS
{

    MsgLogger::LogLevel str2LogLevel(std::string logLevel);
    // Effectively a wrapper around posix_memalign()---this function
    // ensures that the args meet the requirements of posix_memalign()
    int MemAlignedMalloc(void **ptr, size_t alignment, size_t size);

    template <typename T>
    int MemAlignedMalloc(T **ptr, size_t alignment, size_t size) {
        return MemAlignedMalloc((void **) ptr, alignment, size);
    }
}

#endif // KAPPENDER_UTIL_H
