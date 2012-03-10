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
#include "libkfsIO/IOBuffer.h"
#include "libkfsClient/KfsOps.h"
#include <string>
#include <stdlib.h>

namespace KFS
{
    struct HttpGetOp : public KfsOp {
        int httpStatusCode;
        std::string cmd;

        HttpGetOp(const std::string &c) :
            KfsOp(CMD_UNKNOWN, 1), httpStatusCode(-1), cmd(c) { }
        // Send a HTTP GET request to the workbuilder
        void Request(std::ostream &os) {
            os << "GET " << cmd << " HTTP/1.1\r\n";
            os << "Cseq: " << seq << "\r\n";
            if (contentLength == 0) {
                os << "\r\n";
                return;
            }
            os << "Content-Length: " << contentLength << "\r\n\r\n";
            // Mixing << and os.write isn't working.
            // os.write(contentBuf, contentLength);
            std::string s(contentBuf, contentLength);
            os << s;
        }
        void ParseResponseHeaderSelf(const Properties &prop) {
            httpStatusCode = prop.getValue("Http-Status-Code", 200);
        }
        std::string Show() const {
            std::ostringstream os;

            os << " GET " << cmd;
            return os.str();
        }
    };

    MsgLogger::LogLevel str2LogLevel(std::string logLevel);
    bool IsMessageAvail(IOBuffer *iobuf, int &msgLen);
    // Effectively a wrapper around posix_memalign()---this function
    // ensures that the args meet the requirements of posix_memalign()
    int MemAlignedMalloc(void **ptr, size_t alignment, size_t size);
    template<typename T>
    int MemAlignedMalloc(T **ptr, size_t alignment, size_t size) {
        return MemAlignedMalloc((void **) ptr, alignment, size);
    }
}

#endif // KAPPENDER_UTIL_H
