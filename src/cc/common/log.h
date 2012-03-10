//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: log.h 2129 2011-03-08 21:02:59Z sriramr $
//
// Created 2007/10/17
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
// \brief A logging facility.
//
//----------------------------------------------------------------------------

#ifndef COMMON_LOG_H
#define COMMON_LOG_H

#include "BufferedLogWriter.h"
#include <string.h>

namespace KFS 
{
    // Have a singleton logger for an application
    class MsgLogger : public BufferedLogWriter
    {
    private:
        MsgLogger(const char *filename, LogLevel logLevel,
            const Properties* props, const char* propPrefix);
        ~MsgLogger();
        MsgLogger(const MsgLogger &other);
        MsgLogger& operator=(const MsgLogger &other);
        static MsgLogger *logger;
    public:
        static void Stop();
        static MsgLogger* GetLogger() { return logger; }
        static void Init(const char *filename);
        static void Init(const char *filename, LogLevel logLevel);
        static void Init(const Properties& props, const char* propPrefix = 0);
        static void Init(const char *filename, LogLevel logLevel,
            const Properties* props, const char* propPrefix);
        static void SetLevel(LogLevel logLevel) {
            if (logger) {
                logger->SetLogLevel(logLevel);
            }
        }
        static bool IsLoggerInited() { return (logger != 0); }
        static const char* SourceFileName(const char* name) {
            if (! name) {
                return "";
            }
            const char* const ret = strrchr(name, '/');
            if (! ret || ! ret[1]) {
                return name;
            }
            return ret + 1; 
        }
    };

// The following if prevents arguments evaluation (and possible side effect).

#ifndef KFS_LOG_VA_PRIORITY
#   define KFS_LOG_VA_PRIORITY(logLevel, msg, ...) \
        if (MsgLogger::GetLogger() && \
                MsgLogger::GetLogger()->IsLogLevelEnabled(logLevel)) \
            MsgLogger::GetLogger()->Append(logLevel, "(%s:%d) " \
                msg, MsgLogger::SourceFileName(__FILE__), __LINE__, __VA_ARGS__)
#endif

#ifndef KFS_LOG_PRIORITY
#   define KFS_LOG_PRIORITY(logLevel, msg) \
        KFS_LOG_VA_PRIORITY(logLevel, "%s", msg)
#endif

#ifndef KFS_LOG_DEBUG
#   define KFS_LOG_DEBUG(msg) \
        KFS_LOG_PRIORITY(MsgLogger::kLogLevelDEBUG, msg)
#endif
#ifndef KFS_LOG_VA_DEBUG
#   define KFS_LOG_VA_DEBUG(msg, ...) \
        KFS_LOG_VA_PRIORITY(MsgLogger::kLogLevelDEBUG, msg, __VA_ARGS__)
#endif

#ifndef KFS_LOG_INFO
#   define KFS_LOG_INFO(msg) \
        KFS_LOG_PRIORITY(MsgLogger::kLogLevelINFO, msg)
#endif
#ifndef KFS_LOG_VA_INFO
#   define KFS_LOG_VA_INFO(msg, ...) \
        KFS_LOG_VA_PRIORITY(MsgLogger::kLogLevelINFO, msg, __VA_ARGS__)
#endif

#ifndef KFS_LOG_WARN
#   define KFS_LOG_WARN(msg) \
        KFS_LOG_PRIORITY(MsgLogger::kLogLevelWARN, msg)
#endif
#ifndef KFS_LOG_VA_WARN
#   define KFS_LOG_VA_WARN(msg, ...) \
        KFS_LOG_VA_PRIORITY(MsgLogger::kLogLevelWARN, msg, __VA_ARGS__)
#endif

#ifndef KFS_LOG_ERROR
#   define KFS_LOG_ERROR(msg) \
        KFS_LOG_PRIORITY(MsgLogger::kLogLevelERROR, msg)
#endif
#ifndef KFS_LOG_VA_ERROR
#   define KFS_LOG_VA_ERROR(msg, ...) \
        KFS_LOG_VA_PRIORITY(MsgLogger::kLogLevelERROR, msg, __VA_ARGS__)
#endif

#ifndef KFS_LOG_FATAL
#   define KFS_LOG_FATAL(msg) \
        KFS_LOG_PRIORITY(MsgLogger::kLogLevelFATAL, msg)
#endif
#ifndef KFS_LOG_VA_FATAL
#   define KFS_LOG_VA_FATAL(msg, ...) \
        KFS_LOG_VA_PRIORITY(MsgLogger::kLogLevelFATAL, msg, __VA_ARGS__)
#endif

// The following supports all
// std stream manipulators, has lower # of allocations, and free of possible
// problems with stream object scope / lifetime.
// The price for this is that insertion has to be always terminated with
// KFS_LOG_EOM, otherwise you'll get possibly unintelligible compile time error.
#ifndef KFS_LOG_STREAM
#   define KFS_LOG_STREAM(logLevel) \
    if (MsgLogger::GetLogger() && \
            MsgLogger::GetLogger()->IsLogLevelEnabled(logLevel)) {\
        MsgLogger::StStream _msgStream_015351104260035312( \
            *MsgLogger::GetLogger(), logLevel); \
        _msgStream_015351104260035312.GetStream() << "(" << \
            MsgLogger::SourceFileName(__FILE__) << ":" << __LINE__ << ") "
#   define KFS_LOG_EOM \
        std::flush; \
    } (void)0
#endif

#ifndef KFS_LOG_STREAM_DEBUG
#   define KFS_LOG_STREAM_DEBUG KFS_LOG_STREAM(MsgLogger::kLogLevelDEBUG)
#endif
#ifndef KFS_LOG_STREAM_INFO
#   define KFS_LOG_STREAM_INFO  KFS_LOG_STREAM(MsgLogger::kLogLevelINFO)
#endif
#ifndef KFS_LOG_STREAM_WARN
#   define KFS_LOG_STREAM_WARN  KFS_LOG_STREAM(MsgLogger::kLogLevelWARN)
#endif
#ifndef KFS_LOG_STREAM_ERROR
#   define KFS_LOG_STREAM_ERROR KFS_LOG_STREAM(MsgLogger::kLogLevelERROR)
#endif
#ifndef KFS_LOG_STREAM_FATAL
#   define KFS_LOG_STREAM_FATAL KFS_LOG_STREAM(MsgLogger::kLogLevelFATAL)
#endif

}

#endif // COMMON_LOG_H
