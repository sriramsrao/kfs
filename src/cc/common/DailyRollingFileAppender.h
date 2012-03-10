//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: DailyRollingFileAppender.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2009/02/05
//
// Copyright 2009 Quantcast Corporation. 
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
// \brief Enhance the rolling file appender to rotate logs on a daily basis.
//----------------------------------------------------------------------------

#ifndef COMMON_DAILYROLLINGFILEAPPENDER_H
#define COMMON_DAILYROLLINGFILEAPPENDER_H

#include "log.h"
#include <string>
#include <log4cpp/LayoutAppender.hh>

namespace log4cpp
{
    /**
     * Enhance the file appender and roll the log file on each day.
     */
    class LOG4CPP_EXPORT DailyRollingFileAppender : public LayoutAppender {
    public:
        DailyRollingFileAppender(const std::string &name, const std::string &fileName,
                                 unsigned int maxDaysToKeep = 30,
                                 bool append = true,
                                 mode_t mode = 00644,
                                 LayoutAppender* appender = 0);
        virtual ~DailyRollingFileAppender();
        void setMaxDaysToKeep(unsigned int maxDaysToKeep);
        unsigned int getMaxDaysToKeep() const;
        void rollOver();
        virtual void close();
    protected:
        virtual void _append(const log4cpp::LoggingEvent &event);
        unsigned int _maxDaysToKeep;
        // record the time at which the file that we are currently
        // logging to was created.  In case of a restart, this
        // variable records the last modified time of the file we are
        // currently logging to
        struct tm _logsTime;
        std::string _fileName;
        LayoutAppender& _appender;
    };
}

#endif // COMMON_DAILYROLLINGFILEAPPENDER_H
