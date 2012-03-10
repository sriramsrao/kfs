//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: DailyRollingFileAppender.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2009/02/05
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

#include "DailyRollingFileAppender.h"
#include <log4cpp/FileAppender.hh>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <stdlib.h>
#include <iomanip>

using std::string;

namespace log4cpp {

DailyRollingFileAppender::DailyRollingFileAppender(const string &name, const string &fileName,
                                                   unsigned int maxDaysToKeep,
                                                   bool append, mode_t mode,
                                                   LayoutAppender* appender) :
    LayoutAppender(name),
    _maxDaysToKeep(maxDaysToKeep > 0 ? maxDaysToKeep : 7),
    _fileName(fileName),
    _appender(appender ? *appender : *(new log4cpp::FileAppender(name, fileName, append, mode)))
{
    struct stat statBuf;
    int res;
    time_t t;

    // if the file exists, determine when it was last modified so we know what to rename the file to
    res = ::stat(fileName.c_str(), &statBuf);
    if (res < 0) {
        t = time(NULL);
    } else {
        t = statBuf.st_mtime;
    }
    localtime_r(&t, &_logsTime);
}

DailyRollingFileAppender::~DailyRollingFileAppender()
{
    delete &_appender;
}

void DailyRollingFileAppender::close()
{
    _appender.close();
}

void DailyRollingFileAppender::setMaxDaysToKeep(unsigned int maxDaysToKeep)
{
    _maxDaysToKeep = maxDaysToKeep;
}

unsigned int DailyRollingFileAppender::getMaxDaysToKeep() const
{
    return _maxDaysToKeep;
}

void DailyRollingFileAppender::rollOver()
{
    std::ostringstream filename_stream;
    string lastFn, dirname;
    time_t oldest = time(NULL) - _maxDaysToKeep * 60 * 60 * 24;
    struct dirent **entries;
    int nentries, res;
    string::size_type slash = _fileName.rfind('/');

    filename_stream << _fileName << "." << _logsTime.tm_year + 1900 << "-" 
                    << std::setfill('0') << std::setw(2) << _logsTime.tm_mon + 1 << "-" 
                    << std::setw(2) << _logsTime.tm_mday << std::ends;
    lastFn = filename_stream.str();
    ::rename(_fileName.c_str(), lastFn.c_str());
    _appender.reopen();

    // prune away old files
    if (slash == string::npos)
        dirname = ".";
    else
        dirname.assign(_fileName, 0, slash);

    nentries = scandir(dirname.c_str(), &entries, 0, alphasort);
    if (nentries < 0)
        return;
    for (int i = 0; i < nentries; i++) {
        struct stat statBuf;

        res = ::stat(entries[i]->d_name, &statBuf);
        if ((res < 0) || (!S_ISREG(statBuf.st_mode))) {
            free(entries[i]);
            continue;
        }
        if (statBuf.st_mtime < oldest) {
            string fname = dirname + "/" + entries[i]->d_name;
            // file is too old; prune away
            ::unlink(fname.c_str());
        }
        free(entries[i]);
    }
    free(entries);
}

void DailyRollingFileAppender::_append(const log4cpp::LoggingEvent &event)
{
    struct tm now;
    time_t t = event.timeStamp.getSeconds();

    if (localtime_r(&t, &now) != NULL) {
        if ((now.tm_year != _logsTime.tm_year) ||
            (now.tm_mon != _logsTime.tm_mon) ||
            (now.tm_mday != _logsTime.tm_mday)) {
            rollOver();
            _logsTime = now;
        }
    }
    _appender.doAppend(event);

}

}

