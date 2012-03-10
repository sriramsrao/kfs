//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: statuschecker.h 3365 2011-11-23 22:40:49Z sriramr $
//
// Created 2011/01/04
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
// \brief Check the status of a job by doing a wget on the JT.
//----------------------------------------------------------------------------

#ifndef WORKBUILDER_STATUSCHECKER_H
#define WORKBUILDER_STATUSCHECKER_H
#include "util.h"
#include "common/kfsdecls.h"
#include "libkfsClient/KfsOps.h"
#include <string>

namespace workbuilder
{
    enum JobStatus {
        kFailed    = -1,
        kSucceeded = 0,
        kRunning   = 1,
        kNotFound  = 2,
        kRetry     = 3
    };

    struct JTNotifierOp : public KFS::KfsOp {
        std::string cmd;
        std::string host;

        JTNotifierOp(const std::string &c, const std::string &h) :
            KfsOp(KFS::CMD_UNKNOWN, 1), cmd(c), host(h) {
            // JT doesn't send back cseq
            enableCseqMatching = false;
        }
        // Send a HTTP GET request to the jt
        void Request(std::ostream &os) {
            os << "GET " << cmd << " HTTP/1.1\r\n";
            os << "Host: " << host << "\r\n";
            os << "Cseq: " << seq << "\r\n\r\n";
        }
        std::string Show() const {
            std::ostringstream os;

            os << " GET " << cmd;
            return os.str();
        }
        void ParseResponseHeaderSelf(const KFS::Properties &prop) {
            status = 0;
            contentLength = prop.getValue("Content-Length", 0);
        }
    };

    class StatusChecker {
    public:
        StatusChecker(const std::string &jobTracker, const std::string &jobId);
        void setState(const char *s) {
            mJobState = s;
        }
        std::string getState() {
            return mJobState;
        }
        JobStatus getStatus(bool force = false);
    private:
        KFS::ServerLocation   mJobTrackerLocation;
        std::string      mJobId;
        std::string      mJobState;
        std::string      mUrl;
        time_t      mLastStatusCheck;

        void doStatusCheck();
    };
}

#endif // WORKBUILDER_STATUSCHECKER_H
