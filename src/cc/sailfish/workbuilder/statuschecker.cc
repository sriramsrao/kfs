//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: statuschecker.cc 3365 2011-11-23 22:40:49Z sriramr $
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
//
//----------------------------------------------------------------------------

#include "statuschecker.h"
#include "util.h"
#include "common/log.h"
#include "libkfsClient/KfsClientInt.h"
#include <boost/lexical_cast.hpp>

using namespace workbuilder;
using namespace KFS;

using std::vector;
using std::string;

StatusChecker::StatusChecker(const string &jobTracker, const string &jobId) :
    mJobId(jobId), mLastStatusCheck(0)
{
    vector<string> parts;

    split(jobTracker, parts, ':');
    mJobTrackerLocation.hostname = parts[0];
    mJobTrackerLocation.port = boost::lexical_cast<int>(parts[1]);

    mUrl = "/jobinfo?jobid=" + mJobId;
}

JobStatus StatusChecker::getStatus(bool force)
{
    time_t now = time(0);
    if ((now - mLastStatusCheck > 300) || force) {
        doStatusCheck();
        mLastStatusCheck = time(0);
    }
    if (mJobState == "SUCCEEDED")
        return kSucceeded;
    if (mJobState == "NOTFOUND")
        return kNotFound;
    if ((mJobState == "FAILED") ||
        (mJobState == "KILLED"))
        return kFailed;
    return kRunning;
}

void
StatusChecker::doStatusCheck()
{
    TcpSocket sock;

    if (sock.Connect(mJobTrackerLocation) < 0) {
        KFS_LOG_STREAM_INFO << "Unable to status check: "
            << mJobTrackerLocation.ToString() << KFS_LOG_EOM;
        return;
    }

    JTNotifierOp notifierOp(mUrl, mJobTrackerLocation.hostname);

    int res = DoOpCommon(&notifierOp, &sock);

    if (res < 0) {
        KFS_LOG_STREAM_INFO << "Unable to status check: "
            << mJobTrackerLocation.ToString()
            << " error: " << res << KFS_LOG_EOM;
        return;
    }

    if (notifierOp.contentBuf != NULL) {
        vector<string> parts;

        split(notifierOp.contentBuf, parts, ':');
        mJobState = parts[1].c_str();
    } else {
        mJobState = "NOTFOUND";
    }
}
