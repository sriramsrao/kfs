//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: setnumreducer.cc 3809 2012-02-28 22:22:12Z sriramr $
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

#include "util.h"
#include "setnumreducer.h"
#include "libkfsClient/KfsClientInt.h"
#include <boost/lexical_cast.hpp>

using namespace workbuilder;
using namespace KFS;
using std::string;
using std::vector;

JTNotifier::JTNotifier(const string &jobTracker, const string &jobId) :
    mJobId(jobId)
{
    vector<string> parts;

    split(jobTracker, parts, ':');
    mJobTrackerLocation.hostname = parts[0];
    mJobTrackerLocation.port = boost::lexical_cast<int>(parts[1]);
}

JobStatus
JTNotifier::setNumReducers(int nReducers)
{
    TcpSocket sock;

    if (sock.Connect(mJobTrackerLocation) < 0) {
        KFS_LOG_STREAM_INFO << "Unable to connect to: "
            << mJobTrackerLocation.ToString() << KFS_LOG_EOM;
        return kRetry;
    }

    string cmd = "/setnumreducers?jobid=" + mJobId
        + "&nreducers=" + boost::lexical_cast<string>(nReducers);

    JTNotifierOp notifierOp(cmd, mJobTrackerLocation.hostname);

    int res = DoOpCommon(&notifierOp, &sock);

    if (res < 0) {
        KFS_LOG_STREAM_INFO << "Unable to notify JT: "
            << mJobTrackerLocation.ToString()
            << " error: " << res << KFS_LOG_EOM;
        return kRetry;
    }

    if (notifierOp.contentBuf != NULL) {
        vector<string> parts;

        split(notifierOp.contentBuf, parts, ':');
        mJobState = parts[1].c_str();
    } else {
        mJobState = "NOTFOUND";
    }

    if (mJobState == "SUCCEEDED")
        return kSucceeded;
    if (mJobState == "NOTFOUND")
        return kNotFound;
    if ((mJobState == "FAILED") ||
        (mJobState == "KILLED"))
        return kFailed;
    return kSucceeded;
}

JobStatus
JTNotifier::reRunTask(int mapTaskId)
{
    TcpSocket sock;

    if (sock.Connect(mJobTrackerLocation) < 0) {
        KFS_LOG_STREAM_INFO << "Unable to connect to: "
            << mJobTrackerLocation.ToString() << KFS_LOG_EOM;
        return kRetry;
    }

    string cmd = "/rerunmaptask?jobid=" + mJobId + "&id=" +
        boost::lexical_cast<string>(mapTaskId);

    JTNotifierOp notifierOp(cmd, mJobTrackerLocation.hostname);

    int res = DoOpCommon(&notifierOp, &sock);

    if (res < 0) {
        KFS_LOG_STREAM_INFO << "Unable to notify JT: "
            << mJobTrackerLocation.ToString()
            << " error: " << res << KFS_LOG_EOM;
        return kRetry;
    }

    if (notifierOp.contentBuf != NULL) {
        vector<string> parts;

        split(notifierOp.contentBuf, parts, ':');
        mJobState = parts[1].c_str();
    } else {
        mJobState = "NOTFOUND";
    }

    if (mJobState == "SUCCEEDED")
        return kSucceeded;
    if (mJobState == "NOTFOUND")
        return kNotFound;
    if ((mJobState == "FAILED") ||
        (mJobState == "KILLED"))
        return kFailed;
    return kSucceeded;
}

int
JTNotifier::getNumUnfinishedMappers()
{
    TcpSocket sock;
    int numUnfinishedMaps = -1;

    if (sock.Connect(mJobTrackerLocation) < 0) {
        KFS_LOG_STREAM_INFO << "Unable to connect to: "
            << mJobTrackerLocation.ToString() << KFS_LOG_EOM;
        return -1;
    }

    string cmd = "/numunfinishedmaps?jobid=" + mJobId;

    JTNotifierOp notifierOp(cmd, mJobTrackerLocation.hostname);

    int res = DoOpCommon(&notifierOp, &sock);

    if (res < 0) {
        KFS_LOG_STREAM_INFO << "Unable to notify JT: "
            << mJobTrackerLocation.ToString()
            << " error: " << res << KFS_LOG_EOM;
        return -1;
    }

    if (notifierOp.contentBuf != NULL) {
        vector<string> parts;

        split(notifierOp.contentBuf, parts, ':');
        numUnfinishedMaps = boost::lexical_cast<int>(parts[1].c_str());
    }
    return numUnfinishedMaps;
}

JobStatus
JTNotifier::getNumReducersToPreempt(int &nReducersToPreempt)
{
    TcpSocket sock;

    if (sock.Connect(mJobTrackerLocation) < 0) {
        KFS_LOG_STREAM_INFO << "Unable to connect to: "
            << mJobTrackerLocation.ToString() << KFS_LOG_EOM;
        return kRetry;
    }

    string cmd = "/numreduceslotsover?jobid=" + mJobId;

    JTNotifierOp notifierOp(cmd, mJobTrackerLocation.hostname);

    int res = DoOpCommon(&notifierOp, &sock);

    if (res < 0) {
        KFS_LOG_STREAM_INFO << "Unable to notify JT: "
            << mJobTrackerLocation.ToString()
            << " error: " << res << KFS_LOG_EOM;
        return kRetry;
    }

    if (notifierOp.contentBuf != NULL) {
        vector<string> parts;

        // Format: <job state>:<#>
        split(notifierOp.contentBuf, parts, ':');
        mJobState = parts[0].c_str();
        try {
            nReducersToPreempt = boost::lexical_cast<int>(parts[1].c_str());
        } catch (boost::bad_lexical_cast) { 
            nReducersToPreempt = 0;
        }
    } else {
        mJobState = "NOTFOUND";
    }

    if (mJobState == "SUCCEEDED")
        return kSucceeded;
    if (mJobState == "NOTFOUND")
        return kNotFound;
    if ((mJobState == "FAILED") ||
        (mJobState == "KILLED"))
        return kFailed;
    return kSucceeded;
}
