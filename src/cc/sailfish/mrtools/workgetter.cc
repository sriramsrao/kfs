//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: workgetter.cc 3830 2012-03-05 19:54:45Z sriramr $
//
// Created 2011/01/07
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

#include "workgetter.h"
#include "imerger.h"
#include "libkfsClient/KfsClientInt.h"
#include "common/log.h"
#include <boost/lexical_cast.hpp>
#include <vector>
#include <iostream>
#include <sstream>

using namespace KFS;
using std::string;
using std::vector;
using std::istringstream;

static void
split(const string &str, vector<string> &parts, char sep)
{
    string::size_type start = 0;
    string::size_type slash = 0;

    while (slash != string::npos) {
        assert(slash == 0 || str[slash] == sep);
        slash = str.find(sep, start);
        string nextc = str.substr(start, slash - start);
        start = slash + 1;
        parts.push_back(nextc);
    }
}

Workgetter::Workgetter(const string &workbuilder, const string &jobId,
    int reducerId, IMerger *owner) :
    mJobId(jobId), mReducerId(reducerId), mResponseCode(-1), mHaveWork(false), mIMerger(owner)
{
    vector<string> parts;
    split(workbuilder, parts, ':');
    mWorkbuilderLocation.hostname = parts[0];
    mWorkbuilderLocation.port = boost::lexical_cast<int>(parts[1]);
    mUrl = "/reducerstart?jobid=" + mJobId + "&reducerid="
        + boost::lexical_cast<string>(reducerId);
    mCountersUrl = "/recordcount?jobid=" + mJobId + "&reducerid="
        + boost::lexical_cast<string>(reducerId);
    mUpdate = "/reducerupdate?jobid=" + mJobId + "&reducerid="
        + boost::lexical_cast<string>(mReducerId);
}

Workgetter::~Workgetter()
{

}

void
Workgetter::get()
{
    TcpSocket sock;

    if (sock.Connect(mWorkbuilderLocation) < 0) {
        KFS_LOG_STREAM_INFO << "Unable to connect to: "
            << mWorkbuilderLocation.ToString() << KFS_LOG_EOM;
        return;
    }

    WorkgetterOp getOp(mUrl);

    int res = DoOpCommon(&getOp, &sock);

    if (res < 0) {
        KFS_LOG_STREAM_INFO << "Unable to get work from: "
            << mWorkbuilderLocation.ToString()
            << " error: " << res <<  KFS_LOG_EOM;
        return;
    }

    mResponseCode = getOp.httpStatusCode;

    if ((mResponseCode == 404) || (mResponseCode == 405)) {
        KFS_LOG_STREAM_INFO << "Got a 404/405 response from workbuilder...exiting"
            << KFS_LOG_EOM;
        exit(-1);
    }

    if (getOp.httpStatusCode != 200) {
        return;
    }
    KFS_LOG_STREAM_INFO << "Got back work, with contentLen: "
        << getOp.contentLength << KFS_LOG_EOM;

    setWorkReceived();
    mIMerger->parseAssignment(getOp.contentBuf, getOp.contentLength);
}

bool
Workgetter::shouldTerminateEarly(const unsigned char *nextKey, int keyLen,
    uint32_t recordCountDelta, uint32_t recordCount)
{
    TcpSocket sock;

    if (sock.Connect(mWorkbuilderLocation) < 0) {
        KFS_LOG_STREAM_INFO << "Unable to connect to: "
            << mWorkbuilderLocation.ToString() 
            << " and send it an update..." << KFS_LOG_EOM;
        // keep going in terms of merge work.
        return false;
    }

    string updateUrl = mUpdate + "&delta=" + boost::lexical_cast<string>(recordCountDelta) +
        "&count=" + boost::lexical_cast<string>(recordCount);

    WorkgetterOp updateOp(updateUrl);

    // allocate a buffer that can also be used to hold the response
    updateOp.AttachContent((const char *) nextKey, keyLen, 
        keyLen > 1024 ? keyLen : 1024);

    int res = DoOpCommon(&updateOp, &sock);

    if (res < 0) {
        KFS_LOG_STREAM_INFO << "Unable to send update to: "
            << mWorkbuilderLocation.ToString() 
            << " error: " << res <<  KFS_LOG_EOM;
        return false;
    }

    mResponseCode = updateOp.httpStatusCode;

    if (updateOp.httpStatusCode != 200) {
        // if we aren't told to wind-up, keep going
        return false;
    }
    KFS_LOG_STREAM_INFO << "**PAUSING: " << "Got update response: " 
        << updateOp.contentBuf << " with contentLen: " 
        << updateOp.contentLength << KFS_LOG_EOM;

    // buffer is big enough
    if(strncmp(updateOp.contentBuf, "WIND_UP", 7) == 0) 
        return true;
    // all else
    return false;
}

int
Workgetter::notifyLostChunk(kfsChunkId_t chunkId)
{
    TcpSocket sock;

    if (sock.Connect(mWorkbuilderLocation) < 0) {
        KFS_LOG_STREAM_INFO << "Unable to connect to: "
            << mWorkbuilderLocation.ToString() << KFS_LOG_EOM;
        return -1;
    }

    string cmd = "/chunklost?jobid=" + mJobId +
        "&reducerid=" + boost::lexical_cast<string>(mReducerId) +
        "&chunkid=" + boost::lexical_cast<string>(chunkId) +
        "&ifileid=" + boost::lexical_cast<string>(mIMerger->getPartition());

    WorkgetterOp getOp(cmd);

    int res = DoOpCommon(&getOp, &sock);

    if (res < 0) {
        KFS_LOG_STREAM_INFO << "Unable to notify workbuilder of lost chunk: "
            << mWorkbuilderLocation.ToString()
            << " error: " << res <<  KFS_LOG_EOM;
        return res;
    }
    return getOp.httpStatusCode;
}

void
Workgetter::postNumRecords(int partition, int actual, int expected)
{
    TcpSocket sock;

    if (sock.Connect(mWorkbuilderLocation) < 0) {
        KFS_LOG_STREAM_INFO << "Unable to connect to: "
            << mWorkbuilderLocation.ToString() << KFS_LOG_EOM;
        return;
    }

    mCountersUrl += "&partition=" + boost::lexical_cast<string>(partition);
    mCountersUrl += "&actual=" + boost::lexical_cast<string>(actual);
    mCountersUrl += "&expected=" + boost::lexical_cast<string>(expected);
    WorkgetterOp getOp(mCountersUrl);

    int res = DoOpCommon(&getOp, &sock);

    if (res < 0) {
        KFS_LOG_STREAM_INFO << "Unable to send counters to workbuilder: "
            << mWorkbuilderLocation.ToString()
            << " error: " << res <<  KFS_LOG_EOM;
    }
}
