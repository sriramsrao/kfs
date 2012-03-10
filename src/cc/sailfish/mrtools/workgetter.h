//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: workgetter.h 3830 2012-03-05 19:54:45Z sriramr $
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

#ifndef KAPPENDER_WORKGETTER_H
#define KAPPENDER_WORKGETTER_H

#include "common/kfsdecls.h"
#include "libkfsClient/KfsOps.h"
#include "util.h"

#include <string>
#include <boost/lexical_cast.hpp>

namespace KFS
{
    class IMerger;

    // convenience
    typedef HttpGetOp WorkgetterOp;

    class Workgetter {
    public:
        Workgetter(const std::string &workbuilder, const std::string &jobId,
            int reducerId, IMerger *owner);
        ~Workgetter();
        void setState(const char *s) {
            mJobState = s;
        }
        std::string getState() {
            return mJobState;
        }

        IMerger* getIMerger() {
            return mIMerger;
        }

        void setResponseCode(const std::string &responseCode) {
            mResponseCode = boost::lexical_cast<int>(responseCode);
        }

        int getResponseCode() {
            return mResponseCode;
        }
        void get();

        bool shouldTerminateEarly(const unsigned char *nextKey, int keyLen,
            uint32_t recordCountDelta, uint32_t recordCount);

        int notifyLostChunk(kfsChunkId_t chunkId);

        // Notify workbuilder the # of records we got
        void postNumRecords(int partition, int actual, int expected);

        void setWorkReceived() {
            mHaveWork = true;
        }

        void clearWorkReceived() {
            mHaveWork = false;
        }

        bool haveWork() {
            return mHaveWork;
        }

    private:
        ServerLocation   mWorkbuilderLocation;
        std::string      mJobId;
        std::string      mJobState;
        std::string      mUrl;
        std::string      mUpdate;
        std::string      mCountersUrl;
        int		 mReducerId;
        int              mResponseCode;
        bool             mHaveWork;

        // backpointer to the owning imerger object
        IMerger    *mIMerger;
    };
}

#endif // KAPPENDER_WORKGETTER_H
