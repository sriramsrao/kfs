//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: setnumreducer.h 3809 2012-02-28 22:22:12Z sriramr $
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

#ifndef WORKBUILDER_SETNUMREDUCER_H
#define WORKBUILDER_SETNUMREDUCER_H
#include "util.h"
#include "statuschecker.h"
#include <string>

namespace workbuilder
{
    class JTNotifier {
    public:
        JTNotifier(const std::string &jobTracker, const std::string &jobId);
        void setState(const char *s) {
            mJobState = s;
        }
        std::string getState() {
            return mJobState;
        }
        JobStatus setNumReducers(int nReducers);
        JobStatus reRunTask(int mapTaskId);
        int getNumUnfinishedMappers();
        JobStatus getNumReducersToPreempt(int &nReducersToPreempt);

    private:
        KFS::ServerLocation   mJobTrackerLocation;
        std::string      mJobId;
        std::string      mJobState;
    };
}

#endif // WORKBUILDER_SETNUMREDUCER_H
