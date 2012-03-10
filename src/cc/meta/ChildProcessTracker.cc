
//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: ChildProcessTracker.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2009/04/30
//
// Copyright 2009 Quantcast Corp.
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
// \file ChildProcessTracker.cc
// \brief Handler for tracking child process that are forked off, retrieve
// their exit status.
//
//----------------------------------------------------------------------------

#include <vector>

#include "request.h"
#include "logger.h"
#include "ChildProcessTracker.h"
#include "libkfsIO/Globals.h"
#include "common/log.h"

namespace KFS
{

ChildProcessTrackingTimer gChildProcessTracker;

void ChildProcessTrackingTimer::Track(pid_t pid, MetaRequest *r)
{
	if (mPending.empty()) {
		libkfsio::globalNetManager().RegisterTimeoutHandler(this);
	}
	mPending.insert(std::make_pair(pid, r));
}

void ChildProcessTrackingTimer::Timeout()
{
        while (! mPending.empty()) {
		int         status = 0;
		const pid_t pid    = waitpid(-1, &status, WNOHANG);
		if (pid <= 0) {
			return;
		}
		std::pair<Pending::iterator, Pending::iterator> const range =
			mPending.equal_range(pid);
		if (range.first == range.second) {
			// Assume that all children are reaped here.
			KFS_LOG_STREAM_ERROR <<
				"untracked child exited:" 
				" pid: "     << pid <<
				" status: "  << status <<
			KFS_LOG_EOM;
			continue;
		}
		typedef std::vector<std::pair<pid_t, MetaRequest*> > Requests;
		Requests reqs;
		copy(range.first, range.second, std::back_inserter(reqs));
		mPending.erase(range.first, range.second);
		const bool lastReqFlag = mPending.empty();
		if (lastReqFlag) {
			libkfsio::globalNetManager().UnRegisterTimeoutHandler(this);
		}
		for (Requests::const_iterator it = reqs.begin(); it != reqs.end(); ++it) {
			MetaRequest* const req = it->second;
			req->status = WIFEXITED(status) ? WEXITSTATUS(status) :
				(WIFSIGNALED(status) ? -WTERMSIG(status) : -11111);
			req->suspended = false;
			KFS_LOG_STREAM_INFO <<
				"child exited:" 
				" pid: "     << pid <<
				" status: "  << req->status <<
				" request: " << req->Show() <<
			KFS_LOG_EOM;
			oplog.dispatch(req);
		}
		if (lastReqFlag) {
			return;
		}
	}
}

}
