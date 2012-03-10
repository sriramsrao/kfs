//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: LeaseCleaner.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2006/10/16
//
// Copyright 2008 Quantcast Corp.
// Copyright 2006-2008 Kosmix Corp.
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
// \brief A lease cleaner to periodically cleanout dead leases.  It does so,
// by sending a "MetaLeaseCleanup" op to do the work.
//----------------------------------------------------------------------------

#ifndef META_LEASECLEANER_H
#define META_LEASECLEANER_H

#include "libkfsIO/KfsCallbackObj.h"
#include "libkfsIO/ITimeout.h"
#include "libkfsIO/Event.h"
#include "request.h"

namespace KFS
{

class LeaseCleanerTimeoutImpl;

class LeaseCleaner : public KfsCallbackObj {
public:
	// The interval with which we should do the cleanup
	static const int CLEANUP_INTERVAL_SECS = 60;
	static const int CLEANUP_INTERVAL_MSECS = CLEANUP_INTERVAL_SECS * 1000;

	LeaseCleaner();
	~LeaseCleaner();
	int HandleEvent(int code, void *data);

private:
	/// If a cleanup op is in progress, skip a send
	bool mInProgress;
	LeaseCleanerTimeoutImpl *mTimer;
	/// The op for doing the cleanup
	MetaLeaseCleanup mOp;
};

class LeaseCleanerTimeoutImpl : public ITimeout {
public:
	LeaseCleanerTimeoutImpl(LeaseCleaner *l) : mOwner(l) {
		SetTimeoutInterval(LeaseCleaner::CLEANUP_INTERVAL_MSECS);
	}
	void Timeout() {
		mOwner->HandleEvent(EVENT_TIMEOUT, NULL);
	}
private:
	LeaseCleaner *mOwner;
};

}

#endif // META_LEASECLEANER_H
