//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: ChunkReplicator.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2007/01/18
//
// Copyright 2008 Quantcast Corp.
// Copyright 2007-2008 Kosmix Corp.
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
// 
//----------------------------------------------------------------------------

#include "ChunkReplicator.h"
#include "libkfsIO/Globals.h"
#include <cassert>

using namespace KFS;
using namespace libkfsio;

ChunkReplicator::ChunkReplicator() :
	mInProgress(false), mOp(1, this) 
{ 
	SET_HANDLER(this, &ChunkReplicator::HandleEvent);
	mTimer = new ChunkReplicatorTimeoutImpl(this);
	globalNetManager().RegisterTimeoutHandler(mTimer);
}

ChunkReplicator::~ChunkReplicator()
{
	globalNetManager().UnRegisterTimeoutHandler(mTimer);
	delete mTimer;
}

/// Use the main loop to process the request.
int
ChunkReplicator::HandleEvent(int code, void *data)
{
	static seq_t seqNum = 1;
	switch (code) {
	case EVENT_CMD_DONE:
		mInProgress = false;
		return 0;
	case EVENT_TIMEOUT:
		if (mInProgress)
			return 0;
		mOp.opSeqno = seqNum;
		++seqNum;
		mInProgress = true;
		submit_request(&mOp);
		return 0;
	default:
		assert(!"Unknown event");
		break;
	}
	return 0;
}
