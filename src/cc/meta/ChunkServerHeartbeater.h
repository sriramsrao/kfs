//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: ChunkServerHeartbeater.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2009/02/24
//
// Copyright 2009 Quantcast Corporation. 
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
// \brief A timer that periodically sends a heartbeat message to all chunkservers.
//
//----------------------------------------------------------------------------

#ifndef META_CHUNKSERVERHEARTBEATER_H
#define META_CHUNKSERVERHEARTBEATER_H

#include "libkfsIO/ITimeout.h"
#include "LayoutManager.h"

namespace KFS
{
	class ChunkServerHeartbeater : public ITimeout {
	public:
		ChunkServerHeartbeater(int heartbeatIntervalSec = 60) {
			// send heartbeat once every min by default
			SetTimeoutInterval(heartbeatIntervalSec * 1000);
		};
		// On a timeout send a heartbeat RPC
		void Timeout() {
			gLayoutManager.HeartbeatChunkServers();
		};
	};
}

#endif // META_CHUNKSERVERHEARTBEATER_H
