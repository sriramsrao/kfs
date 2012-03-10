//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: ChunkServerFactory.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2006/09/29
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
// \brief ChunkServerFactory code to remove a dead server.
//----------------------------------------------------------------------------

#include "ChunkServerFactory.h"
#include <algorithm>
using std::find_if;

using namespace KFS;

void
ChunkServerFactory::RemoveServer(const ChunkServer *target)
{
	list<ChunkServerPtr>::iterator i;

	i = find_if(mChunkServers.begin(), mChunkServers.end(),
			ChunkServerMatcher(target));
	if (i != mChunkServers.end()) {
		mChunkServers.erase(i);
	}
}

