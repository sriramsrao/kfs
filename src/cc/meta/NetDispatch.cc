//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: NetDispatch.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2006/06/01
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
// \file NetDispatch.cc
//
// \brief Meta-server network dispatcher.
//
//----------------------------------------------------------------------------

#include "NetDispatch.h"
#include "logger.h"
#include "LayoutManager.h"
#include "libkfsIO/Globals.h"
#include "common/log.h"

using namespace KFS;
using namespace KFS::libkfsio;

NetDispatch KFS::gNetDispatch;

NetDispatch::NetDispatch()
{
        mClientManager = new ClientManager();
        mChunkServerFactory = new ChunkServerFactory();
}

NetDispatch::~NetDispatch()
{
        delete mClientManager;
        delete mChunkServerFactory;
}

//
// Open up the server for connections.
//
void
NetDispatch::Start(int clientAcceptPort, int chunkServerAcceptPort)
{
        // Start the acceptors so that it sets up a connection with the net
        // manager for listening. 
        mClientManager->StartAcceptor(clientAcceptPort);
        mChunkServerFactory->StartAcceptor(chunkServerAcceptPort);
        // Start polling....
	globalNetManager().MainLoop();
}

///
/// Poll the logger to see if any op's have finished execution.  For
/// such ops, send a response back to the client.  Also, if there any
/// layout related RPCs, dispatch them now.
///
void
NetDispatch::Dispatch(MetaRequest *r)
{
	// The Client will send out a response and destroy r.
	if (r->clnt)
		r->clnt->HandleEvent(EVENT_CMD_DONE, (void *) r);
	else if (r->op == META_ALLOCATE) {
		// For truncations, we may need to internally
		// generate an allocation request.  In such a
		// case, run the allocation thru the entire gamut
		// (dispatch the request to chunkserver and log)
		// and then resume processing of the truncation.
		MetaAllocate *alloc = static_cast<MetaAllocate *> (r);
		assert(alloc->req != NULL);
		r = alloc->req;
		assert(r->op == META_TRUNCATE);
		r->suspended = false;
		submit_request(r);
		delete alloc;
	}
	else if (r->op == META_CHANGE_CHUNKVERSIONINC) {
		MetaChangeChunkVersionInc *ccvi = 
			static_cast<MetaChangeChunkVersionInc *> (r);
		if (ccvi->req != NULL) {
			// req->op was waiting for the chunkVersionInc change to
			// make it to disk.  Now that is done, we can push out req->op.
			r = ccvi->req;
			r->suspended = false;
			assert((r->op == META_ALLOCATE) && (r->clnt != NULL));
			r->clnt->HandleEvent(EVENT_CMD_DONE, (void *) r);
		}
		delete ccvi;
	}
	else if ((r->op == META_CHUNK_REPLICATE) ||
		 (r->op == META_CHUNK_SIZE) ||
		 (r->op == META_CHUNK_MAKE_STABLE) ||
		 (r->op == META_BEGIN_MAKE_CHUNK_STABLE)) {
		// For replicating a chunk/computing a chunk's size/make
		// chunk stable, we sent a request to
		// a chunkserver; it did the work and sent back a reply.
		// We have processed the reply and that message is now here.
		// Nothing more to do.  So, get rid of it
		delete r;
	}
	else {
		// somehow, occasionally we are getting checkpoint requests here...
		KFS_LOG_VA_DEBUG("Getting an op (%d) with no client", r->op);
	}
}

