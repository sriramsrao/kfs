//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: NetDispatch.h 1552 2011-01-06 22:21:54Z sriramr $
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
// \file NetDispatch.h
// \brief Meta-server network dispatcher
// 
//----------------------------------------------------------------------------

#ifndef META_NETDISPATCH_H
#define META_NETDISPATCH_H

#include "libkfsIO/NetManager.h"
#include "libkfsIO/Acceptor.h"
#include "ChunkServerFactory.h"
#include "ClientManager.h"

namespace KFS
{
    class NetDispatch {
    public:
        NetDispatch();
        ~NetDispatch();
        void Start(int clientAcceptPort, int chunkServerAcceptPort);
        //!< An RPC just finished execution.  Send it on its merry way
        void Dispatch(MetaRequest *r);
        ChunkServerFactory *GetChunkServerFactory() {
            return mChunkServerFactory;
        }

    private:
        ClientManager *mClientManager; //!< tracks the connected clients
        ChunkServerFactory *mChunkServerFactory; //!< creates chunk servers when they connect
    };

    extern NetDispatch gNetDispatch;
}

#endif // META_NETDISPATCH_H
