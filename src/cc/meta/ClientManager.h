//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: ClientManager.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2006/06/02
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
// \file ClientManager.h
// \brief Create client state machines whenever clients connect to meta server.
// 
//----------------------------------------------------------------------------

#ifndef META_CLIENTMANAGER_H
#define META_CLIENTMANAGER_H

#include "libkfsIO/Acceptor.h"
#include "libkfsIO/KfsCallbackObj.h"
#include "ClientSM.h"

namespace KFS
{

    class ClientManager : public IAcceptorOwner {
    public:
        ClientManager() {
            mAcceptor = NULL;
        };
        virtual ~ClientManager() {
            delete mAcceptor;
        };
        void StartAcceptor(int port) {
            mAcceptor = new Acceptor(port, this);
        };
        KfsCallbackObj *CreateKfsCallbackObj(NetConnectionPtr &conn) {
            // XXX: Should we keep a list of all client state machines?
            return new ClientSM(conn);
        }
    private:
        // The socket object which is setup to accept connections.
        Acceptor *mAcceptor;
    };


}

#endif // META_CLIENTMANAGER_H
