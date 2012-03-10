//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: clientmgr.h 3365 2011-11-23 22:40:49Z sriramr $
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
// \brief A client manager that spins up a client sm in response to a connection.
//
//----------------------------------------------------------------------------

#ifndef WORKBUILDER_CLIENTMGR_H
#define WORKBUILDER_CLIENTMGR_H

#include "libkfsIO/Acceptor.h"
#include "libkfsIO/KfsCallbackObj.h"
#include "clientsm.h"

namespace workbuilder
{
    class ClientSM;

    class ClientManager : public KFS::IAcceptorOwner {
    public:
        ClientManager() {
            mAcceptor = NULL;
        }
        virtual ~ClientManager() {
            delete mAcceptor;
        }
        void StartAcceptor(int port) {
            mAcceptor = new KFS::Acceptor(port, this);
        }
        KFS::KfsCallbackObj *CreateKfsCallbackObj(KFS::NetConnectionPtr &conn) {
            return new ClientSM(conn);
        }
    private:
        // The socket object which is setup to accept connections.
        KFS::Acceptor *mAcceptor;
    };
    extern ClientManager gClientManager;
}

#endif // WORKBUILDER_CLIENTMGR_H
