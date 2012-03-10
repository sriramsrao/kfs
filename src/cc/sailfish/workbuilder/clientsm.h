//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: clientsm.h 3365 2011-11-23 22:40:49Z sriramr $
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
// \brief Declarations for a client SM.  This encapsulates all the
// structures associated with a connection/request.
//
//----------------------------------------------------------------------------

#ifndef WORKBUILDER_CLIENTSM_H
#define WORKBUILDER_CLIENTSM_H
#include "util.h"
#include "workbuilder.h"
#include "libkfsIO/KfsCallbackObj.h"
#include "libkfsIO/NetConnection.h"
#include <string>
namespace workbuilder
{
    class ClientSM : public KFS::KfsCallbackObj {
    public:
        ClientSM(KFS::NetConnectionPtr &conn);
        ~ClientSM();
        //
        // Sequence:
        //  Client connects.
        //   - A new client sm is born
        //   - reads a request out of the connection
        //   - submit the request for execution
        //   - when the request is done, send a response back.
        //   - since this is all http, the connection will get closed by client
        //     and we'll see the close and kill this object.
        int HandleRequest(int code, void *data);
    private:
        KFS::NetConnectionPtr	mNetConnection;

        /// Given a (possibly) complete op in a buffer, run it.
        bool		HandleClientCmd(KFS::IOBuffer *iobuf, int cmdLen);

        /// Op has finished execution.  Send a response to the client.
        void		SendResponse(const WorkbuilderOp &op);

	/// in debug messages print out client IP address so we know
	/// where the command is coming from.
        std::string	mClientIP;
    };
}

#endif // WORKBUILDER_CLIENTSM_H
