//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: ClientSM.h 1552 2011-01-06 22:21:54Z sriramr $
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
// \file ClientSM.h
// \brief Object for handling interactions with a KFS client.
// 
//----------------------------------------------------------------------------

#ifndef META_CLIENTSM_H
#define META_CLIENTSM_H

#include "request.h"
#include "libkfsIO/KfsCallbackObj.h"
#include "libkfsIO/NetConnection.h"

namespace KFS
{
    class Properties;

    class ClientSM : public KfsCallbackObj {
    public:

        ClientSM(NetConnectionPtr &conn);

        ~ClientSM(); 

        //
        // Sequence:
        //  Client connects.
        //   - A new client sm is born
        //   - reads a request out of the connection
        //   - submit the request for execution
        //   - when the request is done, send a response back.
        //
        int HandleRequest(int code, void *data);
        int HandleTerminate(int code, void *data);
        static void SetParameters(const Properties& prop);

    private:
        /// A handle to a network connection
        NetConnectionPtr	mNetConnection;

        /// The op (if any) that is currently being executed
        MetaRequest		*mOp;

	/// If the client sends multiple requests, after the first
	/// request, keep the rest pending; when one op finishes, submit the
	/// next one.
	std::list<MetaRequest *> mPending;

	/// queue length
	int		mPendingLength;
        int             mRecursionCnt;
	/// used to print message about old protocol version once
	int		mClientProtoVers;

        /// Given a (possibly) complete op in a buffer, run it.
        void		HandleClientCmd(IOBuffer *iobuf, int cmdLen);

        /// Op has finished execution.  Send a response to the client.
        void		SendResponse(MetaRequest *op);

	/// submit an op for execution to the request processor
	void		SubmitOp();

        static int sMaxPendingLength;
	static int sMaxReadAhead;
	static int sInactivityTimeout;
        static int sMaxWriteBehind;
    };

}

#endif // META_CLIENTSM_H
