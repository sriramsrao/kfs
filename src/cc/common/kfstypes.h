//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kfstypes.h 2580 2011-05-20 22:25:08Z sriramr $
//
// \brief Common declarations for KFS (meta/chunk/client-lib)
//
// Created 2006/10/20
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
//----------------------------------------------------------------------------

#ifndef COMMON_KFSTYPES_H
#define COMMON_KFSTYPES_H

extern "C" {
#include <stdlib.h>
#include <stdint.h>
}
#include <cerrno>
#include <cstddef>
#include <cassert>

#if defined (__APPLE__)
// tr1 on Apple doesn't define the hash for int64
#include "cxxutil.h"
#endif


namespace KFS {

typedef long long seq_t;        //!< request sequence no. for logging
typedef long long seqid_t;      //!< sequence number id's for file/chunks
typedef seqid_t fid_t;          //!< file ID
typedef seqid_t chunkId_t;      //!< chunk ID
typedef long long chunkOff_t;   //!< chunk offset
const fid_t ROOTFID = 2;        //!< special fid for "/

//!< Every time we change the protocol, rev. this one. We can use this value to
//!< detect clients running old binaries.
const int KFS_CLIENT_PROTO_VERS = 101; 

//!< Declarations as used in the Chunkserver/client-library
typedef int64_t kfsFileId_t;
typedef int64_t kfsChunkId_t;
typedef int64_t kfsSeq_t;

// const size_t CHUNKSIZE = 64u << 20; //!< (64MB)
const size_t CHUNKSIZE = 128u << 20; //!< (128MB)
const int MAX_RPC_HEADER_LEN = 16u << 10; //!< Max length of header in RPC req/response
const short int NUM_REPLICAS_PER_FILE = 3; //!< default degree of replication
const short int MAX_REPLICAS_PER_FILE = 64; //!< max. replicas per chunk of file

//!< If there are over 2000 rpcs pending from a single client, that client alone
//can monopolize the server; shed load.
const uint32_t MAX_PENDING_RPC_PER_CLIENT = 2000;

//!< Default lease interval of 5 mins
const int LEASE_INTERVAL_SECS = 300;

//!< Error codes for KFS specific errors
// version # being presented by client doesn't match what the server has
const int EBADVERS = 1000;

// lease has expired
const int ELEASEEXPIRED = 1001;

// checksum for data on a server is bad; client should read from elsewhere
const int EBADCKSUM = 1002;

// data lives on chunkservers that are all non-reachable
const int EDATAUNAVAIL = 1003;

// an error to indicate a server is busy and can't take on new work
const int ESERVERBUSY = 1004;

// an error occurring during allocation; the client will see this error
// code and retry. 
const int EALLOCFAILED = 1005;

// error to indicate that there is a cluster key mismatch between
// chunkserver and metaserver.
const int EBADCLUSTERKEY = 1006;
}

#endif // COMMON_KFSTYPES_H
