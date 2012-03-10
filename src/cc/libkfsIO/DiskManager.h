//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: DiskManager.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2006/03/16
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
// 
//----------------------------------------------------------------------------

#ifndef _LIBIO_DISK_MANAGER_H
#define _LIBIO_DISK_MANAGER_H

#include <list>

#include "DiskConnection.h"
#include "NetManager.h"
#include "meta/queue.h"

namespace KFS
{

///
/// \file DiskManager.h
/// \brief DiskManager uses aio to schedule disk operations.  It
/// periodically checks the status of the operations and notifies the
/// completion of the associated events.
///

///
/// \brief Define the length of a path name.
///
const int MAX_FILE_NAME_LEN = 256;

// forward declaration
class DiskManagerTimeoutImpl;

class DiskManager {
public:
    DiskManager();
    ~DiskManager();

    /// Register a timeout handler with the NetManager.  The
    /// NetManager will call the handler whenever a timeout occurs.
    void 	Init();
    void 	InitForAIO();

    /// Schedule a read on fd at the specified offset for specified #
    /// of bytes.
    /// @param[in] conn DiskConnection on which the read is scheduled
    /// @param[in] fd file descriptor that will be used in aio_read().
    /// @param[in] data IOBufferData to hold the result of the read
    /// @param[in] offset offset in the file at which read should start
    /// @param[in] numBytes # of bytes to be read
    /// @param[out] resultEvent DiskEventPtr that stores information
    /// about the scheduled read event.
    /// @retval 0 on success; -1 on failure
    int		Read(DiskConnection *conn, int fd,
                     const IOBufferData &data,
                     off_t offset, int numBytes,
                     DiskEventPtr &resultEvent);

    /// Schedule a write
    /// @param[in] conn DiskConnection on which the write is scheduled
    /// @param[in] fd file descriptor that will be used in aio_write().
    /// @param[in] data IOBufferData that holds the data to be written.
    /// @param[in] offset offset in the file at which write should start
    /// @param[in] numBytes # of bytes to be written
    /// @param[out] resultEvent DiskEventPtr that stores information
    /// about the scheduled write event.
    /// @retval 0 on success; -1 on failure
    int 	Write(DiskConnection *conn, int fd,
                      const IOBufferData &data,
                      off_t offset, int numBytes,
                      DiskEventPtr &resultEvent);

    /// Schedule a sync
    /// @param[in] conn DiskConnection on which the sync is scheduled
    /// @param[in] fd file descriptor that will be used in aio_sync().
    /// @param[out] resultEvent DiskEventPtr that stores information
    /// about the scheduled sync event.
    /// @retval 0 on success; -1 on failure
    int		Sync(DiskConnection *conn,
                     int fd, DiskEventPtr &resultEvent);


    /// We reap the completed IOs from the completed queue.  for
    /// completed ones, signal the associated event.
    void 	ReapCompletedIOs();

    void	SetMaxOutstandingIOs(int v) {
        mMaxOutstandingIOs = v;
    }
    int NumDiskIOOutstanding() {
        return mDiskEvents.size();
    }
    void IOCompleted(DiskEvent_t *event) {
        mCompleted.enqueue(event);
    }

private:
    /// list of read/write/sync events that have been scheduled
    std::list<DiskEventPtr>		mDiskEvents;
    /// queue of disk I/Os that have finished and need to be reaped
    MetaQueue<DiskEvent_t>		mCompleted;
    /// if there are too many IOs outstanding, throttle
    bool			mOverloaded;
    /// the limit before we begin throttling
    uint32_t			mMaxOutstandingIOs;

    /// IO has been initiated; update load stats
    void			IOInitiated();
    /// IO has been completed; update load stats
    void			IOCompleted();

};

}

#endif // _LIBIO_DISK_MANAGER_H
