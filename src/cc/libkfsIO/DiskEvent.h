//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: DiskEvent.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2006/03/28
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

#ifndef _LIBIO_DISKEVENT_H
#define _LIBIO_DISKEVENT_H

#include <stdio.h>
#include <aio.h>
#include <boost/shared_ptr.hpp>
#include <string.h>

#if defined (__sun__)
#include <port.h>
#endif

#include "Event.h"
#include "IOBuffer.h"

namespace KFS
{
///
/// \file DiskEvent.h
/// \brief Declarations related to events related to Disk-I/O.
///

///
/// \enum DiskEventOp_t
/// \brief Code corresponding to the various disk events: read/write/sync
///
enum DiskEventOp_t {
    OP_NONE,
    OP_READ,
    OP_WRITE,
    OP_SYNC
};

class DiskConnection;


///
/// \struct DiskEvent_t
/// \brief A disk event stores information about the event
/// (read/write/sync). 
/// Disk events are asynchronous events.  The DiskManager uses aio to
/// (1) schedule the events and (2) retrieve the result of the event
/// execution later.  A scheduled event can be cancelled at any time.
///
/// For I/O, a disk event has two pieces of information: (1) a
/// DiskConnection (@see DiskConnection), that encapsulates the
/// information about the file descriptor, and (2) a buffer for
/// I/O(@see IOBuffer). 
///
struct DiskEvent_t {
    DiskEvent_t(DiskConnectionPtr c, DiskEventOp_t o) {
        op = o;
        conn = c;
        memset(&aio_cb, 0, sizeof(struct aiocb));
        aioStatus = 0;
        status = EVENT_STATUS_NONE;
        notifyDone = true;
    }
    DiskEvent_t(DiskConnectionPtr c, const IOBufferData &d,
                DiskEventOp_t o) {
        op = o;
        conn = c;
        data = d;
        aioStatus = 0;
        memset(&aio_cb, 0, sizeof(struct aiocb));
        status = EVENT_STATUS_NONE;
        notifyDone = true;
    }
    ~DiskEvent_t() {
        assert ((status == EVENT_CANCELLED) || 
                (status == EVENT_DONE) ||
                (status == EVENT_STATUS_NONE));
        // XXX: What if the event isn't cancelled?
        
    }
    /// Cancel the event if possible.
    /// @param fd The file descriptor associated with this event.
    int Cancel(int fd) {
        if (aio_cancel(fd, &aio_cb) == -1) {
            perror("aio cancel: ");
            status = EVENT_CANCELLED;
            return -1;
        }
        status = EVENT_CANCELLED;
        return 0;
    }
    
    /// Returns the string that describes the type of event.
    const char* ToString();

    /// Type of operation associated with this event.
    DiskEventOp_t	op;
    /// DiskConnection associated with this event.
    DiskConnectionPtr	conn;
    /// The buffer on which I/O is to be done on a read or write.
    IOBufferData	data;
    /// The aio related information about the event
    struct aiocb	aio_cb;
#if defined (__sun__)
    port_notify_t	port_notify;
#endif
    /// Status of this event
    EventStatus_t	status;
    int			aioStatus; // status by calling aio_error()
    /// Status of executing the event: That is, return value from
    /// read/write
    ssize_t		retval;
    /// Set if th eupstream owner of this event needs notification when the event
    /// has finished execution
    bool 		notifyDone;
};

///
/// \typedef DiskEventPtr
/// DiskEvent_t are encapsulated in a smart pointer, so that when the
/// last reference is released, appropriate cleanup occurs.
/// 
typedef boost::shared_ptr<DiskEvent_t> DiskEventPtr;

}

#endif // _LIBIO_DISKEVENT_H
