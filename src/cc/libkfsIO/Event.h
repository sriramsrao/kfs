//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: Event.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2006/03/22
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

#ifndef _LIBKFSIO_EVENT_H
#define _LIBKFSIO_EVENT_H

#include "KfsCallbackObj.h"
#include <boost/shared_ptr.hpp>

namespace KFS
{
///
/// \enum EventCode_t
/// Various event codes that a KfsCallbackObj is notified with when
/// events occur.
///
enum EventCode_t {
    EVENT_NEW_CONNECTION,
    EVENT_NET_READ,
    EVENT_NET_WROTE,
    EVENT_NET_ERROR,
    EVENT_DISK_READ,
    EVENT_DISK_WROTE,
    EVENT_DISK_ERROR,
    EVENT_SYNC_DONE,
    EVENT_CMD_DONE,
    EVENT_INACTIVITY_TIMEOUT,
    EVENT_TIMEOUT
};

///
/// \enum EventStatus_t
/// \brief Code corresponding to the status of an event:
/// scheduled/done/cancelled. 
///
enum EventStatus_t {
    EVENT_STATUS_NONE,
    EVENT_SCHEDULED,
    EVENT_DONE,
    EVENT_CANCELLED
};

class Event {
public:
    Event (KfsCallbackObj *callbackObj, void *data, int timeoutMs, bool periodic) {
        mCallbackObj = callbackObj;
        mEventData = data;
        mEventStatus = EVENT_STATUS_NONE;
        mTimeoutMs = timeoutMs;
        mPeriodic = periodic;
        mLongtermWait = 0;
    };

    ~Event() {
        assert(mEventStatus != EVENT_SCHEDULED);
        Cancel();
        mEventData = NULL;
    }

    void SetStatus(EventStatus_t status) {
        mEventStatus = status;
    }

    int EventOccurred() {
        if (mEventStatus == EVENT_CANCELLED)
            return 0;
        mEventStatus = EVENT_DONE;
        return mCallbackObj->HandleEvent(EVENT_TIMEOUT, mEventData);
    }

    void Cancel() {
        mEventStatus = EVENT_CANCELLED;
    }
    
    bool IsPeriodic() {
        return mPeriodic;
    }

    int GetTimeout() {
        return mTimeoutMs;
    }

    void SetLongtermWait(int waitMs) {
        mLongtermWait = waitMs;
    }
    
    int DecLongtermWait(int numMs) {
        mLongtermWait -= numMs;
        if (mLongtermWait < 0)
            mLongtermWait = 0;
        return mLongtermWait;
    }

private:
    EventStatus_t	mEventStatus;
    KfsCallbackObj	*mCallbackObj;
    void		*mEventData;
    int			mTimeoutMs;
    bool		mPeriodic;
    int			mLongtermWait;
};

typedef boost::shared_ptr<Event> EventPtr;

}

#endif // _LIBKFSIO_EVENT_H
