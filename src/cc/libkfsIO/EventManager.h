//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: EventManager.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2006/03/31
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

#ifndef _LIBKFSIO_EVENTMANAGER_H
#define _LIBKFSIO_EVENTMANAGER_H

#include "Event.h"
#include "ITimeout.h"
#include "NetManager.h"

namespace KFS
{

///
/// \file EventManager.h
/// \brief EventManager supports execution of time-based events.
/// Events can be scheduled at milli-second granularity and they are
/// notified whenever the time to execute them arises.  
///

class EventManagerTimeoutImpl;

class EventManager {
public:

    static const int MAX_EVENT_SLOTS = 2000;
    // 10 ms granularity for events
    static const int EVENT_GRANULARITY_MS = 10;

    EventManager();
    ~EventManager();

    ///
    /// Schedule an event for execution.  If the event is periodic, it
    /// will be re-scheduled for execution.
    /// @param[in] event A reference to the event that has to be scheduled.
    /// @param[in] afterMs # of milli-seconds after which the event
    /// should be executed.
    ///
    void	Schedule(EventPtr &event, int afterMs);

    /// Register a timeout handler with the NetManager.  The
    /// NetManager will call the handler whenever a timeout occurs.
    void 	Init();

    /// Whenever a timeout occurs, walk the list of scheduled events
    /// to determine if any need to be signaled.
    void 	Timeout();

private:

    EventManagerTimeoutImpl	*mEventManagerTimeoutImpl;

    /// Events are held in a calendar queue: The calendar queue
    /// consists of a circular array of "slots".  Slots are a
    /// milli-second apart.  At each timeout, the list of events in
    /// the current slot are signaled.
    std::list<EventPtr>	mSlots[MAX_EVENT_SLOTS];
    
    /// Index into the above array that points to where we are
    /// currently.
    int			mCurrentSlot;

    /// Events for which the time of occurence is after the last slot
    /// (i.e., after 20 seconds).
    std::list <EventPtr>	mLongtermEvents;
};

///
/// \class EventManagerTimeoutImpl
/// \brief Implements the ITimeout interface (@see ITimeout).
///
class EventManagerTimeoutImpl : public ITimeout {
public:
    /// The owning object of a EventManagerTimeoutImpl is the EventManager.
    EventManagerTimeoutImpl(EventManager *mgr) {
        mEventManager = mgr;
    };
    /// Callback the owning object whenever a timeout occurs.
    virtual void Timeout() {
        mEventManager->Timeout();
    };
private:
    /// Owning object.
    EventManager		*mEventManager;

};

}

#endif // _LIBKFSIO_EVENTMANAGER_H
