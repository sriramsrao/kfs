//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: ITimeout.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2006/03/25
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

#ifndef LIBIO_I_TIMEOUT_H
#define LIBIO_I_TIMEOUT_H

extern "C" {
#include <sys/types.h>
#include <sys/time.h>
#include <stdint.h>
}

namespace KFS
{

///
/// \file ITimeout.h
/// \brief Define the ITimeout interface.
///
 
/// 
/// \class ITimeout
/// Abstract class that defines a Timeout interface.  Whenever a
/// timeout occurs, the Timeout() method will be invoked.  An optional
/// setting, interval can be specified, which signifies the time
/// interval between successive invocations of Timeout().
/// 
/// NOTE: Timeout interface supports only a pseudo-real-time timers.
/// There is no guarantee that the desired interval will hold between
/// successive invocations of Timeout().
///
class ITimeout {
public:
    ITimeout() : mIntervalMs(0), mDisabled(false), mLastCall(0) { }
    virtual ~ITimeout() { }
    void Disable() {
        mDisabled = true;
    }
    /// Specify the interval in milli-seconds at which the timeout
    /// should occur. 
    void SetTimeoutInterval(int intervalMs, bool resetTimer = false) {
        mDisabled = false;
        mIntervalMs = intervalMs;
        if (resetTimer) {
            ResetTimer();
        }
    }

    int GetTimeElapsed() {
        return (NowMs() - mLastCall);
    }

    void ResetTimer() {
        mLastCall = NowMs();
    }

    static int64_t NowMs() {
        struct timeval timeNow;
        gettimeofday(&timeNow, 0);
        return (int64_t(timeNow.tv_sec) * 1000 + timeNow.tv_usec / 1000);
    }

    /// Whenever a timer expires (viz., a call to select returns),
    /// this method gets invoked.  Depending on the time-interval
    /// specified, the timeout is appropriately invoked.
    void TimerExpired(int64_t nowMs) {
        if (mDisabled)
            return;
        if (mIntervalMs <= 0 || nowMs >= mLastCall + mIntervalMs) {
            Timeout();
            mLastCall = nowMs;
        }
    }
    
    /// This method will be invoked when a timeout occurs.
    virtual void Timeout() = 0;
protected:
    int		mIntervalMs;
    bool        mDisabled;
private:
    int64_t 	mLastCall;
};

}

#endif // LIBIO_I_TIMEOUT_H
