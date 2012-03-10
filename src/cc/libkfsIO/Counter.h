//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: Counter.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2006/07/20
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
// \brief Counter for statistics gathering.
//
//----------------------------------------------------------------------------

#ifndef LIBKFSIO_COUNTER_H
#define LIBKFSIO_COUNTER_H

#include <stdint.h>
#include <algorithm>
#include <string>
#include <sstream>
#include <tr1/unordered_map>

namespace KFS
{

class Counter;

/// Map from a counter name to the associated Counter object
typedef std::tr1::unordered_map<std::string, Counter *> CounterMap;
typedef std::tr1::unordered_map<std::string, Counter *>::const_iterator CounterMapIterator;


/// Counters in KFS are currently setup to track a single "thing".
/// If we need to track multiple related items (such as, network
/// connections and how much I/O is done on them), then we need to
/// have multiple counters one for each and then display the
/// accumulated statistics in some way.
class Counter {
public:
    // XXX: add threshold values for counts

    Counter() : mName(""), mCount(0), mTimeSpent(0.0) { }
    Counter(const char *name) : mName(name), mCount(0), mTimeSpent(0.0) { }
    virtual ~Counter() { }

    /// Print out some information about this counter
    virtual void Show(std::ostringstream &os) {
        os << mName << ": " << mCount << "," << mTimeSpent << "\r\n";
    }

    void SetName(const char *name) {
        mName = name;
    }
    
    /// Update the counter 
    virtual void Update(int amount) { mCount += amount; }

    virtual void Update(float timeSpent) { mTimeSpent += timeSpent; }

    virtual void Set(int c) { mCount = c; }

    /// Reset the state of this counter
    virtual void Reset() { mCount = 0; mTimeSpent = 0.0; }

    const std::string & GetName() const {
        return mName;
    }
    uint64_t GetValue() const {
        return mCount;
    }
protected:
    /// Name of this counter object
    std::string mName;
    /// Value of this counter
    uint64_t mCount;
    /// time related statistics
    float mTimeSpent;
};

class ShowCounter {
    std::ostringstream &os;
public:
    ShowCounter(std::ostringstream &o) : os(o) { }
    void operator() (std::tr1::unordered_map<std::string, Counter *>::value_type v) {
        Counter *c = v.second;
            
        c->Show(os);
    }
};

///
/// Counter manager that tracks all the counters in the system.  The
/// manager can be queried for statistics.
///
class CounterManager {
public:
    CounterManager() { };
    ~CounterManager() {
        
        mCounters.clear();
    }
    
    /// Add a counter object
    /// @param[in] counter   The counter to be added
    void AddCounter(Counter *counter) {
        mCounters[counter->GetName()] = counter;
    }

    /// Remove a counter object
    /// @param[in] counter   The counter to be removed
    void RemoveCounter(Counter *counter) {
        const std::string & name = counter->GetName();
        CounterMapIterator iter = mCounters.find(name);

        if (iter == mCounters.end())
            return;

        mCounters.erase(name);
    }

    /// Given a counter's name, retrieve the associated counter
    /// object.
    /// @param[in] name   Name of the counter to be retrieved
    /// @retval The associated counter object if one exists; NULL
    /// otherwise. 
    Counter *GetCounter(const std::string &name) {
        CounterMapIterator iter = mCounters.find(name);
        Counter *c;

        if (iter == mCounters.end())
            return NULL;
        c = iter->second;
        return c;
    }
    
    /// Print out all the counters in the system, one per line.  Each
    /// line is terminated with a "\r\n".  If there are no counters,
    /// then we print "\r\n".
    void Show(std::ostringstream &os) {
        if (mCounters.size() == 0) {
            os << "\r\n";
            return;
        }

	std::for_each(mCounters.begin(), mCounters.end(), ShowCounter(os));
    }

private:
    /// Map that tracks all the counters in the system
    CounterMap  mCounters;
};

}

#endif // LIBKFSIO_COUNTER_H
