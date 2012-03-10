//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: Counter.cc 1552 2011-01-06 22:21:54Z sriramr $
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
// \brief Implementation of the CounterManager and declarations for a
//few counters.
//----------------------------------------------------------------------------

#include "Counter.h"

using namespace KFS;

/// A few commonly needed counters
Counter gOpenNetFds("Open network fds");
Counter gOpenDiskFds("Open disk fds");
Counter gNetBytesRead("Bytes read from network");
Counter gNetBytesWritten("Bytes written to network");
Counter gDiskBytesRead("Bytes read from disk");
Counter gDiskBytesWritten("Bytes written to disk");
// track the # of failed read/writes
Counter gDiskIOErrors("Disk I/O errors");

CounterManager gCounterManager;

void InitCounterManager()
{
    gCounterManager.AddCounter(&gOpenNetFds);
    gCounterManager.AddCounter(&gOpenDiskFds);

    gCounterManager.AddCounter(&gNetBytesRead);
    gCounterManager.AddCounter(&gNetBytesWritten);

    gCounterManager.AddCounter(&gDiskBytesRead);
    gCounterManager.AddCounter(&gDiskBytesWritten);

    gCounterManager.AddCounter(&gDiskIOErrors);

}


