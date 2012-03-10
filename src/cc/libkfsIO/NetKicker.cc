//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: NetKicker.cc 1552 2011-01-06 22:21:54Z sriramr $
//
//
// Copyright 2008 Quantcast Corp.
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
// Implementation of the net kicker object.
//----------------------------------------------------------------------------

#include "NetKicker.h"
#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>
#include <algorithm>
#include "qcdio/qcmutex.h"
#include "qcdio/qcstutils.h"

using namespace KFS;

class NetKicker::Impl
{
public:
    Impl()
        : mMutex(),
          mWritten(0)
    {
        const int res = pipe(mPipeFds);
        if (res < 0) {
            perror("Pipe: ");
            mPipeFds[0] = -1;
            mPipeFds[1] = -1;
            return;
        }
        fcntl(mPipeFds[0], F_SETFL, O_NONBLOCK);
        fcntl(mPipeFds[1], F_SETFL, O_NONBLOCK);
    }
    void Kick()
    {
        QCStMutexLocker lock(mMutex);
        if (mWritten <= 0) {
            mWritten++;
            char buf = 'k';
            write(mPipeFds[1], &buf, sizeof(buf));
        }
    }
    int Drain()
    {
        QCStMutexLocker lock(mMutex);
        while (mWritten > 0) {
            char buf[64];
            const int res = read(mPipeFds[0], buf, sizeof(buf));
            if (res > 0) {
                mWritten -= std::min(mWritten, res);
            } else {
                break;
            }
        }
        return (mWritten);
    }
    int GetFd() const { return mPipeFds[0]; }
private:
    QCMutex mMutex;
    int     mWritten;
    int     mPipeFds[2];

private:
   Impl(const Impl&);
   Impl& operator=(const Impl&); 
};

NetKicker::NetKicker()
    : mImpl(*new Impl())
{}

NetKicker::~NetKicker()
{
    delete &mImpl;
}

void
NetKicker::Kick()
{
    mImpl.Kick();
}

int 
NetKicker::Drain()
{
    return mImpl.Drain();
}

int 
NetKicker::GetFd() const
{
    return mImpl.GetFd();
}
