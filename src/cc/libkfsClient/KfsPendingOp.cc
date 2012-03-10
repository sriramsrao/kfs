//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: KfsPendingOp.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2008/12/28
//
// Copyright 2008,2009 Quantcast Corp.
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

#include "KfsPendingOp.h"
#include "KfsClientInt.h"
#include "qcdio/qcstutils.h"
#include "qcdio/qcdebug.h"

using namespace KFS;

KfsPendingOp::KfsPendingOp(
    KfsClientImpl& inImpl)
    : QCRunnable(),
      mImpl(inImpl),
      mFd(-1),
      mReadFlag(false),
      mStopFlag(false),
      mResult(0),
      mThread(),
      mMutex(),
      mWakeUpCond()
{
    const int kStackSize = 128 << 10;
    mThread.Start(this, kStackSize);
}

KfsPendingOp::~KfsPendingOp()
{
    KfsPendingOp::Stop();
}

    void
KfsPendingOp::Stop()
{
    QCStMutexLocker theLock(mMutex);
    mStopFlag = true;
    mWakeUpCond.Notify();
    theLock.Unlock();
    mThread.Join();
}

    bool
KfsPendingOp::Start(
    int  inFd,
    bool inReadFlag)
{
    if (inFd < 0) {
        return false;
    }
    QCStMutexLocker theLock(mMutex);
    mFd       = inFd;
    mReadFlag = inReadFlag;
    mWakeUpCond.Notify();
    return true;
}

    /* virtual */ void
KfsPendingOp::Run()
{
    QCStMutexLocker theLocker(mMutex);
    while (! mStopFlag) {
        mWakeUpCond.Wait(mMutex);
        if (mFd < 0) {
            continue;
        }
        const int  theFd       = mFd;
        const bool theReadFlag = mReadFlag;
        ssize_t    theResult;
        mFd = -1;
        {
            QCStMutexUnlocker theUnlocker(mMutex);
            if (theReadFlag) {
                MutexLock theLock(&mImpl.GetMutex());
                const off_t thePos = mImpl.GetIoBufferSize(theFd) <= 0 ? -1 :
                    mImpl.Tell(theFd);
                char theByte;
                theResult = thePos < 0 ? -1 : mImpl.Read(theFd, &theByte, 1);
                if (theResult > 0) {
                    mImpl.Seek(theFd, thePos);
                }
            } else {
                // If sync fails the buffer remains dirty.
                const bool kFlushOnlyIfHasFullChecksumBlock = true;
                theResult = mImpl.Sync(theFd, kFlushOnlyIfHasFullChecksumBlock);
            }
        }
        mResult = theResult;
    }
}
