//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: qcmutex.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2008/10/30
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

#ifndef QCMUTEX_H
#define QCMUTEX_H

#include <pthread.h>
#include <errno.h>
#include <stdint.h>

class QCMutex
{
public:
    typedef int64_t Time;

    QCMutex();
    ~QCMutex();
    bool Lock()
        { return Locked(pthread_mutex_lock(&mMutex)); }
    bool Lock(
        Time inTimeoutNanoSec);
    bool TryLock()
    {
        const int theErr = pthread_mutex_trylock(&mMutex);
        return (theErr != EBUSY && Locked(theErr));
    }
    bool Unlock()
    {
        const bool theUnlockedFlag = Unlocked();
        const int theErr = pthread_mutex_unlock(&mMutex);
        if (theErr) {
            RaiseError("QCMutex::Unlock", theErr);
        }
        return theUnlockedFlag;
    }
    bool IsOwned()
        { return (::pthread_equal(mOwner, ::pthread_self()) != 0); }

private:
    int             mLockCnt;
    pthread_t       mOwner;
    pthread_mutex_t mMutex;

    void RaiseError(
        const char* inMsgPtr,
        int         inSysError = 0);
    bool Locked(
        int inErr)
    {
        if (inErr) {
            RaiseError("QCMutex::Locked", inErr);
        }
        if (mLockCnt < 0) {
            RaiseError("QCMutex::Locked mLockCnt < 0");
        }
        if (mLockCnt++ == 0) {
            mOwner = ::pthread_self();
        }
        return true;
    }
    bool Unlocked()
    {
        if (mLockCnt <= 0) {
            RaiseError("QCMutex::Unlocked mLockCnt <= 0");
        }
        const bool theUnlockedFlag = --mLockCnt == 0;
        if (theUnlockedFlag) {
            mOwner = pthread_t();
        }
        return theUnlockedFlag;
    }
    friend class QCCondVar;

private:
    // No copies.
    QCMutex(
        const QCMutex& inMutex);
    QCMutex& operator=(
        const QCMutex& inMutex);
};

class QCCondVar
{
public:
    typedef QCMutex::Time Time;

    QCCondVar();
    ~QCCondVar();
    bool Wait(
        QCMutex& inMutex)
    {
        inMutex.Unlocked();
        const int theErr = pthread_cond_wait(&mCond, &inMutex.mMutex);
        if (theErr) {
            RaiseError("QCCondVar::Wait", theErr);
        }
        return inMutex.Locked(theErr);
    }
    bool Wait(
        QCMutex& inMutex,
        Time     inTimeoutNanoSec);
    void Notify()
    {
        const int theErr = pthread_cond_signal(&mCond);
        if (theErr) {
            RaiseError("QCCondVar::Notify", theErr);
        }
    }
    void NotifyAll()
    {
        const int theErr = pthread_cond_broadcast(&mCond);
        if (theErr) {
            RaiseError("QCCondVar::Notify", theErr);
        }
    }

private:
    pthread_cond_t mCond;

    void RaiseError(
        const char* inMsgPtr,
        int         inSysError = 0);

private:
    // No copies.
    QCCondVar(
        const QCCondVar& inCondVar);
    QCCondVar& operator=(
        const QCCondVar& inCondVar);
};

#endif /* QCMUTEX_H */

