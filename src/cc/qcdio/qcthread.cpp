//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: qcthread.cpp 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2008/11/01
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

#include "qcthread.h"
#include "qcmutex.h"
#include "qcstutils.h"
#include "qcutils.h"
#include "qcdllist.h"
#include "qcdebug.h"

class QCStartedThreadList
{
public:
    typedef QCDLListOp<QCThread, 0> ThreadList;

    QCStartedThreadList()
        : mMutex(),
          mHead(0, "QCThread list head"),
          mMainThread(pthread_self()),
          mCount(0)
        {}
    ~QCStartedThreadList()
        { QCASSERT(mCount == 0); }
    void Insert(
        QCThread& inThread)
    {
        QCStMutexLocker theLock(mMutex);
        ThreadList::Insert(inThread, ThreadList::GetPrev(mHead));
        mCount++;
    }
    void Remove(
        QCThread& inThread)
    {
        QCStMutexLocker theLock(mMutex);
        ThreadList::Remove(inThread);
        mCount--;
    }
    int GetThreadCount()
    {
        QCStMutexLocker theLock(mMutex);
        return mCount;
    }

private:
    QCMutex   mMutex;
    QCThread  mHead;
    pthread_t mMainThread;
    int       mCount;
};
static QCStartedThreadList sThreadList;


QCThread::QCThread(
    QCRunnable* inRunnablePtr /* = 0 */,
    const char* inNamePtr     /* = 0 */)
    : QCRunnable(),
      mStartedFlag(false),
      mThread(),
      mRunnablePtr(inRunnablePtr),
      mName(inNamePtr ? inNamePtr : "")
{
    QCStartedThreadList::ThreadList::Init(*this);
}

    /* virtual */QCThread::
QCThread::~QCThread()
{
    QCThread::Join();
}

    int
QCThread::TryToStart(
    QCRunnable* inRunnablePtr /* = 0 */,
    int         inStackSize   /* = -1 */,
    const char* inNamePtr     /* = 0 */)
{
    if (mStartedFlag) {
        return EINVAL;
    }
    pthread_attr_t theStackSizeAttr;
    int theErr = pthread_attr_init(&theStackSizeAttr);
    if (theErr != 0) {
        return theErr;
    }
    if (inStackSize > 0 && (theErr = pthread_attr_setstacksize(
            &theStackSizeAttr, inStackSize)) != 0) {
        pthread_attr_destroy(&theStackSizeAttr);
        return theErr;
    }
    if (inNamePtr) {
        mName = inNamePtr;
    }
    if (inRunnablePtr) {
        mRunnablePtr = inRunnablePtr;
    }
    if (! mRunnablePtr) {
        mRunnablePtr = this;
    }
    mStartedFlag = true;
    theErr = pthread_create(
        &mThread, &theStackSizeAttr, &Runner, mRunnablePtr);
    pthread_attr_destroy(&theStackSizeAttr);
    if (theErr != 0) {
        mStartedFlag = false;
        return theErr;
    }
    sThreadList.Insert(*this);
    return theErr;
}

   void
QCThread::Join()
{
    if (! mStartedFlag) {
        return;
    }
    const int theErr = pthread_join(mThread, 0);
    if (theErr) {
        FatalError("pthread_join", theErr);
    }
    mStartedFlag = false;
    sThreadList.Remove(*this);
}

    void
QCThread::FatalError(
    const char* inErrMsgPtr,
    int         inSysError)
{
    QCUtils::FatalError(inErrMsgPtr, inSysError);
}

    /* static */ void*
QCThread::Runner(
    void* inArgPtr)
{
    reinterpret_cast<QCRunnable*>(inArgPtr)->Run();
    return 0;
}

    /* static */ std::string
QCThread::GetErrorMsg(
    int inErrorCode)
{
    return QCUtils::SysError(inErrorCode);
}

    /* static */ int
QCThread::GetThreadCount()
{
    return sThreadList.GetThreadCount();
}
