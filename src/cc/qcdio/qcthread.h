//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: qcthread.h 1552 2011-01-06 22:21:54Z sriramr $
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

#ifndef QCTHREAD_H
#define QCTHREAD_H

#include <pthread.h>
#include <string>

class QCRunnable
{
public:
    virtual void Run() = 0;

protected:
    QCRunnable()
        {}
    virtual ~QCRunnable()
        {}
};

class QCThread : public QCRunnable
{
public:
    QCThread(
        QCRunnable* inRunnablePtr = 0,
        const char* inNamePtr     = 0);
    virtual ~QCThread();
    void Start(
        QCRunnable* inRunnablePtr = 0,
        int         inStackSize   = -1,
        const char* inNamePtr     = 0)
    {
        const int theErr = TryToStart(inRunnablePtr, inStackSize, inNamePtr);
        if (theErr) {
            FatalError("TryToStart", theErr);
        }
    }
    int TryToStart(
        QCRunnable* inRunnablePtr = 0,
        int         inStackSize   = -1,
        const char* inNamePtr     = 0);
    void Join();
    virtual void Run()
        {}
    bool IsStarted() const
        { return mStartedFlag; }
    std::string GetName() const
        { return mName; }
    static std::string GetErrorMsg(
        int inErrorCode);
    static int GetThreadCount(); 

private:
    bool        mStartedFlag;
    pthread_t   mThread;
    QCRunnable* mRunnablePtr;
    std::string mName;
    QCThread*   mPrevPtr[1];
    QCThread*   mNextPtr[1];
    template<typename, unsigned int> friend class QCDLListOp;

    static void* Runner(
        void* inArgPtr);
    void FatalError(
        const char* inErrMsgPtr,
        int         inSysError);
    void Insert(
        QCThread& inAfter);
    void Remove();
private:
    // No copies.
    QCThread(
        const QCThread& inThread);
    QCThread& operator=(
        const QCThread& inThread);
};

#endif /* QCTHREAD_H */
