//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: KfsPendingOp.h 1552 2011-01-06 22:21:54Z sriramr $
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

#ifndef KFS_PENDING_OP_H
#define KFS_PENDING_OP_H

#include "common/kfstypes.h"
#include "qcdio/qcthread.h"
#include "qcdio/qcmutex.h"

namespace KFS {

class KfsClientImpl;

class KfsPendingOp : public QCRunnable
{
public:
    KfsPendingOp(
        KfsClientImpl& inImpl);
    ~KfsPendingOp();
    bool Start(
        int  inFd,
        bool inReadFlag);
    bool IsPending() const
        { return (mFd >= 0); }
    void Stop();
    virtual void Run();

private:
    KfsClientImpl& mImpl;
    volatile int   mFd;
    bool           mReadFlag;
    bool           mStopFlag;
    ssize_t        mResult;
    QCThread       mThread;
    QCMutex        mMutex;
    QCCondVar      mWakeUpCond;
};

} /* namespace KFS */

#endif /* KFS_PENDING_OP_H */
