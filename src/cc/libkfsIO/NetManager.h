//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: NetManager.h 3106 2011-09-22 00:14:11Z sriramr $
//
// Created 2006/03/14
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

#ifndef _LIBIO_NETMANAGER_H
#define _LIBIO_NETMANAGER_H

#include <sys/time.h>

#include "ITimeout.h"
#include "NetConnection.h"

class QCFdPoll;
namespace KFS
{

///
/// \file NetManager.h
/// The net manager provides facilities for multiplexing I/O on network
/// connections.  It keeps a list of connections on which it has to
/// call select.  Whenever an "event" occurs on a connection (viz.,
/// read/write/error), it calls back the connection to handle the
/// event.  
/// 
/// The net manager also provides support for timeout notification.
/// Whenever a call to select returns, that is an occurence of a
/// timeout.  Interested handlers can register with the net manager to
/// be notified of timeout.  In the current implementation, the
/// timeout interval is mSelectTimeout.
//

class NetManager {
public:
    NetManager(int timeoutMs = 1000);
    ~NetManager();
    /// Add a connection to the net manager's list of connections that
    /// are used for building poll vector.
    /// @param[in] conn The connection that should be added.
    void AddConnection(NetConnectionPtr &conn);
    void RegisterTimeoutHandler(ITimeout *handler);
    void UnRegisterTimeoutHandler(ITimeout *handler);

    void SetForkedChild()
        {  mIsForkedChild = true; }
    /// This API can be used to limit the backlog of outgoing data.
    /// Whenever the backlog exceeds the threshold, poll vector bits
    /// are turned off for incoming traffic.
    void SetBacklogLimit(int64_t v)
        { mMaxOutgoingBacklog = v; }
    void ChangeDiskOverloadState(bool v);

    // Return the IP address of the machine we are on
    static std::string GetMyIP();
    // Return the IP address/26
    static std::string GetMyS26Address();

    ///
    /// This function never returns.  It builds a poll vector, calls
    /// select(), and then evaluates the result of select():  for
    /// connections on which data is I/O is possible---either for
    /// reading or writing are called back.  In the callback, the
    /// connections should take appropriate action.  
    ///
    /// NOTE: When a connection is closed (such as, via a call to
    /// NetConnection::Close()), then it automatically falls out of
    /// the net manager's list of connections that are polled.
    ///  
    void MainLoop();
    void Wakeup();

    void Shutdown()
        { mRunFlag = false; }
    time_t GetStartTime() const
        { return mStartTime; }
    time_t Now() const
        { return mNow; }
    time_t UpTime() const
        { return (mNow - mStartTime); }
    bool IsRunning() const
        { return mRunFlag; }
    int64_t GetTimerOverrunCount() const
        { return mTimerOverrunCount; }
    int64_t GetTimerOverrunSec() const
        { return mTimerOverrunSec; }

    // Primarily for debugging, to simulate network failures.
    class PollEventHook
    {
    public:
        virtual void Add(NetManager& netMgr, NetConnection& conn)    {}
        virtual void Remove(NetManager& netMgr, NetConnection& conn) {}
        virtual void Event(
            NetManager& netMgr, NetConnection& conn, int& pollEvent) = 0;
    protected:
        PollEventHook()  {}
        virtual ~PollEventHook() {}
    };
    PollEventHook* SetPollEventHook(PollEventHook* hook = 0)
    {
        PollEventHook* const prev = mPollEventHook;
        mPollEventHook = hook;
        return prev;
    }
    // Hack to use net manager's timer wheel, with no fd/socket.
    // Has about 100 bytes overhead.
    class Timer
    {
    public:
        Timer(NetManager& netManager, KfsCallbackObj& obj, int tmSec = -1)
            : mHandler(netManager, obj, tmSec)
            {}
        void RemoveTimeout()
            { SetTimeout(-1); }
        void SetTimeout(int tmSec)
            { mHandler.SetTimeout(tmSec); }
        void ResetTimeout()
            { mHandler.ResetTimeout(); }
        time_t GetRemainingTime() const
            { return mHandler.GetRemainingTime(); }
        time_t GetStartTime() const
            { return mHandler.mStartTime; }
        int GetTimeout() const
            { return mHandler.mConn->GetInactivityTimeout(); }
        void ScheduleTimeoutNoLaterThanIn(int tmSec)
            { mHandler.ScheduleTimeoutNoLaterThanIn(tmSec); }
        // Negative timeouts are infinite, always greater than non negative.
        static int MinTimeout(int tmL, int tmR)
            { return ((tmR < 0 || (tmL < tmR && tmL >= 0)) ? tmL : tmR); }

    private:
        struct Handler : public KfsCallbackObj
        {
            Handler(NetManager& netManager, KfsCallbackObj& obj, int tmSec);
            ~Handler()
                { Handler::Cleanup(); }
            void SetTimeout(int tmSec);
            time_t GetRemainingTime() const;
            int EventHandler(int type, void* data);
            void Cleanup();
            void ResetTimeout();
            void ScheduleTimeoutNoLaterThanIn(int tmSec);
            inline time_t Now() const;

            KfsCallbackObj&  mObj;
            time_t           mStartTime;
            TcpSocket        mSock;
            NetConnectionPtr mConn;
        private:
            Handler(const Handler&);
            Handler& operator=(const Handler&);
        };
        Handler mHandler;
    private:
        Timer(const Timer&);
        Timer& operator=(const Timer&);
    };

    /// Method used by NetConnection only.
    static void Update(NetConnection::NetManagerEntry& entry, int fd, bool resetTimer);
    static inline const NetManager* GetNetManager(const NetConnection& conn);
private:
    class Waker;
    typedef NetConnection::NetManagerEntry::List List;
    enum { kTimerWheelSize = (1 << 8) };

    /// Timer wheel.
    List                mTimerWheel[kTimerWheelSize + 1];
    List                mRemove;
    List::iterator      mTimerWheelBucketItr;
    NetConnection*      mCurConnection;
    int                 mCurTimerWheelSlot;
    int                 mConnectionsCount;
    /// when the system is overloaded--either because of disk or we
    /// have too much network I/O backlogged---we avoid polling fd's for
    /// read.  this causes back-pressure and forces the clients to
    /// slow down
    bool		mDiskOverloaded;
    bool		mNetworkOverloaded;
    bool                mIsOverloaded;
    volatile bool       mRunFlag;
    bool                mShutdownFlag;
    bool                mTimerRunningFlag;
    bool                mIsForkedChild;
    /// timeout interval specified in the call to select().
    const int           mTimeoutMs;
    const time_t        mStartTime;
    time_t              mNow;
    int64_t		mMaxOutgoingBacklog;
    int64_t             mNumBytesToSend;
    int64_t             mTimerOverrunCount;
    int64_t             mTimerOverrunSec;
    QCFdPoll&           mPoll;
    Waker&              mWaker;
    PollEventHook*      mPollEventHook;

    /// Handlers that are notified whenever a call to select()
    /// returns.  To the handlers, the notification is a timeout signal.
    std::list<ITimeout *>	mTimeoutHandlers;

    void CheckIfOverloaded();
    void CleanUp();
    inline void UpdateTimer(NetConnection::NetManagerEntry& entry, int timeOut);
    void UpdateSelf(NetConnection::NetManagerEntry& entry, int fd, bool resetTimer);
private:
    NetManager(const NetManager&);
    NetManager& operator=(const NetManager&);
};

}

#endif // _LIBIO_NETMANAGER_H
