//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: NetManager.cc 3106 2011-09-22 00:14:11Z sriramr $
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

#include <cerrno>
#include <limits>
#include <unistd.h>
#include <fcntl.h>
#include <netdb.h>
#include <arpa/inet.h>

#include "NetManager.h"
#include "TcpSocket.h"

#include "common/log.h"
#include "qcdio/qcfdpoll.h"
#include "qcdio/qcutils.h"
#include "qcdio/qcmutex.h"
#include "qcdio/qcstutils.h"

using std::list;
using std::min;
using std::max;
using std::numeric_limits;
using std::string;
using namespace KFS;
using namespace KFS::libkfsio;

class NetManager::Waker
{
public:
    Waker()
        : mMutex(),
          mWritten(0),
          mSleepingFlag(false),
          mWakeFlag(false)
    {
        const int res = pipe(mPipeFds);
        if (res < 0) {
            perror("Pipe: ");
            mPipeFds[0] = -1;
            mPipeFds[1] = -1;
            abort();
            return;
        }
        fcntl(mPipeFds[0], F_SETFL, O_NONBLOCK);
        fcntl(mPipeFds[1], F_SETFL, O_NONBLOCK);
    }
    ~Waker()
    {
        for (int i = 0; i < 2; i++) {
            if (mPipeFds[i] >= 0) {
                close(mPipeFds[i]);
            }
        }
    }
    bool Sleep()
    {
        QCStMutexLocker lock(mMutex);
        mSleepingFlag = ! mWakeFlag;
        mWakeFlag = false;
        return mSleepingFlag;
    }
    int Wake()
    {
        QCStMutexLocker lock(mMutex);
        mSleepingFlag = false;
        while (mWritten > 0) {
            char buf[64];
            const int res = read(mPipeFds[0], buf, sizeof(buf));
            if (res > 0) {
                mWritten -= min(mWritten, res);
            } else {
                break;
            }
        }
        return (mWritten);
    }
    void Wakeup()
    {
        QCStMutexLocker lock(mMutex);
        mWakeFlag = true;
        if (mSleepingFlag && mWritten <= 0) {
            mWritten++;
            const char buf = 'k';
            write(mPipeFds[1], &buf, sizeof(buf));
        }
    }
    int GetFd() const { return mPipeFds[0]; }
private:
    QCMutex mMutex;
    int     mWritten;
    int     mPipeFds[2];
    bool    mSleepingFlag;
    bool    mWakeFlag;

private:
   Waker(const Waker&);
   Waker& operator=(const Waker&); 
};

NetManager::NetManager(int timeoutMs)
    : mRemove(),
      mTimerWheelBucketItr(mRemove.end()),
      mCurConnection(0),
      mCurTimerWheelSlot(0),
      mConnectionsCount(0),
      mDiskOverloaded(false),
      mNetworkOverloaded(false),
      mIsOverloaded(false),
      mRunFlag(true),
      mShutdownFlag(false),
      mTimerRunningFlag(false),
      mIsForkedChild(false),
      mTimeoutMs(timeoutMs),
      mStartTime(time(0)),
      mNow(mStartTime),
      mMaxOutgoingBacklog(0),
      mNumBytesToSend(0),
      mTimerOverrunCount(0),
      mTimerOverrunSec(0),
      mPoll(*(new QCFdPoll())),
      mWaker(*(new Waker())),
      mPollEventHook(0)
{}

NetManager::~NetManager()
{
    NetManager::CleanUp();
    delete &mPoll;
    delete &mWaker;
}

void
NetManager::AddConnection(NetConnectionPtr &conn)
{
    if (mShutdownFlag) {
        return;
    }
    NetConnection::NetManagerEntry* const entry =
        conn->GetNetManagerEntry();
    if (! entry) {
        return;
    }
    if (entry->mNetManager && entry->mNetManager != this) {
        KFS_LOG_STREAM_FATAL <<
            "attempt to add connection to different net manager" <<
        KFS_LOG_EOM;
        abort();
    }
    if (! entry->mAdded) {
        entry->mTimerWheelSlot = kTimerWheelSize;
        entry->mListIt = mTimerWheel[kTimerWheelSize].insert(
            mTimerWheel[kTimerWheelSize].end(), conn);
        mConnectionsCount++;
        assert(mConnectionsCount > 0);
        entry->mAdded = true;
        entry->mNetManager = this;
        if (mPollEventHook) {
            mPollEventHook->Add(*this, *conn);
        }
    }
    conn->Update();
}

void
NetManager::RegisterTimeoutHandler(ITimeout *handler)
{
    list<ITimeout *>::iterator iter;
    for (iter = mTimeoutHandlers.begin(); iter != mTimeoutHandlers.end(); 
         ++iter) {
        if (*iter == handler) {
            return;
        }
    }
    mTimeoutHandlers.push_back(handler);
}

void
NetManager::UnRegisterTimeoutHandler(ITimeout *handler)
{
    if (handler == NULL)
        return;

    list<ITimeout *>::iterator iter;
    for (iter = mTimeoutHandlers.begin(); iter != mTimeoutHandlers.end(); 
            ++iter) {
        if (*iter == handler) {
            // Not not remove list element: this can be called when iterating
            // trough the list.
            *iter = 0;
            return;
        }
    }
}

inline void
NetManager::UpdateTimer(NetConnection::NetManagerEntry& entry, int timeOut)
{
    assert(entry.mAdded);

    int timerWheelSlot;
    if (timeOut < 0) {
        timerWheelSlot = kTimerWheelSize;
    } else if ((timerWheelSlot = mCurTimerWheelSlot +
            // When the timer is running the effective wheel size "grows" by 1:
            // leave (move) entries with timeouts >= kTimerWheelSize in (to) the
            // current slot.
            min((kTimerWheelSize - (mTimerRunningFlag ? 0 : 1)), timeOut)) >=
            kTimerWheelSize) {
        timerWheelSlot -= kTimerWheelSize;
    }
    // This method can be invoked from timeout handler.
    // Make sure  that the entry doesn't get moved to the end of the current
    // list, which can be traversed by the timer.
    if (timerWheelSlot != entry.mTimerWheelSlot) {
        if (mTimerWheelBucketItr == entry.mListIt) {
            ++mTimerWheelBucketItr;
        }
        mTimerWheel[timerWheelSlot].splice(
            mTimerWheel[timerWheelSlot].end(),
            mTimerWheel[entry.mTimerWheelSlot], entry.mListIt);
        entry.mTimerWheelSlot = timerWheelSlot;
    }
}

inline static int CheckFatalSysError(int err, const char* msg)
{
    if (err) {
        KFS_LOG_STREAM_FATAL << QCUtils::SysError(err, msg) << KFS_LOG_EOM;
        abort();
    }
    return err;
}

void
NetManager::Update(NetConnection::NetManagerEntry& entry, int fd, bool resetTimer)
{
    if (entry.mNetManager) {
        entry.mNetManager->UpdateSelf(entry, fd, resetTimer);
    }
}

void
NetManager::UpdateSelf(NetConnection::NetManagerEntry& entry, int fd, bool resetTimer)
{
    if ((! entry.mAdded) || mIsForkedChild) {
        return;
    }
    assert(*entry.mListIt);
    NetConnection& conn = **entry.mListIt;
    assert(fd >= 0 || ! conn.IsGood());
    // Always check if connection has to be removed: this method always
    // called before socket fd gets closed.
    if (! conn.IsGood() || fd < 0) {
        if (entry.mFd >= 0) {
            CheckFatalSysError(
                mPoll.Remove(entry.mFd),
                "failed to removed fd from poll set"
            );
            entry.mFd = -1;
        }
        assert(mConnectionsCount > 0 &&
            entry.mWriteByteCount >= 0 &&
            entry.mWriteByteCount <= mNumBytesToSend);
        entry.mAdded = false;
        mConnectionsCount--;
        mNumBytesToSend -= entry.mWriteByteCount;
        if (mTimerWheelBucketItr == entry.mListIt) {
            ++mTimerWheelBucketItr;
        }
        mRemove.splice(mRemove.end(),
            mTimerWheel[entry.mTimerWheelSlot], entry.mListIt);
        // Do not reset entry->mNetManager, it is an error to add connection to
        // a different net manager even after close.
        if (mPollEventHook) {
            mPollEventHook->Remove(*this, **entry.mListIt);
        }
        return;
    }
    if (&conn == mCurConnection) {
        // Defer all updates for the currently dispatched connection until the
        // end of the event dispatch loop.
        return;
    }
    // Update timer.
    if (resetTimer) {
        const int timeOut = conn.GetInactivityTimeout();
        if (timeOut >= 0) {
            entry.mExpirationTime = mNow + timeOut;
        }
        UpdateTimer(entry, timeOut);
    }
    // Update pending send.
    assert(entry.mWriteByteCount >= 0 &&
        entry.mWriteByteCount <= mNumBytesToSend);
    mNumBytesToSend -= entry.mWriteByteCount;
    entry.mWriteByteCount = max(0, conn.GetNumBytesToWrite());
    mNumBytesToSend += entry.mWriteByteCount;
    // Update poll set.
    const bool in  = conn.IsReadReady() &&
        (! mIsOverloaded || entry.mEnableReadIfOverloaded);
    const bool out = conn.IsWriteReady() || entry.mConnectPending;
    if (in != entry.mIn || out != entry.mOut) {
        assert(fd >= 0);
        const int op =
            (in ? QCFdPoll::kOpTypeIn : 0) + (out ? QCFdPoll::kOpTypeOut : 0);
        if ((fd != entry.mFd || op == 0) && entry.mFd >= 0) {
            CheckFatalSysError(
                mPoll.Remove(entry.mFd),
                "failed to removed fd from poll set"
            );
            entry.mFd = -1;
        }
        if (entry.mFd < 0) {
            if (op && CheckFatalSysError(
                    mPoll.Add(fd, op, &conn),
                    "failed to add fd to poll set") == 0) {
                entry.mFd = fd;
            }
        } else {
            CheckFatalSysError(
                mPoll.Set(fd, op, &conn),
                "failed to change pool flags"
            );
        }
        entry.mIn  = in  && entry.mFd >= 0;
        entry.mOut = out && entry.mFd >= 0;
    }
}

void
NetManager::Wakeup()
{
    mWaker.Wakeup();
}

void
NetManager::MainLoop()
{
    mNow = time(0);
    time_t lastTimerTime = mNow;
    CheckFatalSysError(
        mPoll.Add(mWaker.GetFd(), QCFdPoll::kOpTypeIn),
        "failed to add net waker's fd to the poll set"
    );
    const int timerOverrunWarningTime(mTimeoutMs / (1000/2));
    while (mRunFlag) {
        const bool wasOverloaded = mIsOverloaded;
        CheckIfOverloaded();
        if (mIsOverloaded != wasOverloaded) {
            KFS_LOG_STREAM_INFO <<
                (mIsOverloaded ?
                    "System is now in overloaded state" :
                    "Clearing system overload state") <<
                " " << mNumBytesToSend << " bytes to send" <<
            KFS_LOG_EOM;
            // Turn on read only if returning from overloaded state.
            // Turn off read in the event processing loop if overloaded, and
            // read event is pending. 
            // The "lazy" processing here is to reduce number of system calls.
            if (! mIsOverloaded) {
                for (int i = 0; i <= kTimerWheelSize; i++) {
                    for (List::iterator c = mTimerWheel[i].begin();
                            c != mTimerWheel[i].end(); ) {
                        assert(*c);
                        NetConnection& conn = **c;
                        ++c;
                        conn.Update(false);
                    }
                }
            }
        }
        const int ret = mPoll.Poll(
            mConnectionsCount + 1,
            mWaker.Sleep() ? mTimeoutMs : 0
        );
        mWaker.Wake();
        if (ret < 0 && ret != -EINTR && ret != -EAGAIN) {
            KFS_LOG_STREAM_ERROR <<
                QCUtils::SysError(-ret, "poll error") <<
            KFS_LOG_EOM;
        }
        const int64_t nowMs = ITimeout::NowMs();
        mNow = time_t(nowMs / 1000);
        // Unregister will set pointer to 0, but will never remove the list
        // node, so that the iterator always remains valid.
        for (list<ITimeout *>::iterator it = mTimeoutHandlers.begin();
                it != mTimeoutHandlers.end(); ) {
            if (*it) {
                (*it)->TimerExpired(nowMs);
            }
            if (*it) {
                ++it;
            } else {
                it = mTimeoutHandlers.erase(it);
            }
        }
        /// Process poll events.
        int   op;
        void* ptr;
        while (mPoll.Next(op, ptr)) {
            if (op == 0 || ! ptr) {
                continue;
            }
            NetConnection& conn = *reinterpret_cast<NetConnection*>(ptr);
            if (! conn.GetNetManagerEntry()->mAdded) {
                // Skip stale event, the conection should be in mRemove list.
                continue;
            }
            // Defer update for this connection.
            mCurConnection = &conn;
            if (mPollEventHook) {
                mPollEventHook->Event(*this, conn, op);
            }
            if ((op & (QCFdPoll::kOpTypeIn | QCFdPoll::kOpTypeHup)) != 0 &&
                    conn.IsGood() && (! mIsOverloaded ||
                    conn.GetNetManagerEntry()->mEnableReadIfOverloaded)) {
                conn.HandleReadEvent();
            }
            if ((op & QCFdPoll::kOpTypeOut) != 0 && conn.IsGood()) {
                conn.HandleWriteEvent();
            }
            if ((op & QCFdPoll::kOpTypeError) != 0 && conn.IsGood()) {
                conn.HandleErrorEvent();
            }
            // Try to write, if the last write was sucessfull.
            conn.StartFlush();
            // Update the connection.
            mCurConnection = 0;
            conn.Update();
        }
        mRemove.clear();
        mNow = time(0);
        int slotCnt = min(int(kTimerWheelSize), int(mNow - lastTimerTime));
        if (lastTimerTime + timerOverrunWarningTime < mNow) {
            KFS_LOG_STREAM_INFO <<
                "timer overrun " << (mNow - lastTimerTime) <<
                " seconds detected" <<
            KFS_LOG_EOM;
            mTimerOverrunCount++;
            mTimerOverrunSec += mNow - lastTimerTime;
        }
        mTimerRunningFlag = true;
        while (slotCnt-- > 0) {
            List& bucket = mTimerWheel[mCurTimerWheelSlot];
            mTimerWheelBucketItr = bucket.begin();
            while (mTimerWheelBucketItr != bucket.end()) {
                assert(*mTimerWheelBucketItr);
                NetConnection& conn = **mTimerWheelBucketItr;
                assert(conn.IsGood());
                ++mTimerWheelBucketItr;
                NetConnection::NetManagerEntry& entry =
                    *conn.GetNetManagerEntry();
                const int timeOut = conn.GetInactivityTimeout();
                if (timeOut < 0) {
                    // No timeout, move it to the corresponding list.
                    UpdateTimer(entry, timeOut);
                } else if (entry.mExpirationTime <= mNow) {
                    conn.HandleTimeoutEvent();
                } else {
                    // Not expired yet, move to the new slot, taking into the
                    // account possible timer overrun.
                    UpdateTimer(entry,
                        slotCnt + int(entry.mExpirationTime - mNow));
                }
            }
            if (++mCurTimerWheelSlot >= kTimerWheelSize) {
                mCurTimerWheelSlot = 0;
            }
            mRemove.clear();
        }
        mTimerRunningFlag = false;
        lastTimerTime = mNow;
        mTimerWheelBucketItr = mRemove.end();
    }
    CheckFatalSysError(
        mPoll.Remove(mWaker.GetFd()),
        "failed to removed net kicker's fd from poll set"
    );
    CleanUp();
}

void
NetManager::CheckIfOverloaded()
{
    if (mMaxOutgoingBacklog > 0) {
        if (!mNetworkOverloaded) {
            mNetworkOverloaded = (mNumBytesToSend > mMaxOutgoingBacklog);
        } else if (mNumBytesToSend <= mMaxOutgoingBacklog / 2) {
            // network was overloaded and that has now cleared
            mNetworkOverloaded = false;
        }
    }
    mIsOverloaded = mDiskOverloaded || mNetworkOverloaded;
}

void
NetManager::ChangeDiskOverloadState(bool v)
{
    if (mDiskOverloaded == v)
        return;
    mDiskOverloaded = v;
}

void
NetManager::CleanUp()
{
    mShutdownFlag = true;
    mTimeoutHandlers.clear();
    for (int i = 0; i <= kTimerWheelSize; i++) {
        for (mTimerWheelBucketItr = mTimerWheel[i].begin();
                mTimerWheelBucketItr != mTimerWheel[i].end(); ) {
            NetConnection* const conn = mTimerWheelBucketItr->get();
            ++mTimerWheelBucketItr;
            if (conn) {
                if (conn->IsGood()) {
                    conn->HandleErrorEvent();
                }
            }
        }
        assert(mTimerWheel[i].empty());
        mRemove.clear();
    }
    mTimerWheelBucketItr = mRemove.end();
}

inline const NetManager*
NetManager::GetNetManager(const NetConnection& conn)
{
    return conn.GetNetManagerEntry()->mNetManager;
}

inline time_t
NetManager::Timer::Handler::Now() const
{
    return GetNetManager(*mConn)->Now();
}

NetManager::Timer::Handler::Handler(NetManager& netManager, KfsCallbackObj& obj, int tmSec)
    : KfsCallbackObj(),
      mObj(obj),
      mStartTime(tmSec >= 0 ? netManager.Now() : 0),
      mSock(numeric_limits<int>::max()), // Fake fd, for IsGood()
      mConn(new NetConnection(&mSock, this, false, false))
{
    SET_HANDLER(this, &Handler::EventHandler);
    mConn->SetMaxReadAhead(0); // Do not add this to poll.
    mConn->SetInactivityTimeout(tmSec);
    netManager.AddConnection(mConn);
}

void
NetManager::Timer::Handler::SetTimeout(int tmSec)
{
    const int prevTm = mConn->GetInactivityTimeout();
    mStartTime = Now();
    if (prevTm != tmSec) {
        mConn->SetInactivityTimeout(tmSec); // Reset timer.
    } else {
        mConn->Update(); // Reset timer.
    }
}

time_t
NetManager::Timer::Handler::GetRemainingTime() const
{
    const int tmSec = mConn->GetInactivityTimeout();
    if (tmSec < 0) {
        return tmSec;
    }
    const time_t next = mStartTime + tmSec;
    const time_t now  = Now();
    return (next > now ? next - now : 0);
}

int
NetManager::Timer::Handler::EventHandler(int type, void* /* data */)
{
    switch (type) {
        case EVENT_NET_ERROR: // Invoked from net manager cleanup code.
            Cleanup();
            // Fall through
        case EVENT_INACTIVITY_TIMEOUT:
            mStartTime = Now();
            return mObj.HandleEvent(EVENT_INACTIVITY_TIMEOUT, 0);
        default:
            assert(! "unexpected event type");
    }
    return 0;
}

void
NetManager::Timer::Handler::Cleanup()
{
    mConn->Close();
    // Reset fd to prevent calling close().
    mSock = TcpSocket();
}

void
NetManager::Timer::Handler::ResetTimeout()
{
    if (mConn->GetInactivityTimeout() >= 0) {
        mStartTime = Now();
        mConn->Update();
    }
}

void
NetManager::Timer::Handler::ScheduleTimeoutNoLaterThanIn(int tmSec)
{
    if (tmSec < 0) {
        return;
    }
    const int    curTimeout = mConn->GetInactivityTimeout();
    const time_t now        = Now();
    if (curTimeout < 0 || now + tmSec < mStartTime + curTimeout) {
        mStartTime = now;
        if (curTimeout != tmSec) {
            mConn->SetInactivityTimeout(tmSec);
        } else {
            mConn->Update();
        }
    }
}

string
NetManager::GetMyIP() 
{
    char myHost[256];
    struct sockaddr_in myAddr = { 0 };

    gethostname(myHost, 256);

    if (! inet_aton(myHost, &myAddr.sin_addr)) {
        // do the conversion if we weren't handed an IP address
        struct hostent * const hostInfo = gethostbyname(myHost);
        if (hostInfo == NULL || hostInfo->h_addrtype != AF_INET ||
                hostInfo->h_length < (int)sizeof(myAddr.sin_addr)) {
            KFS_LOG_STREAM_ERROR <<
                " hostent: " << (const void*)hostInfo <<
                " type: "    << (hostInfo ? hostInfo->h_addrtype : -1) <<
                " size: "    << (hostInfo ? hostInfo->h_length   : -1) <<
#if defined __APPLE__
                "herrno: " << h_errno <<
                ", errstr = " << hstrerror(h_errno) <<
#endif
            KFS_LOG_EOM;
            return "";
        }
        memcpy(&myAddr.sin_addr, hostInfo->h_addr, sizeof(myAddr.sin_addr));
    }

    char ipname[INET_ADDRSTRLEN + 7];

    if (inet_ntop(AF_INET, &(myAddr.sin_addr), ipname, INET_ADDRSTRLEN) == NULL)
        return "unknown";
    ipname[INET_ADDRSTRLEN] = 0;
    return ipname;
}

// Return IP address /26
string
NetManager::GetMyS26Address() 
{
    char myHost[256];
    struct sockaddr_in myAddr = { 0 };

    gethostname(myHost, 256);

    if (! inet_aton(myHost, &myAddr.sin_addr)) {
        // do the conversion if we weren't handed an IP address
        struct hostent * const hostInfo = gethostbyname(myHost);
        if (hostInfo == NULL || hostInfo->h_addrtype != AF_INET ||
                hostInfo->h_length < (int)sizeof(myAddr.sin_addr)) {
            KFS_LOG_STREAM_ERROR <<
                " hostent: " << (const void*)hostInfo <<
                " type: "    << (hostInfo ? hostInfo->h_addrtype : -1) <<
                " size: "    << (hostInfo ? hostInfo->h_length   : -1) <<
#if defined __APPLE__
                "herrno: " << h_errno <<
                ", errstr = " << hstrerror(h_errno) <<
#endif
            KFS_LOG_EOM;
            return "";
        }
        memcpy(&myAddr.sin_addr, hostInfo->h_addr, sizeof(myAddr.sin_addr));
    }

    const uint32_t mask = htonl(0xffffffc0);  // a /26 address
    myAddr.sin_addr.s_addr &= mask;
    char ipname[INET_ADDRSTRLEN + 7];

    if (inet_ntop(AF_INET, &(myAddr.sin_addr), ipname, INET_ADDRSTRLEN) == NULL)
        return "unknown";
    ipname[INET_ADDRSTRLEN] = 0;
    return ipname;
}
