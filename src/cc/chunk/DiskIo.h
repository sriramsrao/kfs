//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: DiskIo.h 1575 2011-01-10 22:26:35Z sriramr $
//
// Created 2009/01/17
//
// Copyright 2009 Quantcast Corp.
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

#ifndef _DISKIO_H
#define _DISKIO_H

#include <sys/types.h>

#include <boost/shared_ptr.hpp>
#include <vector>

#include "libkfsIO/IOBuffer.h"

#include "qcdio/qcdiskqueue.h"
#include "qcdio/qcdllist.h"

namespace KFS
{

class KfsCallbackObj;
class IOBuffer;
class DiskQueue;
class Properties;
class BufferManager;

///
/// Disk DiskIo encapsulates an fd and some disk IO requests.  On
/// a given disk connection, you can do either a READ or a WRITE, but not
/// both.
///
class DiskIo : private QCDiskQueue::IoCompletion
{
public:
    struct Counters
    {
        typedef int64_t Counter;

        Counter mReadCount;
        Counter mReadByteCount;
        Counter mReadErrorCount;
        Counter mWriteCount;
        Counter mWriteByteCount;
        Counter mWriteErrorCount;
        Counter mSyncCount;
        Counter mSyncErrorCount;
        void Clear()
        {
            mReadCount       = 0;
            mReadByteCount   = 0;
            mReadErrorCount  = 0;
            mWriteCount      = 0;
            mWriteByteCount  = 0;
            mWriteErrorCount = 0;
            mSyncCount       = 0;
            mSyncErrorCount  = 0;
        }
    };
    static bool Init(
        const Properties& inProperties,
        std::string*      inErrMessagePtr = 0);
    static bool StartIoQueue(
        const char*   inDirNamePtr,
        unsigned long inDeviceId,
        int           inMaxOpenFiles,
        std::string*  inErrMessagePtr = 0);
    static bool Shutdown(
        std::string* inErrMessagePtr = 0);
    static bool RunIoCompletion();
    static size_t GetMaxRequestSize();
    static int GetFdCountPerFile();
    static BufferManager& GetBufferManager();
    static void GetCounters(
        Counters& outCounters);

    class File
    {
    public:
        File()
            : mQueuePtr(0),
              mFileIdx(-1),
              mReadOnlyFlag(false),
              mSpaceReservedFlag(false)
            {}
        ~File()
        {
            if (File::IsOpen()) {
                File::Close();
            }
        }
        bool Open(
            const char*  inFileNamePtr,
            off_t        inMaxFileSize          = -1,
            bool         inReadOnlyFlag         = false,
            bool         inReserveFileSpaceFlag = false,
            bool         inCreateFlag           = false,
            std::string* inErrMessagePtr        = 0);
        bool IsOpen() const
            { return (mFileIdx >= 0); }
        bool Close(
            off_t        inFileSize      = -1,
            std::string* inErrMessagePtr = 0);
        DiskQueue* GetDiskQueuePtr() const
            { return mQueuePtr; }
        int GetFileIdx() const
            { return mFileIdx; }
        bool IsReadOnly() const
            { return mReadOnlyFlag; }
        bool ReserveSpace(
            std::string* inErrMessagePtr = 0);
        bool GrowFile(
            off_t inTargetSz, std::string *inMessagePtr);
        bool ResetFilesize(
            off_t inTargetSz, std::string *inMessagePtr);
        void UnReserveSpace(
            off_t    inStartOffset, off_t inLen);
        void GetDiskQueuePendingCount(
            int&     outFreeRequestCount,
            int&     outRequestCount,
            int64_t& outReadBlockCount,
            int64_t& outWriteBlockCount,
            int&     outBlockSize);
    private:
        DiskQueue* mQueuePtr;
        int        mFileIdx;
        bool       mReadOnlyFlag:1;
        bool       mSpaceReservedFlag:1;

        void Reset();
    };
    typedef boost::shared_ptr<File> FilePtr;

    DiskIo(
        FilePtr         inFilePtr,
        KfsCallbackObj* inCallbackObjPtr);

    ~DiskIo();

    /// Close the connection.  This will cause the events scheduled on
    /// this connection to be cancelled.
    void Close();

    /// Schedule a read on this connection at the specified offset for numBytes.
    /// @param[in] numBytes # of bytes that need to be read.
    /// @param[in] offset offset in the file at which to start reading data from.
    /// @retval # of bytes for which read was successfully scheduled;
    /// -1 if there was an error. 
    ssize_t Read(
        off_t  inOffset,
        size_t inNumBytes);

    /// Schedule a write.  
    /// @param[in] numBytes # of bytes that need to be written
    /// @param[in] offset offset in the file at which to start writing data.
    /// @param[in] buf IOBuffer which contains data that should be written
    /// out to disk.
    /// @retval # of bytes for which write was successfully scheduled;
    /// -1 if there was an error. 
    ssize_t Write(
        off_t     inOffset,
        size_t    inNumBytes,
        IOBuffer* inBufferPtr);

    /// Sync the previously written data to disk.
    /// @param[in] inNotifyDoneFlag if set, notify upstream objects that the
    /// sync operation has finished.
    int Sync(
        bool inNotifyDoneFlag);

private:
    typedef std::vector<IOBufferData> IoBuffers;
    /// Owning KfsCallbackObj.
    KfsCallbackObj* const  mCallbackObjPtr;
    FilePtr                mFilePtr;
    QCDiskQueue::RequestId mRequestId;
    IoBuffers              mIoBuffers;
    size_t                 mReadBufOffset;
    size_t                 mReadLength;
    ssize_t                mIoRetCode;
    QCDiskQueue::RequestId mCompletionRequestId;
    QCDiskQueue::Error     mCompletionCode;
    DiskIo*                mPrevPtr[1];
    DiskIo*                mNextPtr[1];

    void RunCompletion();
    void IoCompletion(
        IOBuffer* inBufferPtr,
        int       inRetCode,
        bool      inSyncFlag = false);
    virtual bool Done(
        QCDiskQueue::RequestId      inRequestId,
        QCDiskQueue::FileIdx        inFileIdx,
        QCDiskQueue::BlockIdx       inStartBlockIdx,
        QCDiskQueue::InputIterator& inBufferItr,
        int                         inBufferCount,
        QCDiskQueue::Error          inCompletionCode,
        int                         inSysErrorCode,
        int64_t                     inIoByteCount);

    friend class QCDLListOp<DiskIo, 0>;
    friend class DiskIoQueues;

private:
    // No copies.
    DiskIo(
        const DiskIo& inDiskIo);
    DiskIo& operator=(
        const DiskIo& inDiskIo);
};

typedef boost::shared_ptr<DiskIo> DiskIoPtr;

}

#endif /* _DISKIO_H */
