//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: IOBuffer.h 1552 2011-01-06 22:21:54Z sriramr $
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

#ifndef _LIBIO_IOBUFFER_H
#define _LIBIO_IOBUFFER_H

#include <stdio.h>
#include <cassert>
#include <list>
#include <exception>
#include <streambuf>
#include <ostream>
#include <istream>
#include <limits>

#include <boost/shared_ptr.hpp>
#include <boost/pool/pool_alloc.hpp> 

namespace KFS
{

///
/// \file IOBuffer.h
/// \brief Buffers used for I/O in KFS.
///
/// An IOBuffer in KFS is a stream of IOBufferData.  Each IOBufferData
/// is an immutable buffer---that is, there is a single producer for
/// data in the buffer and that data maybe consumed by multiple
/// consumers.
///
/// In the current implementation, IOBufferData objects are single
/// producer, multiple consumers.
///

///
/// \class IOBufferData
/// \brief An IOBufferData contains a buffer and associated
/// producer/consumer points.

namespace libkfsio
{
    class IOBufferAllocator
    {
    protected:
        IOBufferAllocator()
            {}
        virtual ~IOBufferAllocator()
            {}
        IOBufferAllocator& operator=(const IOBufferAllocator&)
            { return *this; }
    public:
        virtual size_t GetBufferSize() const = 0;
        virtual char*  Allocate()            = 0;
        virtual void   Deallocate(char* buf) = 0;
    };
    /// API to set the default allocation when allocating
    /// IOBufferData().  The default allocation unit is 4K unless
    /// changed by this API call.
    bool SetIOBufferAllocator(IOBufferAllocator* allocator);
}

class IOBufferData
{
public:
    IOBufferData();
    IOBufferData(int bufsz);
    IOBufferData(char* buf, int offset, int size,
        libkfsio::IOBufferAllocator& allocator);
    IOBufferData(char* buf, int bufSize, int offset, int size);

    /// Create an IOBufferData blob by sharing data block from other;
    /// set the producer/consumer based on the start/end positions
    /// that are passed in
    IOBufferData(const IOBufferData &other, char *s, char *e);
    ~IOBufferData();

    ///
    /// Read data from file descriptor into the buffer.
    /// @param[in] fd file descriptor to be used for reading.
    /// @result Returns the # of bytes read
    ///
    int Read(int fd, int maxReadAhead /* = -1 */);

    ///
    /// Write data from the buffer to the file descriptor.
    /// @param[in] fd file descriptor to be used for writing.
    /// @result Returns the # of bytes written
    ///
    int Write(int fd);

    ///
    /// Copy data into the buffer.  For doing a copy, data is appended
    /// to the buffer starting at the offset corresponding to
    /// mProducer.  # of bytes copied is min (# of bytes, space
    /// avail), where space avail = mEnd - mProducer.
    ///
    /// NOTE: As a result of copy, the "producer" pointer is not
    /// advanced. 
    ///
    /// @param[out] buf A containing the data to be copied in.
    /// @param[in] numBytes # of bytes to be copied.
    /// @retval Returns the # of bytes copied.
    ///
    int CopyIn(const char *buf, int numBytes);
    int CopyIn(const IOBufferData *other, int numBytes);
    ///
    /// Copy data out the buffer.  For doing a copy, data is copied
    /// out of the buffer starting at the offset corresponding to
    /// mConsumer.  # of bytes copied is min (# of bytes, bytes
    /// avail), where bytes avail = mProducer - mConsumer.
    ///
    /// NOTE: As a result of copy, the "consumer" pointer is not
    /// advanced. 
    ///
    /// @param[out] buf A containing the data to be copied in.
    /// @param[in] numBytes # of bytes to be copied.
    /// @retval Returns the # of bytes copied.
    ///
    int CopyOut(char *buf, int numBytes) const;

    char *Producer() { return mProducer; }
    char *Consumer() { return mConsumer; }
    const char *Producer() const { return mProducer; }
    const char *Consumer() const { return mConsumer; }

    ///
    /// Some data has been filled in the buffer.  So, advance
    /// mProducer.
    /// @param[in] nbytes # of bytes of data filled
    /// @retval # of bytes filled in this buffer.
    ///
    int Fill(int nbytes); 
    int ZeroFill(int nbytes);

    ///
    /// Some data has been consumed from the buffer.  So, advance
    /// mConsumer.
    /// @param[in] nbytes # of bytes of data consumed
    /// @retval # of bytes consumed from this buffer.
    ///
    int Consume(int nbytes);

    ///
    /// Remove some data from the end of the buffer.  So, pull back
    /// mProducer
    /// @param[in] nbytes # of bytes of data to be trimmed
    /// @retval # of bytes in this buffer.
    ///
    int Trim(int nbytes);

    /// Returns the # of bytes available for consumption.
    int BytesConsumable() const { return mProducer - mConsumer; }

    /// Return the space available in the buffer
    size_t SpaceAvailable() const { return mEnd - mProducer; }
    int IsFull() const { return mProducer >= mEnd; }
    int IsEmpty() const { return mProducer <= mConsumer; }
    /// Returns true if has whole data buffer.
    bool HasCompleteBuffer() const {
        return (mData.get() == mConsumer &&
            mConsumer + sDefaultBufferSize == mEnd);
    }
    bool IsShared() const {
        return (! mData.unique());
    }
    static int GetDefaultBufferSize() {
        return sDefaultBufferSize;
    }

private:
    /// Data buffer that is ref-counted for sharing.
    typedef boost::shared_ptr<char> IOBufferBlockPtr;
    IOBufferBlockPtr mData;
    /// Pointers that correspond to the start/end of the buffer
    char             *mEnd;
    /// Pointers into mData that correspond to producer/consumer
    char             *mProducer;
    char             *mConsumer;

    /// Allocate memory and init the pointers.
    inline void Init(char* buf, int bufSize);
    inline void Init(char* buf,
        libkfsio::IOBufferAllocator& allocator);

    inline int MaxAvailable(int numBytes) const;
    inline int MaxConsumable(int numBytes) const;

    static int sDefaultBufferSize;
};


///
/// \struct IOBuffer
/// An IOBuffer consists of a list of IOBufferData.  It provides
/// API's for reading/writing data to/from the buffer.  Operations on
/// IOBuffer translates to operations on appropriate IOBufferData.
///
class IOBuffer
{
private:
    typedef std::list<
        IOBufferData,
        boost::fast_pool_allocator<IOBufferData>
    > BList;
public:
    typedef BList::const_iterator iterator;

    IOBuffer();
    ~IOBuffer();

    IOBuffer *Clone() const;

    /// Append the IOBufferData block to the list stored in this buffer.
    /// Unlike methods with IOBuffer as argument, this method will not
    /// Consume() or change buf in any way, the underlying buffer will be
    /// shared.
    void Append(const IOBufferData &buf);

    /// Append the contents of ioBuf to this buffer.
    int Append(IOBuffer *ioBuf);

    int Read(int fd, int maxReadAhead = -1);
    int Write(int fd);

    /// Move data from one buffer to another.  This involves (mostly)
    /// shuffling pointers without incurring data copying.
    /// The requirement is that "other" better have as much bytes as
    /// we are trying to move.
    /// @param[in] other  Buffer from which data has to be moved
    /// @param[in] numBytes  # of bytes of data to be moved over
    /// @retval Returns the # of bytes moved.
    ///
    int Move(IOBuffer *other, int numBytes);
    /// Move whole buffer.
    void Move(IOBuffer *other);

    /// Replace data in the range
    /// [offset, offset + min(numBytes, other->BytesConsumable())
    /// The range [BytesConsumable(), offset) is zero filled.
    /// In addition this method has the same effect as other->Consume(numBytes).
    /// @param[in] other  Buffer from which data has to be spliced
    /// @param[in] offset  The offset at which data has to be spliced in
    /// @param[in] numBytes  # of bytes of data to be moved over
    ///
    void Replace(IOBuffer *other, int offset, int numBytes);
    /// Same as Replace, except it ensures that all buffers in the destination
    /// fully utilized: IsFull() && HasCompleteBuffer()
    /// It copies over min(srcBuf->BytesConsumable(), numBytes) into this. 
    /// If offset > this->BytesConsumable(), the this is zero filled.
    /// This method "consumes" min(srcBuf->BytesConsumable(), numBytes) from
    /// srcBuf.
    void ReplaceKeepBuffersFull(IOBuffer* srcBuf, int offset, int numBytes);

    /// Zero fill the buffer for length numBytes.
    /// @param[in] numBytes  # of bytes to be zero-filled.
    void ZeroFill(int numBytes);

    ///
    /// Copy data into the buffer.  For doing a copy, data is appended
    /// to the last buffer in mBuf.  If the amount of data to be
    /// copied exceeds space in the last buffer, additional buffers
    /// are allocated and copy operation runs to finish.
    ///
    /// NOTE: As a result of copy, the "producer" portion of an
    /// IOBufferData is not advanced. 
    ///
    /// @param[in] buf A containing the data to be copied in.
    /// @param[in] numBytes # of bytes to be copied in.
    /// @retval Returns the # of bytes copied.
    ///
    int CopyIn(const char *buf, int numBytes);

    int Copy(const IOBuffer* buf, int numBytes);

    ///
    /// Copy data out of the buffer.  For doing a copy, data is copied
    /// from the first buffer in mBuf.  If the amount of data to be
    /// copied exceeds what is available in the first buffer, the list
    /// of buffers is walked to copy out data.
    ///
    /// NOTE: As a result of copy, the "consumer" portion of an
    /// IOBufferData is not advanced. 
    ///
    /// @param[out] buf A null-terminated buffer containing the data
    /// copied out.
    /// @param[in] bufLen Length of buf passed in.  At most bufLen
    /// bytes are copied out.
    /// @retval Returns the # of bytes copied.
    ///
    int CopyOut(char *buf, int bufLen) const;
    
    ///
    /// Consuming data in the IOBuffer translates to advancing the
    /// "consumer" point on underlying IOBufferData.  From the head
    /// of the list, the consumer point will be advanced on sufficient
    /// # of buffers.
    /// @retval Returns the # of bytes consumed.
    ///
    int Consume(int nbytes);

    /// Returns the # of bytes that are available for consumption.
    int BytesConsumable() const
        { return mByteCount; }

    /// Trim data from the end of the buffer to nbytes.  This is the
    /// converse of consume, where data is removed from the front of
    /// the buffer.
    void Trim(int nbytes);

    /// Ensures HasCompleteBuffer() returns true for all buffers,
    /// and all buffers possibly except the last one are full.
    void MakeBuffersFull();

    /// Trim at buffer boundary
    void TrimAtBufferBoundaryLeaveOnly(int& offset, int& numBytes);

    /// Searches for a string in the buffer, strstr() equivalent. 
    /// @param[in] offset to start search from.
    /// @param[in] str    string to search for.
    /// @retval Returns position of  the beginning of the "str" if found,
    /// or -1 if not.
    int IndexOf(int offset, const char* str) const;

    /// Returns true if buffer has no data.
    bool IsEmpty() const
        { return mByteCount <= 0; }

    /// Zero fill, if needed the last buffer to make it full.
    /// @retval Returns number of bytes added.
    int ZeroFillLast();

    /// Returns bytes available for consumption in the last buffer
    /// @retval # of bytes consumable in the last buffer.
    int BytesConsumableLast() const
        { return (mBuf.empty() ? 0 : mBuf.back().BytesConsumable()); }

    /// Returns available space in the last buffer.
    /// @retval available space in the last buffer.
    int SpaceAvailableLast() const
        { return (mBuf.empty() ? 0 : mBuf.back().SpaceAvailable()); }
    
    /// Retruns true if the last the buffer is full
    bool IsLastFull() const 
    { return mBuf.empty() ? true : mBuf.back().IsFull(); }

    /// Remove all data.
    void Clear()
    {
        mBuf.clear();
        mByteCount = 0;
    }

    /// Buffer list iterator.
    /// Do not modify IOBufferData pointed by the iterator, or its content.
    iterator begin() const { return mBuf.begin(); }
    iterator end()   const { return mBuf.end();   }

    /// Debug
    void Verify() const;

    /// This is to create istream ostream with StreamBuffer(iobuffer);
    class StreamBuffer : public std::streambuf
    {
    public:
        StreamBuffer(
            IOBuffer& iobuf,
            int       maxReadLength = std::numeric_limits<int>::max())
            : std::streambuf(),
              mMaxReadLength(maxReadLength),
              mCur(iobuf.begin()),
              mIoBuf(iobuf)
            {}
        void Reset(int maxReadLength = std::numeric_limits<int>::max())
        {
            mCur = mIoBuf.begin();
            mMaxReadLength = maxReadLength;
        }
    protected:
        virtual int underflow();
        virtual int overflow(int c = EOF);
        virtual std::streamsize xsputn(const char * s, std::streamsize n);
    private:
        int       mMaxReadLength;
        iterator  mCur;
        IOBuffer& mIoBuf;
    private:
        StreamBuffer(const StreamBuffer&);
        StreamBuffer& operator=(const StreamBuffer&);
    };
    class OStream;
    class IStream;
private:
    BList mBuf;
    int   mByteCount;
#ifdef DEBUG_IOBuffer
    unsigned int mDebugChecksum;
#endif
    inline void DebugChecksum(const char* buf, int len);
    inline void DebugChecksum(const IOBufferData& buf);
    inline void DebugChecksum(const IOBuffer& buf, int numBytes);
    inline void DebugVerify() const;
    inline void DebugVerify(bool updateChecksum);

    inline static BList::iterator SplitBufferListAt(BList& buf, int& nBytes);
    IOBuffer(const IOBuffer& buf);
    IOBuffer& operator=(const IOBuffer& buf);
};

class IOBuffer::OStream :
    public  IOBuffer,
    private IOBuffer::StreamBuffer,
    public  std::ostream
{
public:
    OStream()
        : IOBuffer(),
          IOBuffer::StreamBuffer(*this, 0),
          std::ostream(this)
    {}
};

class IOBuffer::IStream :
    private IOBuffer::StreamBuffer,
    public  std::istream
{
public:
    IStream(
        IOBuffer& iobuf,
        int       maxReadLength = std::numeric_limits<int>::max())
        : IOBuffer::StreamBuffer(iobuf, maxReadLength),
          std::istream(this)
    {}
    void Rewind(int maxReadLength) {
        StreamBuffer::Reset(maxReadLength);
        rdbuf(this);
    }
};

}

#endif // _LIBIO_IOBUFFER_H
