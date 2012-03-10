//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: KfsClientInt.h 3485 2011-12-14 23:26:12Z sriramr $
//
// Created 2006/04/18
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

#ifndef LIBKFSCLIENT_KFSCLIENTINT_H
#define LIBKFSCLIENT_KFSCLIENTINT_H

#include <string>
#include <vector>
#include <tr1/unordered_map>
#include <poll.h>
#include "common/log.h"
#include "common/hsieh_hash.h"
#include "common/kfstypes.h"
#include "libkfsIO/TcpSocket.h"
#include "libkfsIO/Checksum.h"

#include "libkfsIO/TelemetryClient.h"

#include "KfsAttr.h"

#include "KfsOps.h"
#include "LeaseClerk.h"
 
#include "concurrency.h"
#include "KfsPendingOp.h"
#include "KfsAsyncRW.h"

namespace KFS {

/// Set this to 1MB: 64K * 16
const size_t MIN_BYTES_PIPELINE_IO = CHECKSUM_BLOCKSIZE * 16 * 4;

/// Do io in 1MB blocks if possible.
const size_t MAX_BYTES_PER_WRITE_IO = ((1u << 20) + CHECKSUM_BLOCKSIZE - 1) /
    CHECKSUM_BLOCKSIZE * CHECKSUM_BLOCKSIZE;
const size_t MAX_BYTES_PER_READ_IO = MAX_BYTES_PER_WRITE_IO;

/// If an op fails because the server crashed, retry the op.  This
/// constant defines the # of retries before declaring failure.
const int DEFAULT_NUM_RETRIES_PER_OP = 6;

/// Whenever an op fails, we need to give time for the server to
/// recover.  So, introduce a delay of 5 secs between retries.
const int RETRY_DELAY_SECS = 5;

/// Whenever we have issues with lease failures, we retry the op after 5 secs
const int LEASE_RETRY_DELAY_SECS = 5;

/// Directory entries that we may have cached are valid for 30 secs;
/// after that force a revalidataion.
const int FILE_CACHE_ENTRY_VALID_TIME = 30;

///
/// A KfsClient maintains a file-table that stores information about
/// KFS files on that client.  Each file in the file-table is composed
/// of some number of chunks; the meta-information about each
/// chunk is stored in a chunk-table associated with that file.  Thus, given a
/// <file-id, offset>, we can map it to the appropriate <chunk-id,
/// offset within the chunk>; we can also find where that piece of
/// data is located and appropriately access it.
///

///
/// \brief Buffer for speeding up small reads and writes; holds
/// on to a piece of data from one chunk.
///
struct ChunkBuffer {
    // set the client buffer to be fairly big...for sequential reads,
    // we will hit the network few times and on each occasion, we read
    // a ton and thereby get decent performance; having a big buffer
    // obviates the need to do read-ahead :-)
    
    ChunkBuffer():chunkno(-1), start(0), length(0), dirty(false), buf(NULL), bufsz(0) { }
    ~ChunkBuffer() { delete [] buf; }
    void invalidate() { 
        chunkno = -1; start = 0; length = 0; dirty = false; 
        delete [] buf;
        buf = 0;
    }
    void allocate() {
        if (! buf && bufsz > 0) {
            // XXX: align this to 16-byte boundary
            // see IOBuffer.cc code
            buf = new char[bufsz];
        }
    }
    int chunkno;		// which chunk
    off_t start;		// offset with chunk
    size_t length;	// length of valid data
    bool dirty;		// must flush to server if true
    char *buf;	// the data
    size_t bufsz;
};

struct ChunkServerConn {
    /// name/port of the chunk server to which this socket is
    /// connected.
    ServerLocation location;
    /// connected TCP socket.  If this object is copy constructed, we
    /// really can't afford this socket to close when the "original"
    /// is destructed. To protect such issues, make this a smart pointer.
    TcpSocketPtr   sock;

    ChunkServerConn(const ServerLocation &l) :
        location(l) { 
        sock.reset(new TcpSocket());
    }
    
    void Connect(bool nonblockingConnect = false) {
        if (sock->IsGood())
            return;
        int res;

        res = sock->Connect(location, nonblockingConnect);
        if (res == -EINPROGRESS) {
            // 30 seconds: poll expects values in milli-seconds
            int pollTimeout = 30 * 1000;
            struct pollfd pfd;

            pfd.fd = sock->GetFd();
            pfd.events = POLLOUT;
            pfd.revents = 0;

            res = poll(&pfd, 1, pollTimeout);
            if ((res > 0) && (pfd.revents & POLLOUT)) {
                    // connection completed
                    return;
            }
            KFS_LOG_VA_INFO("Non-blocking connect to location %s failed", location.ToString().c_str());
            res = -EHOSTUNREACH;
        }
        if (res < 0) {
            sock.reset(new TcpSocket());
        }
            
    }
    bool operator == (const ServerLocation &other) const {
        return other == location;
    }
};

class KfsClientImpl;

///
/// The following class is used for chunk read ahead:
/// Start() sends chunk read request to chunk server, and
/// Read() retrieves the data.
/// Reset() cancels read request by resetting chunk server connection.
///
class PendingChunkRead
{
public:
    enum { kMaxReadRequest = 1 << 20 };

    PendingChunkRead(KfsClientImpl& impl, size_t readAhead);
    ~PendingChunkRead();
    bool Start(int fd, size_t off);
    ssize_t Read(char *buf, size_t numBytes);
    bool IsValid() const { return (mFd >= 0); }
    void Reset() { Start(-1, 0); }
    void SetReadAhead(size_t readAhead) { mReadAhead = readAhead; }
    size_t GetReadAhead() const { return mReadAhead; }
    off_t GetChunkOffset() const { return (IsValid() ? mReadOp.offset : -1); }
private:
    ReadOp         mReadOp;
    TcpSocket*     mSocket;
    KfsClientImpl& mImpl;
    int            mFd;
    size_t         mReadAhead;
};

///
/// \brief Location of the file pointer in a file consists of two
/// parts: the offset in the file, which then translates to a chunk #
/// and an offset within the chunk.  Also, for performance, we do some
/// client-side buffering (for both reads and writes).  The buffer
/// stores data corresponding to the "current" chunk.
///
struct FilePosition {
    FilePosition() {
	fileOffset = chunkOffset = 0;
	chunkNum = 0;
        preferredServer = NULL;
        pendingChunkRead = 0;
        prefetchReq = NULL;
        prefetchIndexReq = NULL;
        lastSocketIOTime = time(0);
    }
    ~FilePosition() {
        delete pendingChunkRead;
        delete prefetchReq;
        delete prefetchIndexReq;
    }
    void Reset() {
	fileOffset = chunkOffset = 0;
	chunkNum = 0;
        chunkServers.clear();
        preferredServer = NULL;
        prefetchReq = NULL;
        prefetchIndexReq = NULL;
        CancelPendingRead();
    }

    off_t	fileOffset; // offset within the file
    /// which chunk are we at: this is an index into fattr.chunkTable[]
    int32_t	chunkNum;
    /// offset within the chunk
    off_t	chunkOffset;

    /// when was the last IO done on the socket.
    time_t      lastSocketIOTime;

    /// transaction id info for record append
    std::vector<WriteInfo> writeId;
    
    std::vector<ChunkServerConn> chunkServers;

    /// For reads as well as meta requests about a chunk, this is the
    /// preferred server to goto.  This is a pointer to a socket in
    /// the vector<ChunkServerConn> structure. 
    TcpSocket *preferredServer;

    /// Track the location of the preferred server so we can print debug messages
    ServerLocation preferredServerLocation;

    PendingChunkRead* pendingChunkRead;
    /// for read prefetching
    AsyncReadReq *prefetchReq;

    /// for index prefetching
    AsyncGetIndexReq *prefetchIndexReq;

    void ResetServers() {
        writeId.clear();
        chunkServers.clear();
        preferredServer = NULL;
        CancelPendingRead();
    }

    TcpSocketPtr GetPreferredChunkServerSockPtr() {
        std::vector<ChunkServerConn>::iterator iter;
        TcpSocketPtr sock;
        
        iter = std::find(chunkServers.begin(), chunkServers.end(), 
                         preferredServerLocation);
        if (iter != chunkServers.end())
            return iter->sock;
        return sock;
    }

    TcpSocket *GetChunkServerSocket(const ServerLocation &loc, bool nonblockingConnect = false) {
        std::vector<ChunkServerConn>::iterator iter;

        iter = std::find(chunkServers.begin(), chunkServers.end(), loc);
        if (iter != chunkServers.end()) {
            iter->Connect(nonblockingConnect);
            TcpSocket *s = iter->sock.get();

            if (s->IsGood())
                return s;
            return NULL;
        }

        // Bit of an issue here: The object that is being pushed is
        // copy constructed; when that object is destructed, the
        // socket it has will go.  To avoid that, we need the socket
        // to be a smart pointer.
        chunkServers.push_back(ChunkServerConn(loc));
        chunkServers[chunkServers.size()-1].Connect(nonblockingConnect);

        TcpSocket *s = chunkServers[chunkServers.size()-1].sock.get();
        if (s->IsGood())
            return s;
        return NULL;
    }

    /// take out the chunkserver from the list of servers that we can talk to.  
    void AvoidServer(const ServerLocation &loc) {
        std::vector<ChunkServerConn>::iterator iter;

        iter = std::find(chunkServers.begin(), chunkServers.end(), loc);
        if (iter != chunkServers.end())
            chunkServers.erase(iter);

        if (preferredServerLocation == loc)
            preferredServer = NULL;
    }

    void ResetPreferredServerConnection() {
        if (preferredServer)
            preferredServer->Close();
        preferredServer = GetChunkServerSocket(preferredServerLocation);
    }

    void SetPreferredServer(const ServerLocation &loc, bool nonblockingConnect = false) {
        preferredServer = GetChunkServerSocket(loc, nonblockingConnect);
        preferredServerLocation = loc;
    }

    const ServerLocation &GetPreferredServerLocation() const {
        return preferredServerLocation;
    }

    TcpSocket *GetPreferredServer() {
        return preferredServer;
    }

    int GetPreferredServerAddr(struct sockaddr_in &saddr) {
        if (preferredServer == NULL)
            return -1;
        return preferredServer->GetPeerName((struct sockaddr *) &saddr, sizeof(struct sockaddr_in));
    }
    void CancelPendingRead() {
        if (pendingChunkRead) {
            pendingChunkRead->Reset();
        }
    }
    void CancelNonAdjacentPendingRead() {
        if (pendingChunkRead &&
                pendingChunkRead->GetChunkOffset() != chunkOffset) {
            pendingChunkRead->Reset();
        }
    }
};

typedef std::tr1::unordered_map<std::string, int, Hsieh_hash_fcn> NameToFdMap;
    typedef std::tr1::unordered_map<std::string, int, Hsieh_hash_fcn>::iterator NameToFdMapIter;

///
/// \brief A table of entries that describe each open KFS file.
///
struct FileTableEntry {
    // the fid of the parent dir in which this entry "resides"
    kfsFileId_t parentFid;
    // stores the name of the file/directory.
    std::string	name;

    // store a pointer to the associated name-cache entry
    // NameToFdMapIter pathCacheIter;

    // the full pathname
    std::string pathname;

    // one of O_RDONLY, O_WRONLY, O_RDWR; when it is 0 for a file,
    // this entry is used for attribute caching
    int		openMode;
    FileAttr	fattr;
    std::map <int, ChunkAttr> cattr;
    // the position in the file at which the next read/write will occur
    FilePosition currPos;
    /// the user has set a marker beyond which reads should return EOF
    off_t eofMark;
    /// For the current chunk, do some amount of buffering on the
    /// client.  This helps absorb network latencies for small
    /// reads/writes.
    ChunkBuffer buffer;
    // for LRU reclamation of file table entries, track when this
    // entry was last accessed
    time_t	lastAccessTime;
    // directory entries are cached; ala NFS, keep the entries cached
    // for a max of 30 secs; after that revalidate
    time_t	validatedTime;

    bool skipHoles;
    unsigned int instance;
    int appendPending;
    bool didAppend;

    FileTableEntry(kfsFileId_t p, const char *n, unsigned int instance):
	parentFid(p), name(n), eofMark(-1), 
        lastAccessTime(0), validatedTime(0), 
        skipHoles(false), instance(instance), appendPending(0),
        didAppend(false) { }
};

class KfsProtocolWorker;
class MutexLock;
///
/// The implementation object.
///
class KfsClientImpl {

public:
    KfsClientImpl();
    ~KfsClientImpl();

    ///
    /// @param[in] metaServerHost  Machine on meta is running
    /// @param[in] metaServerPort  Port at which we should connect to
    /// @retval 0 on success; -1 on failure
    ///
    int Init(const std::string metaServerHost, int metaServerPort);

    void SetUGI(const std::string &userName, const std::string &groupName);

    ServerLocation GetMetaserverLocation() const {
        return mMetaServerLoc;
    }

    bool IsInitialized() { return mIsInitialized; };

    ///
    /// Provide a "cwd" like facility for KFS.
    /// @param[in] pathname  The pathname to change the "cwd" to
    /// @retval 0 on sucess; -errno otherwise
    ///
    int Cd(const char *pathname);

    /// Get cwd
    /// @retval a string that describes the current working dir.
    ///
    std::string GetCwd();

    ///
    /// Make a directory hierarcy in KFS.  If the parent dirs are not
    /// present, they are also made.
    /// @param[in] pathname		The full pathname such as /.../dir
    /// @param[in] permissions Unix style perms
    /// @retval 0 if mkdir is successful; -errno otherwise
    int Mkdirs(const char *pathname, int permissions);

    ///
    /// Make a directory in KFS.
    /// @param[in] pathname		The full pathname such as /.../dir
    /// @param[in] permissions Unix style perms
    /// @retval 0 if mkdir is successful; -errno otherwise
    int Mkdir(const char *pathname, int permissions);

    ///
    /// Remove a directory in KFS.
    /// @param[in] pathname		The full pathname such as /.../dir
    /// @retval 0 if rmdir is successful; -errno otherwise
    int Rmdir(const char *pathname);

    ///
    /// Remove a directory hierarchy in KFS.
    /// @param[in] pathname		The full pathname such as /.../dir
    /// @retval 0 if rmdir is successful; -errno otherwise
    int Rmdirs(const char *pathname);

    int RmdirsFast(const char *pathname);

    ///
    /// Read a directory's contents
    /// @param[in] pathname	The full pathname such as /.../dir
    /// @param[out] result	The contents of the directory
    /// @retval 0 if readdir is successful; -errno otherwise
    int Readdir(const char *pathname, std::vector<std::string> &result);

    ///
    /// Read a directory's contents and retrieve the attributes
    /// @param[in] pathname	The full pathname such as /.../dir
    /// @param[out] result	The files in the directory and their attributes.
    /// @param[in] computeFilesize  By default, compute file size
    /// @retval 0 if readdirplus is successful; -errno otherwise
    ///
    int ReaddirPlus(const char *pathname, std::vector<KfsFileAttr> &result,
                    bool computeFilesize = true);

    ///
    /// Do a du on the metaserver side for pathname and return the #
    /// of files/bytes in the directory tree starting at pathname.
    /// @retval 0 if readdirplus is successful; -errno otherwise
    ///
    int GetDirSummary(const char *pathname, uint64_t &numFiles, uint64_t &numBytes);

    ///
    /// Stat a file and get its attributes.
    /// @param[in] pathname	The full pathname such as /.../foo
    /// @param[out] result	The attributes that we get back from server
    /// @param[in] computeFilesize  When set, for files, the size of
    /// file is computed and the value is returned in result.st_size
    /// @retval 0 if stat was successful; -errno otherwise
    ///
    int Stat(const char *pathname, struct stat &result, bool computeFilesize = true);

    ///
    /// Return the # of chunks in the file specified by the fully qualified pathname.
    /// -1 if there is an error.
    ///
    int GetNumChunks(const char *pathname);

    int UpdateFilesize(int fd);

    ///
    /// Helper APIs to check for the existence of (1) a path, (2) a
    /// file, and (3) a directory.
    /// @param[in] pathname	The full pathname such as /.../foo
    /// @retval status: True if it exists; false otherwise
    ///
    bool Exists(const char *pathname);
    bool IsFile(const char *pathname);
    bool IsDirectory(const char *pathname);

    bool IsEof(int fd);

    /// Debug API to print out the size/location of each block of a file.
    int EnumerateBlocks(const char *pathname);

    /// Given a file in KFS, verify that all N copies of each chunk are identical.
    /// @retval status code
    bool CompareChunkReplicas(const char *pathname, string &md5sum);

    /// API to verify that checksums computed on source data matches
    /// what was pushed into KFS.  This verification is done by
    /// pulling KFS checksums from all the replicas for each chunk.
    /// @retval status code
    bool VerifyDataChecksums(const char *pathname, const std::vector<uint32_t> &checksums);
    bool VerifyDataChecksums(int fd, off_t offset, const char *buf, off_t numBytes);

    ///
    /// Create a file which is specified by a complete path.
    /// @param[in] pathname that has to be created
    /// @param[in] permissions Unix style perms
    /// @param[in] numReplicas the desired degree of replication for
    /// the file.
    /// @param[in] exclusive  create will fail if the exists (O_EXCL flag)
    /// @retval on success, fd corresponding to the created file;
    /// -errno on failure.
    ///
    int Create(const char *pathname, int permissions, 
        int numReplicas = 3, bool exclusive = false);

    ///
    /// Remove a file which is specified by a complete path.
    /// @param[in] pathname that has to be removed
    /// @retval status code
    ///
    int Remove(const char *pathname);

    ///
    /// Rename file/dir corresponding to oldpath to newpath
    /// @param[in] oldpath   path corresponding to the old name
    /// @param[in] newpath   path corresponding to the new name
    /// @param[in] overwrite  when set, overwrite the newpath if it
    /// exists; otherwise, the rename will fail if newpath exists
    /// @retval 0 on success; -1 on failure
    ///
    int Rename(const char *oldpath, const char *newpath, bool overwrite = true);

    int CoalesceBlocks(const char *srcPath, const char *dstPath, off_t *dstStartOffset);

    ///
    /// Set the mtime for a path
    /// @param[in] pathname  for which mtime has to be set
    /// @param[in] mtime     the desired mtime
    /// @retval status code
    ///
    int SetMtime(const char *pathname, const struct timeval &mtime);

    ///
    /// Open a file
    /// @param[in] pathname that has to be opened
    /// @param[in] openFlags modeled after open().  The specific set
    /// of flags currently supported are:
    /// O_CREAT, O_CREAT|O_EXCL, O_RDWR, O_RDONLY, O_WRONLY, O_TRUNC, O_APPEND
    /// @param[in] numReplicas if O_CREAT is specified, then this the
    /// desired degree of replication for the file
    /// @retval fd corresponding to the opened file; -errno on failure
    ///
    int Open(const char *pathname, int openFlags, int numReplicas = 3);

    ///
    /// Return file descriptor for an open file
    /// @param[in] pathname of file
    /// @retval file descriptor if open, error code < 0 otherwise
    int Fileno(const char *pathname);

    ///
    /// Close a file
    /// @param[in] fd that corresponds to a previously opened file
    /// table entry.
    ///
    int Close(int fd);

    ///
    /// Append a record to the chunk that we are writing to in the
    /// file with one caveat: the record should not straddle chunk
    /// boundaries.
    /// @param[in] fd that correpsonds to the file open for writing
    /// @param[in] buf the record that should be appended
    /// @param[in] reclen the length of the record
    /// @retval Status code
    ///
    int RecordAppend(int fd, const char *buf, int reclen);
    int AtomicRecordAppend(int fd, const char *buf, int reclen);

    /// See the comments in KfsClient.h
    int ReadPrefetch(int fd, char *buf, size_t numBytes);
    int WaitForPrefetch();

    int WriteAsync(int fd, const char *buf, size_t numBytes);
    int WriteAsyncCompletionHandler(int fd);

    void EnableAsyncRW() {
        mAsyncer.Start();
    }
    void DisableAsyncRW() {
        mAsyncer.Stop();
    }

    ///
    /// Read/write the desired # of bytes to the file, starting at the
    /// "current" position of the file.
    /// @param[in] fd that corresponds to a previously opened file
    /// table entry.
    /// @param buf For read, the buffer will be filled with data; for
    /// writes, this buffer supplies the data to be written out.
    /// @param[in] numBytes   The # of bytes of I/O to be done.
    /// @retval On success, return of bytes of I/O done (>= 0);
    /// on failure, return status code (< 0).
    ///
    ssize_t Read(int fd, char *buf, size_t numBytes);
    ssize_t Write(int fd, const char *buf, size_t numBytes);

    /// If there are any holes in a file, such as those at the end of
    /// a chunk, skip over them.  
    void SkipHolesInFile(int fd);

    ///
    /// \brief Sync out data that has been written (to the "current" chunk).
    /// @param[in] fd that corresponds to a file that was previously
    /// opened for writing.
    ///
    int Sync(int fd, bool flushOnlyIfHasFullChecksumBlock = false);

    /// \brief Adjust the current position of the file pointer similar
    /// to the seek() system call.
    /// @param[in] fd that corresponds to a previously opened file
    /// @param[in] offset offset to which the pointer should be moved
    /// relative to whence.
    /// @param[in] whence one of SEEK_CUR, SEEK_SET, SEEK_END
    /// @retval On success, the offset to which the filer
    /// pointer was moved to; (off_t) -1 on failure.
    ///
    off_t Seek(int fd, off_t offset, int whence, bool flushIfBufDirty = true);
    /// In this version of seek, whence == SEEK_SET
    off_t Seek(int fd, off_t offset);

    /// Return the current position of the file pointer in the file.
    /// @param[in] fd that corresponds to a previously opened file
    /// @retval value returned is analogous to calling ftell()
    off_t Tell(int fd);

    ///
    /// Truncate a file to the specified offset.
    /// @param[in] fd that corresponds to a previously opened file
    /// @param[in] offset  the offset to which the file should be truncated
    /// @retval status code
    int Truncate(int fd, off_t offset);

    ///
    /// Truncation, but going in the reverse direction: delete chunks
    /// from the beginning of the file to the specified offset
    /// @param[in] fd that corresponds to a previously opened file
    /// @param[in] offset  the offset before which the chunks should
    /// be deleted
    /// @retval status code
    int PruneFromHead(int fd, off_t offset);

    ///
    /// Retrieve the per chunk index---in this case, we get the chunk
    /// index for the chunk that corresponds to the current file
    /// position.  
    /// @param[in] fd  Previously opened fd for a file in KFS.
    /// @param[out] buffer  Buffer that holds the chunk index.  This
    /// memory is dynamically allocated; it is the caller's
    /// responsibility to free it.
    //  @param[out] indexSize Size of the index
    /// @retval status code
    ///
    int GetChunkIndex(int fd, char **buffer, int &indexSize);
    int GetChunkIndexPrefetch(int fd);

    ///
    /// Given a starting offset/length, return the location of all the
    /// chunks that cover this region.  By location, we mean the name
    /// of the chunkserver that is hosting the chunk. This API can be
    /// used for job scheduling.
    ///
    /// @param[in] pathname	The full pathname of the file such as /../foo
    /// @param[in] start	The starting byte offset
    /// @param[in] len		The length in bytes that define the region
    /// @param[out] locations	The location(s) of various chunks
    /// @retval status: 0 on success; -errno otherwise
    ///
    int GetDataLocation(const char *pathname, off_t start, off_t len,
                        std::vector< std::vector <std::string> > &locations);

    int GetDataLocation(int fd, off_t start, off_t len,
                        std::vector< std::vector <std::string> > &locations);

    ///
    /// Get the degree of replication for the pathname.
    /// @param[in] pathname	The full pathname of the file such as /../foo
    /// @retval count
    ///
    int16_t GetReplicationFactor(const char *pathname);

    ///
    /// Set the degree of replication for the pathname.
    /// @param[in] pathname	The full pathname of the file such as /../foo
    /// @param[in] numReplicas  The desired degree of replication.
    /// @retval -1 on failure; on success, the # of replicas that will be made.
    ///
    int16_t SetReplicationFactor(const char *pathname, int16_t numReplicas);

    // Next sequence number for operations.
    // This is called in a thread safe manner.
    kfsSeq_t nextSeq() { return mCmdSeqNum++; }

    size_t SetDefaultIoBufferSize(size_t size);
    size_t GetDefaultIoBufferSize() const;
    size_t SetIoBufferSize(int fd, size_t size);
    size_t GetIoBufferSize(int fd) const;

    size_t SetDefaultReadAheadSize(size_t size);
    size_t GetDefaultReadAheadSize() const;
    size_t SetReadAheadSize(int fd, size_t size);
    size_t GetReadAheadSize(int fd) const;
    pthread_mutex_t& GetMutex() { return mMutex; }

    /// A read for an offset that is after the specified value will result in EOF
    void SetEOFMark(int fd, off_t offset);

    TelemetryClient &GetTelemetryReporter() {
        return mTelemetryReporter;
    }
    void SetMaxNumRetriesPerOp(int maxNumRetries) {
        mMaxNumRetriesPerOp = maxNumRetries;
    }

    kfsChunkId_t GetCurrChunkId(int fd) const;
private:
     /// Maximum # of files a client can have open.
    static const int MAX_FILES = 512000;

    /// Primitive support for concurrent access in the KFS client: at
    /// each entry point from the public interfaces, grab the mutex
    /// before doing any work.  This ensures that all requests to the
    /// meta/chunk servers are serialized.
    pthread_mutex_t mMutex;

    /// Seed to the random number generator
    unsigned    mRandSeed;
    bool	mIsInitialized;
    /// where is the meta server located
    ServerLocation mMetaServerLoc;

    /// User/group information
    std::string mUserName;
    std::string mGroupName;

    LeaseClerk  mLeaseClerk;

    /// a tcp socket that holds the connection with the server
    TcpSocket	mMetaServerSock;
    /// seq # that we send in each command
    kfsSeq_t	mCmdSeqNum;

    /// The current working directory in KFS
    std::string	mCwd;

    std::string mHostname;

    /// keep a table of open files/directory handles.
    std::vector <FileTableEntry *> mFileTable;
    NameToFdMap mPathCache;

    TelemetryClient mTelemetryReporter;
    /// set of slow nodes as flagged by the telemetry service
    std::vector<struct in_addr> mSlowNodes;
    size_t mDefaultIoBufferSize;
    size_t mDefaultReadAheadSize;
    KfsPendingOp mPendingOp;

    Asyncer mAsyncer;
    std::vector<AsyncWriteReq *> mAsyncWrites;
    unsigned int mFileInstance;
    KfsProtocolWorker* mProtocolWorker;
    int mMaxNumRetriesPerOp;

    /// Check that fd is in range
    bool valid_fd(int fd) { return (fd >= 0 && fd < MAX_FILES && (size_t)fd < mFileTable.size() && mFileTable[fd]); }

    /// Connect to the meta server and return status.
    /// @retval true if connect succeeds; false otherwise.
    bool ConnectToMetaServer();

    /// Lookup the attributes of a file given its parent file-id
    /// @param[in] parentFid  file-id of the parent directory
    /// @param[in] filename   filename whose attributes are being
    /// asked
    /// @param[out] result   the resultant attributes
    /// @param[in] computeFilesize  when set, for files, the size of
    /// the file is computed and returned in result.fileSize
    /// @retval 0 on success; -errno otherwise
    ///
    int LookupAttr(kfsFileId_t parentFid, const char *filename,
		   KfsFileAttr &result, bool computeFilesize);

    /// Helper functions that operate on individual chunks.

    /// Allocate the "current" chunk of fd.
    /// @param[in] fd  The index from mFileTable[] that corresponds to
    /// the file being accessed
    /// @param[in] append  Set if we append a chunk to the file.  When
    /// records are appended to a file by multiple writers, each
    /// writer writes to a unique chunk.  In this setting, the client
    /// does not know the file offset at which the chunk should be
    /// allocated (only the metaserver knows).  
    ///
    int AllocChunk(int fd, bool append = false);

    /// Open the "current" chunk of fd.  This involves setting up the
    /// socket to the chunkserver and determining the size of the chunk.
    /// For reads, since there are 3 copies of a chunk, we use the
    /// nonblocking connect; this allows us to switch servers one of
    /// the replicas is non-reachable
    ///
    /// @param[in] fd  The index from mFileTable[] that corresponds to
    /// the file being accessed
    int OpenChunk(int fd, bool nonblockingConnect = false);
    
    /// Close the "current" chunk of the fd.  This might involve
    /// either relinquishing read lease or notifying a chunkserver
    /// that writes from this client are done (and that the
    /// chunkserver can give up the write lease appropriately).
    int CloseChunk(int fd);

    bool IsChunkReadable(int fd, int &leaseStatus);

    /// Given a chunkid, is our lease on that chunk "good"?  That is,
    /// if it is close to expiring, renew it; if it is expired, get a
    /// new one.
    /// @param[in] chunkId  The chunk for which we are trying to get a
    /// "good" lease.
    /// @param[in] pathname  The full path to the file that contains the chunk.
    /// @retval true if our lease is good; false otherwise.
    bool IsChunkLeaseGood(int fd, kfsChunkId_t chunkId, const std::string &pathname, int &leaseStatus);


    /// Helper function that reads from the "current" chunk.
    /// @param[in] fd  The file from which data is to be read
    /// @param[out] buf  The buffer which will be filled with data
    /// @param[in] numBytes  The desired # of bytes to be read
    /// @retval  On success, # of bytes read; -1 on error
    ///
    ssize_t ReadChunk(int fd, char *buf, size_t numBytes);

    /// Helper function that reads from the "current" chunk from the
    /// chunk server.  For performance, depending on the # of bytes to
    /// be read, the read could be pipelined to overlap disk/network
    /// transfer from the chunkserver.
    ///
    /// @param[in] fd  The file from which data is to be read
    /// @param[out] buf  The buffer which will be filled with data
    /// @param[in] numBytes  The desired # of bytes to be read
    /// @retval  On success, # of bytes read; -1 on error
    ///
    ssize_t ReadFromServer(int fd, char *buf, size_t numBytes);

    /// Helper function that does a single read op to the chunkserver
    /// to get data back.
    /// @param[in] fd  The file from which data is to be read
    /// @param[out] buf  The buffer which will be filled with data
    /// @param[in] numBytes  The desired # of bytes to be read
    /// @retval  On success, # of bytes read; -1 on error
    ///
    ssize_t DoSmallReadFromServer(int fd, char *buf, size_t numBytes);

    /// Helper function that breaks up a single read into a bunch of
    /// small reads and pipelines the read to reduce latency.
    ssize_t DoLargeReadFromServer(int fd, char *buf, size_t numBytes);

    /// Helper function that copies out data from the chunk buffer
    /// corresponding to the "current" chunk.
    /// @param[in] fd  The file from which data is to be read
    /// @param[out] buf  The buffer which will be filled with data
    /// @param[in] numBytes  The desired # of bytes to be read
    /// @retval  # of bytes copied ( value >= 0).
    ///
    size_t CopyFromChunkBuf(int fd, char *buf, size_t numBytes);

    /// Helper function that zero-fills a buffer whenever there are
    /// holes in a file.
    /// @param[in] fd  The file from which data is to be read
    /// @param[out] buf  The buffer which will be filled with 0
    /// @param[in] numBytes  The desired # of bytes to be filled with 0
    /// @retval  # of bytes copied ( value >= 0).
    ///
    size_t ZeroFillBuf(int fd, char *buf, size_t numBytes);

    /// Given a chunk, find out which chunk-server is hosting it.  It
    /// is possible that no server is hosting the chunk---if there is
    /// a hole in the file.
    /// @retval status code: 0 on success; < 0 => failure
    int LocateChunk(int fd, int chunkNum);


    // Helper functions to deal with write and buffering at the client.

    /// Write the data to the chunk buffer and ack the application.
    /// This can be used for doing delayed write-backs.
    /// @param[in] fd  The file to which data is to be written
    /// @param[out] buf  The buffer with the data to be written out
    /// @param[in] numBytes  The desired # of bytes to be written
    /// @retval  # of bytes written; -1 on failure
    ssize_t WriteToBuffer(int fd, const char *buf, size_t numBytes);

    /// Flush the contents of the chunk buffer back to the chunk
    /// server.
    /// @param[in] fd  The file to which data is to be written
    /// @retval  # of bytes written; -1 on failure
    ssize_t FlushBuffer(int fd, bool flushOnlyIfHasFullChecksumBlock = false);

    /// Helper function that does the write RPC.
    /// @param[in] fd  The file to which data is to be written
    /// @param[in] offset  The offset in the chunk at which data has
    /// to be written out
    /// @param[out] buf  The buffer with the data to be written out
    /// @param[in] numBytes  The desired # of bytes to be written
    /// @retval  # of bytes written; -1 on failure
    ///
    ssize_t WriteToServer(int fd, off_t offset, const char *buf, size_t numBytes);

    /// Helper function that does a single write op to the server.
    /// @param[in] fd  The file to which data is to be written
    /// @param[in] offset  The offset in the chunk at which data has
    /// to be written out
    /// @param[out] buf  The buffer with the data to be written out
    /// @param[in] numBytes  The desired # of bytes to be written
    /// @retval  # of bytes written; -1 on failure
    ///
    ssize_t DoSmallWriteToServer(int fd, off_t offset, const char *buf, size_t numBytes);

    /// Helper function that does a pipelined write to server.
    /// Basically, break a write into smaller writes and pipeline them.
    ssize_t DoLargeWriteToServer(int fd, off_t offset, const char *buf, size_t numBytes);

    /// Request a chunk allocation with the metaserver if necessary.
    /// The 2nd argument "forces" an allocation with the server.
    int DoAllocation(int fd, bool force = false);

    // Return true if the attributes of the "current" chunk are
    // known, i.e., they were downloaded from meta server.
    bool IsCurrChunkAttrKnown(int fd);

    /// Get the size of the "current" chunk from the chunkserver.
    /// @param[in] fd  The index in mFileTable[] that corresponds to a
    /// previously opened file.
    int SizeChunk(int fd);

    /// Given a kfsfid with some # of chunks, compute the size of the
    /// file.  This involves looking up the size of the last chunk of
    /// the file and then adding with the size of the remaining (full) chunks.
    off_t ComputeFilesize(kfsFileId_t kfsfid);

    /// Given the attributes for a set of files and the location info
    /// of the last chunk of each file, compute the filesizes for each file
    void ComputeFilesizes(vector<KfsFileAttr> &fattrs, vector<FileChunkInfo> &lastChunkInfo);

    /// Helper function: given a starting index to the two vectors,
    /// compute the file sizes for each file whose last chunk is
    /// stored in chunkserver at location loc.
    void ComputeFilesizes(vector<KfsFileAttr> &fattrs, vector<FileChunkInfo> &lastChunkInfo,
                          uint32_t startIdx, const ServerLocation &loc);

    FileTableEntry *FdInfo(int fd) { return mFileTable[fd]; }
    FilePosition *FdPos(int fd) { return &FdInfo(fd)->currPos; }
    FileAttr *FdAttr(int fd) { return &FdInfo(fd)->fattr; }
    ChunkBuffer *FdBuffer(int fd) { return &FdInfo(fd)->buffer; }
    ChunkAttr *GetCurrChunk(int fd) {
	return &FdInfo(fd)->cattr[FdPos(fd)->chunkNum];
    }
    void ClearCurrChunkAttr(int fd) {
        FdInfo(fd)->cattr.erase(FdPos(fd)->chunkNum);
    }

    /// Do the work for an op with the metaserver; if the metaserver
    /// dies in the middle, retry the op a few times before giving up.
    int DoMetaOpWithRetry(KfsOp *op);

    /// Do the work for pipelined read: send a few
    /// requests to plumb the pipe and then whenever an op finishes,
    /// submit a new one.
    int DoPipelinedRead(int fd, std::vector<ReadOp *> &ops, TcpSocket *sock);

    int DoPipelinedWrite(int fd, std::vector<WritePrepareOp *> &ops, TcpSocket *masterSock);

    /// Helpers for pipelined write
    int AllocateWriteId(int fd, off_t offset, size_t numBytes, 
                        bool isForRecordAppend, std::vector<WriteInfo> &writeId, 
                        TcpSocket *masterSock);

    int PushData(int fd, vector<WritePrepareOp *> &ops, 
                 uint32_t start, uint32_t count, size_t &numBytes, TcpSocket *masterSock);

    int SendCommit(int fd, off_t offset, size_t numBytes, vector<uint32_t> &checksums,
                   vector<WriteInfo> &writeId, TcpSocket *masterSock,
                   WriteSyncOp &sop);

    int GetCommitReply(WriteSyncOp &sop, TcpSocket *masterSock);

    // this is going away...
    // int IssueCommit(int fd, std::vector<WriteInfo> &writeId, TcpSocket *masterSock);

    /// Get a response from the server, where, the response is
    /// terminated by "\r\n\r\n".
    int GetResponse(char *buf, int bufSize, int *delims, TcpSocket *sock);

    /// Given a path, get the parent fileid and the name following the
    /// trailing "/"
    int GetPathComponents(const char *pathname, kfsFileId_t *parentFid,
			  std::string &name);

    /// File table management utilities: find a free entry in the
    /// table, find the entry corresponding to a pathname, "mark" an
    /// entry in the table as in use, and "mark" an entry in the table
    /// as free.
    int FindFreeFileTableEntry();

    bool IsFileTableEntryValid(int fte);

    /// Wrapper function that calls LookupFileTableEntry with the parentFid
    int LookupFileTableEntry(const char *pathname);

    /// Return the file table entry corresponding to parentFid/name,
    /// where "name" is either a file or directory that resides in
    /// directory corresponding to parentFid.
    int LookupFileTableEntry(kfsFileId_t parentFid, const char *name);

    /// Given a parent fid and name, get the corresponding entry in
    /// the file table.  Note: if needed, attributes will be
    /// downloaded from the server.
    int Lookup(kfsFileId_t parentFid, const char *name);

    // name -- is the last component of the pathname
    int ClaimFileTableEntry(kfsFileId_t parentFid, const char *name, std::string pathname);
    int AllocFileTableEntry(kfsFileId_t parentFid, const char *name, std::string pathname);
    void ReleaseFileTableEntry(int fte);

    /// Helper functions that interact with the leaseClerk to
    /// get/renew leases
    int GetLease(int fd, kfsChunkId_t chunkId, const std::string &pathname);
    void RenewLease(kfsChunkId_t chunkId, const std::string &pathname);
    void RelinquishLease(kfsChunkId_t chunkId);

    bool GetDataChecksums(const ServerLocation &loc, 
                          kfsChunkId_t chunkId, 
                          uint32_t *checksums);

    bool VerifyDataChecksums(int fte, const vector<uint32_t> &checksums);
    bool VerifyChecksum(ReadOp* op, TcpSocket* sock);

    int GetChunkFromReplica(const ServerLocation &loc, kfsChunkId_t chunkId,
                            int64_t chunkVersion, char *buffer);

    int ReaddirPlus(const char *pathname, kfsFileId_t dirFid, 
                    std::vector<KfsFileAttr> &result, bool computeFilesize = true,
                    bool updateClientCache = true);

    int Rmdirs(const std::string &parentDir, kfsFileId_t parentFid, const std::string &dirname, kfsFileId_t dirFid);
    int Remove(const std::string &parentDir, kfsFileId_t parentFid, const std::string &entryName);

    int AtomicRecordAppend(int fd, const char *buf, int reclen, MutexLock& lock);

    friend class PendingChunkRead;
};


// Helper functions
extern int DoOpSend(KfsOp *op, TcpSocket *sock);
extern int DoOpResponse(KfsOp *op, TcpSocket *sock);
extern int DoOpCommon(KfsOp *op, TcpSocket *sock);

}

#endif // LIBKFSCLIENT_KFSCLIENTINT_H
