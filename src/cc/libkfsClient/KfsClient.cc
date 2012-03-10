//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: KfsClient.cc 3485 2011-12-14 23:26:12Z sriramr $
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
// \file KfsClient.cc
// \brief Kfs Client-library code.
//
//----------------------------------------------------------------------------

#include "KfsClient.h"
#include "KfsClientInt.h"

#include "common/config.h"
#include "common/properties.h"
#include "common/log.h"
#include "meta/kfstypes.h"
#include "libkfsIO/Checksum.h"
#include "libkfsIO/Globals.h"
#include "Utils.h"
#include "KfsProtocolWorker.h"

extern "C" {
#include <signal.h>
#include <openssl/md5.h>
#include <stdlib.h>
}

#include <cstdio>
#include <cstdlib>

#include <cerrno>
#include <iostream>
#include <string>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <boost/scoped_array.hpp>

using std::string;
using std::ostringstream;
using std::istringstream;
using std::min;
using std::max;
using std::map;
using std::vector;
using std::sort;
using std::transform;
using std::random_shuffle;

using std::cout;
using std::endl;

using namespace KFS;

const int CMD_BUF_SIZE = 1024;

// Set the default timeout for server I/O's to be 3 mins for now.
// This is intentionally large so that we can do stuff in gdb and not
// have the client timeout in the midst of a debug session.
struct timeval gDefaultTimeout = {
#ifdef NDEBUG
    30
#else
    180
#endif
, 0
};


namespace {
    Properties & theProps()
    {
        static Properties p;
        return p;
    }
}

static inline void KfsClientEnsureInited()
{
    if (! MsgLogger::IsLoggerInited()) {
        // Initialize the logger
        MsgLogger::Init(NULL);
        MsgLogger::SetLevel(MsgLogger::kLogLevelINFO);
        //MsgLogger::SetLevel(MsgLogger::kLogLevelDEBUG);
    }
    libkfsio::InitGlobals();
}

KfsClientFactory *
KFS::getKfsClientFactory()
{
    return KfsClientFactory::Instance();
}

KfsClientPtr
KfsClientFactory::GetClient(const char *propFile)
{
    bool verbose = false;
#ifdef DEBUG
    verbose = true;
#endif
    if (theProps().loadProperties(propFile, '=', verbose) != 0) {
        KfsClientPtr clnt;
	return clnt;
    }

    return GetClient(theProps().getValue("metaServer.name", ""),
                     theProps().getValue("metaServer.port", -1));

}

class MatchingServer {
    ServerLocation loc;
public:
    MatchingServer(const ServerLocation &l) : loc(l) { }
    bool operator()(KfsClientPtr &clnt) const {
        return clnt->GetMetaserverLocation() == loc;
    }
    bool operator()(const ServerLocation &other) const {
        return other == loc;
    }
};

KfsClientPtr
KfsClientFactory::GetClient(const std::string metaServerHost, int metaServerPort)
{
    vector<KfsClientPtr>::iterator iter;
    ServerLocation loc(metaServerHost, metaServerPort);

    iter = find_if(mClients.begin(), mClients.end(), MatchingServer(loc));
    if (iter != mClients.end())
        return *iter;

    KfsClientPtr clnt;

    clnt.reset(new KfsClient());

    clnt->Init(metaServerHost, metaServerPort);
    if (clnt->IsInitialized())
        mClients.push_back(clnt);
    else
        clnt.reset();

    return clnt;
}


KfsClient::KfsClient()
{
    mImpl = new KfsClientImpl();
}

KfsClient::~KfsClient()
{
    delete mImpl;
}

void
KfsClient::SetLogLevel(string logLevel)
{
    if (logLevel == "DEBUG")
        MsgLogger::SetLevel(MsgLogger::kLogLevelDEBUG);
    else if (logLevel == "INFO")
        MsgLogger::SetLevel(MsgLogger::kLogLevelINFO);
    else if (logLevel == "WARN")
        MsgLogger::SetLevel(MsgLogger::kLogLevelWARN);
}

int 
KfsClient::Init(const std::string metaServerHost, int metaServerPort)
{
    return mImpl->Init(metaServerHost, metaServerPort);
}

bool 
KfsClient::IsInitialized()
{
    return mImpl->IsInitialized();
}

void
KfsClient::SetUGI(const string &userName, const string &groupName)
{
    mImpl->SetUGI(userName, groupName);
}

int
KfsClient::Cd(const char *pathname)
{
    return mImpl->Cd(pathname);
}

string
KfsClient::GetCwd()
{
    return mImpl->GetCwd();
}

int
KfsClient::Mkdirs(const char *pathname, int permissions)
{
    return mImpl->Mkdirs(pathname, permissions);
}

int 
KfsClient::Mkdir(const char *pathname, int permissions)
{
    return mImpl->Mkdir(pathname, permissions);
}

int 
KfsClient::Rmdir(const char *pathname)
{
    return mImpl->Rmdir(pathname);
}

int 
KfsClient::Rmdirs(const char *pathname)
{
    return mImpl->Rmdirs(pathname);
}

int 
KfsClient::RmdirsFast(const char *pathname)
{
    return mImpl->RmdirsFast(pathname);
}

int 
KfsClient::Readdir(const char *pathname, std::vector<std::string> &result)
{
    return mImpl->Readdir(pathname, result);
}

int 
KfsClient::ReaddirPlus(const char *pathname, std::vector<KfsFileAttr> &result)
{
    return mImpl->ReaddirPlus(pathname, result);
}

int 
KfsClient::GetDirSummary(const char *pathname, uint64_t &numFiles, uint64_t &numBytes)
{
    return mImpl->GetDirSummary(pathname, numFiles, numBytes);
}

int 
KfsClient::Stat(const char *pathname, struct stat &result, bool computeFilesize)
{
    return mImpl->Stat(pathname, result, computeFilesize);
}

int 
KfsClient::GetNumChunks(const char *pathname)
{
    return mImpl->GetNumChunks(pathname);
}

int 
KfsClient::UpdateFilesize(int fd)
{
    return mImpl->UpdateFilesize(fd);
}

bool 
KfsClient::Exists(const char *pathname)
{
    return mImpl->Exists(pathname);
}

bool 
KfsClient::IsFile(const char *pathname)
{
    return mImpl->IsFile(pathname);
}

bool 
KfsClient::IsDirectory(const char *pathname)
{
    return mImpl->IsDirectory(pathname);
}

bool 
KfsClient::IsEof(int fd)
{
    return mImpl->IsEof(fd);
}

int
KfsClient::EnumerateBlocks(const char *pathname)
{
    return mImpl->EnumerateBlocks(pathname);
}

bool
KfsClient::CompareChunkReplicas(const char *pathname, string &md5sum)
{
    return mImpl->CompareChunkReplicas(pathname, md5sum);
}

bool
KfsClient::VerifyDataChecksums(const char *pathname, const vector<uint32_t> &checksums)
{
    return mImpl->VerifyDataChecksums(pathname, checksums);
}

bool
KfsClient::VerifyDataChecksums(int fd, off_t offset, const char *buf, off_t numBytes)
{
    return mImpl->VerifyDataChecksums(fd, offset, buf, numBytes);
}

int 
KfsClient::Create(const char *pathname, int permissions,
    int numReplicas, bool exclusive)
{
    return mImpl->Create(pathname, permissions, numReplicas, exclusive);
}

int 
KfsClient::Remove(const char *pathname)
{
    return mImpl->Remove(pathname);
}

int 
KfsClient::Rename(const char *oldpath, const char *newpath, bool overwrite)
{
    return mImpl->Rename(oldpath, newpath, overwrite);
}

int
KfsClient::CoalesceBlocks(const char *srcPath, const char *dstPath, off_t *dstStartOffset)
{
    return mImpl->CoalesceBlocks(srcPath, dstPath, dstStartOffset);
}

int 
KfsClient::SetMtime(const char *pathname, const struct timeval &mtime)
{
    return mImpl->SetMtime(pathname, mtime);
}

int 
KfsClient::Open(const char *pathname, int openFlags, int numReplicas)
{
    return mImpl->Open(pathname, openFlags, numReplicas);
}

int 
KfsClient::Fileno(const char *pathname)
{
    return mImpl->Fileno(pathname);
}

int 
KfsClient::Close(int fd)
{
    return mImpl->Close(fd);
}

int
KfsClient::RecordAppend(int fd, const char *buf, int reclen)
{
    return mImpl->RecordAppend(fd, buf, reclen);
}

int
KfsClient::AtomicRecordAppend(int fd, const char *buf, int reclen)
{
    return mImpl->AtomicRecordAppend(fd, buf, reclen);
}

void
KfsClient::EnableAsyncRW()
{
    mImpl->EnableAsyncRW();
}

void
KfsClient::DisableAsyncRW()
{
    mImpl->DisableAsyncRW();
}

int
KfsClient::ReadPrefetch(int fd, char *buf, size_t numBytes)
{
    return mImpl->ReadPrefetch(fd, buf, numBytes);
}

int
KfsClient::WaitForPrefetch()
{
    return mImpl->WaitForPrefetch();
}

ssize_t 
KfsClient::Read(int fd, char *buf, size_t numBytes)
{
    return mImpl->Read(fd, buf, numBytes);
}

ssize_t 
KfsClient::Write(int fd, const char *buf, size_t numBytes)
{
    return mImpl->Write(fd, buf, numBytes);
}

int
KfsClient::WriteAsync(int fd, const char *buf, size_t numBytes)
{
    return mImpl->WriteAsync(fd, buf, numBytes);
}

int
KfsClient::WriteAsyncCompletionHandler(int fd)
{
    return mImpl->WriteAsyncCompletionHandler(fd);
}

void
KfsClient::SkipHolesInFile(int fd)
{
    mImpl->SkipHolesInFile(fd);
}

int 
KfsClient::Sync(int fd, bool flushOnlyIfHasFullChecksumBlock)
{
    return mImpl->Sync(fd, flushOnlyIfHasFullChecksumBlock);
}

off_t 
KfsClient::Seek(int fd, off_t offset, int whence)
{
    return mImpl->Seek(fd, offset, whence);
}

off_t 
KfsClient::Seek(int fd, off_t offset)
{
    return mImpl->Seek(fd, offset, SEEK_SET);
}

off_t 
KfsClient::Tell(int fd)
{
    return mImpl->Tell(fd);
}

int 
KfsClient::Truncate(int fd, off_t offset)
{
    return mImpl->Truncate(fd, offset);
}

int
KfsClient::GetChunkIndex(int fd, char **buffer, int &indexSize)
{
    return mImpl->GetChunkIndex(fd, buffer, indexSize);
}

int
KfsClient::GetChunkIndexPrefetch(int fd)
{
    return mImpl->GetChunkIndexPrefetch(fd);
}


int 
KfsClient::PruneFromHead(int fd, off_t offset)
{
    return mImpl->PruneFromHead(fd, offset);
}

int 
KfsClient::GetDataLocation(const char *pathname, off_t start, off_t len,
                           std::vector< std::vector <std::string> > &locations)
{
    return mImpl->GetDataLocation(pathname, start, len, locations);
}

int 
KfsClient::GetDataLocation(int fd, off_t start, off_t len,
                           std::vector< std::vector <std::string> > &locations)
{
    return mImpl->GetDataLocation(fd, start, len, locations);
}

int16_t 
KfsClient::GetReplicationFactor(const char *pathname)
{
    return mImpl->GetReplicationFactor(pathname);
}

int16_t 
KfsClient::SetReplicationFactor(const char *pathname, int16_t numReplicas)
{
    return mImpl->SetReplicationFactor(pathname, numReplicas);
}

ServerLocation
KfsClient::GetMetaserverLocation() const
{
    return mImpl->GetMetaserverLocation();
}

void
KfsClient::SetDefaultIOTimeout(int nsecs)
{
    gDefaultTimeout.tv_sec = nsecs;
}

void
KfsClient::GetDefaultIOTimeout(struct timeval &timeout)
{
    timeout = gDefaultTimeout;
}

size_t
KfsClient::SetDefaultIoBufferSize(size_t size)
{
    return mImpl->SetDefaultIoBufferSize(size);
}

size_t
KfsClient::GetDefaultIoBufferSize() const
{
    return mImpl->GetDefaultIoBufferSize();
}

size_t
KfsClient::SetIoBufferSize(int fd, size_t size)
{
    return mImpl->SetIoBufferSize(fd, size);
}

size_t
KfsClient::GetIoBufferSize(int fd) const
{
    return mImpl->GetIoBufferSize(fd);
}

size_t
KfsClient::SetDefaultReadAheadSize(size_t size)
{
    return mImpl->SetDefaultReadAheadSize(size);
}

size_t
KfsClient::GetDefaultReadAheadSize() const
{
    return mImpl->GetDefaultReadAheadSize();
}

size_t
KfsClient::SetReadAheadSize(int fd, size_t size)
{
    return mImpl->SetReadAheadSize(fd, size);
}

void
KfsClient::SetEOFMark(int fd, off_t offset)
{
    mImpl->SetEOFMark(fd, offset);
}

size_t
KfsClient::GetReadAheadSize(int fd) const
{
    return mImpl->GetReadAheadSize(fd);
}

kfsChunkId_t
KfsClient::GetCurrChunkId(int fd) const
{
    return mImpl->GetCurrChunkId(fd);
}

//
// Now, the real work is done by the impl object....
//

KfsClientImpl::KfsClientImpl()
    : mPendingOp(*this),
      mFileInstance(0),
      mProtocolWorker(0),
      mMaxNumRetriesPerOp(DEFAULT_NUM_RETRIES_PER_OP)
{
    pthread_mutexattr_t mutexAttr;
    int rval;
    const int hostnamelen = 256;
    char hostname[hostnamelen];

    if (gethostname(hostname, hostnamelen)) {
        perror("gethostname: ");
        exit(-1);
    }

    mHostname = hostname;
    mUserName = "WhoDat";
    mGroupName = "nobody";

    // store the entry for "/"
    int UNUSED_ATTR rootfte = ClaimFileTableEntry(KFS::ROOTFID, "/", "/");
    assert(rootfte == 0);
    mFileTable[0]->fattr.fileId = KFS::ROOTFID;
    mFileTable[0]->fattr.isDirectory = true;

    mCwd = "/";
    mIsInitialized = false;
    mCmdSeqNum = 0;

    // Setup the mutex to allow recursive locking calls.  This
    // simplifies things when a public method (eg., read) in KFS client calls
    // another public method (eg., seek) and both need to take the lock
    rval = pthread_mutexattr_init(&mutexAttr);
    assert(rval == 0);
    rval = pthread_mutexattr_settype(&mutexAttr, PTHREAD_MUTEX_RECURSIVE);
    assert(rval == 0);
    rval = pthread_mutex_init(&mMutex, &mutexAttr);
    assert(rval == 0);

    // whenever a socket goes kaput, don't crash the app
    signal(SIGPIPE, SIG_IGN);

    // to reduce memory footprint, keep a decent size buffer; we used to have
    // 64MB before
    const size_t BUF_SIZE = min(KFS::CHUNKSIZE, size_t(4) << 20);
    mDefaultIoBufferSize  = BUF_SIZE;
    mDefaultReadAheadSize = min(BUF_SIZE, size_t(1) << 20);
    // for random # generation, seed it
    srand(getpid());
    // turn off the read-ahead thread for now
    mPendingOp.Stop();
}

KfsClientImpl::~KfsClientImpl()
{
    mPendingOp.Stop();
    delete mProtocolWorker;
    std::vector <FileTableEntry *>::iterator it = mFileTable.begin();
    while (it != mFileTable.end()) {
        delete *it++;
    }
}

int KfsClientImpl::Init(const string metaServerHost, int metaServerPort)
{
    KfsClientEnsureInited();

    mRandSeed = time(NULL);

    mMetaServerLoc.hostname = metaServerHost;
    mMetaServerLoc.port = metaServerPort;

    KFS_LOG_VA_DEBUG("Connecting to metaserver at: %s:%d",
                     metaServerHost.c_str(), metaServerPort);

    if (!mMetaServerLoc.IsValid()) {
	mIsInitialized = false;
        KFS_LOG_VA_ERROR("Invalid metaserver location %s:%d; will be unable to connect",
                         metaServerHost.c_str(), metaServerPort);
	return -1;
    }

    for (int attempt = 0; ;) {
        if (ConnectToMetaServer()) {
            mIsInitialized = true;
            break;
        }
        mIsInitialized = false;
        if (++attempt >= mMaxNumRetriesPerOp) {
            KFS_LOG_VA_ERROR("Unable to connect to metaserver at: %s:%d; retrying...",
                             metaServerHost.c_str(), metaServerPort);
            break;
        }
    }
    if (!mIsInitialized) {
        KFS_LOG_VA_ERROR("Unable to connect to metaserver at: %s:%d; giving up",
                         metaServerHost.c_str(), metaServerPort);
        return -1;
    }


    // setup the telemetry stuff...
    struct ip_mreq imreq;
    string srvIp = "10.2.0.10";
    int srvPort = 12000;
    int multicastPort = 13000;

    imreq.imr_multiaddr.s_addr = inet_addr("226.0.0.1");
    imreq.imr_interface.s_addr = INADDR_ANY; // use DEFAULT interface
    
    // will setup this for release
    mTelemetryReporter.Init(imreq, multicastPort, srvIp, srvPort);

    mIsInitialized = true;
    return 0;
}

bool
KfsClientImpl::ConnectToMetaServer()
{
    return mMetaServerSock.Connect(mMetaServerLoc) >= 0;
}


void
KfsClientImpl::SetUGI(const string &userName, const string &groupName)
{
    mUserName = userName;
    mGroupName = groupName;
}

/// A notion of "cwd" in KFS.
///
int
KfsClientImpl::Cd(const char *pathname)
{
    MutexLock l(&mMutex);

    struct stat s;
    string path = build_path(mCwd, pathname);
    int status = Stat(path.c_str(), s);

    if (status < 0) {
	KFS_LOG_VA_DEBUG("Non-existent path: %s", pathname);
	return -ENOENT;
    }

    if (!S_ISDIR(s.st_mode)) {
	KFS_LOG_VA_DEBUG("Non-existent dir: %s", pathname);
	return -ENOTDIR;
    }

    // strip the trailing '/'
    string::size_type pathlen = path.size();
    string::size_type rslash = path.rfind('/');
    if (rslash + 1 == pathlen) {
        // path looks like: /.../; so, get rid of the last '/'
        path.erase(rslash);
    }

    mCwd = path;
    return 0;
}

///
/// To allow tools to get at "pwd"
///
string
KfsClientImpl::GetCwd()
{
    return mCwd;
}


///
/// Make a directory hierarchy in KFS.
///
int
KfsClientImpl::Mkdirs(const char *pathname, int permissions)
{
    MutexLock l(&mMutex);

    int res;
    string path = pathname;
    string component;
    const char slash = '/';
    string::size_type startPos = 1, endPos;
    bool done = false;

    //
    // Walk from the root down to the last part of the path making the
    // directory hierarchy along the way.  If any of the components of
    // the path is a file, error out.
    //
    while (!done) {
        endPos = path.find(slash, startPos);
        if (endPos == string::npos) {
            done = true;
            component = pathname;
        } else {
            component = path.substr(0, endPos);
            startPos = endPos + 1;
        }
	if (Exists(component.c_str())) {
	    if (IsFile(component.c_str()))
		return -ENOTDIR;
	    continue;
	}
	res = Mkdir(component.c_str(), permissions);
        if (res == -EEXIST) {
            // when there are multiple clients trying to make the same
            // directory hierarchy, the first one wins; the subsequent
            // ones get a EEXIST and that is not fatal
            continue;
        }
	if (res < 0)
	    return res;
    }

    return 0;
}
 
///
/// Make a directory in KFS.
/// @param[in] pathname		The full pathname such as /.../dir
/// @retval 0 if mkdir is successful; -errno otherwise
int
KfsClientImpl::Mkdir(const char *pathname, int permissions)
{
    MutexLock l(&mMutex);

    kfsFileId_t parentFid;
    string dirname;
    int res = GetPathComponents(pathname, &parentFid, dirname);
    if (res < 0)
	return res;

    MkdirOp op(nextSeq(), parentFid, dirname.c_str());
    DoMetaOpWithRetry(&op);
    if (op.status < 0) {
	return op.status;
    }

    // Everything is good now...
    int fte = ClaimFileTableEntry(parentFid, dirname.c_str(), pathname);
    if (fte < 0)	// Too many open files
	return -EMFILE;

    mFileTable[fte]->fattr.fileId = op.fileId;
    // setup the times and such
    mFileTable[fte]->fattr.Init(true);

    return 0;
}

///
/// Remove a directory in KFS.
/// @param[in] pathname		The full pathname such as /.../dir
/// @retval 0 if rmdir is successful; -errno otherwise
int
KfsClientImpl::Rmdir(const char *pathname)
{
    MutexLock l(&mMutex);

    string dirname;
    kfsFileId_t parentFid;
    int res = GetPathComponents(pathname, &parentFid, dirname);
    if (res < 0)
	return res;

    int fte = LookupFileTableEntry(parentFid, dirname.c_str());
    if (fte > 0)
	ReleaseFileTableEntry(fte);
    RmdirOp op(nextSeq(), parentFid, dirname.c_str(), pathname);
    (void)DoMetaOpWithRetry(&op);
    return op.status;
}

///
/// Remove a directory hierarchy in KFS.
/// @param[in] pathname		The full pathname such as /.../dir
/// @retval 0 if rmdir is successful; -errno otherwise
int
KfsClientImpl::Rmdirs(const char *pathname)
{
    MutexLock l(&mMutex);

    vector<KfsFileAttr> entries;
    int res;

    if ((res = ReaddirPlus(pathname, entries, false)) < 0)
        return res;

    string dirname = pathname;

    int len = dirname.size();
    if (dirname[len - 1] == '/')
        dirname.erase(len - 1);

    for (size_t i = 0; i < entries.size(); i++) {
        if ((entries[i].filename == ".") || (entries[i].filename == ".."))
            continue;

        string d = dirname;
        
        d += "/" + entries[i].filename;
        if (entries[i].isDirectory) {
            res = Rmdirs(d.c_str());
        } else {
            res = Remove(d.c_str());
        }
        if (res < 0)
            break;
    }
    if (res < 0)
        return res;

    res = Rmdir(pathname);

    return res;

}

///
/// Remove a directory hierarchy in KFS.
/// @param[in] pathname		The full pathname such as /.../dir
/// @retval 0 if rmdir is successful; -errno otherwise
int
KfsClientImpl::RmdirsFast(const char *pathname)
{
    MutexLock l(&mMutex);

    vector<KfsFileAttr> entries;
    int res;

    if ((res = ReaddirPlus(pathname, entries, false)) < 0)
        return res;

    string dirname = pathname;

    int len = dirname.size();
    if (dirname[len - 1] == '/')
        dirname.erase(len - 1);

    int fte = LookupFileTableEntry(pathname);
    kfsFileId_t dirFid = mFileTable[fte]->fattr.fileId;

    for (size_t i = 0; i < entries.size(); i++) {
        if ((entries[i].filename == ".") || (entries[i].filename == ".."))
            continue;

        string d = dirname;
        
        d += "/" + entries[i].filename;
        if (entries[i].isDirectory) {
            res = Rmdirs(dirname, dirFid, entries[i].filename, entries[i].fileId);
        } else {
            res = Remove(dirname, dirFid, entries[i].filename);
        }
        if (res < 0)
            break;
    }
    if (res < 0)
        return res;

    res = Rmdir(pathname);

    return res;
}

int
KfsClientImpl::Rmdirs(const string &parentDir, kfsFileId_t parentFid,
                      const string &dirname, kfsFileId_t dirFid)
{
    vector<KfsFileAttr> entries;
    int res;
    string p = parentDir + "/" + dirname;

    // don't compute any filesize; don't update client cache
    res = ReaddirPlus(p.c_str(), dirFid, entries, false, false);
    if (res < 0)
        return res;

    for (size_t i = 0; i < entries.size(); i++) {
        if ((entries[i].filename == ".") || (entries[i].filename == ".."))
            continue;

        if (entries[i].isDirectory) {
            res = Rmdirs(p, dirFid, entries[i].filename, entries[i].fileId);
        } else {
            res = Remove(p, dirFid, entries[i].filename);
        }
        if (res < 0)
            break;
    }
    if (res < 0)
        return res;

    RmdirOp op(nextSeq(), parentFid, dirname.c_str(), p.c_str());
    (void)DoMetaOpWithRetry(&op);
    return op.status;

    return res;
}

int
KfsClientImpl::Remove(const string &dirname, kfsFileId_t dirFid, const string &filename)
{
    string pathname = dirname + "/" + filename;
    RemoveOp op(nextSeq(), dirFid, filename.c_str(), pathname.c_str());
    (void)DoMetaOpWithRetry(&op);
    return op.status;
}

///
/// Read a directory's contents.  This is analogous to READDIR in
/// NFS---just reads the directory contents and returns the names;
/// you'll need to lookup the attributes next.  The resulting
/// directory entries are sorted lexicographically.
///
/// XXX NFS READDIR also returns the file ids, and we should do
/// the same here.
///
/// @param[in] pathname	The full pathname such as /.../dir
/// @param[out] result	The filenames in the directory
/// @retval 0 if readdir is successful; -errno otherwise
int
KfsClientImpl::Readdir(const char *pathname, vector<string> &result)
{
    MutexLock l(&mMutex);

    int fte = LookupFileTableEntry(pathname);
    if (fte < 0) {
	// open the directory for reading
	fte = Open(pathname, O_RDONLY);
    }

    if (fte < 0)
	return fte;

    if (!mFileTable[fte]->fattr.isDirectory)
	return -ENOTDIR;

    kfsFileId_t dirFid = mFileTable[fte]->fattr.fileId;

    ReaddirOp op(nextSeq(), dirFid);
    DoMetaOpWithRetry(&op);
    int res = op.status;
    if (res < 0)
	return res;

    istringstream ist;
    char filename[MAX_FILENAME_LEN];
    assert(op.contentBuf != NULL);
    ist.str(op.contentBuf);
    result.resize(op.numEntries);
    for (int i = 0; i < op.numEntries; ++i) {
	// ist >> result[i];
	ist.getline(filename, MAX_FILENAME_LEN);
	result[i] = filename;
        // KFS_LOG_VA_DEBUG("Entry: %s", filename);
    }
    sort(result.begin(), result.end());
    return res;
}

///
/// Read a directory's contents and get the attributes.  This is
/// analogous to READDIRPLUS in NFS.  The resulting directory entries
/// are sort lexicographically.
///
/// @param[in] pathname	The full pathname such as /.../dir
/// @param[out] result	The filenames in the directory and their attributes
/// @retval 0 if readdir is successful; -errno otherwise
int
KfsClientImpl::ReaddirPlus(const char *pathname, vector<KfsFileAttr> &result,
                           bool computeFilesize)
{
    MutexLock l(&mMutex);

    int fte = LookupFileTableEntry(pathname);
    if (fte < 0)	 // open the directory for reading
	fte = Open(pathname, O_RDONLY);
    if (fte < 0)
	   return fte;

    FileAttr *fa = FdAttr(fte);
    if (!fa->isDirectory)
	return -ENOTDIR;

    return ReaddirPlus(pathname, fa->fileId, result, computeFilesize);
}

int
KfsClientImpl::ReaddirPlus(const char *pathname, kfsFileId_t dirFid, 
                           vector<KfsFileAttr> &result, bool computeFilesize,
                           bool updateClientCache)
{
    ReaddirPlusOp op(nextSeq(), dirFid);
    (void)DoMetaOpWithRetry(&op);
    int res = op.status;
    if (res < 0) {
	return res;
    }

    vector<FileChunkInfo> fileChunkInfo;
    istringstream ist;
    string entryInfo;
    boost::scoped_array<char> line;
    int count = 0, linelen = 1 << 20, numchars;
    const string entryDelim = "Begin-entry";
    string s(op.contentBuf, op.contentLength);

    ist.str(s);

    KFS_LOG_VA_DEBUG("# of entries: %d", op.numEntries);

    line.reset(new char[linelen]);

    // the format is:
    // Begin-entry <values> Begin-entry <values>
    // the last entry doesn't have a end-marker
    while (count < op.numEntries) {
        ist.getline(line.get(), linelen);

        numchars = ist.gcount();
        if (numchars != 0) {
            if (line[numchars - 2] == '\r')
                line[numchars - 2] = '\0';

            KFS_LOG_VA_DEBUG("entry: %s", line.get());

            if (line.get() != entryDelim) {
                entryInfo += line.get();
                entryInfo += "\r\n";
                continue;
            }
            // we hit a delimiter; if this is the first one, we
            // continue so that we can build up the key/value pairs
            // for the entry.
            if (entryInfo == "")
                continue;
        }
        count++;
        // sanity
        if (entryInfo == "")
            continue;

        // previous entry is all done...process it
        Properties prop;
        KfsFileAttr fattr;
        string s;
        istringstream parserStream(entryInfo);
        const char separator = ':';

        prop.loadProperties(parserStream, separator, false);
        fattr.filename = prop.getValue("Name", "");
        fattr.fileId = prop.getValue("File-handle", -1);
        s = prop.getValue("Type", "");
        fattr.isDirectory = (s == "dir");

        s = prop.getValue("M-Time", "");
        GetTimeval(s, fattr.mtime);

        s = prop.getValue("C-Time", "");
        GetTimeval(s, fattr.ctime);

        s = prop.getValue("CR-Time", "");
        GetTimeval(s, fattr.crtime);

        entryInfo = "";

        fattr.numReplicas = prop.getValue("Replication", 1);
        fattr.fileSize = prop.getValue("File-size", (off_t) -1);
        if (fattr.fileSize != -1) {
            KFS_LOG_VA_DEBUG("Got file size from server for %s: %lld", 
                             fattr.filename.c_str(), fattr.fileSize);
        }

        // get the location info for the last chunk
        FileChunkInfo lastChunkInfo(fattr.filename, fattr.fileId);

        lastChunkInfo.lastChunkOffset = prop.getValue("Chunk-offset", (off_t) 0);
        lastChunkInfo.chunkCount = prop.getValue("Chunk-count", 0);
        lastChunkInfo.numReplicas = prop.getValue("Replication", 1);
        lastChunkInfo.cattr.chunkId = prop.getValue("Chunk-handle", (kfsFileId_t) -1);
        lastChunkInfo.cattr.chunkVersion = prop.getValue("Chunk-version", (int64_t) -1);

        int numReplicas = prop.getValue("Num-replicas", 0);
        string replicas = prop.getValue("Replicas", "");

        if (replicas != "") {
            istringstream ser(replicas);
            ServerLocation loc;

            for (int i = 0; i < numReplicas; ++i) {
                ser >> loc.hostname;
                ser >> loc.port;
                lastChunkInfo.cattr.chunkServerLoc.push_back(loc);
            }
        }
        fileChunkInfo.push_back(lastChunkInfo);
        result.push_back(fattr);
    }

    if (computeFilesize) {

        for (uint32_t i = 0; i < result.size(); i++) {
            if ((fileChunkInfo[i].chunkCount == 0) || (result[i].isDirectory)) {
                result[i].fileSize = 0;
                continue;
            }

            int fte = LookupFileTableEntry(dirFid, result[i].filename.c_str());

            if (fte >= 0) {
                result[i].fileSize = mFileTable[fte]->fattr.fileSize;
            } 
        }
        ComputeFilesizes(result, fileChunkInfo);

        for (uint32_t i = 0; i < result.size(); i++) 
            if (result[i].fileSize < 0)
                result[i].fileSize = 0;

    }

    // if there are too many entries in the dir, then the caller is
    // probably scanning the directory.  don't put it in the cache
    if ((result.size() > 128) || (!updateClientCache)) {
        sort(result.begin(), result.end());
        return res;
    }

    string dirname = build_path(mCwd, pathname);
    string::size_type len = dirname.size();
    if ((len > 0) && (dirname[len - 1] == '/'))
        dirname.erase(len - 1);
    
    for (uint32_t i = 0; i < result.size(); i++) {
        int fte = LookupFileTableEntry(dirFid, result[i].filename.c_str());

        if (fte >= 0) {
            // if we computed the filesize, then we stash it; otherwise, we'll
            // set the value to -1 and force a recompute later...
            mFileTable[fte]->fattr.fileSize = result[i].fileSize;
            continue;
        }

        if (fte < 0) {
            string fullpath;
            if ((result[i].filename == ".") || (result[i].filename == ".."))
                fullpath = "";
            else
                fullpath = dirname + "/" + result[i].filename;

            fte = AllocFileTableEntry(dirFid, result[i].filename.c_str(), fullpath);
            if (fte < 0)
                continue;
        }

        mFileTable[fte]->fattr.fileId = result[i].fileId;
        mFileTable[fte]->fattr.mtime = result[i].mtime;
        mFileTable[fte]->fattr.ctime = result[i].ctime;
        mFileTable[fte]->fattr.ctime = result[i].crtime;
        mFileTable[fte]->fattr.isDirectory = result[i].isDirectory;
        mFileTable[fte]->fattr.chunkCount = fileChunkInfo[i].chunkCount;
        mFileTable[fte]->fattr.numReplicas = fileChunkInfo[i].numReplicas;

        mFileTable[fte]->openMode = 0;
        // if we computed the filesize, then we stash it; otherwise, we'll
        // set the value to -1 and force a recompute later...
        mFileTable[fte]->fattr.fileSize = result[i].fileSize;
    }

    sort(result.begin(), result.end());

    return res;
}

///
/// Do du on the metaserver side; much faster than recursive traversal.
///
/// @param[in] pathname	The full pathname such as /.../dir
/// @param[out] numFiles # of files in the directory tree
/// @param[out] numBytes # of bytes used by the directory tree
/// @retval 0 if getdirsummary is successful; -errno otherwise
int
KfsClientImpl::GetDirSummary(const char *pathname, uint64_t &numFiles, uint64_t &numBytes)
{
    MutexLock l(&mMutex);

    int fte = LookupFileTableEntry(pathname);
    if (fte < 0)	 // open the directory for reading
	fte = Open(pathname, O_RDONLY);
    if (fte < 0)
	   return fte;

    FileAttr *fa = FdAttr(fte);
    if (!fa->isDirectory)
	return -ENOTDIR;

    kfsFileId_t dirFid = fa->fileId;

    GetDirSummaryOp op(nextSeq(), dirFid);
    (void)DoMetaOpWithRetry(&op);
    int res = op.status;
    if (res < 0) {
	return res;
    }
    numFiles = op.numFiles;
    numBytes = op.numBytes;
    return 0;
}

int
KfsClientImpl::Stat(const char *pathname, struct stat &result, bool computeFilesize)
{
    MutexLock l(&mMutex);

    KfsFileAttr kfsattr;

    int fte = LookupFileTableEntry(pathname);

    if (fte >= 0) {
	kfsattr = mFileTable[fte]->fattr;
    }

    // either we don't have the attributes cached or it is a file and
    // we are asked to compute the size and we don't know the size,
    // for directories, the metaserver keeps track of the size of the
    // dir tree; whenever it is stat'ed we should lookup the updated
    // size of the dir. tree
    if ((fte < 0) || kfsattr.isDirectory ||
        (computeFilesize && (kfsattr.fileSize < 0))) {
	kfsFileId_t parentFid;
	string filename;
	int res = GetPathComponents(pathname, &parentFid, filename);
	if (res == 0) {
	    res = LookupAttr(parentFid, filename.c_str(), kfsattr, computeFilesize);
        }
	if (res < 0)
	    return res;
    }

    KFS_LOG_VA_DEBUG("Size of %s is %d", pathname, kfsattr.fileSize);

    memset(&result, 0, sizeof (struct stat));
    result.st_mode = kfsattr.isDirectory ? S_IFDIR : S_IFREG;
    result.st_size = kfsattr.fileSize;
    result.st_atime = kfsattr.crtime.tv_sec;
    result.st_mtime = kfsattr.mtime.tv_sec;
    result.st_ctime = kfsattr.ctime.tv_sec;
    return 0;
}

int
KfsClientImpl::GetNumChunks(const char *pathname)
{
    MutexLock l(&mMutex);
    kfsFileId_t parentFid;
    string filename;
    
    int res = GetPathComponents(pathname, &parentFid, filename);
    if (res == 0) {
        LookupOp op(nextSeq(), parentFid, filename.c_str());

        (void)DoMetaOpWithRetry(&op);
        if (op.status < 0)
            return op.status;
        
        return op.fattr.chunkCount;

    }
    return -1;
}


bool
KfsClientImpl::Exists(const char *pathname)
{
    MutexLock l(&mMutex);

    struct stat dummy;

    return Stat(pathname, dummy, false) == 0;
}

bool
KfsClientImpl::IsFile(const char *pathname)
{
    MutexLock l(&mMutex);

    struct stat statInfo;

    if (Stat(pathname, statInfo, false) != 0)
	return false;
    
    return S_ISREG(statInfo.st_mode);
}

bool
KfsClientImpl::IsDirectory(const char *pathname)
{
    MutexLock l(&mMutex);

    struct stat statInfo;

    if (Stat(pathname, statInfo, false) != 0)
	return false;
    
    return S_ISDIR(statInfo.st_mode);
}

bool
KfsClientImpl::IsEof(int fd)
{
    MutexLock l(&mMutex);

    if (!valid_fd(fd))
	return true;

    return FdPos(fd)->fileOffset >= FdAttr(fd)->fileSize;
}

int
KfsClientImpl::LookupAttr(kfsFileId_t parentFid, const char *filename,
	              KfsFileAttr &result, bool computeFilesize)
{
    MutexLock l(&mMutex);

    if (parentFid < 0)
	return -EINVAL;
    
    int fte = LookupFileTableEntry(parentFid, filename);
    LookupOp op(nextSeq(), parentFid, filename);

    (void)DoMetaOpWithRetry(&op);
    if (op.status < 0)
        return op.status;

    result = op.fattr;

    result.fileSize = op.fattr.fileSize;
    
    if ((!result.isDirectory) && computeFilesize) {
        if (result.fileSize < 0) {
            result.fileSize = ComputeFilesize(result.fileId);
            if (result.fileSize < 0) {
                // We are asked for filesize and if we can't compute it, fail
                return -EIO;
            }
        }
    }

    if (fte >= 0) {
        // if we computed the filesize, then we stash it; otherwise, we'll
        // set the value to -1 and force a recompute later...
        mFileTable[fte]->fattr.fileSize = result.fileSize;
        return 0;
    }

    // cache the entry if possible
    fte = AllocFileTableEntry(parentFid, filename, "");
    if (fte < 0)		
	return op.status;
    
    mFileTable[fte]->fattr = op.fattr;
    mFileTable[fte]->openMode = 0;
    // if we computed the filesize, then we stash it; otherwise, we'll
    // set the value to -1 and force a recompute later...
    mFileTable[fte]->fattr.fileSize = result.fileSize;

    return op.status;
}

int
KfsClientImpl::Create(const char *pathname, int permissions,
    int numReplicas, bool exclusive)
{
    MutexLock l(&mMutex);

    kfsFileId_t parentFid;
    string filename;
    int res = GetPathComponents(pathname, &parentFid, filename);
    if (res < 0) {
	KFS_LOG_VA_DEBUG("status %d for pathname %s", res, pathname);
	return res;
    }

    if (filename.size() >= MAX_FILENAME_LEN)
	return -ENAMETOOLONG;

    CreateOp op(nextSeq(), parentFid, filename.c_str(), numReplicas, exclusive);
    (void)DoMetaOpWithRetry(&op);
    if (op.status < 0) {
	KFS_LOG_VA_DEBUG("status %d from create RPC", op.status);
	return op.status;
    }

    // Everything is good now...
    int fte = ClaimFileTableEntry(parentFid, filename.c_str(), pathname);
    if (fte < 0) {	// XXX Too many open files
	KFS_LOG_VA_DEBUG("status %d from ClaimFileTableEntry", fte);
	return fte;
    }

    FileAttr *fa = FdAttr(fte);
    fa->fileId = op.fileId;
    fa->Init(false);	// is an ordinary file

    FdInfo(fte)->openMode = O_RDWR;

    return fte;
}

int
KfsClientImpl::Remove(const char *pathname)
{
    MutexLock l(&mMutex);

    kfsFileId_t parentFid;
    string filename;
    int res = GetPathComponents(pathname, &parentFid, filename);
    if (res < 0)
	return res;

    int fte = LookupFileTableEntry(parentFid, filename.c_str());
    if (fte > 0)
	ReleaseFileTableEntry(fte);

    RemoveOp op(nextSeq(), parentFid, filename.c_str(), pathname);
    (void)DoMetaOpWithRetry(&op);
    return op.status;
}

int
KfsClientImpl::Rename(const char *oldpath, const char *newpath, bool overwrite)
{
    MutexLock l(&mMutex);

    kfsFileId_t parentFid;
    string oldfilename;
    int res = GetPathComponents(oldpath, &parentFid, oldfilename);
    if (res < 0)
	return res;

    string absNewpath = build_path(mCwd, newpath);
    RenameOp op(nextSeq(), parentFid, oldfilename.c_str(),
                absNewpath.c_str(), oldpath, overwrite);
    (void)DoMetaOpWithRetry(&op);

    KFS_LOG_VA_DEBUG("Status of renaming %s -> %s is: %d", 
                     oldpath, newpath, op.status);

    // update the path cache
    if (op.status == 0) {
        int fte = LookupFileTableEntry(parentFid, oldfilename.c_str());
        if (fte > 0) {
            string oldn = string(oldpath);
            NameToFdMapIter iter = mPathCache.find(oldn);
        
            if (iter != mPathCache.end())
                mPathCache.erase(iter);

            mPathCache[absNewpath] = fte;
            mFileTable[fte]->pathname = absNewpath;
        }
    }

    return op.status;
}

int
KfsClientImpl::CoalesceBlocks(const char *srcPath, const char *dstPath, off_t *dstStartOffset)
{
    MutexLock l(&mMutex);

    CoalesceBlocksOp op(nextSeq(), srcPath, dstPath);
    (void)DoMetaOpWithRetry(&op);
    *dstStartOffset = op.dstStartOffset;
    return op.status;
}

int
KfsClientImpl::SetMtime(const char *pathname, const struct timeval &mtime)
{
    MutexLock l(&mMutex);

    SetMtimeOp op(nextSeq(), pathname, mtime);
    (void)DoMetaOpWithRetry(&op);
    return op.status;
}

int
KfsClientImpl::Fileno(const char *pathname)
{
    kfsFileId_t parentFid;
    string filename;
    int res = GetPathComponents(pathname, &parentFid, filename);
    if (res < 0)
	return res;

    return LookupFileTableEntry(parentFid, filename.c_str());
}

int
KfsClientImpl::Open(const char *pathname, int openMode, int numReplicas)
{
    MutexLock l(&mMutex);

    kfsFileId_t parentFid;
    string filename;
    int res = GetPathComponents(pathname, &parentFid, filename);
    if (res < 0)
	return res;

    if (filename.size() >= MAX_FILENAME_LEN)
	return -ENAMETOOLONG;

    LookupOp op(nextSeq(), parentFid, filename.c_str());
    (void)DoMetaOpWithRetry(&op);

    if (op.status < 0) {
	if (openMode & O_CREAT) {
	    // file doesn't exist.  Create it
	    const int fte = Create(pathname, numReplicas, openMode & O_EXCL);
            if (fte >= 0 && (openMode & O_APPEND) != 0 &&
                    ! mFileTable[fte]->fattr.isDirectory) {
                mFileTable[fte]->openMode |= O_APPEND;
            }
            return fte;
	}
	return op.status;
    } else {
        // file exists; now fail open if: O_CREAT | O_EXCL
        if ((openMode & (O_CREAT|O_EXCL)) == (O_CREAT|O_EXCL))
            return -EEXIST;
    }

    // if the app opens the same file over and over, we can use the
    // cached attributes from the first open for subsequent ones.
    const int cachedFte = LookupFileTableEntry(parentFid, filename.c_str());

    const int fte = AllocFileTableEntry(parentFid, filename.c_str(), pathname);
    if (fte < 0)		// Too many open files
	return fte;

    // O_RDONLY is 0 and we use 0 to tell if the entry isn't used
    FileTableEntry* const entry = mFileTable[fte];
    if ((openMode & O_RDWR) || (openMode & O_RDONLY))
	entry->openMode = O_RDWR;
    else if (openMode & O_WRONLY)
	entry->openMode = O_WRONLY;
    else
        // in this mode, we open the file to cache the attributes
        entry->openMode = 0;

    // We got a path...get the fattr
    entry->fattr = op.fattr;

    if ((cachedFte > 0) && (mFileTable[cachedFte]->fattr.fileSize > 0)) {
            entry->fattr.fileSize = 
                mFileTable[cachedFte]->fattr.fileSize;
    } else {
        if (entry->fattr.chunkCount > 0) {
            entry->fattr.fileSize =
                ComputeFilesize(op.fattr.fileId);
        }
    }

    if (openMode & O_TRUNC)
	Truncate(fte, 0);

    if ((openMode & O_APPEND) != 0  &&
            ! entry->fattr.isDirectory &&
            (entry->openMode & (O_RDWR | O_WRONLY)) != 0) {
	entry->openMode |= O_APPEND;
    }

    if (! entry->fattr.isDirectory &&
            ((openMode & O_RDWR) == O_RDWR || (openMode & O_RDONLY) == O_RDONLY) &&
            mDefaultReadAheadSize > 0) {
        entry->currPos.pendingChunkRead =
            new PendingChunkRead(*this, mDefaultReadAheadSize);
    }

    return fte;
}

int
KfsClientImpl::Close(int fd)
{
    KfsProtocolWorker::FileInstance fileInstance;
    KfsProtocolWorker::FileId       fileId;    
    std::string                     pathName;
    int                             status = 0;
    {
        MutexLock l(&mMutex);

        if (! valid_fd(fd)) {
	    return -EBADF;
        }
        FileTableEntry& entry = *mFileTable[fd];
        fileId = (entry.didAppend &&
            (entry.openMode & O_APPEND) != 0 &&
            mProtocolWorker
        ) ? entry.fattr.fileId : -1;
        pathName     = entry.pathname;
        fileInstance = entry.instance;
        if (entry.buffer.dirty) {
            status = FlushBuffer(fd);
            if (status > 0) {
                status = 0;
            }
            if (entry.buffer.dirty) {
                status = -EIO;
            }
        }
        CloseChunk(fd);
        KFS_LOG_VA_DEBUG("Closing filetable entry: %d", fd);
        ReleaseFileTableEntry(fd);
    }
    if (fileId <= 0) {
        return status;
    }
    const int ret = mProtocolWorker->Execute(
        KfsProtocolWorker::kRequestTypeWriteAppendClose,
        fileInstance,
        fileId,
        pathName
    );
    return (status == 0 ? ret : status);
}

void
KfsClientImpl::SkipHolesInFile(int fd)
{
    MutexLock l(&mMutex);

    if (!valid_fd(fd))
	return;
    mFileTable[fd]->skipHoles = true;
}

int
KfsClientImpl::Sync(int fd, bool flushOnlyIfHasFullChecksumBlock)
{
    MutexLock l(&mMutex);

    if (! valid_fd(fd)) {
	return -EBADF;
    }
    FileTableEntry& entry = *mFileTable[fd];
    if (entry.buffer.dirty) {
       const int status = FlushBuffer(fd, flushOnlyIfHasFullChecksumBlock);
       if (status < 0)
	   return status;
    }
    if ((entry.openMode & O_APPEND) != 0 && entry.appendPending > 0 &&
            mProtocolWorker && entry.didAppend) {
        const KfsProtocolWorker::FileId       fileId       = entry.fattr.fileId;
        const KfsProtocolWorker::FileInstance fileInstance = entry.instance;
        const string                          pathName     = entry.pathname;
        entry.appendPending = 0;
        l.Release();
        return mProtocolWorker->Execute(
            KfsProtocolWorker::kRequestTypeWriteAppend,
            fileInstance,
            fileId,
            pathName,
            0,
            0
        );
    }
    return 0;
}

int
KfsClientImpl::Truncate(int fd, off_t offset)
{
    MutexLock l(&mMutex);

    if (!valid_fd(fd))
	return -EBADF;

    // for truncation, file should be opened for writing
    if (mFileTable[fd]->openMode == O_RDONLY)
	return -EBADF;

    const int syncRes = Sync(fd);
    if (syncRes < 0) {
	return syncRes;
    }

    // invalidate buffer in case it is past new EOF
    ChunkBuffer * const cb = FdBuffer(fd);
    cb->invalidate();
    FilePosition *pos = FdPos(fd);
    pos->ResetServers();

    FileAttr *fa = FdAttr(fd);
    TruncateOp op(nextSeq(), FdInfo(fd)->pathname.c_str(), fa->fileId, offset);
    (void)DoMetaOpWithRetry(&op);
    int res = op.status;

    if (res == 0) {
	fa->fileSize = offset;
	if (fa->fileSize == 0)
	    fa->chunkCount = 0;
	// else
	// chunkcount is off...but, that is ok; it is never exposed to
	// the end-client.

	gettimeofday(&fa->mtime, NULL);
	// force a re-lookup of locations
	FdInfo(fd)->cattr.clear();
    }
    return res;
}

int
KfsClientImpl::PruneFromHead(int fd, off_t offset)
{
    MutexLock l(&mMutex);

    if (!valid_fd(fd))
	return -EBADF;

    // for truncation, file should be opened for writing
    if (mFileTable[fd]->openMode == O_RDONLY)
	return -EBADF;

    const int syncRes = Sync(fd);
    if (syncRes < 0) {
	return syncRes;
    }
    // round-down to the nearest chunk block start offset
    offset = (offset / CHUNKSIZE) * CHUNKSIZE;

    FileAttr *fa = FdAttr(fd);
    TruncateOp op(nextSeq(), FdInfo(fd)->pathname.c_str(), fa->fileId, offset);
    op.pruneBlksFromHead = true;
    (void)DoMetaOpWithRetry(&op);
    int res = op.status;

    if (res == 0) {
	// chunkcount is off...but, that is ok; it is never exposed to
	// the end-client.

	gettimeofday(&fa->mtime, NULL);
	// force a re-lookup of locations
	FdInfo(fd)->cattr.clear();
    }
    return res;
}

int
KfsClientImpl::GetChunkIndexPrefetch(int fd)
{
    MutexLock l(&mMutex);

    if (!valid_fd(fd))
	return -EBADF;

    FilePosition *pos = FdPos(fd);
    int res = -1;

    res = LocateChunk(fd, pos->chunkNum);
    if (res < 0)
        return res;

    ChunkAttr *chunk = GetCurrChunk(fd);
    if (chunk->chunkId == (kfsChunkId_t) -1)
        return -1;

    if (pos->prefetchReq != NULL) {
        // request is already pending or it doesn't make any sense to do it
        return 0;
    }

    // do a non-blocking connect
    res = OpenChunk(fd, true);
    if (res < 0)
        return -1;

    TcpSocketPtr sockPtr = pos->GetPreferredChunkServerSockPtr();    
    pos->prefetchIndexReq = new AsyncGetIndexReq(fd, sockPtr, nextSeq(), 
        chunk->chunkId);

    mAsyncer.Enqueue(pos->prefetchIndexReq);
    return 0;
}

int
KfsClientImpl::GetChunkIndex(int fd, char **buffer, int &indexSize)
{
    MutexLock l(&mMutex);

    if (!valid_fd(fd))
	return -EBADF;

    FilePosition *pos = FdPos(fd);
    int res = -1;

    if (pos->prefetchIndexReq != NULL) {
        ssize_t numIO;

        while (pos->prefetchIndexReq->inQ) {
            AsyncGetIndexReq *req;

            mAsyncer.Dequeue(&req);
            req->inQ = false;
        }
        *buffer = (char *) pos->prefetchIndexReq->buf;
        indexSize = pos->prefetchIndexReq->numDone;
        numIO = pos->prefetchIndexReq->numDone;
        pos->prefetchIndexReq->buf = NULL;
        delete pos->prefetchIndexReq;
        pos->prefetchIndexReq = NULL;
        if (numIO >= 0)
            return 0;
        // didn't work...go the sync route
        if (pos->preferredServer)
            pos->preferredServer->Close();
        pos->preferredServer = NULL;
    }

    res = LocateChunk(fd, pos->chunkNum);
    if (res < 0)
        return res;

    ChunkAttr *chunk = GetCurrChunk(fd);
    if (chunk->chunkId == (kfsChunkId_t) -1)
        return -1;
    res = OpenChunk(fd, true);
    if (res < 0)
        return -1;
    GetChunkIndexOp gci(nextSeq(), chunk->chunkId);
    
    (void) DoOpCommon(&gci, FdPos(fd)->preferredServer);
    if (gci.status >= 0) {
        *buffer = gci.contentBuf;
        indexSize = gci.indexSize;
        // give up the buffer; it is the caller's responsibility to free it
        gci.ReleaseContentBuf();
    }
    return gci.status >= 0 ? 0 : gci.status;
}

int
KfsClientImpl::GetDataLocation(const char *pathname, off_t start, off_t len,
                           vector< vector <string> > &locations)
{
    MutexLock l(&mMutex);

    int fd;

    // Non-existent
    if (!IsFile(pathname)) 
        return -ENOENT;

    // load up the fte
    fd = LookupFileTableEntry(pathname);
    if (fd < 0) {
        // Open the file and cache the attributes
        fd = Open(pathname, 0);
        // we got too many open files?
        if (fd < 0)
            return fd;
    }

    return GetDataLocation(fd, start, len, locations);
}

int
KfsClientImpl::GetDataLocation(int fd, off_t start, off_t len,
                               vector< vector <string> > &locations)
{
    MutexLock l(&mMutex);

    int res;
    // locate each chunk and get the hosts that are storing the chunk.
    for (off_t pos = start; pos < start + len; pos += KFS::CHUNKSIZE) {
        ChunkAttr *chunkAttr;
        int chunkNum = pos / KFS::CHUNKSIZE;

        if ((res = LocateChunk(fd, chunkNum)) < 0) {
            return res;
        }

        chunkAttr = &(mFileTable[fd]->cattr[chunkNum]);
        
        vector<string> hosts;
        for (vector<string>::size_type i = 0; i < chunkAttr->chunkServerLoc.size(); i++)
            hosts.push_back(chunkAttr->chunkServerLoc[i].hostname);

        locations.push_back(hosts);
    }

    return 0;
}

int16_t
KfsClientImpl::GetReplicationFactor(const char *pathname)
{
    MutexLock l(&mMutex);

    int fd;

    // Non-existent
    if (!IsFile(pathname)) 
        return -ENOENT;

    // load up the fte
    fd = LookupFileTableEntry(pathname);
    if (fd < 0) {
        // Open the file for reading...this'll get the attributes setup
        fd = Open(pathname, 0);
        // we got too many open files?
        if (fd < 0)
            return fd;
    }
    return mFileTable[fd]->fattr.numReplicas;
}

int16_t
KfsClientImpl::SetReplicationFactor(const char *pathname, int16_t numReplicas)
{
    MutexLock l(&mMutex);

    int res, fd;

    // Non-existent
    if (!Exists(pathname)) 
        return -ENOENT;

    // load up the fte
    fd = LookupFileTableEntry(pathname);
    if (fd < 0) {
        // Open the file and get the attributes cached
        fd = Open(pathname, 0);
        // we got too many open files?
        if (fd < 0)
            return fd;
    }
    ChangeFileReplicationOp op(nextSeq(), FdAttr(fd)->fileId, numReplicas);
    (void) DoMetaOpWithRetry(&op);

    if (op.status == 0) {
        FdAttr(fd)->numReplicas = op.numReplicas;
        res = op.numReplicas;
    } else
        res = op.status;

    return res;
}

void
KfsClientImpl::SetEOFMark(int fd, off_t offset)
{
    MutexLock l(&mMutex);

    if (!valid_fd(fd) || mFileTable[fd]->fattr.isDirectory)
        return;

    FdInfo(fd)->eofMark = offset;
}

off_t
KfsClientImpl::Seek(int fd, off_t offset)
{
    return Seek(fd, offset, SEEK_SET);
}

off_t
KfsClientImpl::Seek(int fd, off_t offset, int whence, bool flushIfBufDirty)
{
    MutexLock l(&mMutex);

    if (!valid_fd(fd) || mFileTable[fd]->fattr.isDirectory)
	return (off_t) -EBADF;

    FilePosition *pos = FdPos(fd);
    off_t newOff;
    switch (whence) {
    case SEEK_SET:
	newOff = offset;
	break;
    case SEEK_CUR:
	newOff = pos->fileOffset + offset;
	break;
    case SEEK_END:
	newOff = mFileTable[fd]->fattr.fileSize + offset;
	break;
    default:
	return (off_t) -EINVAL;
    }

    int32_t chunkNum = newOff / KFS::CHUNKSIZE;
    // If we are changing chunks, we need to reset the socket so that
    // it eventually points to the right place
    if (pos->chunkNum != chunkNum) {
	ChunkBuffer *cb = FdBuffer(fd);
        if (flushIfBufDirty) {
            if (cb->dirty) {
                FlushBuffer(fd);
            }
            assert(!cb->dirty);
            // better to panic than silently lose a write
            if (cb->dirty) {
                KFS_LOG_ERROR("Unable to flush data to server...aborting");
                abort();
            }
        }
        // if there is an async write for this chunk, don't close it yet
        if (mAsyncWrites.size() == 0)
            CloseChunk(fd);
        // Disconnect from all the servers we were connected for this chunk
	pos->ResetServers();
    }

    pos->fileOffset = newOff;
    pos->chunkNum = chunkNum;
    pos->chunkOffset = newOff % KFS::CHUNKSIZE;

    return newOff;
}

off_t KfsClientImpl::Tell(int fd)
{
    MutexLock l(&mMutex);

    if (!valid_fd(fd) || mFileTable[fd]->fattr.isDirectory)
	return (off_t) -EBADF;

    return mFileTable[fd]->currPos.fileOffset;
}

///
/// Send a request to the meta server to allocate a chunk.
/// @param[in] fd   The index for an entry in mFileTable[] for which
/// space should be allocated.
/// @param[in] numBytes  The # of bytes we will write to this file
/// @retval 0 if successful; -errno otherwise
///
int
KfsClientImpl::AllocChunk(int fd, bool append)
{
    FileAttr *fa = FdAttr(fd);
    assert(valid_fd(fd) && !fa->isDirectory);

    struct timeval startTime, endTime;
    double timeTaken;
    
    gettimeofday(&startTime, NULL);

    AllocateOp op(nextSeq(), fa->fileId, FdInfo(fd)->pathname);
    FilePosition *pos = FdPos(fd);
    if (!append)
        op.fileOffset = ((pos->fileOffset / KFS::CHUNKSIZE) * KFS::CHUNKSIZE);
    else
        op.append = true;

    (void) DoMetaOpWithRetry(&op);
    if (op.status < 0) {
	// KFS_LOG_VA_DEBUG("AllocChunk(%d)", op.status);
	return op.status;
    }

    if (append)
        pos->fileOffset = op.fileOffset;

    ChunkAttr chunk;
    chunk.chunkId = op.chunkId;
    chunk.chunkVersion = op.chunkVersion;
    chunk.chunkServerLoc = op.chunkServers;
    FdInfo(fd)->cattr[pos->chunkNum] = chunk;

    FdPos(fd)->ResetServers();
    // for writes, [0] is the master; that is the preferred server
    if (op.chunkServers.size() > 0) {
        FdPos(fd)->SetPreferredServer(op.chunkServers[0]);
        // if we can't connect to head of daisy chain, retry
        if (FdPos(fd)->preferredServer == NULL) {
            string s = op.chunkServers[0].ToString();
            KFS_LOG_VA_INFO("Unable to connect to %s, retrying allocation", s.c_str());
            return -EHOSTUNREACH;
        }
        int ret = SizeChunk(fd);
        if (ret < 0) {
            KFS_LOG_VA_INFO("Unable to get size for chunk %lld; error = %d; need to retry", 
                            op.chunkId, ret);
            return ret;
        }
    }

    KFS_LOG_VA_DEBUG("Fileid: %lld, chunk : %lld, version: %lld, hosted on:",
                     fa->fileId, chunk.chunkId, chunk.chunkVersion);

    for (uint32_t i = 0; i < op.chunkServers.size(); i++) {
	KFS_LOG_VA_DEBUG("%s", op.chunkServers[i].ToString().c_str());
    }

    gettimeofday(&endTime, NULL);
    
    timeTaken = (endTime.tv_sec - startTime.tv_sec) +
        (endTime.tv_usec - startTime.tv_usec) * 1e-6;

    KFS_LOG_VA_DEBUG("Total Time to allocate chunk: %.4f secs", timeTaken);

    return op.status;
}

///
/// Given a chunk of file, find out where the chunk is hosted.
/// @param[in] fd  The index for an entry in mFileTable[] for which
/// we are trying find out chunk location info.
///
/// @param[in] chunkNum  The index in
/// mFileTable[fd]->cattr[] corresponding to the chunk for
/// which we are trying to get location info.
///
///
int
KfsClientImpl::LocateChunk(int fd, int chunkNum)
{
    assert(valid_fd(fd) && !mFileTable[fd]->fattr.isDirectory);

    if (chunkNum < 0)
	return -EINVAL;

    map <int, ChunkAttr>::iterator c;
    c = mFileTable[fd]->cattr.find(chunkNum);

    // Avoid unnecessary look ups.
    if (c != mFileTable[fd]->cattr.end() && c->second.chunkId > 0)
	return 0;

    GetAllocOp op(nextSeq(), mFileTable[fd]->fattr.fileId,
		  (off_t) chunkNum * KFS::CHUNKSIZE);
    op.filename = mFileTable[fd]->pathname;
    (void)DoMetaOpWithRetry(&op);

    // take whatever the server gave you...the metaserver
    // returns the chunkid/version even if the chunk is lost
    ChunkAttr chunk;
    chunk.chunkId = op.chunkId;
    chunk.chunkVersion = op.chunkVersion;

    if (op.status < 0) {
        mFileTable[fd]->cattr[chunkNum] = chunk;
	string errstr = ErrorCodeToStr(op.status);
	KFS_LOG_VA_DEBUG("LocateChunk (%d): %s", op.status, errstr.c_str());
	return op.status;
    }

    chunk.chunkServerLoc = op.chunkServers;
    mFileTable[fd]->cattr[chunkNum] = chunk;

    return 0;
}

bool
KfsClientImpl::IsCurrChunkAttrKnown(int fd)
{
    map <int, ChunkAttr> *c = &FdInfo(fd)->cattr;
    return c->find(FdPos(fd)->chunkNum) != c->end();
}

size_t
KfsClientImpl::SetDefaultIoBufferSize(size_t size)
{
    MutexLock lock(&mMutex);
    mDefaultIoBufferSize = (size + CHECKSUM_BLOCKSIZE - 1) /
                CHECKSUM_BLOCKSIZE * CHECKSUM_BLOCKSIZE;
    return mDefaultIoBufferSize;
}

size_t
KfsClientImpl::GetDefaultIoBufferSize() const
{
    MutexLock lock(&const_cast<KfsClientImpl*>(this)->mMutex);
    return mDefaultIoBufferSize;
}

size_t
KfsClientImpl::SetIoBufferSize(int fd, size_t size)
{
    MutexLock lock(&mMutex);
    if (fd < 0 || size_t(fd) >= mFileTable.size() || ! mFileTable[fd]) {
        return 0;
    }
    ChunkBuffer * const cb = FdBuffer(fd);
    if (cb->bufsz != size) {
        if (cb->dirty) {
            FlushBuffer(fd);
        }
        if (! cb->dirty) {
            cb->invalidate();
            cb->bufsz = (size + CHECKSUM_BLOCKSIZE - 1) /
                CHECKSUM_BLOCKSIZE * CHECKSUM_BLOCKSIZE;
        }
    }
    return cb->bufsz;
}

size_t
KfsClientImpl::GetIoBufferSize(int fd) const
{
    KfsClientImpl& mutableSelf = *const_cast<KfsClientImpl*>(this);
    MutexLock lock(&mutableSelf.mMutex);
    if (fd < 0 || size_t(fd) >= mFileTable.size() || ! mFileTable[fd]) {
        return 0;
    }
    ChunkBuffer * const cb = mutableSelf.FdBuffer(fd);
    return cb->bufsz;
}

size_t
KfsClientImpl::SetDefaultReadAheadSize(size_t size)
{
    MutexLock lock(&mMutex);
    mDefaultReadAheadSize = (size + CHECKSUM_BLOCKSIZE - 1) /
                CHECKSUM_BLOCKSIZE * CHECKSUM_BLOCKSIZE;
    return mDefaultReadAheadSize;
}

size_t
KfsClientImpl::GetDefaultReadAheadSize() const
{
    MutexLock lock(&const_cast<KfsClientImpl*>(this)->mMutex);
    return mDefaultReadAheadSize;
}

size_t
KfsClientImpl::SetReadAheadSize(int fd, size_t size)
{
    MutexLock lock(&mMutex);
    if (fd < 0 || size_t(fd) >= mFileTable.size() || ! mFileTable[fd]) {
        return 0;
    }
    FilePosition& pos = *FdPos(fd);
    const size_t readAhead = min(size_t(PendingChunkRead::kMaxReadRequest),
        (size + CHECKSUM_BLOCKSIZE - 1) /
            CHECKSUM_BLOCKSIZE * CHECKSUM_BLOCKSIZE);
    if (pos.pendingChunkRead) {
        if (readAhead > 0) {
            pos.pendingChunkRead->SetReadAhead(readAhead);
        } else {
            delete pos.pendingChunkRead;
            pos.pendingChunkRead = 0;
        }
    } else if (readAhead > 0) {
        pos.pendingChunkRead = new PendingChunkRead(*this, readAhead);
    }
    return readAhead;
}

size_t
KfsClientImpl::GetReadAheadSize(int fd) const
{
    KfsClientImpl& mutableSelf = *const_cast<KfsClientImpl*>(this);
    MutexLock lock(&mutableSelf.mMutex);
    if (fd < 0 || size_t(fd) >= mFileTable.size() || ! mFileTable[fd]) {
        return 0;
    }
    const FilePosition& pos = *mutableSelf.FdPos(fd);
    return (pos.pendingChunkRead ? pos.pendingChunkRead->GetReadAhead() : 0);
}

kfsChunkId_t
KfsClientImpl::GetCurrChunkId(int fd) const
{
    KfsClientImpl& mutableSelf = *const_cast<KfsClientImpl*>(this);
    MutexLock lock(&mutableSelf.mMutex);
    if (fd < 0 || size_t(fd) >= mFileTable.size() || ! mFileTable[fd]) {
        return (kfsChunkId_t) -1;
    }
    const ChunkAttr& chunk = *mutableSelf.GetCurrChunk(fd);
    return chunk.chunkId;
}

///
/// Helper function that does the work for sending out an op to the
/// server.
///
/// @param[in] op the op to be sent out
/// @param[in] sock the socket on which we communicate with server
/// @retval 0 on success; -1 on failure
/// (On failure, op->status contains error code.)
///
int
KFS::DoOpSend(KfsOp *op, TcpSocket *sock)
{
    ostringstream os;

    if ((sock == NULL ) || (!sock->IsGood())) {
	// KFS_LOG_VA_DEBUG("Trying to do I/O on a closed socket..failing it");
	op->status = -EHOSTUNREACH;
	return -1;
    }

    op->Request(os);
    int numIO = sock->DoSynchSend(os.str().c_str(), os.str().length());
    if (numIO <= 0) {
	sock->Close();
	KFS_LOG_DEBUG("Send failed...closing socket");
	op->status = -EHOSTUNREACH;
	return -1;
    }
    if (op->contentLength > 0) {
	numIO = sock->DoSynchSend(op->contentBuf, op->contentLength);
	if (numIO <= 0) {
	    sock->Close();
	    KFS_LOG_DEBUG("Send failed...closing socket");
	    op->status = -EHOSTUNREACH;
	    return -1;
	}
    }
    return 0;
}

/// Get a response from the server.  The response is assumed to
/// terminate with "\r\n\r\n".
/// @param[in/out] buf that should be filled with data from server
/// @param[in] bufSize size of the buffer
///
/// @param[out] delims the position in the buffer where "\r\n\r\n"
/// occurs; in particular, the length of the response string that ends
/// with last "\n" character.  If the buffer got full and we couldn't
/// find "\r\n\r\n", delims is set to -1.
///
/// @param[in] sock the socket from which data should be read
/// @retval # of bytes that were read; 0/-1 if there was an error
///
static int
GetResponse(char *buf, int bufSize, int *delims, TcpSocket *sock)
{
    *delims = -1;
    int pos = 0;

    for (; ;) {
        struct timeval timeout = gDefaultTimeout;

	int nread = sock->DoSynchPeek(buf + pos, bufSize - pos, timeout);
	if (nread <= 0) {
            if (nread == -ETIMEDOUT)
                return nread;
            if (nread < 0 && (errno == EINTR || errno == EAGAIN)) {
                continue;
            }
	    return nread;
        }
	for (int i = std::max(pos, 3); i < pos + nread; i++) {
	    if ((buf[i - 3] == '\r') &&
		(buf[i - 2] == '\n') &&
		(buf[i - 1] == '\r') &&
		(buf[i] == '\n')) {
		// valid stuff is from 0..i; so, length of resulting
		// string is i+1.
                i++;
		while (pos < i) {
                    if ((nread = sock->Recv(buf + pos, i - pos)) <= 0) {
                        if (nread < 0 && (errno == EINTR || errno == EAGAIN)) {
                            continue;
                        }
                        return nread;
                    }
                    pos += nread;
                }
		*delims = i;
		if (i < bufSize) {
                    buf[i] = 0;
                }
		return i;
	    }
	}
        // Unload data from socket, otherwise peek will return immediately.
        if ((nread = sock->Recv(buf + pos, nread)) <= 0) {
            if (nread < 0 && (errno == EINTR || errno == EAGAIN)) {
                continue;
            }
            return nread;
        }
        pos += nread;
    }
    return -ENOBUFS;
}

struct CharBufInputStream :
    private std::streambuf,
    public  std::istream
{
    CharBufInputStream(const char* InBuf, int len)
        : std::streambuf(),
          std::istream(this)
    {
        char* const buf = const_cast<char*>(InBuf);
        std::streambuf::setg(buf, buf, buf + len);
    }
};

///
/// From a response, extract out seq # and content-length.
///
static void
GetSeqContentLen(const char *resp, int respLen,
	         kfsSeq_t *seq, int *contentLength, Properties& prop)
{
    CharBufInputStream ist(resp, respLen);
    const char separator = ':';

    prop.clear();
    prop.loadProperties(ist, separator, false);
    *seq = prop.getValue("Cseq", (kfsSeq_t) -1);
    *contentLength = prop.getValue("Content-length", -1);
    if (*contentLength < 0)
        *contentLength = prop.getValue("Content-Length", 0);
}

///
/// Helper function that does the work of getting a response from the
/// server and parsing it out.
///
/// @param[in] op the op for which a response is to be gotten
/// @param[in] sock the socket on which we communicate with server
/// @retval 0 on success; -1 on failure
/// (On failure, op->status contains error code.)
///
int
KFS::DoOpResponse(KfsOp *op, TcpSocket *sock)
{
    int numIO;
    char buf[CMD_BUF_SIZE];
    int nread = 0, len;
    ssize_t navail, nleft;
    kfsSeq_t resSeq;
    int contentLen;
    bool printMatchingResponse = false;
    Properties prop;

    if ((sock == NULL) || (!sock->IsGood())) {
	op->status = -EHOSTUNREACH;
	KFS_LOG_DEBUG("Trying to do I/O on a closed socket..failing it");
	return -1;
    }

    while (1) {
	memset(buf, '\0', CMD_BUF_SIZE);

	numIO = GetResponse(buf, CMD_BUF_SIZE, &len, sock);

	assert(numIO != -ENOBUFS);

	if (numIO <= 0) {
	    if (numIO == -ENOBUFS) {
		op->status = -1;
	    } else if (numIO == -ETIMEDOUT) {
		op->status = -ETIMEDOUT;
		sock->Close();
		KFS_LOG_DEBUG("Get response recv timed out...");
	    } else {
		KFS_LOG_DEBUG("Get response failed...closing socket");
		sock->Close();
		op->status = -EHOSTUNREACH;
	    }

	    return -1;
	}

	assert(len > 0);

	GetSeqContentLen(buf, len, &resSeq, &contentLen, prop);
        if (!op->enableCseqMatching) {
            resSeq = op->seq;
        }

	if (resSeq == op->seq) {
            if (printMatchingResponse) {
                KFS_LOG_VA_DEBUG("Seq #'s match (after mismatch seq): Expect: %lld, got: %lld",
                                 op->seq, resSeq);

            }
	    break;
	}
	KFS_LOG_VA_DEBUG("Seq #'s dont match: Expect: %lld, got: %lld",
                         op->seq, resSeq);
        printMatchingResponse = true;

        if (contentLen > 0) {
            struct timeval timeout = gDefaultTimeout;
            int len = sock->DoSynchDiscard(contentLen, timeout);
            if (len != contentLen) {
                sock->Close();
                op->status = -EHOSTUNREACH;
                return -1;
            }
        }
    }

    contentLen = op->contentLength;

    op->ParseResponseHeader(prop);

    if (op->contentLength == 0) {
	// restore it back: when a write op is sent out and this
	// method is invoked with the same op to get the response, the
	// op's status should get filled in; we shouldn't be stomping
	// over content length.
	op->contentLength = contentLen;
	return numIO;
    }

    // This is the annoying part...we may have read some of the data
    // related to attributes already.  So, copy them out and then read
    // whatever else is left

    if (op->contentBufLen == 0) {
	op->contentBuf = new char[op->contentLength + 1];
	op->contentBuf[op->contentLength] = '\0';
    }

    // len bytes belongs to the RPC reply.  Whatever is left after
    // stripping that data out is the data.
    navail = numIO - len;
    if (navail > 0) {
	assert(navail <= (ssize_t)op->contentLength);
	memcpy(op->contentBuf, buf + len, navail);
    }
    nleft = op->contentLength - navail;

    assert(nleft >= 0);

    if (nleft > 0) {
	struct timeval timeout = gDefaultTimeout;

	nread = sock->DoSynchRecv(op->contentBuf + navail, nleft, timeout);
	if (nread == -ETIMEDOUT) {
	    KFS_LOG_DEBUG("Recv timed out...");
	    op->status = -ETIMEDOUT;
	} else if (nread <= 0) {
	    KFS_LOG_DEBUG("Recv failed...closing socket");
	    op->status = -EHOSTUNREACH;
	    sock->Close();
	}

	if (nread <= 0) {
	    return 0;
	}
    }

    return nread + numIO;
}


///
/// Common work for each op: build a request; send it to server; get a
/// response; parse it.
///
/// @param[in] op the op to be done
/// @param[in] sock the socket on which we communicate with server
///
/// @retval # of bytes read from the server.
///
int
KFS::DoOpCommon(KfsOp *op, TcpSocket *sock)
{
    if (sock == NULL) {
	KFS_LOG_VA_DEBUG("%s: send failed; no socket", op->Show().c_str());
	assert(sock);
	return -EHOSTUNREACH;
    }

    int res = DoOpSend(op, sock);
    if (res < 0) {
	KFS_LOG_VA_DEBUG("%s: send failure code: %d", op->Show().c_str(), res);
	return res;
    }

    res = DoOpResponse(op, sock);

    if (res < 0) {
	KFS_LOG_VA_DEBUG("%s: recv failure code: %d", op->Show().c_str(), res);
	return res;
    }

    if (op->status < 0) {
	string errstr = ErrorCodeToStr(op->status);
	KFS_LOG_VA_DEBUG("%s failed with code(%d): %s", op->Show().c_str(), op->status, errstr.c_str());
    }

    return res;
}

///
/// To compute the size of a file, determine what the last chunk in
/// the file happens to be (from the meta server); then, for the last
/// chunk, find its size and then add the size of remaining chunks
/// (all of which are assumed to be full).  The reason for asking the
/// meta server about the last chunk (and simply using chunkCount) is
/// that random writes with seeks affect where the "last" chunk of the
/// file happens to be: for instance, a file could have chunkCount = 1, but
/// that chunk could be the 10th chunk in the file---the first 9
/// chunks are just holes.
//
struct RespondingServer {
    KfsClientImpl *client;
    const ChunkLayoutInfo &layout;
    int *status;
    off_t *size;
    RespondingServer(KfsClientImpl *cli, const ChunkLayoutInfo &lay,
		     off_t *sz, int *st):
	    client(cli), layout(lay), status(st), size(sz) { }
    bool operator() (ServerLocation loc)
    {
	TcpSocket sock;

        *status = -EIO;
        *size = -1;

        if (sock.Connect(loc) < 0) {
            *size = 0;
	    return false;
        }

	SizeOp sop(client->nextSeq(), layout.chunkId, layout.chunkVersion);
        sop.status = -1;
	int numIO = DoOpCommon(&sop, &sock);
	if ((numIO < 0) || (!sock.IsGood())) {
            return false;
        }

	*status = sop.status;
	if (*status >= 0)
            *size = sop.size;

        return *status >= 0;
    }
};

struct RespondingServer2 {
    KfsClientImpl *client;
    const ChunkLayoutInfo &layout;
    RespondingServer2(KfsClientImpl *cli, const ChunkLayoutInfo &lay) :
        client(cli), layout(lay) { }
    ssize_t operator() (ServerLocation loc)
    {
	TcpSocket sock;

        if (sock.Connect(loc) < 0) {
            return -1;
        }

	SizeOp sop(client->nextSeq(), layout.chunkId, layout.chunkVersion);
	int numIO = DoOpCommon(&sop, &sock);
	if (numIO < 0 && !sock.IsGood()) {
            return -1;
        }

        return sop.size;
    }
};

int
KfsClientImpl::UpdateFilesize(int fd)
{
    MutexLock l(&mMutex);

    if ((!valid_fd(fd)) || (mFileTable[fd] == NULL))
	return -EBADF;

    off_t res = ComputeFilesize(FdAttr(fd)->fileId);
    if (res >= 0) {
        FdAttr(fd)->fileSize = res;
    }
    return 0;
}

off_t
KfsClientImpl::ComputeFilesize(kfsFileId_t kfsfid)
{
    GetLayoutOp lop(nextSeq(), kfsfid);
    (void)DoMetaOpWithRetry(&lop);
    if (lop.status < 0) {
        KFS_LOG_VA_INFO("Unable to compute filesize for: %lld", kfsfid);
	return -1;
    }

    if (lop.ParseLayoutInfo()) {
	KFS_LOG_DEBUG("Unable to parse layout info");
	return -1;
    }

    if (lop.chunks.size() == 0)
	return 0;

    vector <ChunkLayoutInfo>::reverse_iterator last = lop.chunks.rbegin();
    off_t filesize = last->fileOffset;
    off_t endsize = 0;
    int rstatus = 0;
    RespondingServer responder(this, *last, &endsize, &rstatus);
    vector <ServerLocation>::iterator s =
	    find_if(last->chunkServers.begin(), last->chunkServers.end(),
			    responder);

    if (s == last->chunkServers.end()) {
        // means can't get size from any of the chunkservers
        KFS_LOG_VA_INFO("Unable to connect to any server for size on fid: %lld",
                        kfsfid);
        return -1;
    }

    // Here, we connected to someone to get the size
    if (rstatus < 0) {
        KFS_LOG_VA_INFO("Unable to get size for %lld: RespondingServer status %d", 
                        kfsfid, rstatus);
        return -1;
    }

    if ((filesize == 0) && (endsize == 0) && (lop.chunks.size() > 0)) {
        // Make sure that the filesize is really 0: the file has one
        // chunk, but the size of that chunk is 0.  Sanity check with
        // all the servers that is really the case
        RespondingServer2 responder2(this, *last);
        vector<ssize_t> chunksize;
        
        // Get the size for the chunk from all the responding servers
        chunksize.reserve(last->chunkServers.size());
        for (uint32_t i = 0; i < last->chunkServers.size(); i++)
            chunksize[i] = 0;
        transform(last->chunkServers.begin(), last->chunkServers.end(), chunksize.begin(), 
                  responder2);
        for (uint32_t i = 0; i < last->chunkServers.size(); i++) {
            if (chunksize[i] > 0) {
                endsize = chunksize[i];
                break;
            }
        }
    }
    filesize += endsize;

    return filesize;
}

void
KfsClientImpl::ComputeFilesizes(vector<KfsFileAttr> &fattrs, vector<FileChunkInfo> &lastChunkInfo)
{
    for (uint32_t i = 0; i < lastChunkInfo.size(); i++) {
        if (lastChunkInfo[i].chunkCount == 0)
            continue;
        if (fattrs[i].fileSize >= 0)
            continue;
        for (uint32_t j = 0; j < lastChunkInfo[i].cattr.chunkServerLoc.size(); j++) {
            TcpSocket sock;
            ServerLocation loc = lastChunkInfo[i].cattr.chunkServerLoc[j];

            // get all the filesizes we can from this server
            ComputeFilesizes(fattrs, lastChunkInfo, i, loc);
        }

        bool alldone = true;
        for (uint32_t j = i; j < fattrs.size(); j++) {
            if (fattrs[j].fileSize < 0) {
                alldone = false;
                break;
            }
        }
        if (alldone)
            break;
        
    }
}

void
KfsClientImpl::ComputeFilesizes(vector<KfsFileAttr> &fattrs, vector<FileChunkInfo> &lastChunkInfo,
                                uint32_t startIdx, const ServerLocation &loc)
{
    TcpSocket sock;
    vector<ServerLocation>::const_iterator iter;

    if (sock.Connect(loc) < 0) {
        return;
    }

   for (uint32_t i = startIdx; i < lastChunkInfo.size(); i++) {
        if (lastChunkInfo[i].chunkCount == 0)
            continue;
        if (fattrs[i].fileSize >= 0)
            continue;
        
        iter = find_if(lastChunkInfo[i].cattr.chunkServerLoc.begin(),
                       lastChunkInfo[i].cattr.chunkServerLoc.end(),
                       MatchingServer(loc));
        if (iter == lastChunkInfo[i].cattr.chunkServerLoc.end())
            continue;

        SizeOp sop(nextSeq(), lastChunkInfo[i].cattr.chunkId, lastChunkInfo[i].cattr.chunkVersion);
	int numIO = DoOpCommon(&sop, &sock);
	if (numIO < 0 && !sock.IsGood()) {
            return;
        }
        if (sop.status >= 0) {
            lastChunkInfo[i].cattr.chunkSize = sop.size;
            fattrs[i].fileSize = lastChunkInfo[i].lastChunkOffset + lastChunkInfo[i].cattr.chunkSize;
        }
   }

}


// A simple functor to match chunkserver by hostname
class ChunkserverMatcher {
    string myHostname;
public:
    ChunkserverMatcher(const string &l) :
        myHostname(l) { }
    bool operator()(const ServerLocation &loc) const {
        return loc.hostname == myHostname;
    }
};

class ChunkserverMatcherByIp {
    in_addr_t hostaddr;
public:
    ChunkserverMatcherByIp(const string &hostname) {
        hostaddr = inet_addr(hostname.c_str());
    }
    bool operator()(in_addr &l) const {
        return hostaddr == l.s_addr;
    }
};

int
KfsClientImpl::OpenChunk(int fd, bool nonblockingConnect)
{
    if (!IsCurrChunkAttrKnown(fd)) {
        FileAttr *fa = FdAttr(fd);
        KFS_LOG_VA_INFO("Open chunk on fileid: %lld fail since current chunk attr are not known",
                        fa->fileId);
	// Nothing known about this chunk
	return -EINVAL;
    }

    ChunkAttr *chunk = GetCurrChunk(fd);
    if (chunk->chunkId == (kfsChunkId_t) -1) {
        FileAttr *fa = FdAttr(fd);
        KFS_LOG_VA_INFO("Open chunk on fileid: %lld fail since current chunk id is -1",
                        fa->fileId);
	chunk->chunkSize = 0;
	// don't send bogus chunk id's
	return -EINVAL;
    }
    
    if (mFileTable[fd]->openMode & O_WRONLY) {
        // connect only to the chunk master
        FdPos(fd)->SetPreferredServer(chunk->chunkServerLoc[0], nonblockingConnect);
        return (FdPos(fd)->GetPreferredServer() == NULL) ? -EHOSTUNREACH : SizeChunk(fd);
    }
    
    // try the local server first
    vector <ServerLocation>::iterator s =
        find_if(chunk->chunkServerLoc.begin(), chunk->chunkServerLoc.end(), 
                ChunkserverMatcher(mHostname));
    if (s != chunk->chunkServerLoc.end()) {
        FdPos(fd)->SetPreferredServer(*s, nonblockingConnect);
        if (FdPos(fd)->GetPreferredServer() != NULL) {
            KFS_LOG_VA_DEBUG("Picking local server: %s", s->ToString().c_str());
            return SizeChunk(fd);
        }
    }

    vector<ServerLocation> loc = chunk->chunkServerLoc;
    
    // take out the slow servers if we can
    mTelemetryReporter.getNotification(mSlowNodes);
    bool allNodesSlow = mSlowNodes.size() > 0;
    if (allNodesSlow) {
        for (vector<ServerLocation>::size_type i = 0; i != loc.size();
             ++i) {
            vector<struct in_addr>::iterator iter = find_if(mSlowNodes.begin(), mSlowNodes.end(),
                                                            ChunkserverMatcherByIp(loc[i].hostname));
            if (iter == mSlowNodes.end()) {
                // not all nodes are slow; so, we can eliminate slow nodes
                allNodesSlow = false;
                break;
            }
        }
    }

  try_again:
    // pick one at random avoiding slow nodes
    random_shuffle(loc.begin(), loc.end());

    for (vector<ServerLocation>::size_type i = 0;
         (FdPos(fd)->GetPreferredServer() == NULL && i != loc.size());
         i++) {
        if (!allNodesSlow) {
            vector<struct in_addr>::iterator iter = find_if(mSlowNodes.begin(), mSlowNodes.end(),
                                                            ChunkserverMatcherByIp(loc[i].hostname));
            if (iter != mSlowNodes.end()) {
                KFS_LOG_VA_INFO("For chunk %lld, avoiding slow node: %s", 
                                chunk->chunkId, loc[i].ToString().c_str());
                continue;
            }
        }

        FdPos(fd)->SetPreferredServer(loc[i], nonblockingConnect);
        if (FdPos(fd)->GetPreferredServer() != NULL)
            KFS_LOG_VA_DEBUG("For chunk %lld, randomly chose: %s", 
                             chunk->chunkId, loc[i].ToString().c_str());
    }
    if (FdPos(fd)->GetPreferredServer() == NULL) {
        if (!allNodesSlow) {
            // the non-slow node isn't responding; so try one of the slow nodes
            allNodesSlow = true;
            KFS_LOG_VA_INFO("Retrying to find a server for chunk = %lld", chunk->chunkId);
            goto try_again;
        }
            
        KFS_LOG_VA_INFO("Unable to find a server for chunk = %lld", chunk->chunkId);
    }

    return (FdPos(fd)->GetPreferredServer() == NULL) ? -EHOSTUNREACH : SizeChunk(fd);
}

int
KfsClientImpl::CloseChunk(int fd)
{
    ChunkAttr *chunk = GetCurrChunk(fd);

    if ((chunk == NULL) || (chunk->chunkId < 0) || (chunk->chunkServerLoc.size() == 0))
        return 0;

    // If we have a read lease on the chunk, give that up
    RelinquishLease(chunk->chunkId);
        
    // This is for a write lease; notify the chunkservers that we are
    // not going to send any more data and so, the chunk master can
    // relinquish the write lease
    FilePosition *pos = FdPos(fd);
    TcpSocket *masterSock = pos->preferredServer;

    KFS_LOG_VA_DEBUG("Relinquishing lease on chunk=%ld, @offset = %ld, chunksize = %d",
                     chunk->chunkId, pos->fileOffset, (int) chunk->chunkSize);

    if (masterSock != NULL) {
        // close the chunk since we are moving onto the next one
        CloseOp cop(nextSeq(), chunk->chunkId);

        std::map<int, ChunkAttr>::iterator citer = FdInfo(fd)->cattr.find(pos->chunkNum);
        if (citer != FdInfo(fd)->cattr.end()) {
            cop.chunkServerLoc = citer->second.chunkServerLoc;
        }

        DoOpCommon(&cop, masterSock);
        return cop.status;
    }
    return 0;
}

int
KfsClientImpl::SizeChunk(int fd)
{
    ChunkAttr *chunk = GetCurrChunk(fd);

    assert(FdPos(fd)->preferredServer != NULL);
    if (FdPos(fd)->preferredServer == NULL) {
        KFS_LOG_VA_INFO("Size chunk on chunk %lld fail (no socket)",
                        chunk->chunkId);
        return -EHOSTUNREACH;
    }

    SizeOp op(nextSeq(), chunk->chunkId, chunk->chunkVersion);
    op.size = 0;

    (void)DoOpCommon(&op, FdPos(fd)->preferredServer);
    if (op.status >= 0)
        chunk->chunkSize = op.size;

    KFS_LOG_VA_DEBUG("Chunk: %lld, size = %zd",
	             chunk->chunkId, chunk->chunkSize);

    return op.status;
}

///
/// Wrapper for retrying ops with the metaserver.
///
int
KfsClientImpl::DoMetaOpWithRetry(KfsOp *op)
{
    int res;

    if (!mMetaServerSock.IsGood())
	ConnectToMetaServer();

    for (int attempt = 0; ;) {
        op->status = 0;
	res = DoOpCommon(op, &mMetaServerSock);
        if ((res < 0) && (op->status == 0))
            op->status = res;

	if (op->status != -EHOSTUNREACH && op->status != -ETIMEDOUT)
	    break;
        if (++attempt >= mMaxNumRetriesPerOp)
            break;
	Sleep(RETRY_DELAY_SECS);
	ConnectToMetaServer();
	// re-issue the op with a new sequence #
	op->seq = nextSeq();
    }
    return res;
}

static bool
null_fte(const FileTableEntry *ft)
{
    return (ft == NULL);
}

//
// Rank entries by access time, but putting all directories before files
//
static bool
fte_compare(const FileTableEntry *first, const FileTableEntry *second)
{
    bool dir1 = first->fattr.isDirectory;
    bool dir2 = second->fattr.isDirectory;

    if (dir1 == dir2)
        return first->lastAccessTime < second->lastAccessTime;
    else if ((!dir1) && (first->openMode == 0))
        return dir1;
    else if ((!dir2) && (second->openMode == 0))
        return dir2;

    return dir1;
}

int
KfsClientImpl::FindFreeFileTableEntry()
{
    vector <FileTableEntry *>::iterator b = mFileTable.begin();
    vector <FileTableEntry *>::iterator e = mFileTable.end();
    vector <FileTableEntry *>::iterator i = find_if(b, e, null_fte);
    if (i != e)
	return i - b;		// Use NULL entries first

    int last = mFileTable.size();
    if (last != MAX_FILES) {	// Grow vector up to max. size
        mFileTable.push_back(NULL);
        return last;
    }

    // recycle directory entries or files open for attribute caching
    vector <FileTableEntry *>::iterator oldest = min_element(b, e, fte_compare);
    if ((*oldest)->fattr.isDirectory || ((*oldest)->openMode == 0)) {
        ReleaseFileTableEntry(oldest - b);
        return oldest - b;
    }

    return -EMFILE;		// No luck
}

class FTMatcher {
    kfsFileId_t parentFid;
    string myname;
public:
    FTMatcher(kfsFileId_t f, const char *n): parentFid(f), myname(n) { }
    bool operator () (FileTableEntry *ft) {
	return (ft != NULL &&
	        ft->parentFid == parentFid &&
	        ft->name == myname);
    }
};

bool
KfsClientImpl::IsFileTableEntryValid(int fte)
{
    // The entries for files open for read/write are valid.  This is a
    // handle that is given to the application. The entries for
    // directories need to be revalidated every N secs.  The one
    // exception for directory entries is that for "/"; that is always
    // 2 and is valid.  That entry will never be deleted from the fs.
    // Any other directory can be deleted and we don't want to hold on
    // to stale entries.
    time_t now = time(NULL);

    if (((!FdAttr(fte)->isDirectory) && (FdInfo(fte)->openMode != 0)) ||
        (FdAttr(fte)->fileId == KFS::ROOTFID) ||
        (now - FdInfo(fte)->validatedTime < FILE_CACHE_ENTRY_VALID_TIME))
        return true;

    return false;
}

int
KfsClientImpl::LookupFileTableEntry(kfsFileId_t parentFid, const char *name)
{
    FTMatcher match(parentFid, name);
    vector <FileTableEntry *>::iterator i;
    i = find_if(mFileTable.begin(), mFileTable.end(), match);
    if (i == mFileTable.end())
        return -1;
    int fte = i - mFileTable.begin();

    if (IsFileTableEntryValid(fte))
        return fte;

    KFS_LOG_VA_DEBUG("Entry for <%lld, %s> is likely stale; forcing revalidation", 
                     parentFid, name);
    // the entry maybe stale; force revalidation
    ReleaseFileTableEntry(fte);

    return -1;

}

int
KfsClientImpl::LookupFileTableEntry(const char *pathname)
{
    string p(pathname);
    NameToFdMapIter iter = mPathCache.find(p);

    if (iter != mPathCache.end()) {
        int fte = iter->second;

        if (IsFileTableEntryValid(fte)) {
            assert(mFileTable[fte]->pathname == pathname);
            return fte;
        }
        ReleaseFileTableEntry(fte);
        return -1;
    }

    kfsFileId_t parentFid;
    string name;
    int res = GetPathComponents(pathname, &parentFid, name);
    if (res < 0)
	return res;

    return LookupFileTableEntry(parentFid, name.c_str());
}

int
KfsClientImpl::ClaimFileTableEntry(kfsFileId_t parentFid, const char *name,
                                   string pathname)
{
    int fte = LookupFileTableEntry(parentFid, name);
    if (fte >= 0)
	return fte;

    return AllocFileTableEntry(parentFid, name, pathname);
}

int
KfsClientImpl::AllocFileTableEntry(kfsFileId_t parentFid, const char *name,
                                   string pathname)
{
    int fte = FindFreeFileTableEntry();

    if (fte >= 0) {
        /*
          if (parentFid != KFS::ROOTFID)
            KFS_LOG_VA_INFO("Alloc'ing fte: %d for %d, %s", fte,
            parentFid, name);
        */

	mFileTable[fte] = new FileTableEntry(parentFid, name, ++mFileInstance);
        mFileTable[fte]->validatedTime = mFileTable[fte]->lastAccessTime = 
            time(NULL);
        if (pathname != "") {
            string fullpath = build_path(mCwd, pathname.c_str());
            mPathCache[pathname] = fte;
            // mFileTable[fte]->pathCacheIter = mPathCache.find(pathname);
        }
        mFileTable[fte]->pathname = pathname;
        mFileTable[fte]->buffer.bufsz = mDefaultIoBufferSize;
    }
    return fte;
}

void
KfsClientImpl::ReleaseFileTableEntry(int fte)
{
    if (mFileTable[fte]->pathname != "")
        mPathCache.erase(mFileTable[fte]->pathname);
    /*
    if (mFileTable[fte]->pathCacheIter != mPathCache.end())
        mPathCache.erase(mFileTable[fte]->pathCacheIter);
    */
    KFS_LOG_VA_DEBUG("Closing filetable entry: %d, openmode = %d, path = %s", 
                     fte, mFileTable[fte]->openMode, mFileTable[fte]->pathname.c_str());

    delete mFileTable[fte];
    mFileTable[fte] = NULL;
}

///
/// Given a parentFid and a file in that directory, return the
/// corresponding entry in the file table.  If such an entry has not
/// been seen before, download the file attributes from the server and
/// save it in the file table.
///
int
KfsClientImpl::Lookup(kfsFileId_t parentFid, const char *name)
{
    int fte = LookupFileTableEntry(parentFid, name);
    if (fte >= 0)
	return fte;

    LookupOp op(nextSeq(), parentFid, name);
    (void) DoMetaOpWithRetry(&op);
    if (op.status < 0) {
	return op.status;
    }
    // Everything is good now...
    fte = ClaimFileTableEntry(parentFid, name, "");
    if (fte < 0) // too many open files
	return -EMFILE;

    FileAttr *fa = FdAttr(fte);
    *fa = op.fattr;

    return fte;
}

///
/// Given a path, break it down into: parentFid and filename.  If the
/// path does not begin with "/", the current working directory is
/// inserted in front of it.
/// @param[in] path	The path string that needs to be extracted
/// @param[out] parentFid  The file-id corresponding to the parent dir
/// @param[out] name    The filename following the final "/".
/// @retval 0 on success; -errno on failure
///
int
KfsClientImpl::GetPathComponents(const char *pathname, kfsFileId_t *parentFid,
	                     string &name)
{
    const char slash = '/';
    string pathstr = build_path(mCwd, pathname);

    string::size_type pathlen = pathstr.size();
    if (pathlen == 0 || pathstr[0] != slash)
	return -EINVAL;

    // find the trailing '/'
    string::size_type rslash = pathstr.rfind('/');
    if (rslash + 1 == pathlen) {
	// path looks like: /.../.../; so get rid of the last '/'
	pathstr.erase(rslash);
	pathlen = pathstr.size();
	rslash = pathstr.rfind('/');
    }

    if (pathlen == 0)
	name = "/";
    else {
	// the component of the name we want is between trailing slash
	// and the end of string
	name.assign(pathstr, rslash + 1, string::npos);
	// get rid of the last component of the path as we have copied
	// it out.
	pathstr.erase(rslash + 1, string::npos);
	pathlen = pathstr.size();
    }
    if (name.size() == 0)
	return -EINVAL;

    *parentFid = KFS::ROOTFID;
    if (pathlen == 0)
	return 0;

    // Verify that the all the components in pathname leading to
    // "name" are directories.
    string::size_type start = 1;
    while (start != string::npos) {
	string::size_type next = pathstr.find(slash, start);
	if (next == string::npos)
	    break;

	if (next == start)
	    return -EINVAL;		// don't allow "//" in path
	string component(pathstr, start, next - start);
	int fte = Lookup(*parentFid, component.c_str());
	if (fte < 0)
	    return fte;
	else if (!FdAttr(fte)->isDirectory)
	    return -ENOTDIR;
	else
	    *parentFid = FdAttr(fte)->fileId;
	start = next + 1; // next points to '/'
    }

    KFS_LOG_VA_DEBUG("file-id for dir: %s (file = %s) is %lld",
	             pathstr.c_str(), name.c_str(), *parentFid);
    return 0;
}

string
KFS::ErrorCodeToStr(int status)
{

    if (status == 0)
	return "";

    char buf[4096];
    char *errptr = NULL;

#if defined (__APPLE__) || defined(__sun__)
    if (strerror_r(-status, buf, sizeof buf) == 0)
	errptr = buf;
    else {
        strcpy(buf, "<unknown error>");
        errptr = buf;
    }
#else
    if ((errptr = strerror_r(-status, buf, sizeof buf)) == NULL) {
        strcpy(buf, "<unknown error>");
        errptr = buf;
    }
#endif
    return string(errptr);

}

int
KfsClientImpl::GetLease(int fd, kfsChunkId_t chunkId, const string &pathname)
{
    int res;
    ChunkAttr *chunk = GetCurrChunk(fd);
    int prevTimeout = 0, currTimeout;

    assert(chunkId >= 0);

    // force a size recomputation for the chunk since we are trying to
    // get a read lease on it.
    chunk->chunkSize = 0;

    for (int attempt = 0; ;) {
	LeaseAcquireOp op(nextSeq(), chunkId, pathname.c_str());
	res = DoMetaOpWithRetry(&op);
	if (op.status == 0) {
            mLeaseClerk.RegisterLease(op.chunkId, op.leaseId);
            if (chunk->chunkSize <= 0) {
                FileAttr *fa = FdAttr(fd);

                res = SizeChunk(fd);

                FilePosition *pos = FdPos(fd);

                if (res < 0) {
                    if (pos && pos->preferredServer) {
                        pos->preferredServer->Close();
                        pos->preferredServer = NULL;
                    }
                    // need to re-open the chunkserver connection
                    res = OpenChunk(fd, true);
                    if (res < 0) {
                        KFS_LOG_VA_INFO("Unable to open chunk for Fileid: %lld, chunkId: %lld, error = %d",
                                        fa->fileId, chunkId, res);
                        return -EHOSTUNREACH;
                    }
                }
                // we could recompute the filesize here; but that'd be
                // too expensive---recomputing the file's size every
                // time we acquire a lease.
                string s;

                if (pos && pos->preferredServer) {
                    s = pos->GetPreferredServerLocation().ToString();
                }
                KFS_LOG_VA_DEBUG("Server = %s: Fileid: %lld, chunkId: %lld, size: %lld, chunksize: %d",
                                 s.c_str(), fa->fileId, chunkId, fa->fileSize, (int) chunk->chunkSize);
                
            }
        }
	if (op.status != -EBUSY) {
	    res = op.status;
	    break;
	}
        // if all attempts failed, we need to propogate that;
        // otherwise, we sent the RPC to the metaserver and report
        // that the lease renew was successful.
        res = -EBUSY;
        attempt++;
	KFS_LOG_STREAM_INFO << "Server says lease on chunk " << chunkId << " is busy"
            << (attempt >= mMaxNumRetriesPerOp ? "" : "...waiting") <<
        KFS_LOG_EOM;
        if (attempt >= mMaxNumRetriesPerOp)
            break;
	// Sleep(LEASE_RETRY_DELAY_SECS);
	// Server says the lease is busy...so wait; adaptively increase timeout
        currTimeout = prevTimeout + (1 << (attempt - 1));
        Sleep(currTimeout);
        prevTimeout = currTimeout;
    }
    return res;
}

void
KfsClientImpl::RenewLease(kfsChunkId_t chunkId, const string &pathname)
{
    int64_t leaseId;

    int res = mLeaseClerk.GetLeaseId(chunkId, leaseId);
    if (res < 0)
	return;

    LeaseRenewOp op(nextSeq(), chunkId, leaseId, pathname.c_str());
    res = DoOpCommon(&op, &mMetaServerSock);
    if (op.status == 0) {
	mLeaseClerk.LeaseRenewed(op.chunkId);
	return;
    }
    if (op.status == -EINVAL) {
	mLeaseClerk.UnRegisterLease(op.chunkId);
    }
}

void
KfsClientImpl::RelinquishLease(kfsChunkId_t chunkId)
{
    int64_t leaseId;

    int res = mLeaseClerk.GetLeaseId(chunkId, leaseId);
    if (res < 0)
	return;

    KFS_LOG_VA_DEBUG("sending lease relinquish for: chunk=%lld, lease=%lld", chunkId, leaseId);

    LeaseRelinquishOp op(nextSeq(), chunkId, leaseId);
    res = DoOpCommon(&op, &mMetaServerSock);
    
    mLeaseClerk.LeaseRelinquished(chunkId);
}

int
KfsClientImpl::EnumerateBlocks(const char *pathname)
{
    struct stat s;
    int res, fte;

    MutexLock l(&mMutex);

    if ((res = Stat(pathname, s, false))  < 0) {
        cout << "Unable to stat path: " << pathname << ' ' <<
            ErrorCodeToStr(res) << endl;
        return -ENOENT;
    }

    if (S_ISDIR(s.st_mode)) {
        cout << "Path: " << pathname << " is a directory" << endl;
        return -EISDIR;
    }
    
    fte = LookupFileTableEntry(pathname);
    assert(fte >= 0);

    KFS_LOG_VA_DEBUG("Fileid for %s is: %d", pathname, FdAttr(fte)->fileId);

    GetLayoutOp lop(nextSeq(), FdAttr(fte)->fileId);
    (void)DoMetaOpWithRetry(&lop);
    if (lop.status < 0) {
        cout << "Get layout failed on path: " << pathname << " "
             << ErrorCodeToStr(lop.status) << endl;
	return lop.status;
    }

    if (lop.ParseLayoutInfo()) {
        cout << "Unable to parse layout for path: " << pathname << endl;
	return -1;
    }

    off_t filesize = 0;

    for (vector<ChunkLayoutInfo>::const_iterator i = lop.chunks.begin();
         i != lop.chunks.end(); ++i) {
        RespondingServer2 responder(this, *i);
        vector<ssize_t> chunksize;
        ssize_t c;

        // Get the size for the chunk from all the responding servers
        chunksize.reserve(i->chunkServers.size());
        transform(i->chunkServers.begin(), i->chunkServers.end(), chunksize.begin(), responder);

        cout << i->fileOffset << '\t' << i->chunkId << endl;
        if (i->chunkServers.size() == 0) {
            cout << "Chunk is lost?" << endl;
            // assume chunk is full
            filesize += KFS::CHUNKSIZE;
            continue;
        }
        c = chunksize[0];
        for (uint32_t k = 0; k < i->chunkServers.size(); k++) {
            cout << "\t\t" << i->chunkServers[k].ToString() << '\t' << chunksize[k];
            if (chunksize[k] != c)
                // we got a size mismatch
                cout << "*********";
            cout << endl;
        }

        for (uint32_t k = 0; k < i->chunkServers.size(); k++) {
            if (chunksize[k] >= 0) {
                filesize += chunksize[k];
                break;
            }
        }
    }
    cout << "File size computed from chunksizes: " << filesize << endl;
    return 0;
}


bool KfsClientImpl::GetDataChecksums(const ServerLocation &loc, 
                                     kfsChunkId_t chunkId, 
                                     uint32_t *checksums)
{
    TcpSocket sock;
    GetChunkMetadataOp op(nextSeq(), chunkId);
    uint32_t numChecksums = CHUNKSIZE / CHECKSUM_BLOCKSIZE;

    if (sock.Connect(loc) < 0) {
        return false;
    }

    int numIO = DoOpCommon(&op, &sock);
    if (op.status == -KFS::EBADCKSUM) {
        KFS_LOG_STREAM_INFO << "Server " << loc.ToString()
                            << " reports checksum mismatch for scrub read on chunk = " 
                            << chunkId
                            << KFS_LOG_EOM;
        struct sockaddr_in saddr;
        char ipname[INET_ADDRSTRLEN];
        
        sock.GetPeerName((struct sockaddr *) &saddr, sizeof(struct sockaddr_in));
        inet_ntop(AF_INET, &(saddr.sin_addr), ipname, INET_ADDRSTRLEN);
            
        mTelemetryReporter.publish(saddr.sin_addr, -1.0, "CHECKSUM_MISMATCH");
    }

    if (numIO < 0 && !sock.IsGood()) {
        return false;
    }
    if (op.contentLength < numChecksums * sizeof(uint32_t)) {
        return false;
    }
    memcpy((char *) checksums, op.contentBuf, numChecksums * sizeof(uint32_t));
    return true;
}

bool
KfsClientImpl::VerifyDataChecksums(const char *pathname, const vector<uint32_t> &checksums)
{
    struct stat s;
    int res, fte;

    MutexLock l(&mMutex);

    if ((res = Stat(pathname, s, false))  < 0) {
        cout << "Unable to stat path: " << pathname << ' ' <<
            ErrorCodeToStr(res) << endl;
        return false;
    }

    if (S_ISDIR(s.st_mode)) {
        cout << "Path: " << pathname << " is a directory" << endl;
        return false;
    }
    
    fte = LookupFileTableEntry(pathname);
    assert(fte >= 0);

    return VerifyDataChecksums(fte, checksums);
}    

bool
KfsClientImpl::VerifyDataChecksums(int fd, off_t offset, const char *buf, off_t numBytes)
{
    MutexLock l(&mMutex);
    vector<uint32_t> checksums;

    if (FdAttr(fd)->isDirectory) {
        cout << "Can't verify checksums on a directory" << endl;
        return false;
    }

    boost::scoped_array<char> tempBuf;

    tempBuf.reset(new char[CHECKSUM_BLOCKSIZE]);
    for (off_t i = 0; i < numBytes; i += CHECKSUM_BLOCKSIZE) {
        uint32_t bytesToCopy = min(CHECKSUM_BLOCKSIZE, (uint32_t) (numBytes - i));
        if (bytesToCopy != CHECKSUM_BLOCKSIZE)
            memset(tempBuf.get(), 0, CHECKSUM_BLOCKSIZE);
        memcpy(tempBuf.get(), buf, bytesToCopy);
        uint32_t cksum = ComputeBlockChecksum(tempBuf.get(), CHECKSUM_BLOCKSIZE);
        
        checksums.push_back(cksum);
    }

    return VerifyDataChecksums(fd, checksums);

}

bool
KfsClientImpl::VerifyDataChecksums(int fte, const vector<uint32_t> &checksums)
{
    GetLayoutOp lop(nextSeq(), FdAttr(fte)->fileId);
    (void)DoMetaOpWithRetry(&lop);
    if (lop.status < 0) {
        cout << "Get layout failed with error: "
             << ErrorCodeToStr(lop.status) << endl;
	return false;
    }

    if (lop.ParseLayoutInfo()) {
        cout << "Unable to parse layout info!" << endl;
	return false;
    }

    int blkstart = 0;
    uint32_t numChecksums = CHUNKSIZE / CHECKSUM_BLOCKSIZE;

    for (vector<ChunkLayoutInfo>::const_iterator i = lop.chunks.begin();
         i != lop.chunks.end(); ++i) {
        boost::scoped_array<uint32_t> chunkChecksums;

        chunkChecksums.reset(new uint32_t[numChecksums]);

        for (uint32_t k = 0; k < i->chunkServers.size(); k++) {
            
            if (!GetDataChecksums(i->chunkServers[k], i->chunkId, chunkChecksums.get())) {
                KFS_LOG_VA_INFO("Didn't get checksums from server %s", i->chunkServers[k].ToString().c_str());
                return false;
            }

            bool mismatch = false;
            for (uint32_t v = 0; v < numChecksums; v++) {
                if (blkstart + v >= checksums.size())
                    break;
                if (chunkChecksums[v] != checksums[blkstart + v])  {
                    cout << "computed: " << chunkChecksums[v] << " got: " << checksums[blkstart + v] << endl;
                    KFS_LOG_VA_INFO("Checksum mismatch for block %d on server %s", blkstart + v, 
                                    i->chunkServers[k].ToString().c_str());
                    mismatch = true;
                }
            }
            if (mismatch)
                return false;
        }
        blkstart += numChecksums;
    }
    return true;
}

bool 
KfsClientImpl::CompareChunkReplicas(const char *pathname, string &md5sum)
{
    struct stat s;
    int res, fte;

    MutexLock l(&mMutex);

    if ((res = Stat(pathname, s, false))  < 0) {
        cout << "Unable to stat path: " << pathname << ' ' <<
            ErrorCodeToStr(res) << endl;
        return false;
    }

    if (S_ISDIR(s.st_mode)) {
        cout << "Path: " << pathname << " is a directory" << endl;
        return false;
    }
    
    fte = LookupFileTableEntry(pathname);

    GetLayoutOp lop(nextSeq(), FdAttr(fte)->fileId);
    (void)DoMetaOpWithRetry(&lop);
    if (lop.status < 0) {
        cout << "Get layout failed with error: "
             << ErrorCodeToStr(lop.status) << endl;
	return false;
    }

    if (lop.ParseLayoutInfo()) {
        cout << "Unable to parse layout info!" << endl;
	return false;
    }

    MD5_CTX ctx;
    unsigned char md5digest[MD5_DIGEST_LENGTH];
    bool match = true;
    boost::scoped_array<char> buf1, buf2;
    int nbytes;

    MD5_Init(&ctx);

    buf1.reset(new char [CHUNKSIZE]);
    buf2.reset(new char [CHUNKSIZE]);
    for (vector<ChunkLayoutInfo>::const_iterator i = lop.chunks.begin();
         i != lop.chunks.end(); ++i) {

        nbytes = GetChunkFromReplica(i->chunkServers[0], i->chunkId, i->chunkVersion, buf1.get());
        if (nbytes < 0) {
            KFS_LOG_VA_INFO("Didn't get data from server %s", i->chunkServers[0].ToString().c_str());
            match = false;
            continue;
        }

        KFS_LOG_VA_DEBUG("For chunk %ld size read from : %s; nbytes =  %d",
                         i->chunkId,
                         i->chunkServers[0].ToString().c_str(), nbytes);

        MD5_Update(&ctx, (unsigned char *) buf1.get(), nbytes);

        for (uint32_t k = 1; k < i->chunkServers.size(); k++) {
            int n;
            n = GetChunkFromReplica(i->chunkServers[k], i->chunkId, i->chunkVersion, buf2.get());
            if (n < 0) {
                KFS_LOG_VA_INFO("Didn't get data from server %s", i->chunkServers[k].ToString().c_str());
                match = false;
                continue;
            }
            if (nbytes != n) {
                KFS_LOG_VA_INFO("For chunk %ld size differs between <%s, %d> and <%s, %d>", 
                                i->chunkId,
                                i->chunkServers[0].ToString().c_str(), nbytes,
                                i->chunkServers[k].ToString().c_str(), n);
                match = false;
                continue;
            }
            if (memcmp(buf1.get(), buf2.get(), nbytes) != 0) {
                KFS_LOG_VA_INFO("For chunk %ld data differs between <%s, %d> and <%s, %d>", 
                                i->chunkId,
                                i->chunkServers[0].ToString().c_str(), nbytes,
                                i->chunkServers[k].ToString().c_str(), n);
                match = false;
                continue;
            }
            KFS_LOG_VA_DEBUG("For chunk %ld size read from : %s; nbytes =  %d",
                             i->chunkId,
                             i->chunkServers[k].ToString().c_str(), nbytes);
        }
    }
    MD5_Final(md5digest, &ctx);
    char md5s[2 * MD5_DIGEST_LENGTH + 1];
    md5s[2 * MD5_DIGEST_LENGTH] = '\0';
    for (uint32_t i = 0; i < MD5_DIGEST_LENGTH; i++)
        sprintf(md5s + i * 2, "%02x", md5digest[i]);
    md5sum = md5s;
    
    return match;
}

int
KfsClientImpl::GetChunkFromReplica(const ServerLocation &loc, kfsChunkId_t chunkId,
                                   int64_t chunkVersion, char *buffer)
{
    TcpSocket sock;
    int res;

    if (sock.Connect(loc) < 0) {
        return -1;
    }

    char *curr = buffer;
    int nread = 0;
    const int nparts = CHUNKSIZE / (1 << 20);
    for (int i = 0; i < nparts; i++) {
        ReadOp op(nextSeq(), chunkId, chunkVersion);
        op.numBytes = 1 << 20;
        op.AttachContentBuf(curr, op.numBytes);
        curr += op.numBytes;
        op.offset = (i * op.numBytes);

        res = DoOpCommon(&op, &sock);
        if (res >= 0) {
            // on a success, the status contains the # of bytes read
            if (op.status < 0)
                res = op.status;
            else {
                res = op.contentLength;
                nread += res;
            }
        }
        op.ReleaseContentBuf();
        op.contentLength = 0;
        if (op.status < 0)
            break;
    }
    return nread;
}

KfsClientFactory::KfsClientFactory()
{
    KfsClientEnsureInited();
    sForGdbToFindInstance = this;
}

KfsClientFactory::~KfsClientFactory()
{
    MsgLogger::Stop();
}

/* static */ KfsClientFactory *
KfsClientFactory::Instance()
{
    static KfsClientFactory instance;
    return &instance;
}

KfsClientFactory* KfsClientFactory::sForGdbToFindInstance = 0;
