//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: ChunkServer_main.cc 3107 2011-09-22 00:14:31Z sriramr $
//
// Created 2006/03/22
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

#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <openssl/hmac.h>
#include <openssl/md5.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <limits.h>

#include <string>
#include <vector>

#include "common/properties.h"
#include "libkfsIO/NetManager.h"
#include "libkfsIO/Globals.h"
#include "libkfsIO/NetErrorSimulator.h"
#include "qcdio/qcutils.h"

#include "ChunkServer.h"
#include "ClientManager.h"
#include "ChunkManager.h"
#include "ChunkSortHelper.h"
#include "Logger.h"
#include "AtomicRecordAppender.h"
#include "RemoteSyncSM.h"

using namespace KFS;
using std::string;
using std::vector;
using std::cout;
using std::endl;

static string gLogDir;
static vector<string> gChunkDirs;
static string gMD5Sum;

static ServerLocation gMetaServerLoc;
static int64_t gTotalSpace;			// max. storage space to use
static int gChunkServerClientPort;	// Port at which kfs clients connect to us
static string gChunkServerHostname;	// Our hostname to use (instead of using gethostname() )

static Properties gProp;
static const char *gClusterKey;
static int gChunkServerRackId;
static int gChunkServerCleanupOnStart;

static int ReadChunkServerProperties(char *fileName);
static void computeMD5(const char *pathname);

static void SigQuitHandler(int /* sig */)
{
    write(1, "SIGQUIT\n", 8);
    libkfsio::globalNetManager().Shutdown();
}

// Fork is more reliable, but might confuse existing scripts. Using debugger
// with fork is a little bit more involved.
// The intention here is to do graceful restart, this is not intended as an
// external "nanny" / monitoring / watchdog.
extern char **environ;

class Restarter
{
public:
    Restarter()
        : mCwd(0),
          mArgs(0),
          mEnv(0),
          mMaxGracefulRestartSeconds(60 * 6),
          mExitOnRestartFlag(false)
        {}
    ~Restarter()
        { Cleanup(); }
    bool Init(int argc, char **argv)
    {
        ::alarm(0);
        if (::signal(SIGALRM, &Restarter::SigAlrmHandler) == SIG_ERR) {
            QCUtils::FatalError("signal(SIGALRM)", errno);
        }
        Cleanup();
        if (argc < 1 || ! argv) {
            return false;
        }
        for (int len = PATH_MAX; len < PATH_MAX * 1000; len += PATH_MAX) {
            mCwd = (char*)::malloc(len);
            if (! mCwd || ::getcwd(mCwd, len)) {
                break;
            }
            const int err = errno;
            ::free(mCwd);
            mCwd = 0;
            if (err != ERANGE) {
                break;
            }
        }
        if (! mCwd) {
            return false;
        }
        mArgs = new char*[argc + 1];
        int i;
        for (i = 0; i < argc; i++) {
            if (! (mArgs[i] = ::strdup(argv[i]))) {
                Cleanup();
                return false;
            }
        }
        mArgs[i] = 0;
        char** ptr = environ;
        for (i = 0; *ptr; i++, ptr++)
            {}
        mEnv = new char*[i + 1];
        for (i = 0, ptr = environ; *ptr; ) {
            if (! (mEnv[i++] = ::strdup(*ptr++))) {
                Cleanup();
                return false;
            }
        }
        mEnv[i] = 0;
        return true;
    }
    void SetParameters(const Properties& props, string prefix)
    {
        mMaxGracefulRestartSeconds = props.getValue(
            prefix + "maxGracefulRestartSeconds",
            mMaxGracefulRestartSeconds
        );
        mExitOnRestartFlag = props.getValue(
            prefix + "exitOnRestartFlag",
            mExitOnRestartFlag
        );
    }
    string Restart()
    {
        if (! mCwd || ! mArgs || ! mEnv || ! mArgs[0] || ! mArgs[0][0]) {
            return string("not initialized");
        }
        if (! mExitOnRestartFlag) {
            struct stat res = {0};
            if (::stat(mCwd, &res) != 0) {
                return QCUtils::SysError(errno, mCwd);
            }
            if (! S_ISDIR(res.st_mode)) {
                return (mCwd + string(": not a directory"));
            }
            string execpath(mArgs[0][0] == '/' ? mArgs[0] : mCwd);
            if (mArgs[0][0] != '/') {
                if (! execpath.empty() &&
                        execpath.at(execpath.length() - 1) != '/') {
                    execpath += "/";
                }
                execpath += mArgs[0];
            } 
            if (::stat(execpath.c_str(), &res) != 0) {
                return QCUtils::SysError(errno, execpath.c_str());
            }
            if (! S_ISREG(res.st_mode)) {
                return (execpath + string(": not a file"));
            }
        }
        if (::signal(SIGALRM, &Restarter::SigAlrmHandler) == SIG_ERR) {
            QCUtils::FatalError("signal(SIGALRM)", errno);
        }
        if (mMaxGracefulRestartSeconds > 0) {
            if (sInstance) {
                return string("restart in progress");
            }
            sInstance = this;
            if (::atexit(&Restarter::RestartSelf)) {
                sInstance = 0;
                return QCUtils::SysError(errno, "atexit");
            }
            ::alarm((unsigned int)mMaxGracefulRestartSeconds);
            libkfsio::globalNetManager().Shutdown();
        } else {
            ::alarm((unsigned int)-mMaxGracefulRestartSeconds);
            Exec();
        }
        return string();
    }
private:
    char*  mCwd;
    char** mArgs;
    char** mEnv;
    int    mMaxGracefulRestartSeconds;
    bool   mExitOnRestartFlag;

    static Restarter* sInstance;

    static void FreeArgs(char** args)
    {
        if (! args) {
            return;
        }
        char** ptr = args;
        while (*ptr) {
            ::free(*ptr++);
        }
        delete [] args;
    }
    void Cleanup()
    {
        free(mCwd);
        mCwd = 0;
        FreeArgs(mArgs);
        mArgs = 0;
        FreeArgs(mEnv);
        mEnv = 0;
    }
    void Exec()
    {
        if (mExitOnRestartFlag) {
            _exit(0);
        }
#ifdef QC_OS_NAME_LINUX
        ::clearenv();
#else
        environ = 0;
#endif
        if (mEnv) {
            for (char** ptr = mEnv; *ptr; ptr++) {
                if (::putenv(*ptr)) {
                    QCUtils::FatalError("putenv", errno);
                }
            }
        }
        if (::chdir(mCwd) != 0) {
            QCUtils::FatalError(mCwd, errno);
        }
        execvp(mArgs[0], mArgs);
        QCUtils::FatalError(mArgs[0], errno);
    }
    static void RestartSelf()
    {
        if (! sInstance) {
            ::abort();
        }
        sInstance->Exec();
    }
    static void SigAlrmHandler(int /* sig */)
    {
        write(2, "SIGALRM\n", 8);
        ::abort();
    }
};
Restarter* Restarter::sInstance = 0;
static Restarter sRestarter;

string RestartChunkServer()
{
    return sRestarter.Restart();
} 

int
main(int argc, char **argv)
{
    if (argc < 2) {
        cout << "Usage: " << argv[0] <<
            " <properties file> {<msg log file>}"
            " {max log files} {max log file size}" <<
        endl;
        exit(0);
    }

    sRestarter.Init(argc, argv);
    MsgLogger::Init(argc > 2 ? argv[2] : 0);

    // set the coredump size to unlimited
    struct rlimit rlim;
    int err;
    rlim.rlim_cur = RLIM_INFINITY;
    rlim.rlim_max = RLIM_INFINITY;
    err = setrlimit(RLIMIT_CORE, &rlim);
    if (err) {
        KFS_LOG_VA_INFO("Unable to increase coredump file size: %d", err);
    }

    if (ReadChunkServerProperties(argv[1]) != 0) {
        cout << "Bad properties file: " << argv[1] << " aborting...\n";
        exit(-1);
    }

    // Initialize things...
    libkfsio::InitGlobals();

    KFS_LOG_INFO("Starting chunkserver...");
    
    // would like to limit to 200MB outstanding
    // globalNetManager().SetBacklogLimit(200 * 1024 * 1024);

    // compute the MD5 of the binary
    computeMD5(argv[0]);

    KFS_LOG_VA_INFO("md5sum to send to metaserver: %s", gMD5Sum.c_str());

    gChunkServer.Init();
    gChunkManager.Init(gChunkDirs, gTotalSpace, gProp);
    gLogger.Init(gLogDir);
    gMetaServerSM.SetMetaInfo(gMetaServerLoc, gClusterKey, gChunkServerRackId, gMD5Sum, gProp);

    signal(SIGPIPE, SIG_IGN);
    signal(SIGQUIT, SigQuitHandler);

    // gChunkServerCleanupOnStart is a debugging option---it provides
    // "silent" cleanup
    if (gChunkServerCleanupOnStart == 0) {
        gChunkManager.Restart();
    }

    gChunkServer.MainLoop(gChunkServerClientPort, gChunkServerHostname);
    NetErrorSimulatorConfigure(libkfsio::globalNetManager());

    return 0;
}

#if defined(__sun__)
static void
computeMD5(const char *pathname)
{

}

#else

static void
computeMD5(const char *pathname)
{
    MD5_CTX ctx;
    struct stat s;
    int fd;
    unsigned char md5sum[MD5_DIGEST_LENGTH];

    if (stat(pathname, &s) != 0)
	return;
    
    fd = open(pathname, O_RDONLY);
    MD5_Init(&ctx);
    unsigned char *buf = (unsigned char *) mmap(0, s.st_size, PROT_EXEC, MAP_SHARED, fd, 0);
    if (buf != NULL) {
        MD5(buf, s.st_size, md5sum);
        munmap(buf, s.st_size);
        char md5digest[2 * MD5_DIGEST_LENGTH + 1];
        md5digest[2 * MD5_DIGEST_LENGTH] = '\0';
        for (uint32_t i = 0; i < MD5_DIGEST_LENGTH; i++)
            sprintf(md5digest + i * 2, "%02x", md5sum[i]);
        gMD5Sum = md5digest;
        KFS_LOG_VA_DEBUG("md5sum calculated from binary: %s", gMD5Sum.c_str());
    }
    close(fd);
}

#endif

static bool
make_if_needed(const char *dirname, bool check)
{
    struct stat s;
    int res = stat(dirname, &s);

    if (check && (res < 0)) {
        // stat failed; maybe the drive is down.  Keep going
        cout << "Stat on dir: " << dirname  << " failed.  Moving on..." << endl;
        return true;
    }

    if ((res == 0) && S_ISDIR(s.st_mode))
	return true;

    return mkdir(dirname, 0755) == 0;
}

///
/// Read and validate the configuration settings for the chunk
/// server. The configuration file is assumed to contain lines of the
/// form: xxx.yyy.zzz = <value>
/// @result 0 on success; -1 on failure
/// @param[in] fileName File that contains configuration information
/// for the chunk server.
///
static int
ReadChunkServerProperties(char *fileName)
{
    string::size_type curr = 0, next;
    string chunkDirPaths;

    if (gProp.loadProperties(fileName, '=', true) != 0)
        return -1;

    gMetaServerLoc.hostname = gProp.getValue("chunkServer.metaServer.hostname", "");
    gMetaServerLoc.port = gProp.getValue("chunkServer.metaServer.port", -1);
    if (!gMetaServerLoc.IsValid()) {
        cout << "Aborting...bad meta-server host or port: ";
        cout << gMetaServerLoc.hostname << ':' << gMetaServerLoc.port << '\n';
        return -1;
    }

    gChunkServerClientPort = gProp.getValue("chunkServer.clientPort", -1);
    if (gChunkServerClientPort < 0) {
        cout << "Aborting...bad client port: " << gChunkServerClientPort << '\n';
        return -1;
    }
    cout << "Using chunk server client port: " << gChunkServerClientPort << '\n';

    gChunkServerHostname = gProp.getValue("chunkServer.hostname", "");
    if (gChunkServerHostname.size() > 0)
    {
      cout << "Using chunk server hostname: " << gChunkServerHostname << '\n';
    }
    
    // Paths are space separated directories for storing chunks
    chunkDirPaths = gProp.getValue("chunkServer.chunkDir", "chunks");

    while (curr < chunkDirPaths.size()) {
        string component;

        next = chunkDirPaths.find(' ', curr);
        if (next == string::npos)
            next = chunkDirPaths.size();

        component.assign(chunkDirPaths, curr, next - curr);

        curr = next + 1;

        if ((component == " ") || (component == "")) {
            continue;
        }

        if (!make_if_needed(component.c_str(), true)) {
            cout << "Aborting...failed to create " << component << '\n';
            return -1;
        }

        // also, make the directory for holding stale and dirty chunks in each "partition"
        string staleChunkDir = GetStaleChunkPath(component);
        // if the parent dir exists, make the stale chunks directory
        make_if_needed(staleChunkDir.c_str(), false);

        string dirtyChunkDir = GetDirtyChunkPath(component);
        // if the parent dir exists, make the dirty chunks directory
        make_if_needed(dirtyChunkDir.c_str(), false);

        string sortedChunkDir = GetSortedChunkPath(component);
        make_if_needed(sortedChunkDir.c_str(), false);

        cout << "Using chunk dir = " << component << '\n';

        gChunkDirs.push_back(component);
    }

    gLogDir = gProp.getValue("chunkServer.logDir", "logs");
    if (!make_if_needed(gLogDir.c_str(), false)) {
	cout << "Aborting...failed to create " << gLogDir << '\n';
	return -1;
    }
    cout << "Using log dir = " << gLogDir << '\n';

    gTotalSpace = gProp.getValue("chunkServer.totalSpace", (long long) 0);
    cout << "Total space = " << gTotalSpace << '\n';

    gChunkServerCleanupOnStart = gProp.getValue("chunkServer.cleanupOnStart", 0);
    cout << "cleanup on start = " << gChunkServerCleanupOnStart << endl;

    gChunkServerRackId = gProp.getValue("chunkServer.rackId", (int) -1);
    // HACK alert here...

    string myS26 = NetManager::GetMyS26Address();
    // Rack id setup for aragog cluster
    if (myS26 == "10.128.16.192")
        gChunkServerRackId = 2;
    else if (myS26 == "10.128.17.0")
        gChunkServerRackId = 3;
    else if (myS26 == "10.128.17.64")
        gChunkServerRackId = 4;
    else if (myS26 == "10.128.17.128")
        gChunkServerRackId = 5;
    else if (myS26 == "10.128.17.192")
        gChunkServerRackId = 6;

    cout << "Chunk server rack: " << gChunkServerRackId << endl;

    gClusterKey = gProp.getValue("chunkServer.clusterKey", "");
    cout << "using cluster key = " << gClusterKey << endl;
    
    if (gMD5Sum == "") {
        gMD5Sum = gProp.getValue("chunkServer.md5sum", "");
    }
    gClientManager.SetTimeouts(
        gProp.getValue("chunkServer.client.ioTimeoutSec",    5 * 60),
        gProp.getValue("chunkServer.client.idleTimeoutSec", 10 * 60)
    );

    gChunkSortHelperManager.SetParameters(gProp);

    gAtomicRecordAppendManager.SetParameters(gProp);
    RemoteSyncSM::SetResponseTimeoutSec(
        gProp.getValue("chunkServer.remoteSync.responseTimeoutSec",
            RemoteSyncSM::GetResponseTimeoutSec())
    );
    RemoteSyncSM::SetTraceRequestResponse(
        gProp.getValue("chunkServer.remoteSync.traceRequestResponse", false)
    );
    NetErrorSimulatorConfigure(
        libkfsio::globalNetManager(),
        gProp.getValue("chunkServer.netErrorSimulator", "")
    );

    MsgLogger::GetLogger()->SetLogLevel(
        gProp.getValue("chunkServer.loglevel",
        MsgLogger::GetLogLevelNamePtr(MsgLogger::GetLogger()->GetLogLevel())));
    MsgLogger::GetLogger()->SetMaxLogWaitTime(0);
    MsgLogger::GetLogger()->SetParameters(gProp, "chunkServer.msgLogWriter.");
    sRestarter.SetParameters(gProp, "chunkServer.");
    

    return 0;
}
