//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: KfsToolsCommon.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2009/04/18
//
//
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
//----------------------------------------------------------------------------


extern "C" {
#include <unistd.h>
#include <sys/types.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/time.h>
}

#include <cstdlib>
#include <cstring>
#include <istream>
#include <sstream>
#include <iostream>    
#include <fstream>
#include <cerrno>
#include <cassert>
#include <boost/scoped_array.hpp>

#include "KfsToolsCommon.h"

using namespace std;
using boost::scoped_array;
using namespace KFS;
using namespace KFS::tools;

string
KFS::tools::getRemotePath(const std::string & host, const int & port, const std::string & path)
{
      ostringstream oss (ostringstream::out);

      oss << "'kfs://" << host << ":" << port;
      
      if (path.length() < 1 || path[0] != '/') oss << "/";
      
      oss << path << "'";
      
      return oss.str();
}

bool
KFS::tools::getInt(const string & fromString, int &toInt)
{
    if (fromString.length() < 1) return false;
    
    std::istringstream i ( fromString );

    int val;
    char c;
    
    if ( ! ( i >> val ) || i.get ( c ) )
    {
	return false;
    }
    
    toInt = val;
    return true;
}

void
KFS::tools::parseServer(const string & serverDesc, string & serverHost, int & serverPort)
{
    if (serverDesc.length() < 1) return;
    
    size_t idx = serverDesc.find(':');
    
    if (idx != string::npos)
    {
	serverHost = serverDesc.substr(0, idx);
	getInt(serverDesc.substr(idx + 1), serverPort);
    }
    else
    {
	serverHost = serverDesc;
    }
}

void
KFS::tools::getEnvServer(string & serverHost, int & serverPort)
{
    const char * envServer = getenv("KFS_SERVER_HOST");
    const char * envPort = getenv("KFS_SERVER_PORT");
    
    if (envServer)
    {
	parseServer(envServer, serverHost, serverPort);
    }
    
    if (envPort)
    {
	getInt(string(envPort), serverPort);
    }
}

bool
KFS::tools::parsePath(const string & pathDesc, string & serverHost,
		int & serverPort, string & filePath, bool & isRemote)
{
    if (pathDesc.length() < 1) return false;
    
    if (pathDesc.find("file://") == 0)
    {
	// Local path without 'file://' - 7 characters
	filePath = pathDesc.substr(7);
	
	if (filePath.length() < 1) filePath=".";
	
	isRemote = false;
	
	assert (filePath.length() > 0);
	
	return true;
    }
    else if (pathDesc.find("kfs://") != 0)
    {
	filePath = pathDesc;
	isRemote = false;
	
	assert (filePath.length() > 0);
	
	return true;
    }
    
    // Here pDesc contains only (optional) server:port and /path information
    
    size_t idx = pathDesc.find("/", 6);
    
    // No '/' found - something is wrong!
    // We don't require hostname and/or port to be provided, but we need
    // '/' to use as the beginning of the path!
    if (idx == string::npos) return false;
    
    // File path within KFS starts at '/' character
    filePath = pathDesc.substr(idx);
    idx -= 6;
    
    // In case there was no server specified (like in: 'kfs:///my_file.txt')
    // try to get its parameters from environment.
    getEnvServer(serverHost, serverPort);
    
    // But it might be specified, in which case we want to try to read it
    // from pathDesc - if it contains only host, port read from environment (if any)
    // is used.
    parseServer(pathDesc.substr(6, idx), serverHost, serverPort);
    
    isRemote = true;
    
    assert (filePath.length() > 0);
    assert (filePath[0] == '/');
    
    return true;
}

//
// Guts of the work to copy the file.
//

//
// BackupFile3 is not used?
//
// static int
// BackupFile3(KfsClientPtr kfsClient, string srcfilename, string kfsfilename)
// {
//     // 64MB buffers
//     const int bufsize = 1 << 26;
//     scoped_array<char> kfsBuf;
//     int kfsfd, nRead;
//     long long n = 0;
//     ifstream ifs;
//     int res;
// 
//     assert (kfsClient != 0);
//     
//     ifs.open(srcfilename.c_str(), ios_base::in);
//     if (!ifs) {
//         cout << "Unable to open: " << srcfilename << endl;
//         exit(-1);
//     }
// 
//     kfsfd = kfsClient->Create(kfsfilename.c_str());
//     if (kfsfd < 0) {
//         cout << "Create " << kfsfilename << " failed: " << kfsfd << endl;
//         exit(-1);
//     }
//     kfsBuf.reset(new char[bufsize]);
// 
//     while (!ifs.eof()) {
//         ifs.read(kfsBuf.get(), bufsize);
// 
//         nRead = ifs.gcount();
//         
//         if (nRead <= 0)
//             break;
//         
//         res = kfsClient->Write(kfsfd, kfsBuf.get(), nRead);
//         if (res < 0) {
//             cout << "Write failed with error code: " << res << endl;
//             exit(-1);
//         }
//         n += nRead;
//         // cout << "Wrote: " << n << endl;
//     }
//     ifs.close();
//     kfsClient->Close(kfsfd);
// 
//     return 0;
// }

static int
BackupFile2(KfsClientPtr kfsClient, string srcfilename, string kfsfilename)
{
    // 64MB buffers
    const int bufsize = 1 << 26;
    scoped_array<char> kfsBuf;
    int kfsfd, nRead;
    int srcFd;
    int res;

    assert (kfsClient != 0);
    
    if (srcfilename == "-")
        // dup stdin() and read from there
        srcFd = dup(0);
    else
        srcFd = open(srcfilename.c_str(), O_RDONLY);
    if (srcFd  < 0) {
        cout << "Unable to open: " << srcfilename << endl;
        exit(-1);
    }

    kfsfd = kfsClient->Create(kfsfilename.c_str());
    if (kfsfd < 0) {
        cout << "Create " << kfsfilename << " failed: " << kfsfd << endl;
        exit(-1);
    }
    kfsBuf.reset(new char[bufsize]);

    while (1) {
        nRead = read(srcFd, kfsBuf.get(), bufsize);
        
        if (nRead <= 0)
            break;
        
        res = kfsClient->Write(kfsfd, kfsBuf.get(), nRead);
        if (res < 0) {
            cout << "Write failed with error code: " << res << endl;
            exit(-1);
        }
    }
    close(srcFd);
    kfsClient->Close(kfsfd);

    return 0;
}

static bool
doMkdirs(KfsClientPtr kfsClient, const string & path)
{
    int res;

    assert (kfsClient != 0);
    
    res = kfsClient->Mkdirs(path.c_str());
    if ((res < 0) && (res != -EEXIST)) {
        cout << "Mkdir failed: " << ErrorCodeToStr(res) << endl;
        return false;
    }
    return true;
}

void
KFS::tools::MakeKfsLeafDir(KfsClientPtr kfsClient, const string &sourcePath, string &kfsPath)
{
    string leaf;
    string::size_type slash = sourcePath.rfind('/');

    if (!kfsClient)
    {
	cerr << "Error: No KFS client provided!\n";
	exit(-1);
    }

    // get everything after the last slash
    if (slash != string::npos) {
	leaf.assign(sourcePath, slash+1, string::npos);
    } else {
	leaf = sourcePath;
    }
    if (kfsPath[kfsPath.size()-1] != '/')
        kfsPath += "/";

    kfsPath += leaf;
    doMkdirs(kfsClient, kfsPath);
}

int
KFS::tools::BackupFile(KfsClientPtr kfsClient, const string &sourcePath, const string &kfsPath)
{
    string filename;
    string::size_type slash = sourcePath.rfind('/');

    if (!kfsClient)
    {
	cerr << "Error: No KFS client provided!\n";
	exit(-1);
    }
    
    // get everything after the last slash
    if (slash != string::npos) {
	filename.assign(sourcePath, slash+1, string::npos);
    } else {
	filename = sourcePath;
    }
    
    // for the dest side: if kfsPath is a dir, we are copying to
    // kfsPath with srcFilename; otherwise, kfsPath is a file (that
    // potentially exists) and we are ovewriting/creating it
    if (kfsClient->IsDirectory(kfsPath.c_str())) {
        string dst = kfsPath;

        if (dst[kfsPath.size() - 1] != '/')
            dst += "/";
        
        return BackupFile2(kfsClient, sourcePath, dst + filename);
    }
    
    // kfsPath is the filename that is being specified for the cp
    // target.  try to copy to there...
    return BackupFile2(kfsClient, sourcePath, kfsPath);
}

int
KFS::tools::BackupDir(KfsClientPtr kfsClient, const string &dirname, string &kfsdirname)
{
    string subdir, kfssubdir;
    DIR *dirp;
    struct dirent *fileInfo;

    if (!kfsClient)
    {
	cerr << "Error: No KFS client provided!\n";
	exit(-1);
    }
    
    if ((dirp = opendir(dirname.c_str())) == NULL) {
        perror("opendir: ");
        exit(-1);
    }

    if (!doMkdirs(kfsClient, kfsdirname)) {
        cout << "Unable to make kfs dir: " << kfsdirname << endl;
        closedir(dirp);
	return 1;
    }

    while ((fileInfo = readdir(dirp)) != NULL) {
        if (strcmp(fileInfo->d_name, ".") == 0)
            continue;
        if (strcmp(fileInfo->d_name, "..") == 0)
            continue;
        
        string name = dirname + "/" + fileInfo->d_name;
        struct stat buf;
        stat(name.c_str(), &buf);

        if (S_ISDIR(buf.st_mode)) {
	    subdir = dirname + "/" + fileInfo->d_name;
            kfssubdir = kfsdirname + "/" + fileInfo->d_name;
            BackupDir(kfsClient, subdir, kfssubdir);
        } else if (S_ISREG(buf.st_mode)) {
	    BackupFile2(kfsClient, dirname + "/" + fileInfo->d_name, kfsdirname + "/" + fileInfo->d_name);
        }
    }
    closedir(dirp);
    
    return 0;
}

// 
// Guts of the work
//
static int
RestoreFile2(KfsClientPtr kfsClient, string kfsfilename, string localfilename)
{
    const int bufsize = 65536;
    char kfsBuf[bufsize];
    int kfsfd, n = 0, nRead, toRead;
    int localFd;

    assert (kfsClient != 0);
    
    kfsfd = kfsClient->Open(kfsfilename.c_str(), O_RDONLY);
    if (kfsfd < 0) {
        cout << "Open failed: " << endl;
        exit(-1);
    }

    if (localfilename == "-")
        // send to stdout
        localFd = dup(1);
    else
        localFd = open(localfilename.c_str(), O_WRONLY | O_CREAT, S_IRUSR|S_IWUSR);

    if (localFd < 0) {
        cout << "Unable to open: " << localfilename << endl;
        exit(-1);
    }
    
    while (1) {
        toRead = bufsize;

        nRead = kfsClient->Read(kfsfd, kfsBuf, toRead);
        if (nRead <= 0) {
	    // EOF
            break;
        }
        n += nRead;
        write(localFd, kfsBuf, nRead);
    }
    kfsClient->Close(kfsfd);
    close(localFd);
    return 0;
}

int
KFS::tools::RestoreFile(KfsClientPtr kfsClient, string &kfsPath, string &localPath)
{
    string filename;
    string::size_type slash = kfsPath.rfind('/');
    struct stat statInfo;
    string localParentDir;

    if (!kfsClient)
    {
	cerr << "Error: No KFS client provided!\n";
	exit(-1);
    }
    
    // get everything after the last slash
    if (slash != string::npos) {
	filename.assign(kfsPath, slash+1, string::npos);
    } else {
	filename = kfsPath;
    }
    
    // get the path in local FS.  If we what we is an existing file or
    // directory in local FS, localParentDir will point to it; if localPath is
    // non-existent, then we find the parent dir and check for its
    // existence.  That is, we are trying to handle cp kfs://file/a to
    // /path/b and we are checking for existence of "/path"
    localParentDir = localPath;
    
    if (stat(localPath.c_str(), &statInfo)) {
	slash = localPath.rfind('/');
	if (slash == string::npos)
	{
	    localParentDir = ".";
	    filename = localPath;
	}
	else
	{
	    localParentDir.assign(localPath, 0, slash);
	    
	    // this is the target filename
	    filename.assign(localPath, slash + 1, string::npos);
	}
	
	stat(localParentDir.c_str(), &statInfo);
    }

    if (localPath == "-")
        statInfo.st_mode = S_IFREG;
    
    if (S_ISDIR(statInfo.st_mode)) {
	return RestoreFile2(kfsClient, kfsPath, localParentDir + "/" + filename);
    }
    
    if (S_ISREG(statInfo.st_mode)) {
	return RestoreFile2(kfsClient, kfsPath, localPath);
    }
    
    // need to make the local dir
    cout << "Local Path: " << localPath << " is non-existent!" << endl;
    return -1;
}

int
KFS::tools::RestoreDir(KfsClientPtr kfsClient, string &kfsdirname, string &dirname)
{
    string kfssubdir, subdir;
    int res, retval = 0;
    vector<KfsFileAttr> fileInfo;
    vector<KfsFileAttr>::size_type i;

    if (!kfsClient)
    {
	cerr << "Error: No KFS client provided!\n";
	exit(-1);
    }
    
    if ((res = kfsClient->ReaddirPlus(kfsdirname.c_str(), fileInfo)) < 0) {
        cout << "RestoreDir::ReaddirPlus(" <<  kfsdirname << ") failed: " << res << endl;
        return res;
    }
    
    struct stat statInfo;

    statInfo.st_mode = S_IFREG;
    
    if (stat(dirname.c_str(), &statInfo) < 0)
    {
	if (mkdir(dirname.c_str(), 0777))
	{
	    cout << "Could not create '" << dirname << "' directory.\n";
	    perror("mkdir: ");
	    return 1;
	}
    }
    else if (!S_ISDIR(statInfo.st_mode))
    {
	cout << "'" << dirname << "' exists and is not a firectory\n";
	return 1;
    }    

    for (i = 0; i < fileInfo.size(); ++i) {
        if (fileInfo[i].isDirectory) {
            if ((fileInfo[i].filename == ".") ||
                (fileInfo[i].filename == ".."))
                continue;
	    
	    subdir = dirname + "/" + fileInfo[i].filename;

	    kfssubdir = kfsdirname + "/" + fileInfo[i].filename.c_str();
            res = RestoreDir(kfsClient, kfssubdir, subdir);
            if (res < 0)
                retval = res;

        } else {
            res = RestoreFile2(kfsClient, kfsdirname + "/" + fileInfo[i].filename,
                               dirname + "/" + fileInfo[i].filename);
            if (res < 0)
                retval = res;
        }
    }
    return retval;
}

//
// Guts of the work to copy the file.
//
static int
CopyFile2(KfsClientPtr kfsClient, string srcfilename, string dstfilename)
{
    const int bufsize = 65536;
    char kfsBuf[bufsize];
    int srcfd, dstfd, nRead, toRead;
    long long n = 0;
    int res;
    
    assert (kfsClient != 0);
    
    srcfd = kfsClient->Open(srcfilename.c_str(), O_RDONLY);
    if (srcfd < 0) {
        cout << "Unable to open: " << srcfilename << endl;
        return srcfd;
    }

    dstfd = kfsClient->Create(dstfilename.c_str());
    if (dstfd < 0) {
        cout << "Create " << dstfilename << " failed: " << ErrorCodeToStr(dstfd) << endl;
        return dstfd;
    }

    while (1) {
	toRead = bufsize;
	nRead = kfsClient->Read(srcfd, kfsBuf, toRead);
	if (nRead <= 0)
	    break;

        // write it out
        res = kfsClient->Write(dstfd, kfsBuf, nRead);
        if (res < 0) {
            cout << "Write failed with error code: " << ErrorCodeToStr(res) << endl;
            return res;
        }
        n += nRead;
    }
    kfsClient->Close(srcfd);
    kfsClient->Close(dstfd);

    return 0;
}

int
KFS::tools::CopyFile(KfsClientPtr kfsClient, const string &srcPath, const string &dstPath)
{
    string filename;
    string::size_type slash = srcPath.rfind('/');
    
    if (!kfsClient)
    {
	cerr << "Error: No KFS client provided!\n";
	exit(-1);
    }
    
    // get everything after the last slash
    if (slash != string::npos) {
	filename.assign(srcPath, slash+1, string::npos);
    } else {
	filename = srcPath;
    }

    // for the dest side: if the dst is a dir, we are copying to
    // dstPath with srcFilename; otherwise, dst is a file (that
    // potenitally exists) and we are ovewriting/creating it
    if (kfsClient->IsDirectory(dstPath.c_str())) {
        string dst = dstPath;

        if (dst[dstPath.size() - 1] != '/')
            dst += "/";
        
        return CopyFile2(kfsClient, srcPath, dst + filename);
    }
    
    // dstPath is the filename that is being specified for the cp
    // target.  try to copy to there...
    return CopyFile2(kfsClient, srcPath, dstPath);
}

int
KFS::tools::CopyDir(KfsClientPtr kfsClient, const string & srcDirname, string dstDirname)
{
    vector<string> dirEntries;
    vector<string>::size_type i;
    int res;
    
    if (!kfsClient)
    {
	cerr << "Error: No KFS client provided!\n";
	exit(-1);
    }
    
    if ((res = kfsClient->Readdir(srcDirname.c_str(), dirEntries)) < 0)
    {
        cout << "Readdir(" << srcDirname << ") failed: " << res << endl;
        return res;
    }

    if (!doMkdirs(kfsClient, dstDirname))
    {
	cout << "Unable to make kfs dir: " << dstDirname << endl;
	return res;
    }
    
    for (i = 0; i < dirEntries.size(); ++i)
    {
        if ((dirEntries[i] == ".") || (dirEntries[i] == ".."))
            continue;

	string src = srcDirname + "/" + dirEntries[i];
	string dst = dstDirname + "/" + dirEntries[i];
	
        if (kfsClient->IsDirectory(src.c_str()))
	{
	    res = CopyDir(kfsClient, src, dst);
        }
	else
	{
	    res = CopyFile2(kfsClient, src, dst);
        }
    }
    
    return res;
}

static void
getTimeString(time_t time, char *buf, int bufLen)
{
    struct tm locTime;

    localtime_r(&time, &locTime);
    strftime(buf, bufLen, "%b %e %H:%M", &locTime);
}

static void
printFileInfo(const string &filename, time_t mtime, off_t filesize, bool humanReadable, bool timeInSecs)
{
    char timeBuf[256];

    if (timeInSecs) {
        ostringstream ost;

        ost << mtime;
        strncpy(timeBuf, ost.str().c_str(), 256);
    }
    else
    {
        getTimeString(mtime, timeBuf, 255);
    }
    
    timeBuf[255] = '\0';

    if (!humanReadable) {
        cout << filename << '\t' << timeBuf << '\t' << filesize << endl;
        return;
    }
    if (filesize < (1 << 20)) {
        cout << filename << '\t' << timeBuf << '\t' << (float) (filesize) / (1 << 10) << " K";
    }
    else if (filesize < (1 << 30)) {
        cout << filename << '\t' << timeBuf << '\t' << (float) (filesize) / (1 << 20) << " M";
    }
    else {
        cout << filename << '\t' << timeBuf << '\t' << (float) (filesize) / (1 << 30) << " G";
    }
    cout << endl;
}

static int
doDirList(KfsClientPtr kfsClient, string kfsdirname)
{
    string kfssubdir, subdir;
    int res;
    vector<string> entries;
    vector<string>::size_type i;

    if (kfsClient->IsFile(kfsdirname.c_str())) {
        cout << kfsdirname << endl;
        return 0;
    }
        
    if ((res = kfsClient->Readdir(kfsdirname.c_str(), entries)) < 0) {
        cout << "Readdir failed: " << ErrorCodeToStr(res) << endl;
        return res;
    }

    // we could provide info of whether the thing is a dir...but, later
    for (i = 0; i < entries.size(); ++i) {
        if ((entries[i] == ".") || (entries[i] == ".."))
            continue;
        cout << entries[i] << endl;
    }
    return 0;
}

static int
doDirListPlusAttr(KfsClientPtr kfsClient, string kfsdirname, bool humanReadable, bool timeInSecs)
{
    string kfssubdir, subdir;
    int res;
    vector<KfsFileAttr> fileInfo;
    vector<KfsFileAttr>::size_type i;

    if (kfsClient->IsFile(kfsdirname.c_str())) {
        struct stat statInfo;

        kfsClient->Stat(kfsdirname.c_str(), statInfo);
        printFileInfo(kfsdirname, statInfo.st_mtime, statInfo.st_size, humanReadable, timeInSecs);
        return 0;
    }
    if ((res = kfsClient->ReaddirPlus(kfsdirname.c_str(), fileInfo)) < 0) {
        cout << "doDirListPlusAttr::Readdir plus failed: " << ErrorCodeToStr(res) << endl;
        return res;
    }
    
    for (i = 0; i < fileInfo.size(); ++i) {
        if (fileInfo[i].isDirectory) {
            if ((fileInfo[i].filename == ".") ||
                (fileInfo[i].filename == ".."))
                continue;
            char timeBuf[256];

            getTimeString(fileInfo[i].mtime.tv_sec, timeBuf, 255);
	    timeBuf[255] = 0;

            cout << fileInfo[i].filename << "/" << '\t' << timeBuf << '\t' << "(dir)" << endl;
        } else {
            printFileInfo(fileInfo[i].filename, fileInfo[i].mtime.tv_sec, 
                          fileInfo[i].fileSize, humanReadable, timeInSecs);
        }
    }
    return 0;
}

int
KFS::tools::DirList(KfsClientPtr kfsClient, string kfsdirname, bool longMode, bool humanReadable, bool timeInSecs)
{
    if (longMode)
        return doDirListPlusAttr(kfsClient, kfsdirname, humanReadable, timeInSecs);
    else
        return doDirList(kfsClient, kfsdirname);
}

