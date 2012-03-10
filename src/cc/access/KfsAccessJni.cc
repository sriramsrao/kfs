//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: KfsAccessJni.cc 3487 2011-12-15 00:33:32Z sriramr $
//
// Created 2007/08/24
//
// Portions of this file include code that is:
// Copyright 2011 Yahoo Inc.  All rights reserved.
// Copyright 2008 Quantcast Corp.
// Copyright 2007-2008 Kosmix Corp.
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
// \brief JNI code in C++ world for accesing KFS Client.
//
//----------------------------------------------------------------------------

#include <jni.h>
#include <string>
#include <cstddef>
#include <iostream>
#include <vector>
#include <netinet/in.h>
#include <sstream>

using std::vector;
using std::string;
using std::cout;
using std::endl;
using std::ostringstream;

#include <fcntl.h>
#include "libkfsClient/KfsClient.h"
using namespace KFS;

extern "C" {
    jlong Java_org_kosmix_kosmosfs_access_KfsAccess_initF(
        JNIEnv *jenv, jclass jcls, jstring jpath);

    jlong Java_org_kosmix_kosmosfs_access_KfsAccess_initS(
        JNIEnv *jenv, jclass jcls, jstring jmetaServerHost, jint metaServerPort);

    jint Java_org_kosmix_kosmosfs_access_KfsAccess_setugi(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring juser, jstring jgroup);
    
    jint Java_org_kosmix_kosmosfs_access_KfsAccess_cd(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath);

    jint Java_org_kosmix_kosmosfs_access_KfsAccess_mkdirs(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jint jperms);

    jint Java_org_kosmix_kosmosfs_access_KfsAccess_rmdir(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath);

    jboolean Java_org_kosmix_kosmosfs_access_KfsAccess_compareChunkReplicas(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jobject stringbuffermd5);

    jint Java_org_kosmix_kosmosfs_access_KfsAccess_rmdirs(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath);

    jobjectArray Java_org_kosmix_kosmosfs_access_KfsAccess_readdirplus(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath);

    jobjectArray Java_org_kosmix_kosmosfs_access_KfsAccess_readdir(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jboolean jpreloadattr);

    jint Java_org_kosmix_kosmosfs_access_KfsAccess_remove(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath);

    jint Java_org_kosmix_kosmosfs_access_KfsAccess_rename(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring joldpath, jstring jnewpath,
        jboolean joverwrite);

    jint Java_org_kosmix_kosmosfs_access_KfsAccess_exists(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath);

    jint Java_org_kosmix_kosmosfs_access_KfsAccess_isFile(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath);

    jint Java_org_kosmix_kosmosfs_access_KfsAccess_isDirectory(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath);

    jlong Java_org_kosmix_kosmosfs_access_KfsAccess_filesize(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath);

    jobjectArray Java_org_kosmix_kosmosfs_access_KfsAccess_getDataLocation(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jlong jstart, jlong jlen);

    jshort Java_org_kosmix_kosmosfs_access_KfsAccess_getReplication(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath);

    jshort Java_org_kosmix_kosmosfs_access_KfsAccess_setReplication(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jint jnumReplicas);

    jlong Java_org_kosmix_kosmosfs_access_KfsAccess_getModificationTime(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath);

    jint Java_org_kosmix_kosmosfs_access_KfsAccess_setModificationTime(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jlong jmsec);

    jint Java_org_kosmix_kosmosfs_access_KfsAccess_open(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jstring jmode, jint jnumReplicas);

    jint Java_org_kosmix_kosmosfs_access_KfsAccess_create(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jint jperms,
        jint jnumReplicas, jboolean jexclusive);

    jlong Java_org_kosmix_kosmosfs_access_KfsAccess_setDefaultIoBufferSize(
        JNIEnv *jenv, jclass jcls, jlong jptr, jlong jsize);

    jlong Java_org_kosmix_kosmosfs_access_KfsAccess_getDefaultIoBufferSize(
        JNIEnv *jenv, jclass jcls, jlong jptr);

    jlong Java_org_kosmix_kosmosfs_access_KfsAccess_setDefaultReadAheadSize(
        JNIEnv *jenv, jclass jcls, jlong jptr, jlong jsize);

    jlong Java_org_kosmix_kosmosfs_access_KfsAccess_getDefaultReadAheadSize(
        JNIEnv *jenv, jclass jcls, jlong jptr);

    jlong Java_org_kosmix_kosmosfs_access_KfsAccess_setIoBufferSize(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jlong jsize);

    jlong Java_org_kosmix_kosmosfs_access_KfsAccess_getIoBufferSize(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd);

    jlong Java_org_kosmix_kosmosfs_access_KfsAccess_setReadAheadSize(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jlong jsize);

    jlong Java_org_kosmix_kosmosfs_access_KfsAccess_getReadAheadSize(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd);

    /* Input channel methods */
    jint Java_org_kosmix_kosmosfs_access_KfsInputChannel_read(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jobject buf, jint begin, jint end);

    jint Java_org_kosmix_kosmosfs_access_KfsInputChannel_seek(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jlong joffset);

    jint Java_org_kosmix_kosmosfs_access_KfsInputChannel_tell(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd);

    jint Java_org_kosmix_kosmosfs_access_KfsInputChannel_close(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd);

    /* Output channel methods */
    jint Java_org_kosmix_kosmosfs_access_KfsOutputChannel_write(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jobject buf, jint begin, jint end);

    jint Java_org_kosmix_kosmosfs_access_KfsOutputChannel_atomicRecordAppend(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jobject buf, jint begin, jint end);
        
    jint Java_org_kosmix_kosmosfs_access_KfsOutputChannel_sync(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd);

    jint Java_org_kosmix_kosmosfs_access_KfsOutputChannel_seek(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jlong joffset);

    jint Java_org_kosmix_kosmosfs_access_KfsOutputChannel_tell(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd);

    jint Java_org_kosmix_kosmosfs_access_KfsOutputChannel_close(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd);

}

namespace
{
    inline void setStr(string & dst, JNIEnv * jenv, jstring src)
    {
        char const * s = jenv->GetStringUTFChars(src, 0);
        dst.assign(s);
        jenv->ReleaseStringUTFChars(src, s);
    }
}

jlong Java_org_kosmix_kosmosfs_access_KfsAccess_initF(
    JNIEnv *jenv, jclass jcls, jstring jpath)
{
    KfsClientPtr clnt;

    string path;
    setStr(path, jenv, jpath);
    clnt = getKfsClientFactory()->GetClient(path.c_str());
    if (!clnt)
        return 0;
    return (jlong) (clnt.get());
}

jboolean Java_org_kosmix_kosmosfs_access_KfsAccess_compareChunkReplicas(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jobject stringbuffermd5)
{
    KfsClient *clnt = (KfsClient *) jptr;
    string path , md5Sum;
    setStr(path, jenv, jpath);

    if (clnt->CompareChunkReplicas(path.c_str(), md5Sum) == true)
    {
        jcls = jenv->GetObjectClass(stringbuffermd5);
        jmethodID mid = jenv->GetMethodID (jcls, "append",
                                           "(Ljava/lang/String;)Ljava/lang/StringBuffer;");
        if(mid == 0)
        {
            return false;
        }
        jstring _jstring = jenv->NewStringUTF (md5Sum.c_str());
        jenv->CallObjectMethod (stringbuffermd5, mid, _jstring);
        return true;
    }
    else
    {
        return false;
    }
}

jlong Java_org_kosmix_kosmosfs_access_KfsAccess_initS(
    JNIEnv *jenv, jclass jcls, jstring jmetaServerHost, jint metaServerPort)
{
    KfsClientPtr clnt;
    string path;
    setStr(path, jenv, jmetaServerHost);

    clnt = getKfsClientFactory()->GetClient(path, metaServerPort);
    if (!clnt)
        return 0;
    return (jlong) (clnt.get());
}

jint Java_org_kosmix_kosmosfs_access_KfsAccess_setugi(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring juser, jstring jgroup)
{
    KfsClient *clnt = (KfsClient *) jptr;
    string user;
    string group;
    setStr(user, jenv, juser);
    setStr(group, jenv, jgroup);

    if (!clnt)
        return -1;

    clnt->SetUGI(user, group);

    return 0;
}
    
jint Java_org_kosmix_kosmosfs_access_KfsAccess_cd(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath)
{
    KfsClient *clnt = (KfsClient *) jptr;

    string path;
    setStr(path, jenv, jpath);
    return clnt->Cd(path.c_str());
}

jint Java_org_kosmix_kosmosfs_access_KfsAccess_mkdirs(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jint jperms)
{
    KfsClient *clnt = (KfsClient *) jptr;

    string path;
    setStr(path, jenv, jpath);
    return clnt->Mkdirs(path.c_str(), jperms);
}

jint Java_org_kosmix_kosmosfs_access_KfsAccess_rmdir(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath)
{
    KfsClient *clnt = (KfsClient *) jptr;

    string path;
    setStr(path, jenv, jpath);
    return clnt->Rmdir(path.c_str());
}

jint Java_org_kosmix_kosmosfs_access_KfsAccess_rmdirs(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath)
{
    KfsClient *clnt = (KfsClient *) jptr;

    string path;
    setStr(path, jenv, jpath);
    return clnt->Rmdirs(path.c_str());
}

jobjectArray Java_org_kosmix_kosmosfs_access_KfsAccess_readdirplus(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath)
{
    KfsClient *clnt = (KfsClient *) jptr;
    string path;
    int res;
    jobjectArray jentries;

    setStr(path, jenv, jpath);

    std::vector<KFS::KfsFileAttr> fattr;
    res = clnt->ReaddirPlus(path.c_str(), fattr);
    if ((res < 0) || (fattr.size() == 0))
        return NULL;

    jclass jstrClass = jenv->FindClass("Ljava/lang/String;");

    // send the results as strings to java side; the values are sent in a certain order.
    jentries = jenv->NewObjectArray(fattr.size(), jstrClass, NULL);
    for (vector<string>::size_type i = 0; i < fattr.size(); i++) {
        ostringstream os;
        jstring s;

        os << fattr[i].filename << "\n";
        if (fattr[i].isDirectory)
            os << "true" << "\n";
        else
            os << "false" << "\n";
        os << fattr[i].fileSize << "\n";
        os << ((jlong) fattr[i].mtime.tv_sec) * 1000 << "\n";
        os << fattr[i].numReplicas << "\n";

        s = jenv->NewStringUTF(os.str().c_str());

        jenv->SetObjectArrayElement(jentries, i, s);
        jenv->DeleteLocalRef(s);
    }
    return jentries;
}

jobjectArray Java_org_kosmix_kosmosfs_access_KfsAccess_readdir(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jboolean jpreloadattr)
{
    KfsClient *clnt = (KfsClient *) jptr;

    string path;
    vector<string> entries;
    int res;
    jstring s;
    jobjectArray jentries;

    setStr(path, jenv, jpath);

    if (jpreloadattr) {
        std::vector<KFS::KfsFileAttr> fattr;
        res = clnt->ReaddirPlus(path.c_str(), fattr);
        if (res == 0) {
            for (uint32_t i = 0; i < fattr.size(); i++) {
                entries.push_back(fattr[i].filename);
            }
        }
    }
    else {
        res = clnt->Readdir(path.c_str(), entries);
    }
    if ((res < 0) || (entries.size() == 0))
        return NULL;

    jclass jstrClass = jenv->FindClass("Ljava/lang/String;");

    jentries = jenv->NewObjectArray(entries.size(), jstrClass, NULL);
    for (vector<string>::size_type i = 0; i < entries.size(); i++) {
        s = jenv->NewStringUTF(entries[i].c_str());
        // std::cout << "Setting: " << i << ' ' << entries[i] << std::endl;
        jenv->SetObjectArrayElement(jentries, i, s);
        jenv->DeleteLocalRef(s);
    }

    return jentries;
}

jint Java_org_kosmix_kosmosfs_access_KfsAccess_open(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jstring jmode, jint jnumReplicas)
{
    KfsClient *clnt = (KfsClient *) jptr;

    string path, mode;
    int openMode = 0;

    setStr(path, jenv, jpath);
    setStr(mode, jenv, jmode);

    if ((mode == "r") || (mode == "rw"))
        openMode = O_RDWR;
    else if (mode == "w")
        openMode = O_WRONLY|O_CREAT;
    else if (mode == "a")
        openMode = O_WRONLY | O_APPEND;

    return clnt->Open(path.c_str(), openMode, jnumReplicas);
}

jint Java_org_kosmix_kosmosfs_access_KfsInputChannel_close(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd)
{
    KfsClient *clnt = (KfsClient *) jptr;
    
    return clnt->Close(jfd);
}

jint Java_org_kosmix_kosmosfs_access_KfsOutputChannel_close(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd)
{
    KfsClient *clnt = (KfsClient *) jptr;
    
    return clnt->Close(jfd);
}

jint Java_org_kosmix_kosmosfs_access_KfsAccess_create(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, 
    jint jperms,
    jint jnumReplicas, jboolean jexclusive)
{
    KfsClient *clnt = (KfsClient *) jptr;

    string path;
    setStr(path, jenv, jpath);
    return clnt->Create(path.c_str(), jperms, jnumReplicas, jexclusive);
}

jint Java_org_kosmix_kosmosfs_access_KfsAccess_remove(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath)
{
    KfsClient *clnt = (KfsClient *) jptr;

    string path;
    setStr(path, jenv, jpath);
    return clnt->Remove(path.c_str());
}

jint Java_org_kosmix_kosmosfs_access_KfsAccess_rename(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring joldpath, 
    jstring jnewpath, jboolean joverwrite)
{
    KfsClient *clnt = (KfsClient *) jptr;

    string opath, npath;
    setStr(opath, jenv, joldpath);
    setStr(npath, jenv, jnewpath);

    return clnt->Rename(opath.c_str(), npath.c_str(), joverwrite);
}

jlong Java_org_kosmix_kosmosfs_access_KfsAccess_setDefaultIoBufferSize(
    JNIEnv *jenv, jclass jcls, jlong jptr, jlong jsize)
{
    KfsClient *clnt = (KfsClient *) jptr;
    
    return (jlong)clnt->SetDefaultIoBufferSize(jsize);
}

jlong Java_org_kosmix_kosmosfs_access_KfsAccess_getDefaultIoBufferSize(
    JNIEnv *jenv, jclass jcls, jlong jptr)
{
    KfsClient *clnt = (KfsClient *) jptr;
    
    return (jlong)clnt->GetDefaultIoBufferSize();
}

jlong Java_org_kosmix_kosmosfs_access_KfsAccess_setDefaultReadAheadSize(
    JNIEnv *jenv, jclass jcls, jlong jptr, jlong jsize)
{
    KfsClient *clnt = (KfsClient *) jptr;
    
    return (jlong)clnt->SetDefaultReadAheadSize(jsize);
}

jlong Java_org_kosmix_kosmosfs_access_KfsAccess_getDefaultReadAheadSize(
    JNIEnv *jenv, jclass jcls, jlong jptr)
{
    KfsClient *clnt = (KfsClient *) jptr;
    
    return (jlong)clnt->GetDefaultReadAheadSize();
}

jlong Java_org_kosmix_kosmosfs_access_KfsAccess_setIoBufferSize(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jlong jsize)
{
    KfsClient *clnt = (KfsClient *) jptr;
    
    return (jlong)clnt->SetIoBufferSize(jfd, jsize);
}

jlong Java_org_kosmix_kosmosfs_access_KfsAccess_getIoBufferSize(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd)
{
    KfsClient *clnt = (KfsClient *) jptr;
    
    return (jlong)clnt->GetIoBufferSize(jfd);
}

jlong Java_org_kosmix_kosmosfs_access_KfsAccess_setReadAheadSize(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jlong jsize)
{
    KfsClient *clnt = (KfsClient *) jptr;
    
    return (jlong)clnt->SetReadAheadSize(jfd, jsize);
}

jlong Java_org_kosmix_kosmosfs_access_KfsAccess_getReadAheadSize(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd)
{
    KfsClient *clnt = (KfsClient *) jptr;
    
    return (jlong)clnt->GetReadAheadSize(jfd);
}

jint Java_org_kosmix_kosmosfs_access_KfsOutputChannel_sync(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd)
{
    KfsClient *clnt = (KfsClient *) jptr;
    
    return clnt->Sync(jfd);
}

jint Java_org_kosmix_kosmosfs_access_KfsInputChannel_seek(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jlong joffset)
{
    KfsClient *clnt = (KfsClient *) jptr;
    
    return clnt->Seek(jfd, joffset);
}

jint Java_org_kosmix_kosmosfs_access_KfsInputChannel_tell(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd)
{
    KfsClient *clnt = (KfsClient *) jptr;
    
    return clnt->Tell(jfd);
}

jint Java_org_kosmix_kosmosfs_access_KfsOutputChannel_seek(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jlong joffset)
{
    KfsClient *clnt = (KfsClient *) jptr;
    
    return clnt->Seek(jfd, joffset);
}

jint Java_org_kosmix_kosmosfs_access_KfsOutputChannel_tell(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd)
{
    KfsClient *clnt = (KfsClient *) jptr;
    
    return clnt->Tell(jfd);
}

jint Java_org_kosmix_kosmosfs_access_KfsAccess_exists(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath)
{
    KfsClient *clnt = (KfsClient *) jptr;

    string path;
    setStr(path, jenv, jpath);
    
    if (clnt->Exists(path.c_str()))
        return 1;
    
    return 0;
}

jint Java_org_kosmix_kosmosfs_access_KfsAccess_isFile(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath)
{
    KfsClient *clnt = (KfsClient *) jptr;

    string path;
    setStr(path, jenv, jpath);
    
    if (clnt->IsFile(path.c_str()))
        return 1;
    return 0;
}

jint Java_org_kosmix_kosmosfs_access_KfsAccess_isDirectory(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath)
{
    KfsClient *clnt = (KfsClient *) jptr;

    string path;
    setStr(path, jenv, jpath);
    
    if (clnt->IsDirectory(path.c_str()))
        return 1;
    return 0;
}

jlong Java_org_kosmix_kosmosfs_access_KfsAccess_filesize(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath)
{
    KfsClient *clnt = (KfsClient *) jptr;

    struct stat result;
    string path;
    setStr(path, jenv, jpath);
    
    if (clnt->Stat(path.c_str(), result) != 0)
        return -1;
    
    return result.st_size;
}

jlong Java_org_kosmix_kosmosfs_access_KfsAccess_getModificationTime(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath)
{
    KfsClient *clnt = (KfsClient *) jptr;

    struct stat result;
    string path;
    setStr(path, jenv, jpath);
    
    if (clnt->Stat(path.c_str(), result) != 0)
        return -1;
    
    // The expected return value is in ms
    return ((jlong) result.st_mtime) * 1000;
}

jint Java_org_kosmix_kosmosfs_access_KfsAccess_setModificationTime(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jlong jmsec)
{
    KfsClient *clnt = (KfsClient *) jptr;

    string path;
    setStr(path, jenv, jpath);

    struct timeval mtime;

    // the input is in ms
    mtime.tv_sec = jmsec / 1000;
    mtime.tv_usec = jmsec % 1000;
    if (clnt->SetMtime(path.c_str(), mtime) != 0)
        return -1;
    
    return 0;
}

jobjectArray Java_org_kosmix_kosmosfs_access_KfsAccess_getDataLocation(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jlong jstart, jlong jlen)
{
    KfsClient *clnt = (KfsClient *) jptr;

    string path;
    // for each block, there could be multiple locations due to replication; return them all here
    vector< vector<string> > entries;
    int res;
    jstring s;
    jobjectArray jentries;

    setStr(path, jenv, jpath);

    res = clnt->GetDataLocation(path.c_str(), jstart, jlen, entries);
    if ((res < 0) || (entries.size() == 0))
        return NULL;

    jclass jstrArrClass = jenv->FindClass("[Ljava/lang/String;");
    jclass jstrClass = jenv->FindClass("Ljava/lang/String;");

    // For each block, return its location(s)
    jentries = jenv->NewObjectArray(entries.size(), jstrArrClass, NULL);

    for (vector<string>::size_type i = 0; i < entries.size(); i++) {
        jobjectArray jlocs = jenv->NewObjectArray(entries[i].size(), jstrClass, NULL);
        for (vector<string>::size_type j = 0; j < entries[i].size(); j++) {
            s = jenv->NewStringUTF(entries[i][j].c_str());
            jenv->SetObjectArrayElement(jlocs, j, s);
            jenv->DeleteLocalRef(s);
        }
        jenv->SetObjectArrayElement(jentries, i, jlocs);
        jenv->DeleteLocalRef(jlocs);
    }

    return jentries;
}

jshort Java_org_kosmix_kosmosfs_access_KfsAccess_getReplication(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath)
{
    KfsClient *clnt = (KfsClient *) jptr;

    string path;

    setStr(path, jenv, jpath);
    return clnt->GetReplicationFactor(path.c_str());
}

jshort Java_org_kosmix_kosmosfs_access_KfsAccess_setReplication(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jint jnumReplicas)
{
    KfsClient *clnt = (KfsClient *) jptr;

    string path;

    setStr(path, jenv, jpath);
    return clnt->SetReplicationFactor(path.c_str(), jnumReplicas);
}

jint Java_org_kosmix_kosmosfs_access_KfsInputChannel_read(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jobject buf, jint begin, jint end) 
{
    KfsClient *clnt = (KfsClient *) jptr;

    if(!buf)
        return 0;

    void * addr = jenv->GetDirectBufferAddress(buf);
    jlong cap = jenv->GetDirectBufferCapacity(buf);

    if(!addr || cap < 0)
        return 0;
    if(begin < 0 || end > cap || begin > end)
        return 0;

    addr = (void *)(uintptr_t(addr) + begin);
        
    ssize_t sz = clnt->Read((int) jfd, (char *) addr, (size_t) (end - begin));
    return (jint)sz;
}


jint Java_org_kosmix_kosmosfs_access_KfsOutputChannel_write(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jobject buf, jint begin, jint end) 
{
    KfsClient *clnt = (KfsClient *) jptr;

    if(!buf)
        return 0;

    void * addr = jenv->GetDirectBufferAddress(buf);
    jlong cap = jenv->GetDirectBufferCapacity(buf);

    if(!addr || cap < 0)
        return 0;
    if(begin < 0 || end > cap || begin > end)
        return 0;

    addr = (void *)(uintptr_t(addr) + begin);
        
    ssize_t sz = clnt->Write((int) jfd, (const char *) addr, (size_t) (end - begin));
    return (jint)sz;
}

jint Java_org_kosmix_kosmosfs_access_KfsOutputChannel_atomicRecordAppend(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jobject buf, jint begin, jint end) 
{
    KfsClient *clnt = (KfsClient *) jptr;

    if(!buf)
        return 0;

    void * addr = jenv->GetDirectBufferAddress(buf);
    jlong cap = jenv->GetDirectBufferCapacity(buf);

    if(!addr || cap < 0)
        return 0;
    if(begin < 0 || end > cap || begin > end)
        return 0;

    addr = (void *)(uintptr_t(addr) + begin);
        
    ssize_t sz = clnt->AtomicRecordAppend((int) jfd, (const char *) addr, (size_t) (end - begin));
    return (jint)sz;
}
