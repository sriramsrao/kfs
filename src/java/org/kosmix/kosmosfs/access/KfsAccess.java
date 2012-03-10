/**
 * $Id: KfsAccess.java 377 2009-10-12 23:37:26Z sriramsrao $
 *
 * Created 2007/08/24
 * @author: Sriram Rao
 *
 * Copyright 2008 Quantcast Corp.
 * Copyright 2007-2008 Kosmix Corp.
 *
 * This file is part of Kosmos File System (KFS).
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * \brief Java wrappers to get to the KFS client.
 */

package org.kosmix.kosmosfs.access;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.io.ByteArrayInputStream;

public class KfsAccess
{

    // the pointer in C++
    private long cPtr;

    private final static native
    long initF(String configFn);

    private final static native
    long initS(String metaServerHost, int metaServerPort);

    private final static native
    int cd(long ptr, String  path);

    private final static native
    int mkdirs(long ptr, String  path);

    private final static native
    int rmdir(long ptr, String  path);

    private final static native
    int rmdirs(long ptr, String  path);

    private final static native
    String[] readdir(long ptr, String path, boolean prefetchAttr);

    // for each dir. entry, we get back a key/value pair of KfsFileAttr
    private final static native
    String[] readdirplus(long ptr, String path);

    private final static native
    String[][] getDataLocation(long ptr, String path, long start, long len);

    private final static native
    short getReplication(long ptr, String path);

    private final static native
    short setReplication(long ptr, String path, int numReplicas);

    private final static native
    long getModificationTime(long ptr, String path);

    // pass the mtime in milli-secs
    private final static native
    int setModificationTime(long ptr, String path, long msecs);

    private final static native
    int create(long ptr, String path, int numReplicas, boolean exclusive);

    private final static native
    int remove(long ptr, String path);

    private final static native
    int rename(long ptr, String oldpath, String newpath, boolean overwrite);

    private final static native
    int open(long ptr, String path, String mode, int numReplicas);

    private final static native
    int exists(long ptr, String path);

    private final static native
    int isFile(long ptr, String path);

    private final static native
    int isDirectory(long ptr, String path);

    private final static native
    long filesize(long ptr, String path);

    public final static native
    long setDefaultIoBufferSize(long size);

    public final static native
    long getDefaultIoBufferSize();
    
    private final static native
    boolean compareChunkReplicas(long ptr, String path, StringBuffer md5sum);

    public final static native
    long setDefaultReadAheadSize(long size);

    public final static native
    long getDefaultReadAheadSize();

    private final static native
    long setIoBufferSize(int fd, long size);

    private final static native
    long getIoBufferSize(int fd);

    private final static native
    long setReadAheadSize(int fd, long size);

    private final static native
    long getReadAheadSize(int fd);

    static {
        try {
            System.loadLibrary("kfs_access");
        } catch (UnsatisfiedLinkError e) {
            e.printStackTrace();
            System.err.println("Unable to load kfs_access native library: " + System.getProperty("java.library.path"));
            System.exit(1);
        }
    }

    public KfsAccess(String configFn) throws IOException
    {
        cPtr = initF(configFn);
        if (cPtr == 0) {
            throw new IOException("Unable to initialize KFS Client");
        }
    }

    public KfsAccess(String metaServerHost, int metaServerPort) throws IOException
    {
        cPtr = initS(metaServerHost, metaServerPort);
        if (cPtr == 0) {
            throw new IOException("Unable to initialize KFS Client");
        }
    }

    // most calls wrap to a call on the KfsClient.  For return values,
    // see the comments in libkfsClient/KfsClient.h
    //
    public int kfs_cd(String path)
    {
        return cd(cPtr, path);
    }

    // make the directory hierarchy for path
    public int kfs_mkdirs(String path)
    {
        return mkdirs(cPtr, path);
    }

    // remove the directory specified by path; remove will succeed only if path is empty.
    public int kfs_rmdir(String path)
    {
        return rmdir(cPtr, path);
    }

    // remove the directory tree specified by path; remove will succeed only if path is empty.
    public int kfs_rmdirs(String path)
    {
        return rmdirs(cPtr, path);
    }

    public String[] kfs_readdir(String path)
    {
        return kfs_readdir(path, false);
    }

    public String[] kfs_readdir(String path, boolean prefetchAttr)
    {
        return readdir(cPtr, path, prefetchAttr);
    }

    public KfsFileAttr[] kfs_readdirplus(String path)
    {
        String[] entries = readdirplus(cPtr, path);
        if (entries == null)
            return null;

        KfsFileAttr[] fattr = new KfsFileAttr[entries.length];

        for (int i = 0; i < entries.length; i++) {
            String[] fields = entries[i].split("\n");

            fattr[i] = new KfsFileAttr();

            fattr[i].filename = fields[0];
            if (fields[1].startsWith("true"))
                fattr[i].isDirectory = true;
            else
                fattr[i].isDirectory = false;

            fattr[i].filesize = Long.parseLong(fields[2]);
            fattr[i].modificationTime = Long.parseLong(fields[3]);
            fattr[i].replication = Integer.parseInt(fields[4]);
        }
        return fattr;
    }

    public KfsOutputChannel kfs_append(String path)
    {
        // when you open a previously existing file, the # of replicas is ignored
        int fd = open(cPtr, path, "a", 1);
        if (fd < 0)
            return null;
        return new KfsOutputChannel(cPtr, fd);
    }

    public KfsOutputChannel kfs_create(String path)
    {
        return kfs_create(path, 1);
    }

    public KfsOutputChannel kfs_create(String path, int numReplicas)
    {
        return kfs_create(path, numReplicas, false);
    }

    // if exclusive is specified, then create will succeed only if the
    // doesn't already exist
    public KfsOutputChannel kfs_create(String path, int numReplicas, boolean exclusive)
    {
        int fd = create(cPtr, path, numReplicas, exclusive);
        if (fd < 0)
            return null;
        return new KfsOutputChannel(cPtr, fd);
    }
    
    public boolean kfs_compareChunkReplicas(String path, StringBuffer md5sum)
    {
    	return compareChunkReplicas(cPtr, path, md5sum);
    }

    public KfsOutputChannel kfs_create(String path, int numReplicas, boolean exclusive,
        long bufferSize, long readAheadSize)
    {
        int fd = create(cPtr, path, numReplicas, exclusive);
        if (fd < 0)
            return null;
        if (bufferSize >= 0) {
            setIoBufferSize(fd, bufferSize);
        }
        if (readAheadSize >= 0) {
            setReadAheadSize(fd, readAheadSize);
        }
        return new KfsOutputChannel(cPtr, fd);
    }

    public KfsInputChannel kfs_open(String path)
    {
        int fd = open(cPtr, path, "r", 1);
        if (fd < 0)
            return null;
        return new KfsInputChannel(cPtr, fd);
    }

    public KfsInputChannel kfs_open(String path, long bufferSize, long readAheadSize)
    {
        int fd = open(cPtr, path, "r", 1);
        if (fd < 0)
            return null;
        if (bufferSize >= 0) {
            setIoBufferSize(fd, bufferSize);
        }
        if (readAheadSize >= 0) {
            setReadAheadSize(fd, readAheadSize);
        }
        return new KfsInputChannel(cPtr, fd);
    }

    public int kfs_remove(String path)
    {
        return remove(cPtr, path);
    }

    public int kfs_rename(String oldpath, String newpath)
    {
        return rename(cPtr, oldpath, newpath, true);
    }

    // if overwrite is turned off, rename will succeed only if newpath
    // doesn't already exist
    public int kfs_rename(String oldpath, String newpath, boolean overwrite)
    {
        return rename(cPtr, oldpath, newpath, overwrite);
    }

    public boolean kfs_exists(String path)
    {
        return exists(cPtr, path) == 1;
    }

    public boolean kfs_isFile(String path)
    {
        return isFile(cPtr, path) == 1;
    }

    public boolean kfs_isDirectory(String path)
    {
        return isDirectory(cPtr, path) == 1;
    }
    
    public long kfs_filesize(String path)
    {
        return filesize(cPtr, path);
    }

    // Given a starting byte offset and a length, return the location(s)
    // of all the chunks that cover the region.
    public String[][] kfs_getDataLocation(String path, long start, long len)
    {
        return getDataLocation(cPtr, path, start, len);
    }

    // Return the degree of replication for this file
    public short kfs_getReplication(String path)
    {
        return getReplication(cPtr, path);
    }

    // Request a change in the degree of replication for this file
    // Returns the value that was set by the server for this file
    public short kfs_setReplication(String path, int numReplicas)
    {
        return setReplication(cPtr, path, numReplicas);
    }

    public long kfs_getModificationTime(String path)
    {
        return getModificationTime(cPtr, path);
    }

    public int kfs_setModificationTime(String path, long msecs)
    {
        return setModificationTime(cPtr, path, msecs);
    }

    protected void finalize() throws Throwable
    {
        release();
        super.finalize();
    }

    public void release()
    {
        if (cPtr != 0) {
            
        }

    }

}


