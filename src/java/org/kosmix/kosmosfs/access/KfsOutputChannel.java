/**
 * $Id: KfsOutputChannel.java 148 2008-09-06 20:20:15Z sriramsrao $ 
 *
 * Created 2007/09/11
 *
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
 * \brief An output channel that does buffered I/O.  This is to reduce
 * the overhead of JNI calls.
 */

package org.kosmix.kosmosfs.access;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class KfsOutputChannel implements WritableByteChannel, Positionable
{
    // To get to a byte-buffer from the C++ side as a pointer, need
    // the buffer to be direct memory backed buffer.  So, allocate one
    // for reading/writing.
    private static final int DEFAULT_BUF_SIZE = 1 << 20;
    private ByteBuffer writeBuffer;
    private int kfsFd = -1;
    private long cPtr;

    private final static native
    int close(long ptr, int fd);

    private final static native
    int write(long ptr, int fd, ByteBuffer buf, int begin, int end);

    private final static native
    int sync(long ptr, int fd);

    private final static native
    int seek(long ptr, int fd, long offset);

    private final static native
    long tell(long ptr, int fd);

    public KfsOutputChannel(long ptr, int fd) 
    {
        writeBuffer = ByteBuffer.allocateDirect(DEFAULT_BUF_SIZE);
        writeBuffer.clear();

        kfsFd = fd;
        cPtr = ptr;
    }

    public boolean isOpen()
    {
        return kfsFd > 0;

    }

    // Read/write from the specified fd.  The basic model is:
    // -- fill some data into a direct mapped byte buffer
    // -- send/receive to the other side (Jave->C++ or vice-versa)
    //

    public int write(ByteBuffer src) throws IOException
    {
        if (kfsFd < 0) 
            throw new IOException("File closed");

        int r0 = src.remaining();

        // While the src buffer has data, copy it in and flush
        while(src.hasRemaining())
        {
            if (writeBuffer.remaining() == 0) {
                writeBuffer.flip();
                writeDirect(writeBuffer);
            }

            // Save end of input buffer
            int lim = src.limit();

            // Copy in as much data we have space
            if (writeBuffer.remaining() < src.remaining())
                src.limit(src.position() + writeBuffer.remaining());
            writeBuffer.put(src);

            // restore the limit to what it was
            src.limit(lim);
        }

        int r1 = src.remaining();
        return r0 - r1;
    }

    private void writeDirect(ByteBuffer buf) throws IOException
    {
        if(!buf.isDirect())
            throw new IllegalArgumentException("need direct buffer");

        int pos = buf.position();
        int last = buf.limit();

        if (last - pos == 0)
            return;

        int sz = write(cPtr, kfsFd, buf, pos, last);
        
        if(sz < 0)
            throw new IOException("writeDirect failed");

        // System.out.println("Wrote via JNI: kfsFd: " + kfsFd + " amt: " + sz);

        if (sz == last) {
            buf.clear();
            return;
        }

        if (sz == 0) {
            return;
        }

        // System.out.println("Compacting on kfsfd: " + kfsFd);

        // we wrote less than what is available.  so, shift things
        // over to reflect what was written out.
        ByteBuffer temp = ByteBuffer.allocateDirect(DEFAULT_BUF_SIZE);
        temp.put(buf);
        temp.flip();
        buf.clear();
        buf.put(temp);
    }


    public int sync() throws IOException
    {
        if (kfsFd < 0) 
            throw new IOException("File closed");

        // flush everything
        writeBuffer.flip();
        writeDirect(writeBuffer);

        return sync(cPtr, kfsFd);
    }

    // is modeled after the seek of Java's RandomAccessFile; offset is
    // the offset from the beginning of the file.
    public int seek(long offset) throws IOException
    {
        if (kfsFd < 0) 
            throw new IOException("File closed");

        sync();

        return seek(cPtr, kfsFd, offset);
    }

    public long tell() throws IOException
    {
        if (kfsFd < 0) 
            throw new IOException("File closed");

        // similar issue as read: the position at which we are writing
        // needs to be offset by where the C++ code thinks we are and
        // how much we have buffered
        return tell(cPtr, kfsFd) + writeBuffer.remaining();
    }

    public void close() throws IOException
    {
        if (kfsFd < 0)
            return;

        sync();

        close(cPtr, kfsFd);
        kfsFd = -1;
    }

    protected void finalize() throws Throwable
    {
        if (kfsFd < 0)
            return;
        close();
    }
    
}
