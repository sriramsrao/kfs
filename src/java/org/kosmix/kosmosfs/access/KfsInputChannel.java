/**
 * $Id: KfsInputChannel.java 145 2008-09-06 19:45:34Z sriramsrao $ 
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
 * \brief An input channel that does buffered I/O.  This is to reduce
 * the overhead of JNI calls.
 */

package org.kosmix.kosmosfs.access;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/* A byte channel interface with seek support */
public class KfsInputChannel implements ReadableByteChannel, Positionable
{
    // To get to a byte-buffer from the C++ side as a pointer, need
    // the buffer to be direct memory backed buffer.  So, allocate one
    // for reading/writing.
    private static final int DEFAULT_BUF_SIZE = 1 << 20;
    private ByteBuffer readBuffer;
    private int kfsFd = -1;
    private long cPtr;

    private final static native
    int read(long cPtr, int fd, ByteBuffer buf, int begin, int end);

    private final static native
    int close(long cPtr, int fd);

    private final static native
    int seek(long cPtr, int fd, long offset);

    private final static native
    long tell(long cPtr, int fd);

    public KfsInputChannel(long ptr, int fd) 
    {
        readBuffer = ByteBuffer.allocateDirect(DEFAULT_BUF_SIZE);
        readBuffer.flip();

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

    public int read(ByteBuffer dst) throws IOException
    {
        if (kfsFd < 0) 
            throw new IOException("File closed");

        int r0 = dst.remaining();

        // While the dst buffer has space for more data, fill
        while(dst.hasRemaining())
        {
            // Fill input buffer if it's empty
            if(!readBuffer.hasRemaining())
            {
                readBuffer.clear();
                readDirect(readBuffer);
                readBuffer.flip();

                // If we failed to get anything, call that EOF
                if(!readBuffer.hasRemaining())
                    break;
            }

            // Save end of input buffer
            int lim = readBuffer.limit();

            // If dst buffer can't contain all of input buffer, limit
            // our copy size.
            if(dst.remaining() < readBuffer.remaining())
                readBuffer.limit(readBuffer.position() + dst.remaining());

            // Copy into dst buffer
            dst.put(readBuffer);

            // Restore end of input buffer marker (maybe changed
            // earlier)
            readBuffer.limit(lim);
        }

        // If we copied anything into the dst buffer (or if there was
        // no space available to do so), return the number of bytes
        // copied.  Otherwise return -1 to indicate EOF.
        int r1 = dst.remaining();
        if(r1 < r0 || r0 == 0)
            return r0 - r1;
        else
            return -1;

    }

    private void readDirect(ByteBuffer buf) throws IOException
    {
        if(!buf.isDirect())
            throw new IllegalArgumentException("need direct buffer");

        int pos = buf.position();
        int sz = read(cPtr, kfsFd, buf, pos, buf.limit());
        if(sz < 0)
            throw new IOException("readDirect failed");

        // System.out.println("Read via JNI: kfsFd: " + kfsFd + " amt: " + sz);
        
        buf.position(pos + sz);
    }

    // is modeled after the seek of Java's RandomAccessFile; offset is
    // the offset from the beginning of the file.
    public int seek(long offset) throws IOException
    {
        if (kfsFd < 0) 
            throw new IOException("File closed");

        readBuffer.clear();
        readBuffer.flip();

        return seek(cPtr, kfsFd, offset);
    }

    public long tell() throws IOException
    {
        if (kfsFd < 0) 
            throw new IOException("File closed");

        // we keep some data buffered; so, we ask the C++ side where
        // we are in the file and offset that by the amount in our
        // buffer
        return tell(cPtr, kfsFd) - readBuffer.remaining();
    }

    public void close() throws IOException
    {
        if (kfsFd < 0)
            return;

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
