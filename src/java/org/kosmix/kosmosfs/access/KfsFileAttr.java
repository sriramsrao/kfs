/**
 * $Id: KfsFileAttr.java 92 2008-07-21 21:20:48Z sriramsrao $
 *
 * Created 2008/07/20
 *
 * @author: Sriram Rao 
 *
 * Copyright 2008 Quantcast Corp.
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
 * \brief A struct that defines the kfs file attributes that get returned back.
 */

package org.kosmix.kosmosfs.access;

public class KfsFileAttr
{
    public KfsFileAttr() { }
    public String filename;
    public boolean isDirectory;
    public long filesize;
    public long modificationTime;
    public int replication;
}
