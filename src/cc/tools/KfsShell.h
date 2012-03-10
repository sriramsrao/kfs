//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: KfsShell.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2007/09/26
//
//
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
// \brief A simple shell that lets users navigate KFS directory hierarchy.
// 
//----------------------------------------------------------------------------

#ifndef TOOLS_KFSSHELL_H
#define TOOLS_KFSSHELL_H

extern "C" {
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
}

#include <string>
#include <vector>

namespace KFS
{
    namespace tools
    {

        typedef int (*cmdHandler)(const std::vector<std::string> &args);
        // cmd handlers
        int handleCd(const std::vector<std::string> &args);
        int handleChangeReplication(const std::vector<std::string> &args);
        int handleCopy(const std::vector<std::string> &args);
        int handleFstat(const std::vector<std::string> &args);
        int handleLs(const std::vector<std::string> &args);
        int handleMkdirs(const std::vector<std::string> &args);
        int handleMv(const std::vector<std::string> &args);
        int handleRmdir(const std::vector<std::string> &args);
        int handlePing(const std::vector<std::string> &args);
        int handleRm(const std::vector<std::string> &args);    
        int handlePwd(const std::vector<std::string> &args);
        int handleAppend(const std::vector<std::string> &args);
        // utility functions
        int doMkdirs(const char *path);
        int doRmdir(const char *dirname);
        void GetPathComponents(const std::string &path, 
                               std::string &parent, std::string &name);

    }
}

#endif // TOOLS_KFSSHELL_H
