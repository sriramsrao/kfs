//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kfsshell_main.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2007/09/26
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

#include <iostream>    
#include <fstream>
#include <cerrno>
#include <map>

#include "libkfsClient/KfsClient.h"
#include "common/log.h"
#include "tools/KfsShell.h"
#if HAVE_LIBEDIT
#include <editline/readline.h>
#endif

#include <iostream>
#include <tr1/unordered_map>
using std::cin;
using std::cout;
using std::endl;
using std::map;
using std::vector;
using std::string;

using namespace KFS;
using namespace KFS::tools;

typedef map <string, cmdHandler> CmdHandlers;
typedef map <string, cmdHandler>::iterator CmdHandlersIter;

CmdHandlers handlers;

static void setupHandlers();

/// @retval: status code from executing the last command
static int processCmds(bool quietMode, int nargs, const char **cmdLine);

int
main(int argc, char **argv)
{
    string kfsdirname = "";
    string serverHost = "";
    int port = -1, retval;
    bool help = false;
    bool quietMode = false;
    char optchar;
    bool verboseLogging = false;

    while ((optchar = getopt(argc, argv, "hqs:p:v")) != -1) {
        switch (optchar) {
            case 's':
                serverHost = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 'h':
                help = true;
                break;
            case 'v':
                verboseLogging = true;
                break;
            case 'q':
                quietMode = true;
                break;
            default:
                cout << "Unrecognized flag : " << optchar;
                help = true;
                break;
        }
    }

    if (help || (serverHost == "") || (port < 0)) {
        cout << "Usage: " << argv[0] << " -s <meta server name> -p <port> {-q}" << endl;
        exit(0);
    }

    KfsClientFactory *factory = getKfsClientFactory();

    KfsClientPtr kfsClient = factory->GetClient(serverHost, port);
    if (!kfsClient) {
        cout << "kfs client failed to initialize...exiting" << endl;
        exit(-1);
    }

    if (verboseLogging) {
        KFS::MsgLogger::SetLevel(KFS::MsgLogger::kLogLevelDEBUG);
    } else {
        KFS::MsgLogger::SetLevel(KFS::MsgLogger::kLogLevelINFO);
    }
    
    factory->SetDefaultClient(kfsClient);

    setupHandlers();

    retval = processCmds(quietMode, argc - optind, (const char **) &argv[optind]);

    return retval;
}

void printCmds()
{
    cout << "cd" << endl;
    cout << "changeReplication" << endl;
    cout << "cp" << endl;
    cout << "ls" << endl;
    cout << "mkdir" << endl;
    cout << "mv" << endl;
    cout << "rm" << endl;
    cout << "rmdir" << endl;
    cout << "stat" << endl;
    cout << "pwd" << endl;
    cout << "append" << endl;
}

int handleHelp(const vector<string> &args)
{
    printCmds();
    return 0;
}

void setupHandlers()
{
    handlers["cd"] = handleCd;
    handlers["changeReplication"] = handleChangeReplication;
    handlers["cp"] = handleCopy;
    handlers["ls"] = handleLs;
    handlers["mkdir"] = handleMkdirs;
    handlers["mv"] = handleMv;
    handlers["rmdir"] = handleRmdir;
    // handlers["ping"] = handlePing;
    handlers["rm"] = handleRm;
    handlers["stat"] = handleFstat;
    handlers["pwd"] = handlePwd;
    handlers["help"] = handleHelp;
    handlers["append"] = handleAppend;
}

int processCmds(bool quietMode, int nargs, const char **cmdLine)
{
    char buf[4096];
    string s, cmd;
    int retval = 0;

#if HAVE_LIBEDIT
    using_history();
#endif
    while (1) {
        if (quietMode) {
            if (nargs == 0)
                break;
            s = "";
            for (int i = 0; i < nargs; i++) {
                s = s + cmdLine[i];
                s = s + " ";
            }
            nargs = 0;
        } else {
            // Turn off prompt printing when quiet mode is enabled;
            // this allows scripting with KfsShell
#if HAVE_LIBEDIT
           char *in = readline("KfsShell> ");
           if (!in)
             break;
           add_history(in);
           strncpy(buf, in, 4096);
           buf[4095] = 0;
#else
            cout << "KfsShell> ";
            cin.getline(buf, 4096);
            
            if (cin.eof())
                break;
#endif
            s = buf;
        }

        // buf contains info of the form: <cmd>{<args>}
        // where, <cmd> is one of kfs cmds
        string::size_type curr, next;
        
        // get rid of leading spaces
        curr = s.find_first_not_of(" \t");
        s.erase(0, curr);
        curr = s.find(' ');
        if (curr != string::npos)
            cmd.assign(s, 0, curr);
        else
            cmd = s;

        next = curr;
        // extract out the args
        vector<string> args;
        while (curr != string::npos) {
            string component;

            // curr points to a ' '
            curr++;
            next = s.find(' ', curr);        
            if (next != string::npos)
                component.assign(s, curr, next - curr);
            else
                component.assign(s, curr, string::npos);

            if (component != "")
                args.push_back(component);
            curr = next;
        }

        CmdHandlersIter h = handlers.find(cmd);
        if (h == handlers.end()) {
            cout << "Unknown cmd: " << cmd << endl;
            cout << "Supported cmds are: " << endl;
            printCmds();
            cout << "Type <cmd name> --help for command specific help" << endl;
            continue;
        }
        
        retval = ((*h).second)(args);
    }
    return retval;
}

