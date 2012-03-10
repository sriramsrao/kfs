#!/usr/bin/env python
#
# $Id: kfslaunch.py 36 2007-11-12 02:43:36Z sriramsrao $
#
# Copyright 2007 Kosmix Corp.
#
# This file is part of Kosmos File System (KFS).
#
# Licensed under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.
#
# Script that launches KFS servers on a set of nodes
#
# Assumes the following directory hierarchy:
# rundir/
#       - bin
#       - logs/ --> logs from running the program
#       - scripts
# Use machines.cfg
#

import os,os.path,sys,getopt,popen2
import threading
from ConfigParser import ConfigParser

def usage():
    print "%s [-f, --file <server.cfg>] [-m, --machines <chunkservers.txt>] [ [-s, --start] | [-S, --stop] ]\n" % sys.argv[0]

class Worker(threading.Thread):
    """Worker thread that runs a command on remote node"""
    def __init__(self, c, n):
        threading.Thread.__init__(self)
        self.cmd = c
        self.node = n
    def run(self):
        # capture stderr and ignore the hostkey has changed message
        p = popen2.Popen3(self.cmd, True)
        p.tochild.close()        
        for out in p.fromchild:
            if len(out) > 1:
                print '[%s]: %s' % (self.node, out[:-1])

def readChunkserversFile(machinesFn):
    '''Given a list of chunkserver node names, one per line, construct a config
    for each chunkserver and add that to the config based on the defaults'''
    global config
    defaultChunkOptions = config.options("chunkserver_defaults")
    for l in open(machinesFn, 'r'):
        line = l.strip()
        if (line.startswith('#')):
            # ignore commented out node names
            continue
        section_name = "chunkserver_" + line
        config.add_section(section_name)
        config.set(section_name, "node", line)
        for o in defaultChunkOptions:
            config.set(section_name, o, config.get("chunkserver_defaults", o))

    config.remove_section("chunkserver_defaults")


# Specify whether we want to start/stop services
if __name__ == '__main__':
    (opts, args) = getopt.getopt(sys.argv[1:], "f:m:sSh",
                                 ["file=", "machines=", "start", "stop", "help"])
    op = ""
    machines = ""
    for (o, a) in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit(2)
        if o in ("-f", "--file"):
            filename = a
        elif o in ("-m", "--machines"):
            machines = a
        elif o in ("-s", "--start"):
            op = "-s"
        elif o in ("-S", "--stop"):
            op = "-S"

    if op == "":
        raise getopt.GetoptError, "invalid options"

    if not os.path.exists(filename):
        print "%s : directory doesn't exist\n" % filename
        sys.exit(-1)

    config = ConfigParser()
    config.readfp(open(filename, 'r'))
    if not config.has_section('metaserver'):
        raise config.NoSectionError, "No metaserver section"

    if machines != "":
        readChunkserversFile(machines)

    sections = config.sections()
    workers = []
    for s in sections:
        node = config.get(s, 'node')
        rundir = config.get(s, 'rundir')
        if (s == 'metaserver'):
            runargs = "-m -f bin/MetaServer.prp"
            if config.has_option(s, 'backup_node'):
                bkup_node = config.get(s, 'backup_node')
                runargs = runargs + " -b %s" % (bkup_node)
                if config.has_option(s, 'backup_path'):
                    bkup_path = config.get(s, 'backup_path')
                else:
                    bkup_path = "."
                runargs = runargs + " -p %s" % (bkup_path)
        else:
            runargs = "-c -f bin/ChunkServer.prp"
            
        cmd = "ssh -o StrictHostKeyChecking=no %s 'cd %s; scripts/kfsrun.sh %s %s ' " % \
              (node, rundir, op, runargs)
        w = Worker(cmd, node)
        workers.append(w)
        w.start()
        # os.system(cmd)

    for i in xrange(len(workers)):
        workers[i].join(120.0)
    sys.exit(0)

    
