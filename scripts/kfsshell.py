#!/usr/bin/python
#
# $Id: kfsshell.py 400 2010-08-22 06:07:05Z sriramsrao $
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
# Script that launches KfsShell: get the location of the metaserver
# from the machines.cfg file and launch KfsShell
#
# Look for <bin-dir>/tools/KfsShell
#
# Use machines.cfg
#

import os,os.path,sys,getopt
from ConfigParser import ConfigParser

def usage():
    print "%s [-f, --file <machines.cfg>] [ -b, --bin ]\n" % sys.argv[0]
    
if __name__ == '__main__':
    (opts, args) = getopt.getopt(sys.argv[1:], "b:f:h",
                                 ["bin=", "file=", "help"])
    op = ""
    filename = ""
    bindir = ""
    for (o, a) in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit(2)
        if o in ("-f", "--file"):
            filename = a
        elif o in ("-b", "--bin"):
            bindir = a

    if not os.path.exists(filename):
        print "%s : config file doesn't exist\n" % filename
        sys.exit(-1)

    if not os.path.exists(bindir):
        print "%s : bindir doesn't exist\n" % bindir
        sys.exit(-1)

    config = ConfigParser()
    config.readfp(open(filename, 'r'))
    if not config.has_section('metaserver'):
        raise config.NoSectionError, "No metaserver section"

    node = config.get('metaserver', 'node')
    port = config.getint('metaserver', 'baseport')
    cmd = "%s/tools/kfsshell -s %s -p %d" % (bindir, node, port)
    os.system(cmd)
    
