#!/usr/bin/env python
#
# $Id: slaves.py 181 2008-10-03 23:59:12Z sriramsrao $
#
# Copyright 2008 Quantcast Corp.
# Author: Sriram Rao
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
# Script that logs into a node and runs a command.
# Use for checking liveness of a given node
#

import os,sys,popen2
import threading

class Pinger(threading.Thread):
    def __init__(self, node, cmd):
        threading.Thread.__init__(self)
        self.node = node
        self.cmd = cmd

    def run(self):
        cmd = "ssh -o StrictHostKeyChecking=no %s %s " % (self.node, self.cmd)
        p = popen2.Popen3(cmd, True)
        for out in p.fromchild:
            if len(out) > 1:
                print out.strip()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print "Usage: slaves.py <machines file> <cmd>"
        sys.exit(0)
        
    pingers = []
    for l in open(sys.argv[1], 'r').readlines():
        w = Pinger(l.strip(), sys.argv[2])
        pingers.append(w)

    numPerRound = 100
    for i in xrange(0, len(pingers), numPerRound):
	for j in xrange(0, numPerRound):
		if i + j >= len(pingers):
			break
        	pingers[i + j].start()
	for j in xrange(0, numPerRound):
		if i + j >= len(pingers):
			break
        	pingers[i + j].join(15)
	print "Done with ", i + numPerRound, " workers"
