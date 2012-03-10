#!/usr/bin/env python
#
# $Id: kfssetup.py 36 2007-11-12 02:43:36Z sriramsrao $
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
# Script to setup KFS servers on a set of nodes
# This scripts reads a machines.cfg file that describes the meta/chunk
# servers configurations and installs the binaries/scripts and creates
# the necessary directory hierarchy.
#

import os,sys,os.path,getopt
import socket,threading,popen2
import md5
from ConfigParser import ConfigParser

# Use the python config parser to parse out machines setup
# Input file format for machines.cfg
# [metaserver]
#   type: metaserver
#   clusterkey: <cluster name>
#   node: <value>
#   rundir: <dir>
#   baseport: <port>
#
# [chunkserver1]
#   node: <value>
#   rundir: <dir>
#   baseport: <port>
#   space: <space exported by the server> (n m/g)
#   {chunkdir: <dir>}
# [chunkserver2]
# ...
# [chunkserverN]
# ...
#
# where, space is expressed in units of MB/GB or bytes.
#
# Install on each machine with the following directory hierarchy:
#   rundir/
#        bin/  -- binaries, config file, kfscp/kfslog/kfschunk dirs
#        logs/ -- log output from running the binary
#        scripts/ -- all the helper scripts
# If a path for storing the chunks isn't specified, then it defaults to bin
#

unitsScale = {'g' : 1 << 30, 'm' : 1 << 20, 'k' : 1 << 10, 'b' : 1}
tarProg = 'gtar'
maxConcurrent = 25
chunkserversOnly = 0
md5String = ""

def setupMeta(section, config, outputFn, packageFn):
    """ Setup the metaserver binaries/config files on a node. """
    global chunkserversOnly
    if chunkserversOnly > 0:
        print "Chunkservers only is set; not doing meta"
        return
    key = config.get(section, 'clusterkey')    
    baseport = config.getint(section, 'baseport')
    rundir = config.get(section, 'rundir')
    
    fh = open(outputFn, 'w')    
    print >> fh, "metaServer.clientPort = %d" % baseport
    print >> fh, "metaServer.chunkServerPort = %d" % (baseport + 100)
    print >> fh, "metaServer.clusterKey = %s" % (key)
    print >> fh, "metaServer.cpDir = %s/bin/kfscp" % rundir
    print >> fh, "metaServer.logDir = %s/bin/kfslog" % rundir
    if config.has_option(section, 'loglevel'):
        print >> fh, "metaServer.loglevel = %s" % config.get(section, 'loglevel')

    if config.has_option(section, 'worm'):
        print >> fh, "metaServer.wormMode = 1"
    
    if config.has_option(section, 'numservers'):
        print >> fh, "metaServer.minChunkservers = %s" % config.get(section, 'numservers')

    if config.has_option(section, 'md5sumfilename'):
        print >> fh, "metaServer.md5sumFilename = %s" % config.get(section, 'md5sumfilename')
    fh.close()

    if config.has_option(section, 'webuiConfFile'):
        confFile = config.get(section, 'webuiConfFile')
        fh = open(confFile, 'w')
        print >> fh, "[webserver]"
        print >> fh, "webServer.metaserverPort = %d" % baseport
        print >> fh, "webServer.port = %d" % (baseport + 50)
        print >> fh, "webServer.allMachinesFn = %s/webui/all-machines.txt" % rundir
        print >> fh, "webServer.docRoot = %s/webui/files" % rundir
        fh.close()
        
    cmd = "%s -zcf %s bin/logcompactor bin/metaserver %s lib webui scripts/*" % (tarProg, packageFn, outputFn)
    os.system(cmd)
    installArgs = "-r %s -d %s -m" % (tarProg, rundir)
    return installArgs    

def setupChunkConfig(section, config, outputFn):
    """ Setup the chunkserver binaries/config files on a node. """    
    metaNode = config.get('metaserver', 'node')
    metaToChunkPort = config.getint('metaserver', 'baseport') + 100
    hostname = config.get(section, 'node')
    # for rack-aware replication, we assume that nodes on different racks are on different subnets
    s = socket.gethostbyname(hostname)
    ipoctets = s.split('.')
    rackId = int(ipoctets[2])
    #
    fh = open (outputFn, 'w')
    print >> fh, "chunkServer.metaServer.hostname = %s" % metaNode
    print >> fh, "chunkServer.metaServer.port = %d" % metaToChunkPort
    print >> fh, "chunkServer.clientPort = %d" % config.getint(section, 'baseport')
    print >> fh, "chunkServer.clusterKey = %s" % config.get('metaserver', 'clusterkey')
    print >> fh, "chunkServer.rackId = %d" % (rackId)
    print >> fh, "chunkServer.md5sum = %s" % (md5String)

    space = config.get(section, 'space')
    s = space.split()
    if (len(s) >= 2):
        units = s[1].lower()
    else:
        units = 'b'
    
    value = int(s[0]) * unitsScale[ units[0] ]
    print >> fh, "chunkServer.totalSpace = %d" % value

    rundir = config.get(section, 'rundir')
    if config.has_option(section, 'chunkdir'):
        chunkDir = config.get(section, 'chunkdir')
    else:
        chunkDir = "%s/bin/kfschunk" % (rundir)

    print >> fh, "chunkServer.chunkDir = %s" % (chunkDir)
    print >> fh, "chunkServer.logDir = %s/bin/kfslog" % (rundir)

    if config.has_option(section, 'loglevel'):
        print >> fh, "chunkServer.loglevel = %s" % config.get(section, 'loglevel')
        
    fh.close()
    

def setupChunk(section, config, outputFn, packageFn):
    """ Setup the chunkserver binaries/config files on a node. """
    setupChunkConfig(section, config, outputFn)

    cmd = "%s -zcf %s bin/chunkscrubber bin/chunkserver %s lib scripts/*" % (tarProg, packageFn, outputFn)
    os.system(cmd)

    rundir = config.get(section, 'rundir')
    if config.has_option(section, 'chunkdir'):
        chunkDir = config.get(section, 'chunkdir')
    else:
        chunkDir = "%s/bin/kfschunk" % (rundir)
    
    installArgs = "-r %s -d %s -c \"%s\" " % (tarProg, rundir, chunkDir)
    return installArgs

def usage():
    """ Print out the usage for this program. """
    print "%s [-f, --file <server.cfg>] [-m , --machines <chunkservers.txt>] [-r, --tar <tar|gtar>] \
    [-w, --webui <webui dir>] [ [-b, --bin <dir with binaries>] {-u, --upgrade} | [-U, --uninstall] ]\n" % sys.argv[0]
    return

def copyDir(srcDir, dstDir):
    """ Copy files from src to dest"""
    cmd = "cp -r %s %s" % (srcDir, dstDir)
    os.system(cmd)

def computeMD5(datadir, digest):
    """Update the MD5 digest using the MD5 of all the files in a directory"""
    files = os.listdir(datadir)
    for f in sorted(files):
        path = os.path.join(datadir, f)
        if os.path.isdir(path):
            continue
        fh = open(path, 'r')
        while 1:
            buf = fh.read(4096)
            if buf == "":
                break
            digest.update(buf)
    
def getFiles(buildDir, webuidir):
    """ Copy files from buildDir/bin, buildDir/lib and . to ./bin, ./lib, and ./scripts
    respectively."""
    global md5String
    cmd = "mkdir -p ./scripts; cp ./* scripts; chmod u+w scripts/*"
    os.system(cmd)
    s = "%s/bin" % buildDir
    if (os.path.exists(s + "/amd64")):
        s += "/amd64"
    copyDir(s, './bin')
    digest = md5.new()
    computeMD5('./bin', digest)
    s = "%s/lib" % buildDir
    if (os.path.exists(s + "/amd64")):
        s += "/amd64"
    copyDir(s, './lib')
    computeMD5('./lib', digest)
    md5String = digest.hexdigest()
    copyDir(webuidir, './webui')

def cleanup(fn):
    """ Cleanout the dirs we created. """
    cmd = "rm -rf ./scripts ./bin ./lib ./webui %s " % fn
    os.system(cmd)


class InstallWorker(threading.Thread):
    """InstallWorker thread that runs a command on remote node"""
    def __init__(self, sec, conf, tmpdir, i, m):
        threading.Thread.__init__(self)
        self.section = sec
        self.config = conf
        self.tmpdir = tmpdir
        self.id = i
        self.mode = m
        self.doBuildPkg = 1

    def singlePackageForAll(self, packageFn, installArgs):
        self.doBuildPkg = 0
        self.packageFn = packageFn
        self.installArgs = installArgs

    def buildPackage(self):
        if (self.section == 'metaserver'):
            self.installArgs = setupMeta(self.section, self.config, self.configOutputFn, self.packageFn)
        else:
            self.installArgs = setupChunk(self.section, self.config, self.configOutputFn, self.packageFn)

    def doInstall(self):
        fn = os.path.basename(self.packageFn)
        if (self.section == 'metaserver'):
            if chunkserversOnly > 0:
                return
            c = "scp -pr -o StrictHostKeyChecking=no -q %s kfsinstall.sh %s:/tmp/; ssh -o StrictHostKeyChecking=no %s 'mv /tmp/%s /tmp/kfspkg.tgz; sh /tmp/kfsinstall.sh %s %s ' " % \
                (self.packageFn, self.dest, self.dest, fn, self.mode, self.installArgs)
        else:
            # chunkserver
            configFn = os.path.basename(self.configOutputFn)
            c = "scp -pr -o StrictHostKeyChecking=no -q %s kfsinstall.sh %s %s:/tmp/; ssh -o StrictHostKeyChecking=no %s 'mv /tmp/%s /tmp/kfspkg.tgz; mv /tmp/%s /tmp/ChunkServer.prp; sh /tmp/kfsinstall.sh %s %s ' " % \
                (self.packageFn, self.configOutputFn, self.dest, self.dest, fn, configFn, self.mode, self.installArgs)

        p = popen2.Popen3(c, True)
        for out in p.fromchild:
            if len(out) > 1:
                print '[%s]: %s' % (self.dest, out[:-1])

        
    def cleanup(self):
        if self.doBuildPkg > 0:
            # if we built the package, nuke it
            c = "rm -f %s %s" % (self.configOutputFn, self.packageFn)
        else:
            c = "rm -f %s" % (self.configOutputFn)
        os.system(c)
        c = "ssh -o StrictHostKeyChecking=no %s 'rm -f /tmp/install.sh /tmp/kfspkg.tgz' " % self.dest
        popen2.Popen3(c, True)
        
    def run(self):
        self.configOutputFn = "%s/fn.%d" % (self.tmpdir, self.id)
        if self.doBuildPkg > 0:
            self.packageFn = "%s/kfspkg.%d.tgz" % (self.tmpdir, self.id)
            self.buildPackage()
        else:
            setupChunkConfig(self.section, self.config, self.configOutputFn)

        self.dest = config.get(self.section, 'node')
        self.doInstall()
        self.cleanup()
        
def doInstall(config, builddir, tmpdir, webuidir, upgrade, serialMode):
    if not config.has_section('metaserver'):
        raise config.NoSectionError, "No metaserver section"

    if not os.path.exists(builddir):
        print "%s : directory doesn't exist\n" % builddir
        sys.exit(-1)

    getFiles(builddir, webuidir)
    if os.path.exists('webui'):
        webuiconfFile = os.path.join(webuidir, "server.conf")
        config.set('metaserver', 'webuiConfFile', webuiconfFile)

    workers = []
    i = 0
    sections = config.sections()
    if upgrade == 1:
        mode = "-u"
    else:
        mode = "-i"

    chunkPkgFn = ""
    cleanupFn = ""
    for s in sections:
        w = InstallWorker(s, config, tmpdir, i, mode)
        workers.append(w)
        if serialMode == 1:
            w.start()
            w.join()
        else:
            # same package for all chunkservers
            if (s != 'metaserver'):
                if chunkPkgFn == "":
                    configOutputFn = "%s/fn.common" % (tmpdir)
                    chunkPkgFn = "kfspkg-chunk.tgz"
                    cleanupFn = "%s %s" % (configOutputFn, chunkPkgFn)
                    installArgs = setupChunk(s, config, configOutputFn, chunkPkgFn)
                w.singlePackageForAll(chunkPkgFn, installArgs)
        i = i + 1

    if serialMode == 0:
        for i in xrange(0, len(workers), maxConcurrent):
            #start a bunch 
            for j in xrange(maxConcurrent):
                idx = i + j
                if idx >= len(workers):
                    break
                workers[idx].start()
            #wait for each one to finish
            for j in xrange(maxConcurrent):
                idx = i + j
                if idx >= len(workers):
                    break
                workers[idx].join()
            print "Done with %d workers" % idx
            
    for i in xrange(len(workers)):
        workers[i].join(120.0)
        
    cleanup(cleanupFn)

class UnInstallWorker(threading.Thread):
    """UnInstallWorker thread that runs a command on remote node"""
    def __init__(self, c, n):
        threading.Thread.__init__(self)
        self.cmd = c
        self.node = n
        
    def run(self):
        # capture stderr and ignore the hostkey has changed message
        p = popen2.Popen3(self.cmd, True)
        for out in p.fromchild:
            if len(out) > 1:
                print '[%s]: %s' % (self.node, out[:-1])

def doUninstall(config):
    sections = config.sections()
    workers = []

    for s in sections:
        rundir = config.get(s, 'rundir')
        node = config.get(s, 'node')
        
        if (s == 'metaserver'):
            otherArgs = '-m'
        else:
            # This is a chunkserver; so nuke out chunk dir as well
            if config.has_option(s, 'chunkdir'):
                chunkDir = config.get(s, 'chunkdir')
            else:
                chunkDir = "%s/bin/kfschunk" % (rundir)
            otherArgs = "-c \"%s\"" % (chunkDir)
        
        cmd = "ssh -o StrictHostKeyChecking=no %s 'cd %s; sh scripts/kfsinstall.sh -U -d %s %s' " % \
              (node, rundir, rundir, otherArgs)
        # print "Uninstall cmd: %s\n" % cmd
        # os.system(cmd)
        w = UnInstallWorker(cmd, node)
        workers.append(w)
        w.start()

    print "Started all the workers..waiting for them to finish"        
    for i in xrange(len(workers)):
        workers[i].join(120.0)
    sys.exit(0)

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

if __name__ == '__main__':
    (opts, args) = getopt.getopt(sys.argv[1:], "cb:f:m:r:t:w:hsUu",
                                 ["chunkserversOnly", "build=", "file=", "machines=", "tar=", "tmpdir=",
                                  "webui=", "help", "serialMode", "uninstall", "upgrade"])
    filename = ""
    builddir = ""
    uninstall = 0
    upgrade = 0
    serialMode = 0
    machines = ""
    webuidir = ""
    chunkserversOnly = 0
    # Script probably won't work right if you change tmpdir from /tmp location
    tmpdir = "/tmp"
    for (o, a) in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit(2)
        if o in ("-f", "--file"):
            filename = a
        elif o in ("-b", "--build"):
            builddir = a
        elif o in ("-c", "--chunkserversOnly"):
            chunkserversOnly = 1
        elif o in ("-m", "--machines"):
            machines = a
        elif o in ("-r", "--tar"):
            tarProg = a
        elif o in ("-w", "--webuidir"):
            webuidir = a
        elif o in ("-t", "--tmpdir"):
            tmpdir = a
        elif o in ("-U", "--uninstall"):
            uninstall = 1
        elif o in ("-u", "--upgrade"):
            upgrade = 1
        elif o in ("-s", "--serialMode"):
            serialMode = 1

    if not os.path.exists(filename):
        print "%s : directory doesn't exist\n" % filename
        sys.exit(-1)

    config = ConfigParser()
    config.readfp(open(filename, 'r'))

    if machines != "":
        readChunkserversFile(machines)

    if uninstall == 1:
        doUninstall(config)
    else:
        doInstall(config, builddir, tmpdir, webuidir, upgrade, serialMode)
        
