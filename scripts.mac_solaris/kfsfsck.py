#!/usr/bin/env python
#
# $Id: //depot/main/platform/kosmosfs/scripts.solaris/kfsfsck.py#0 $
#
# Copyright 2008 Quantcast Corp.
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
# KFS fsck
#

import os,sys,os.path,getopt
import socket,threading,popen2
import tempfile
import time
import re
from ConfigParser import ConfigParser
from time import strftime

# Global dict holding chunk info.
gChunkMap = {}
# Global list of live chunk servers
gUpServers = {}

class UpServer:
    """Keep track of an up server state"""
    def __init__(self, info):
        if isinstance(info, str):
            serverInfo = info.split(',')
            # order here is host, port, total, used, util, nblocks, last heard
            for i in xrange(len(serverInfo)):
                s = serverInfo[i].split('=')
                setattr(self, s[0].strip(), s[1].strip())

            if hasattr(self, 's'):
                setattr(self, 'host', self.s)
                delattr(self, 's')

            if hasattr(self, 'p'):
                setattr(self, 'port', self.p)
                delattr(self, 'p')

            self.down = 0
            self.retiring = 0

    def __cmp__(self, other):
        """ Order by IP"""
        return cmp(socket.inet_aton(self.host), socket.inet_aton(other.host))


class ChunkInfo:
    """Structure to hold information about chunk, its hosts and sizes on them"""
    def __init__(self, chunkID, fileID, numServers):
        self.chunkID = chunkID
        self.fileID = fileID
        self.numServers = numServers
        self.chunkHosts = []

    def addChunkHostInfo(self, chunkHostInfo):
        self.chunkHosts.append(chunkHostInfo)

    def printIDs(self):
        print self.chunkID, self.fileID, self.numServers

    def updateChunkSize(self, chunkSize, host, port):
        for chunkHostInfo in self.chunkHosts:
            if ((chunkHostInfo.host == host) and (chunkHostInfo.port == port)):
                chunkHostInfo.chunkSize = chunkSize

    def printChunkInfo(self):
        outline = []
        outline.append (self.chunkID)
        outline.append (self.fileID)
        outline.append (self.numServers)

        for chunkHostInfo in self.chunkHosts:
            chunkHostInfo.printChunkHostInfo(outline)

        return ' '.join(outline)
        
class ChunkHostInfo:
    """Structure used by ChunkInfo to define a host holding a chunk"""
    def __init__(self, host, port, rack):
        self.host = host
        self.port = port
        self.rack = rack
        self.chunkSize = 0
    
    def printChunkHostInfo(self, outline):
        outline.append (self.host) 
        outline.append (self.port) 
        outline.append (self.rack) 
        outline.append (str(self.chunkSize))

    def updateChunkSize(self, chunkSize):
        self.chunkSize = chunkSize

class ServerLocation:
    def __init__(self, **kwds):
        self.__dict__.update(kwds)
        self.status = 0

def mergeAndSaveFile(dumpMetaFile, chunkSizeFile, outFile):
    """ Read dumpMetaFile, chunkSizeFile and generate outFile"""
    dump = open (dumpMetaFile, "r")
    chunk = open (chunkSizeFile, "r")
    out = open (outFile, "w")
    
    cline = ""
    cline = chunk.readline()
    cline = cline.rstrip("\n")

    while dump:
        dline = dump.readline()
        if not dline:
            break
        dline = dline.rstrip("\n")
        
        # Split line parts 
        dlineParts = dline.split(' ')
        
        # Read lines from chunkSize
        numEntries = int(dlineParts[2])
        
        entries = []
        for i in range(numEntries):
            entries.append([dlineParts[i*3 + 3], dlineParts[i*3 + 4], dlineParts[i*3 + 5], 0])
            #entries[i][0] = dlineParts[i*3 + 3]
            #entries[i][1] = dlineParts[i*3 + 4]
            #entries[i][2] = dlineParts[i*3 + 5]
            #entries[i][3] = 0

        while True:
            clineParts = cline.split(' ')
            if ((dlineParts[0] == clineParts[0]) and (dlineParts[1] == clineParts[1])):
                for i in range(numEntries):
                    if ((entries[i][0] == clineParts[3]) and (entries[i][1] == clineParts[4])):
                        entries[i][3] = clineParts[2]
            else:
                break
            cline = chunk.readline()
            cline = cline.rstrip("\n")
            if not cline:
                break

        # Print output
        out.write(dlineParts[0]+" "+dlineParts[1]+" "+dlineParts[2]+" ")
        for i in range(numEntries):
            out.write(str(entries[i][3])+" "+entries[i][0]+" "+entries[i][1]+" "+entries[i][2]+" ")
        out.write("\n")
    out.close()

def saveToFile(fileName):
    """Save gChunkMap to file which could be used by emulator code"""
    outfile = open (fileName, "w")
    chunkInfoKeys = gChunkMap.keys()
    chunkInfoKeys.sort()

    for chunkInfo in chunkInfoKeys:
        c = gChunkMap[chunkInfo]
        outfile.write(c.printChunkInfo())
        outfile.write("\n");

def loadMetaChunkToServerMap (fileName):
    """Read metaserver chunkmap.txt and build gChunkMap hash"""
    if not os.path.exists(fileName):
        print "File ", fileName, " does not exists"
        sys.exit(1)

    infile = open (fileName, "r")
    count = 0
    while infile:
        count = count + 1
        line = infile.readline()
        if not line:
            break
        print "DEBUGME : processing line %s, %d" % (line, count)
        lineParts = line.split(' ')
        gChunkMap[lineParts[0]] = ChunkInfo(lineParts[0], lineParts[1], lineParts[2])
        # Add a ChunkHostInfo
        numServers = int(lineParts[2])
        for i in range(numServers):
            i = i * 3
            gChunkMap[lineParts[0]].addChunkHostInfo(ChunkHostInfo(lineParts[i+3], lineParts[i+4], lineParts[i+5]))

def processUpNodes(nodes):
    """Helper function to process live chunk server nodes"""
    global gUpServers
    servers = nodes.split('\t')
    gUpServers = [UpServer(c) for c in servers if c != '']

def dumpChunkMap(chunkServer):
    """Helper function to send DUMP_CHUNKMAP RPC to chunkServer and read 
    the output sent over socket"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((chunkServer.node, chunkServer.port))
    req = "DUMP_CHUNKMAP\r\nVersion: KFS/1.0\r\nCseq: 1\r\n\r\n"
    sock.send(req)
    sockIn = sock.makefile('r')
    contentLength = 0
    seenLength = 0
    for line in sockIn:
        if line.find('OK') == 0:
            continue
        if line.find('Cseq') == 0:
            continue
        if line.find('Status') == 0:
            continue
        if line.find('\r\n') == 0:
            continue
        if line.find('Content-length') == 0:
            line = line.rstrip("\n")
            lineParts = line.split(' ')
            contentLength = int(lineParts[1])
        else:
            seenLength = seenLength + len(line)
            line = line.rstrip("\n")
            lineParts = line.split(' ')
            if gChunkMap.has_key(lineParts[0]):
                gChunkMap[lineParts[0]].updateChunkSize(lineParts[2], chunkServer.node, str(chunkServer.port))
        if (seenLength == contentLength):
            break

def ping(metaServer):
    """Helper function to send PING to meta server and populate list of
    live chunk server list"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((metaServer.node, metaServer.port))
    req = "PING\r\nVersion: KFS/1.0\r\nCseq: 1\r\n\r\n"
    sock.send(req)
    sockIn = sock.makefile('r')
    for line in sockIn:
        if line.find('Down Servers:') == 0:
            if (len(line.split(':')) < 2):
                break
            pos = line.find(':')
            downNodes = line[pos+1:].strip()
            break
   
        if line.find('Servers:') != 0:
            continue
        nodes = line.split(':')[1].strip()
        processUpNodes(nodes)

    gUpServers.sort()
    sock.close()
 
def dumpMetaServerChunkMap(metaServer, dumpMetaFile, defaultMetaFile, defaultCheckPoint):
    """Helper function to send DUMP_METASERVERCHUNKMAP to meta server 
    and populate list of live chunk server list"""

    # Get latest checkpoint file
    # Gzip latest file and copy it locally
    print "Compressing latest checkpoint %s on %s" % (defaultCheckPoint, metaServer.node)
    if not os.path.exists("./checkpointdir"):
        command = "mkdir ./checkpointdir"
        os.system(command)
    command = "ssh -o StrictHostKeyChecking=no %s gzip -c %s > ./checkpointdir/latest.gz" % (metaServer.node, defaultCheckPoint)
    os.system(command)

    #print "Copying latest checkpoint file %s.gz" % defaultCheckPoint
    #command = "scp -o StrictHostKeyChecking=no %s:%s.gz ./checkpointdir" % (metaServer.node, defaultCheckPoint)
    #os.system(command)

    print "Uncompressing latest checkpoint ./checkpointdir/latest.gz" 
    command = "gunzip -f ./checkpointdir/latest.gz"
    os.system(command)
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((metaServer.node, metaServer.port))
    req = "DUMP_CHUNKTOSERVERMAP\r\nVersion: KFS/1.0\r\nCseq: 1\r\n\r\n"
    sock.send(req)
    sockIn = sock.makefile('r')
    for line in sockIn:
        if line.find('OK') == 0:
            continue
        if line.find('Cseq') == 0:
            continue
        if line.find('Status') == 0:
            continue
        if line.find('\r\n') == 0:
            break
    sock.close()
    
    # Gzip the file and scp over to dumMetaFile.gz and extract it
    print "Compressing chunk map dump %s on %s" % (defaultMetaFile, metaServer.node)
    command = "ssh -o StrictHostKeyChecking=no %s gzip -f %s" % (metaServer.node, defaultMetaFile)
    os.system(command)
    print "Copying chunk map dump %s.gz to %s.gz" % (defaultMetaFile, dumpMetaFile)
    command = "scp -o StrictHostKeyChecking=no %s:%s.gz %s.gz" % (metaServer.node, defaultMetaFile, dumpMetaFile)
    os.system(command)
    print "Uncompressing chunk map dump %s.gz" % (dumpMetaFile)
    command = "gunzip -f %s.gz" % dumpMetaFile
    os.system(command)

    print "Creating symlink chunkmap.txt to %s" % (dumpMetaFile)
    command = "rm chunkmap.txt"
    os.system(command)
    command = "ln -s %s chunkmap.txt" % (dumpMetaFile)
    os.system(command)
        
 
def usage():
    print "Usage : ./kfsfsck --file machines.cfg [--machines machines] [--verbose] [--replicacheck --builddir builddir --networkdef networkdef [--checksize] [--lostfound] [--delete]]\n"
    print "Example : ./kfsfsck -f machines.cfg"
    print "          Would ask metaserver to dump chunk map, get it locally "
    print "          and does basic replica checking displaying stats about "
    print "          Chunks"
    print "        : ./kfsfsck -f machines.cfg -s"
    print "          Would ping chunk servers to get chunk sizes and fill it "
    print "          in output file ./chunkListOutFile"
    print "        : ./kfsfsck -f machines.cfg -r -b ../build -n network.df"
    print "          Would also run replicachecker, which builds metaserver "
    print "          map and does replica verification. Optionally we could"
    print "          also move or delete files with missing blocks."
    print "                                               "
    print "          -f : kfs cluster config file"
    print "          -m : chunk server machine list"
    print "          -r : invoke replicachecker"
    print "          -b : build dir "
    print "          -n : network definition file"
    print "          -s : Checks replica sizes. This operation is very slow "
    print "              as it pings each chunk server to get replica sizes"
    print "          -l : move files with missing blocks to /lost+found "
    print "          -d : delete files with missing blocks"
    print "          -v : Verbose mode prints info about replicas on same rack"

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

def updateChunkServerInfo(host, port, config):
    '''Given host, port read the the config to get list of all chunkDir 
    directories. Do and ls to collect chunkID <-> chunkSize mapping on host'''
    sections = config.sections()

    for s in sections:
        if (s == 'metaserver'):
            continue
        node = config.get(s, 'node')
        node = socket.gethostbyname(node)
        if (node != host):
            continue
        chunkDir = config.get(s, 'chunkDir')
        chunkDirs = chunkDir.split(' ')
        for dir in chunkDirs:
            command = "ssh %s ls -l %s" % (node, dir)
            for line in os.popen(command).readlines():
                line = line.rstrip('\n');
                lineParts = line.split()
                if (len(lineParts) == 9):
                    if ("lost+found" == lineParts[8]):
                        continue
                    chunkSize = lineParts[4]
                    chunkIDParts = lineParts[8].split('.')
                    chunkID = chunkIDParts[1]
                    if gChunkMap.has_key(chunkID):
                        gChunkMap[chunkID].updateChunkSize(chunkSize, host, str(port))

def fastFsck(dumpMetaFile, mytime, verbose):
    '''Execute fast fsck. This just checks consistancy of chunks by looking
    at chunkmap.txt dump from metaserver'''
    infile = open (dumpMetaFile, "r")
    numChunks = 0
    num2Copies = 0
    num3Copies = 0
    numUnderReplicated = 0
    numOverReplicated = 0
    numMissing = 0
    status = "HEALTHY"

    print "***********************************************";

    while infile:
        line = infile.readline()
        if not line:
            break
        line = line.rstrip('\n')
        lineParts = line.split()
        cID = int(lineParts[0])
        fileID = int(lineParts[1])
        numReplicas = int(lineParts[2])
        replicasInfo = lineParts[3:]
        
        replicasSize = (len(replicasInfo)) / 3
        numChunks = numChunks + 1

        if (replicasSize == 0):
            print "Chunk %d missing" % cID
            numMissing = numMissing + 1
        elif (replicasSize < 3):
            print "Chunk %d under replicated having %d copies" % (cID, replicasSize)
            numUnderReplicated = numUnderReplicated + 1
        elif (replicasSize > 3):
            if (verbose):
                print "Chunk %d over replicated having %d copies" % (cID, replicasSize)
            numOverReplicated = numOverReplicated + 1
        elif ((getRack(replicasInfo[0])) == (getRack(replicasInfo[3])) == (getRack(replicasInfo[6]))):
            print "Chunk %d has 3 copies on same rack %s, %s and %s" % (cID, replicasInfo[0], replicasInfo[3], replicasInfo[6])
            num3Copies = num3Copies + 1
        else:
            for i in range(1, replicasSize):
                if (getRack(replicasInfo[i*3]) == getRack(replicasInfo[(i-1)*3])):
                    if (verbose):
                        print "Chunk %d has 2 copies on same rack %s and %s" % (cID, replicasInfo[i*3], replicasInfo[(i-1)*3])
                    num2Copies = num2Copies + 1

    if numMissing:
        status = "CORRUPT"
    # Print Summary
    print "***********************************************"
    print " KFS Summary (%s)" % mytime
    print "***********************************************"
    print " Num Chunks                  : %d" % numChunks
    print " Num Missing Chunks          : %d" % numMissing
    print " Num UnderReplicated Chunks  : %d" % numUnderReplicated
    print " Num OverReplicated Chunks   : %d" % numOverReplicated
    print " Num 2 Replicas on same Rack : %d" % num2Copies
    print " Num 3 Replicas on same Rack : %d" % num3Copies
    print " Status                      : %s" % status
    print "***********************************************"

def getRack(ipAddress):
    ipParts = ipAddress.split('.')
    return ipParts[2]

def dumpChunkServerInfo(host, port, config, chunkListTempFile):
    '''Given host, port read the the config to get list of all chunkDir 
    directories. Do and ls to collect chunkID <-> chunkSize mapping on host'''
    sections = config.sections()

    for s in sections:
        if (s == 'metaserver'):
            continue
        node = config.get(s, 'node')
        node = socket.gethostbyname(node)
        if (node != host):
            continue
        else:
            chunkDir = config.get(s, 'chunkDir')
            chunkDirs = chunkDir.split(' ')
            for dir in chunkDirs:
                command = "ssh %s ls -l %s" % (node, dir)
                for line in os.popen(command).readlines():
                    line = line.rstrip('\n')
                    lineParts = line.split()
                    if (len(lineParts) == 9):
                        if ("lost+found" == lineParts[8]):
                            continue
                        chunkSize = lineParts[4]
                        chunkIDParts = lineParts[8].split('.')
                        chunkID = chunkIDParts[1]
                        fileID = chunkIDParts[0]
                        chunkListTempFile.write("%s %s %s %s %s\n" % (chunkID, fileID, chunkSize, host, port))
        break
    
if __name__ == '__main__':
    (opts, args) = getopt.getopt(sys.argv[1:], "f:vm:srb:n:ldh", ["file=", "verbose",  "machines=", "checksize", "replicacheck", "builddir=", "networkdef=","lostfound","delete", "help"])
    fileName = ""
    now = time.localtime(time.time())
    mytime = time.strftime("%y-%m-%d-%H-%M", now)
    dumpMetaFile = "./chunkmap.txt"+mytime
    outFile = "./chunkListOutFile"
    missingBlocksFile = "./file_missing_blocks.txt"
    machinesFile = ""
    metaServerHost = ""
    metaServerPort = 0
    metaRunDir = ""
    replicaCheckFlag = 0
    buildDir = ""
    kfsCpDir = "./checkpointdir"
    networkDefFile = ""
    emptyChunkSize = 1
    fast = 0
    verbose = 0
    lostFound = 0
    delete = 0

    if not opts:
        usage()
        print "No options specified"
        sys.exit(1)

    for (o, a) in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit(2)
        if o in ("-f", "--file"):
            fileName = a
        elif o in ("-v", "--verbose"):
            verbose = 1
        elif o in ("-m", "--machines"):
            machinesFile = a
        elif o in ("-s", "--checksize"):
            emptyChunkSize = 0
        elif o in ("-r", "--replicacheck"):
            replicaCheckFlag = 1
        elif o in ("-b", "--builddir"):
            buildDir = a
        elif o in ("-n", "--networkdef"):
            networkDefFile = a
        elif o in ("-l", "--lostfound"):
            lostFound = 1
        elif o in ("-d", "--delete"):
            delete = 1

    if not os.path.exists(fileName):
        print "Config file %s : doesn't exist\n" % fileName
        sys.exit(1)

    config = ConfigParser()
    config.readfp(open(fileName, 'r'))

    if machinesFile != "":
        readChunkserversFile(machinesFile)

    if (replicaCheckFlag == 1) and ((buildDir == "") or \
            (networkDefFile == "")):
        usage()
        print "Missing Replica Checker options"
        sys.exit(1)
    if ((lostFound == 1) and (delete == 1)):
        usage()
        print "Please specify either --lostfound or --delete not both"
        sys.exit(1)
    
    sections = config.sections()
    for s in sections:
        if (s == 'metaserver'):
            metaServerHost = config.get(s, 'node')
            metaServerPort = int(config.get(s, 'baseport'))
            metaRunDir = config.get(s, 'rundir')
    # MetaServerLocation. For now we assume we run in on MetaServerHost
    metaServer = ServerLocation(node=metaServerHost, port=metaServerPort)
    
    # Download meta server chunk dump
    defaultMetaFile = metaRunDir + "/chunkmap.txt"
    defaultCheckPoint = metaRunDir + "/bin/kfscp/latest"

    print "Begin ChunkServerMap dump to %s on %s" % (defaultMetaFile, metaServerHost)
    dumpMetaServerChunkMap(metaServer, dumpMetaFile, defaultMetaFile, defaultCheckPoint)
    print "End ChunkServerMap dump to %s on %s" % (defaultMetaFile, metaServerHost)

    if (replicaCheckFlag == 0):
        fast = 1

    if (fast == 1):
        # Check fast fsck by looking at chunk map dump. 
        # Do not ping chunkservers or invoke replicachecker
        print "Executing fast fsck parsing %s" % dumpMetaFile
        fastFsck(dumpMetaFile, mytime, verbose)
        sys.exit(0)

    # ping to get list of Upservers
    ping(metaServer)
    print "Done pinging metaserver"

    # Log details about chunk servers to a file 
    command = "rm ./chunkListTempFile";
    os.system(command)
    chunkListTempFile = open ("./chunkListTempFile", "w")
    
    if (emptyChunkSize == 0):
        # We ping chunk servers only if we need chunk size info
        # For each upServer, collect chunkID->chunkSize and update gChunkMap
        for upServer in gUpServers:
            print "Listing chunk server %s, %s" % (upServer.host, upServer.port)
            dumpChunkServerInfo(upServer.host, upServer.port, config, chunkListTempFile)
    
    chunkListTempFile.close()

    # Sort file and merge them
    command = "sort -n -T . ./chunkListTempFile > ./chunkListTempFile.sort";
    os.system(command)
    command = "mv ./chunkListTempFile.sort ./chunkListTempFile";
    os.system(command)

    # Save final output to file
    mergeAndSaveFile(dumpMetaFile, './chunkListTempFile', outFile)
    print "Generated chunk map file : %s" % outFile

    # If replicaCheckFlag is set, run replica checker using outFile
    if (replicaCheckFlag == 1):
        print "Running replica checker"
        replicaChecker = buildDir + "/bin/emulator/replicachecker"
        #command = "%s -c ./checkpointdir/ \
        #           -n ~/work/kfs/networkdef \
        #           -b %s" % (replicaChecker, outFile)
        command = "%s -c %s -n %s -b %s" % (replicaChecker, kfsCpDir, networkDefFile, outFile)
        if (emptyChunkSize == 0):
            command = command + " -s "
        if (verbose == 1):
            command = command + " -v "
        print command+"\n"
        for line in os.popen(command).readlines():
            print line.rstrip('\n')

    # If --lostfound option specified, move files with missing blocks to 
    # /lost+found
    # If --delete option specified, move files with missing blocks
    if ((lostFound == 1) or (delete == 1)):
        if not os.path.exists(missingBlocksFile):
            sys.exit(0)    
        missingBlocksFiles = open(missingBlocksFile, "r")
        kfsShell = buildDir + "/bin/tools/kfsshell"
        while missingBlocksFiles:
            line = missingBlocksFiles.readline()
            if not line:
                break
            line = line.rstrip('\n')
            if (lostFound == 1):
                print "Moving %s to /lost+found%s" % (line, line)
                command = "%s -s %s -p %s -q mkdir /lost+found" % (kfsShell, metaServer.node, metaServer.port)
                os.system(command)
                command = "%s -s %s -p %s -q mv %s /lost+found%s" % (kfsShell, metaServer.node, metaServer.port, line, line)
                os.system(command)
            elif (delete == 1):
                print "Deleting %s" % (line)
                command = "%s -s %s -p %s -q rm %s" % (kfsShell, metaServer.node, metaServer.port, line)
                os.system(command)

    sys.exit(0)
