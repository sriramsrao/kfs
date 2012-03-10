#!/usr/bin/env python
#
# $Id: kfsstatus.py 385 2010-05-27 15:58:30Z sriramsrao $
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
# A simple webserver that displays KFS status by pinging the metaserver.
#

import os,sys,os.path,getopt
import socket,threading,calendar,time
import SimpleHTTPServer
import SocketServer
from cStringIO import StringIO
from ConfigParser import ConfigParser
import urllib

upServers = {}
downServers = {}
retiringServers = {}
serversByRack = {}
numReallyDownServers = 0
nodesWithDiskErrors = 0
metaserverPort = 20000
docRoot = '.'

class ServerLocation:
    def __init__(self, **kwds):
        self.__dict__.update(kwds)
        self.status = 0

class SystemInfo:
    def __init__(self):
        self.startedAt = ""
        self.totalSpace = 0
        self.usedSpace = 0
        self.wormMode = "Disabled"

systemInfo = SystemInfo()

class DownServer:
    """Keep track of a potentially down server"""
    def __init__(self, info):
        serverInfo = info.split(',')
        for i in xrange(len(serverInfo)):
            s = serverInfo[i].split('=')
            setattr(self, s[0].strip(), s[1].strip())

        if hasattr(self, 's'):
            setattr(self, 'host', self.s)
            delattr(self, 's')

        if hasattr(self, 'p'):
            setattr(self, 'port', self.p)
            delattr(self, 'p')


        self.stillDown = 0

    def __cmp__(self, other):
        """Order by down date"""
        return cmp(time.strptime(other.down), time.strptime(self.down))

    def setStillDown(self):
        self.stillDown = 1

    def printStatusHTML(self, buffer, count):
        if count % 2 == 0:
            trclass = ""
        else:
            trclass = "class=odd"

        if self.stillDown:
            trclass = "class=dead"
        
        print >> buffer, '''<tr ''', trclass, '''><td align="center">''', self.host, '''</td>'''
        print >> buffer, '''<td>''', self.down, '''</td>'''
        print >> buffer, '''<td>''', self.reason, '''</td>'''                
        print >> buffer, '''</tr>'''

class RetiringServer:
    """Keep track of a retiring server"""
    def __init__(self, info):
        serverInfo = info.split(',')
        for i in xrange(len(serverInfo)):
            s = serverInfo[i].split('=')
            setattr(self, s[0].strip(), s[1].strip())

        if hasattr(self, 's'):
            setattr(self, 'host', self.s)
            delattr(self, 's')

        if hasattr(self, 'p'):
            setattr(self, 'port', self.p)
            delattr(self, 'p')
        
    def __cmp__(self, other):
        """Order by start date"""
        return cmp(time.strptime(other.started), time.strptime(self.started))

    def printStatusHTML(self, buffer, count):
        if count % 2 == 0:
            trclass = ""
        else:
            trclass = "class=odd"
        
        print >> buffer, '''<tr ''', trclass, '''><td align="center">''', self.host, '''</td>'''
        print >> buffer, '''<td>''', self.started, '''</td>'''
        print >> buffer, '''<td align="right">''', self.numDone, '''</td>'''
        print >> buffer, '''<td align="right">''', self.numLeft, '''</td>'''        
        print >> buffer, '''</tr>'''
        
class UpServer:
    """Keep track of an up server state"""
    def __init__(self, info):
        if isinstance(info, str):
            serverInfo = info.split(',')
            # order here is host, port, rack, used, free, util, nblocks, last
	    # heard, nblks corrupt, numDrives
            for i in xrange(len(serverInfo)):
                s = serverInfo[i].split('=')
                setattr(self, s[0].strip(), s[1].strip())

            if not hasattr(self, 'numDrives'):
	    	self.numDrives = 0

            if hasattr(self, 'ncorrupt'):
                n = int(self.ncorrupt)
                self.ncorrupt = n
                if self.ncorrupt > 0:
                    global nodesWithDiskErrors
                    nodesWithDiskErrors = nodesWithDiskErrors + 1
                    
            if hasattr(self, 's'):
                setattr(self, 'host', self.s)
                delattr(self, 's')

            if hasattr(self, 'p'):
                setattr(self, 'port', self.p)
                delattr(self, 'p')

            if hasattr(self, 'overloaded'):
                delattr(self, 'overloaded')
                setattr(self, 'overloaded', 1)
            else:
                setattr(self, 'overloaded', 0)            

            self.down = 0
            self.retiring = 0
            
        if isinstance(info, DownServer):
            self.host = info.host
            self.port = info.port
            self.down = 1
            self.retiring = 0            
        
    def __cmp__(self, other):
        """ Order by IP"""
        return cmp(socket.inet_aton(self.host), socket.inet_aton(other.host))

    def setRetiring(self):
        self.retiring = 1

    def printStatusHTML(self, buffer, count):
        if count % 2 == 0:
            trclass = ""
        else:
            trclass = "class=odd"

        trclass = ""

        if self.retiring:
            trclass = "class=retiring"
            
        print >> buffer, '''<tr ''', trclass, '''><td align="center">''', self.host, '''</td>'''
        if self.down:
            print >> buffer, '''</tr>'''            
            return
        print >> buffer, '''<td align="center">''', self.numDrives, '''</td>'''
        util = self.util.split('%')
        self.util = '%.2f' % float(util[0])
        print >> buffer, '''<td>''', self.used, '''</td>'''
        print >> buffer, '''<td>''', self.free, '''</td>'''
        print >> buffer, '''<td>''', self.util, '''</td>'''
        print >> buffer, '''<td align="right">''', self.nblocks, '''</td>'''
        print >> buffer, '''<td align="right">''', self.lastheard, '''</td>'''
        print >> buffer, '''</tr>'''

    def printHTMLNodesWithDiskErrors(self, buffer):        
        if hasattr(self, 'ncorrupt'):
            if self.ncorrupt > 0:
                print >> buffer, '''<tr><td align="center">''', self.host, '''</td>'''
                print >> buffer, '''<td>''', self.ncorrupt, '''</td>'''                
                print >> buffer, '''</tr>'''
                
class RackNode:
    def __init__(self, host, rackId):
        self.host = host
        self.rackId = rackId
        self.wasStarted = 0
        self.isDown = 0
        self.overloaded = 0

    def printHTML(self, buffer, count):
        if count % 2 == 0:
            trclass = ""
        else:
            trclass = "class=odd"

        if self.isDown:
            trclass = "class=dead"

        if self.overloaded == 1:
            trclass = "class=overloaded"
            
        if not self.wasStarted:
            trclass = "class=notstarted"
            
        print >> buffer, '''<tr ''', trclass, '''><td align="center">''', self.host, '''</td> </tr>'''
        
def nodeIsNotUp(d):
    x = [u for u in upServers if u.host == d.host and u.port == d.port]
    return len(x) == 0

def nodeIsRetiring(u):
    global retiringServers    
    x = [r for r in retiringServers if u.host == r.host and u.port == r.port]
    return len(x) > 0

def mergeDownUpNodes():
    ''' in the set of down-nodes, mark those that are still down in red'''
    global upServers, downServers, numReallyDownServers
    reallyDown = [d for d in downServers if nodeIsNotUp(d)]
    numReallyDownServers = 0
    uniqueServers = set()
    for d in reallyDown:
        d.setStillDown()
        s = '%s:%s' % (d.host, d.port)
        uniqueServers.add(s)
    numReallyDownServers = len(uniqueServers)

def mergeRetiringUpNodes():
    ''' merge retiring nodes with up nodes'''
    global upServers
    [u.setRetiring() for u in upServers if nodeIsRetiring(u)]
    
def processUpNodes(nodes):
    global upServers
    servers = nodes.split('\t')
    upServers = [UpServer(c) for c in servers if c != '']

def processDownNodes(nodes):
    global downServers    
    servers = nodes.split('\t')
    if servers != "":
        downServers = [DownServer(c) for c in servers if c != '']
        downServers.sort()

def processRetiringNodes(nodes):
    global retiringServers    
    servers = nodes.split('\t')
    if servers != "":
        retiringServers = [RetiringServer(c) for c in servers if c != '']
        retiringServers.sort()

def bytesToReadable(v):
    s = long(v)
    if (v > (1 << 50)):
        r = "%.2f PB" % (float(v) / (1 << 50))
        return r
    if (v > (1 << 40)):
        r = "%.2f TB" % (float(v) / (1 << 40))
        return r
    if (v > (1 << 30)):
        r = "%.2f GB" % (float(v) / (1 << 30))
        return r
    if (v > (1 << 20)):
        r = "%.2f MB" % (float(v) / (1 << 20))
        return r
    return "%.2f bytes" % (v)
    
def processSystemInfo(sysInfo):
    global systemInfo
    info = sysInfo.split('\t')
    if len(info) < 3:
        return
    systemInfo.startedAt = info[0].split('=')[1]
    # convert the units and we a
    s = long(info[1].split('=')[1])
    systemInfo.totalSpace = bytesToReadable(s)
    s = long(info[2].split('=')[1])    
    systemInfo.usedSpace = bytesToReadable(s)

def updateServerState(rackId, host, server):
    global serversByRack
    if rackId in serversByRack:
        # we really need a find_if()
        for r in serversByRack[rackId]:
            if r.host == host:
                if isinstance(server, UpServer):
                    r.overloaded = server.overloaded
                r.wasStarted = 1
                if hasattr(server, 'stillDown'):
                    r.isDown = server.stillDown
                    if r.isDown:
                        r.overloaded = 0

def splitServersByRack():
    global upServers, downServers, serversByRack
    for u in upServers:
        s = socket.gethostbyname(u.host)
        rackId = int(s.split('.')[2])
        updateServerState(rackId, s, u)
        
    for u in downServers:
        s = socket.gethostbyname(u.host)
        rackId = int(s.split('.')[2])
        updateServerState(rackId, s, u)

        
def ping(metaserver):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((metaserver.node, metaserver.port))
    req = "PING\r\nVersion: KFS/1.0\r\nCseq: 1\r\n\r\n"
    sock.send(req)
    sockIn = sock.makefile('r')
    for line in sockIn:
        if line.find('Down Servers:') == 0:
            if (len(line.split(':')) < 2):
                break
            pos = line.find(':')
            downNodes = line[pos+1:].strip()
            processDownNodes(downNodes)
            break
        if line.find('Retiring Servers:') == 0:
            if (len(line.split(':')) < 2):
                continue
            pos = line.find(':')
            retiringNodes = line[pos+1:].strip()
            processRetiringNodes(retiringNodes)
            continue

        if line.find('WORM:') == 0:
            try:
                global systemInfo
                wormMode = line.split(':')[1].strip()
                if int(wormMode) == 1:
                    systemInfo.wormMode = "Enabled"
                else:
                    systemInfo.wormMode = "Disabled"
            except:
                pass
            
        if line.find('System Info:') == 0:
            if (len(line.split(':')) < 2):
                continue
            pos = line.find(':')
            infoPart = line[pos+1:].strip()
            processSystemInfo(infoPart)
            continue
            
        if line.find('Servers:') != 0:
            continue
        nodes = line.split(':')[1].strip()
        processUpNodes(nodes)        

    mergeDownUpNodes()
    mergeRetiringUpNodes()
    upServers.sort()
    
    sock.close()    

def printStyle(buffer):
    print >> buffer, '''
    <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
<link rel="stylesheet" type="text/css" href="files/kfsstyle.css"/>
<script type="text/javascript" src="files/sorttable/sorttable.js"></script>
<title>KFS Status</title>
</head>
'''
    
def systemStatus(buffer):
    rows = ''
    print >> buffer, '''
<body class="oneColLiqCtr">
<div id="container">
  <div id="mainContent">
    <h1> KFS Status </h1>
    <div class="info-table">
    <table cellspacing="0" cellpadding="0.1em">
    <tbody>
    <tr> <td> Started at </td><td>:</td><td> ''', systemInfo.startedAt, ''' </td></tr>
    <tr> <td> Total space </td><td>:</td><td> ''', systemInfo.totalSpace, ''' </td></tr>
    <tr> <td> Used space </td><td>:</td><td> ''', systemInfo.usedSpace, '''</td></tr>
    <tr> <td> WORM mode </td><td>:</td><td> ''', systemInfo.wormMode,  '''</td></tr>
    <tr> <td> Number of alive nodes</td><td>:</td><td> ''', len(upServers), '''</td></tr>
    <tr> <td> Number of dead nodes</td><td>:</td><td>''', numReallyDownServers, '''</td></tr>
    <tr> <td> Number of retiring nodes </td><td>:</td><td>''', len(retiringServers), '''</td></tr>
    </tbody>
    </table>
    </div>
    <br />

    <div class="floatleft">
     <table class="sortable status-table" id="table1" cellspacing="0" cellpadding="0.1em" summary="Status of nodes in the system: who is up/down and when we last heard from them">
     <caption> All Nodes </caption>
     <thead>
     <tr><th> Chunkserver </th> <th> # of drives </th> <th> Used </th> <th> Free </th> <th> Used% </th> <th> # of blocks </th> <th> Last heard </th> </tr>
     </thead>
     <tbody>
    '''
    count = 0    
    for v in upServers:
        v.printStatusHTML(buffer, count)
        count = count + 1        
    print >> buffer, '''
    </tbody>
    </table></div>'''
    if len(retiringServers) > 0:
        print >> buffer, '''
    <div class="floatleft">
     <table class="status-table" cellspacing="0" cellpadding="0.1em" summary="Status of retiring nodes in the system">
     <caption> <a name="RetiringNodes">Retiring Nodes Status</a> </caption>
     <thead>
     <tr><th> Chunkserver </th> <th> Start </th> <th>  # blks done </th> <th> # blks left </th> </tr>
     </thead>
     <tbody>
        '''
        count = 0
        for v in retiringServers:
            v.printStatusHTML(buffer, count)
            count = count + 1
        print >> buffer, '''
        </tbody>
        </table></div>'''

        # print >> buffer, '''<H2> <a name="DeadNodes">Dead Nodes History</a></H2>
        # <table cellspacing="0" cellpadding="0.1em" summary="Status of retiring nodes in the system">        
    if len(downServers) > 0:
        print >> buffer, '''<div class="floatleft">
        <table class="status-table" cellspacing="0" cellpadding="0.1em" summary="Status of retiring nodes in the system">                
        <caption> <a name="DeadNodes">Dead Nodes History</a></caption>
     <thead>
        <tr><th> Chunkserver </th> <th> Down Since </th> <th> Reason </th> </tr>     
     </thead>
     <tbody>
        '''
        count = 0            
        for v in downServers:
            v.printStatusHTML(buffer, count)
            count = count + 1                    
        print >> buffer, '''
        </tbody>        
        </table></div>'''

    global nodesWithDiskErrors
    if nodesWithDiskErrors > 0:
        print >> buffer, '''<div class="floatleft">
        <table class="status-table" cellspacing="0" cellpadding="0.1em" summary="Status of nodes with disk errors">                
        <caption> <a name="NodesWithDiskErrors">Nodes With Disk Errors</a></caption>
     <thead>
        <tr><th> Chunkserver </th> <th> Number of Errors </th> </tr>     
     </thead>
     <tbody>
        '''
        for v in upServers:
            v.printHTMLNodesWithDiskErrors(buffer)
        print >> buffer, '''
        </tbody>        
        </table></div>'''
        

    print >> buffer, '''
    </div>
    </div>
    </body>
    </html>'''

def printRackViewHTML(rack, servers, buffer):
    '''Print out all the servers in the specified rack'''
    print >> buffer, '''
    <div class="floatleft">
     <table class="network-status-table" cellspacing="0" cellpadding="0.1em" summary="Status of nodes in the rack ''', rack, ''' ">
     <tbody><tr><td><b>Rack : ''', rack,'''</b></td></tr>'''
    count = 0
    for s in servers:
        s.printHTML(buffer, count)
        count = count + 1
    print >> buffer, '''</tbody></table></div>'''

def rackView(buffer):
    global serversByRack
    splitServersByRack()
    numNodes = sum([len(v) for v in serversByRack.itervalues()])
    print >> buffer, '''
<body class="oneColLiqCtr">
<div id="container">
  <div id="mainContent">
	  <table width=100%>
		  <tr>
			  <td>
         <p> Number of nodes: ''', numNodes, ''' </p>
				</td>
				<td align=right>
					<table class="network-status-table" font-size=14>
					<tbody>
					  <tr class=notstarted><td></td><td>Not Started</td></tr>
						<tr class=dead><td></td><td>Dead Node</td></tr>
						<tr class=retiring><td></td><td>Retiring Node</td></tr>
						<tr class=><td ></td><td>Healthy</td></tr>
                                                <tr class=overloaded><td></td><td>Healthy, but not enough space for writes</td></tr>
					</tbody>
				  </table>
			  </td>
		  </tr>
	  </table>
		<hr>'''  
    for rack, servers in serversByRack.iteritems():
        printRackViewHTML(rack, servers, buffer)

    print >> buffer, '''
    </div>
    </div>
    </body>
    </html>'''

    
class Pinger(SimpleHTTPServer.SimpleHTTPRequestHandler):
    def __init__(self, request, client_address, server):
        self.logfp = open('serverlog.txt', 'w')
        SimpleHTTPServer.SimpleHTTPRequestHandler.__init__(self, request, client_address, server)
        
    def setMeta(self, meta):
        self.metaserver = meta
        
    def do_GET(self):
        global metaserverPort, docRoot
        try:
            if self.path.startswith('/favicon.ico'):
                self.send_response(200)
                return
            if self.path.startswith('/files'):
                # skip over '/files/
                fpath = os.path.join(docRoot, self.path[7:])
                try:
                    self.copyfile(urllib.urlopen(fpath), self.wfile)
                except IOError:
                    self.send_response(404)
                return

            metaserver = ServerLocation(node='localhost',
                                        port=metaserverPort)
            ping(metaserver)
            txtStream = StringIO()
                
            printStyle(txtStream)            
            if self.path.startswith('/cluster-view'):
                rackView(txtStream)
            else:
                systemStatus(txtStream)
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.send_header('Content-length', txtStream.tell())
            self.end_headers()
            self.wfile.write(txtStream.getvalue())
            
        except IOError:
            self.send_error(504, 'Unable to ping metaserver')

    def log_request(self, code='-',size='-'):
        print >> self.logfp, self.date_time_string(), '\t', self.address_string(), '\t', code, size
        
if __name__ == '__main__':
    PORT=20001
    allMachinesFile = ""
    if len(sys.argv) != 2:
        print "Usage : ./kfsstatus.py <server.conf>"
        sys.exit()
        
    if not os.path.exists(sys.argv[1]):
        print "Unable to open ", sys.argv[1]
        sys.exit()

    config = ConfigParser()
    config.readfp(open(sys.argv[1], 'r'))
    metaserverPort = config.getint('webserver', 'webServer.metaserverPort')
    docRoot = config.get('webserver', 'webServer.docRoot')
    PORT = config.getint('webserver', 'webServer.port')
    allMachinesFile = config.get('webserver', 'webServer.allMachinesFn')

    if not os.path.exists(allMachinesFile):
        print "Unable to open all machines file: ", allMachinesFile
    else:
        # Read in the list of nodes that we should be running a chunkserver on
        print "Starting HttpServer..."
        for line in open(allMachinesFile, 'r'):
            s = socket.gethostbyname(line.strip())
            rackId = int(s.split('.')[2])
            if rackId in serversByRack:
                serversByRack[rackId].append(RackNode(s, rackId))
            else:
                serversByRack[rackId] = [RackNode(s, rackId)]
        
    httpd = SocketServer.TCPServer(('', PORT), Pinger)
    httpd.serve_forever()
