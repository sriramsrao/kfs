//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: LayoutEmulator.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2008/08/27
//
//
// Copyright 2008 Quantcast Corp.
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
// \brief Emulator for the layout manager: read in a chunk->location
// map; we can then migrate blocks around to experiment with placement algorithms.
//
//----------------------------------------------------------------------------

#include "LayoutEmulator.h"
#include "ChunkServerEmulator.h"
#include "common/kfstypes.h"
#include "common/log.h"
#include "meta/kfstree.h"

#include <algorithm>
#include <cerrno>
#include <fstream>
#include <sstream>
#include <set>
#include <vector>

using namespace KFS;
using std::string;
using std::ifstream;
using std::istringstream;
using std::vector;
using std::for_each;
using std::mem_fun;
using std::cout;
using std::endl;
using std::map;
using std::set;

LayoutEmulator KFS::gLayoutEmulator;

int LayoutEmulator::LoadChunkmap(const string &chunkLocationFn, bool addChunksToReplicationChecker)
{
    ifstream file(chunkLocationFn.c_str());
    int lineno = 0;
    const int MAXLINE = 4096;
    char line[MAXLINE];

    if (file.fail()) {
        KFS_LOG_VA_INFO("Unable to open: %s", chunkLocationFn.c_str());
        return -EEXIST;
    }

    gLayoutManager.GetChunkToServerMap(mChunkToServerMap);

    file.getline(line, MAXLINE);

    while (!file.eof()) {
        if (file.fail())
            break;

        Parse(line, addChunksToReplicationChecker);
        ++lineno;
        file.getline(line, MAXLINE);

    }
    return 0;
}

class MatchingServer {
	ServerLocation loc;
public:
	MatchingServer(const ServerLocation &l) : loc(l) { }
	bool operator() (ChunkServerPtr &s) {
		return s->MatchingServer(loc);
	}
};

void LayoutEmulator::Parse(const char *line, bool addChunksToReplicationChecker)
{
    istringstream ist(line);
    kfsChunkId_t cid;
    fid_t fid;
    int numServers;
    int rack;
    off_t chunksize;

    // format of the file:
    // <chunkid> <fileid> <# of servers> [server location]
    // \n
    // where, <server location>: replica-size name port rack#
    // and we have as many server locations as # of servers

    ist >> cid;
    ist >> fid;

    if (addChunksToReplicationChecker)
        ChangeChunkReplication(cid);

    ist >> numServers;

    MetaFattr *fa = metatree.getFattr(fid);
    
    if (fa == NULL)
        return;

    // compute the chunk's size from the filesize
    vector<MetaChunkInfo *> v;
    metatree.getalloc(fid, v);
    chunksize = 0;

    if (v.size() > 0) {
        MetaChunkInfo *lastChunk = v.back();
        if (lastChunk->chunkId == cid) {
            if (fa->filesize > 0) {
                chunksize = fa->filesize - (fa->chunkcount - 1) * CHUNKSIZE;
                if (chunksize < 0) {
                    cout << "Resetting chunksize for chunk: " << cid << " filesize: " << fa->filesize << endl;
                    chunksize = 0;
                }
            }
        } else
            chunksize = CHUNKSIZE;
    }

    for (int i = 0; i < numServers; i++) {
        ServerLocation loc;
        vector <ChunkServerPtr>::iterator j;

        ist >> loc.hostname;
        ist >> loc.port;
        ist >> rack;

        if (mDoingRebalancePlanning) {
            // don't take chunks that are smaller than 1MB
            if (chunksize < (1 << 20))
                return;
        }

        j = find_if(mChunkServers.begin(), mChunkServers.end(), MatchingServer(loc));
        if (j == mChunkServers.end()) {
            KFS_LOG_VA_INFO("%s: Unable to find server: %s:%d", line,
                            loc.hostname.c_str(), loc.port);
            continue;
        }
        ChunkServerEmulator *c = (ChunkServerEmulator *) ((*j).get());
        UpdateChunkToServerMapping(cid, c);

	// OFF_TYPE_CAST: off_t casted to size_t (twice).
	// Should be fine though since 'chunksize' contains single chunk size.
        c->HostingChunk(cid, chunksize);
        mChunkSize[cid].push_back(chunksize);
    }
}

// override what is in the layout manager (only for the emulator code)
void
LayoutEmulator::ChunkReplicationDone(MetaChunkReplicate *req)
{
    vector<ChunkServerPtr>::iterator source;

    mOngoingReplicationStats->Update(-1);

    // Book-keeping....
    CSMapIter iter = mChunkToServerMap.find(req->chunkId);

    if (iter != mChunkToServerMap.end()) {
        iter->second.ongoingReplications--;
        if (iter->second.ongoingReplications == 0)
            // if all the replications for this chunk are done,
            // then update the global counter.
            mNumOngoingReplications--;

        if (iter->second.ongoingReplications < 0)
            // sanity...
            iter->second.ongoingReplications = 0;
    }

    mNumBlksRebalanced++;

    req->server->ReplicateChunkDone(req->chunkId);

    source = find_if(mChunkServers.begin(), mChunkServers.end(),
                     MatchingServer(req->srcLocation));
    if (source !=  mChunkServers.end()) {
        (*source)->UpdateReplicationReadLoad(-1);
    }

    if (req->status != 0) {
        // Replication failed...we will try again later
        KFS_LOG_VA_INFO("%s: re-replication for chunk %lld failed, code = %d",
			req->server->GetServerLocation().ToString().c_str(),
			req->chunkId, req->status);
        mFailedReplicationStats->Update(1);
        return;
    }

    // replication succeeded: book-keeping

    UpdateChunkToServerMapping(req->chunkId, req->server.get());
}

void
LayoutEmulator::MarkServerDown(const ServerLocation &loc)
{
    vector <ChunkServerPtr>::iterator j;
    j = find_if(mChunkServers.begin(), mChunkServers.end(), MatchingServer(loc));
    if (j == mChunkServers.end()) {
        KFS_LOG_VA_INFO("Unable to find server: %s:%d", loc.hostname.c_str(), loc.port);
        return;
    }

    ChunkServer *c = (*j).get();
    ServerDown(c);

    KFS_LOG_VA_INFO("Taking down server: %s:%d", loc.hostname.c_str(), loc.port);
}

class ChunkIdMatcher {
    chunkId_t myid;
public:
    ChunkIdMatcher(chunkId_t c) : myid(c) { }
    bool operator() (MetaChunkInfo *c) {
        return c->chunkId == myid;
    }
};

seq_t
LayoutEmulator::GetChunkversion(fid_t fid, chunkId_t cid)
{
    vector<MetaChunkInfo *> v;
    vector<MetaChunkInfo *>::iterator chunk;

    metatree.getalloc(fid, v);

    chunk = find_if(v.begin(), v.end(), ChunkIdMatcher(cid));
    if (chunk != v.end()) {
        MetaChunkInfo *mci = *chunk;
        return mci->chunkVersion;
    }
    return 1;
}

vector<size_t>
LayoutEmulator::GetChunkSizes(chunkId_t cid)
{
    ChunkSizeMap::iterator iter2 = mChunkSize.find(cid);
    if (iter2 == mChunkSize.end()) {
        std::vector<size_t> v;
        return v;
    }
    return iter2->second;
}

size_t
LayoutEmulator::GetChunkSize(chunkId_t cid)
{
    ChunkSizeMap::iterator iter2 = mChunkSize.find(cid);

    if (iter2 == mChunkSize.end())
        return CHUNKSIZE;

    // Assume vector's first element is size of chunk
    if (iter2->second.size() == 0)
        return CHUNKSIZE;
    else
        return iter2->second[0];
}

void
LayoutEmulator::AddServer(const ServerLocation &loc, int rack, uint64_t totalSpace, uint64_t usedSpace)
{
    ChunkServerEmulatorPtr c;
    vector<RackInfo>::iterator rackIter;

    c.reset(new ChunkServerEmulator(loc, rack));
    c->SetSpace(totalSpace, usedSpace, 0);

    cout << "Server: " << loc.ToString() << " total space: " << c->GetTotalSpace() << " util: " << c->GetSpaceUtilization() << endl;

    mAvgSpaceUtil += c->GetSpaceUtilization();

    // we compute used space as we add chunks
    c->SetSpace(totalSpace, 0, 0);

    mChunkServers.push_back(c);

    ChunkServerPtr c1 = c;

    rackIter = find_if(mRacks.begin(), mRacks.end(), RackMatcher(rack));
    if (rackIter != mRacks.end()) {
        rackIter->addServer(c1);
    } else {
        RackInfo ri(rack);
        ri.addServer(c1);
        mRacks.push_back(ri);
    }
}

int
LayoutEmulator::SetRebalancePlanOutFile(const string &rebalancePlanFn)
{
    int fd = open(rebalancePlanFn.c_str(), O_WRONLY|O_CREAT, S_IRUSR|S_IWUSR);

    if (fd < 0) {
        cout << "Unable to open: " << rebalancePlanFn << " for writing (error=%d)"
             << fd << endl;
        return -1;
    }
    for (vector<ChunkServerPtr>::iterator i = mChunkServers.begin(); i != mChunkServers.end(); i++) {
        ChunkServerEmulator *cse = (ChunkServerEmulator *) ((*i).get());
        cse->SetRebalancePlanOutFd(fd);
    }
    return 0;
}

class OpDispatcher {
public:
    OpDispatcher() { }
    void operator()(ChunkServerPtr &c) {
        c->Dispatch();
    }
};

int
LayoutEmulator::BuildRebalancePlan()
{
    
    ChunkReplicationChecker();
    for_each(mChunkServers.begin(), mChunkServers.end(), OpDispatcher());
    return mChunkReplicationCandidates.size();
}

void
LayoutEmulator::ExecuteRebalancePlan()
{
    int round = 0;
    // the plan has already been worked out; we just execute
    ToggleRebalancing(false);

    while (1) {
        // this will initiate plan execution
        ChunkReplicationChecker();
        for_each(mChunkServers.begin(), mChunkServers.end(), OpDispatcher());
        if (mChunkReplicationCandidates.size() == 0)
            break;
        ++round;
    }
}

class PrintBlockCount {
public:
    PrintBlockCount() { }
    void operator()(ChunkServerPtr &c) {
        ChunkServerEmulator *cse = (ChunkServerEmulator *) c.get();
        cout << cse->ServerID() << ' ' << cse->GetNumChunks() << ' '
             << cse->GetUsedSpace() << ' ' << cse->GetSpaceUtilization() << endl;
    }
};

void
LayoutEmulator::PrintChunkserverBlockCount()
{
    for_each(mChunkServers.begin(), mChunkServers.end(), PrintBlockCount());
}

void
LayoutEmulator::ReadNetworkDefn(const string &networkFn)
{
    ifstream file(networkFn.c_str());
    const int MAXLINE = 4096;
    char line[MAXLINE];

    if (file.fail()) {
        cout << "Unable to open: " << networkFn << endl;
        return;
    }

    // for starters, we want the nodes between 30-70% full
    mMinRebalanceSpaceUtilThreshold = 0.3;
    mMaxRebalanceSpaceUtilThreshold = 0.6;
    
    mAvgSpaceUtil = 0.0;

    while (!file.eof()) {
        file.getline(line, MAXLINE);

        if (file.fail())
            break;
        istringstream ist(line);
        ServerLocation loc;
        int rack;
        uint64_t totalSpace;
        uint64_t usedSpace;

        ist >> loc.hostname;
        ist >> loc.port;
        ist >> rack;
        ist >> totalSpace;
        ist >> usedSpace;

        AddServer(loc, rack, totalSpace, usedSpace);
    }

    if (mChunkServers.size() == 0) {
        mAvgSpaceUtil = 1.0;
        return;
    }

    // Take the average utilizaiton in the cluster; any node that has
    // utilizaiton outside the average is candidate for rebalancing
    mAvgSpaceUtil /= mChunkServers.size();
    mMinRebalanceSpaceUtilThreshold = mAvgSpaceUtil - (mAvgSpaceUtil * mPercentVariationFromMean);
    mMaxRebalanceSpaceUtilThreshold = mAvgSpaceUtil + (mAvgSpaceUtil * mPercentVariationFromMean);

    cout << "Thresholds: average: " << mAvgSpaceUtil << " min: " << mMinRebalanceSpaceUtilThreshold;
    cout << " max: " << mMaxRebalanceSpaceUtilThreshold << endl;
    
}

class RackAwareReplicationVerifier {
    // keep a count of how many chunks have replication on the same rack
    int &sameRack;
    // count of how many chunks are under-replicated
    int &underReplicated;
    // count of how many chunk with mismatch in replica sizes
    int &mismatchSizes;
    // count of how many chunk replicas are of zero size
    int &zeroSizes;
    // count of how many chunk replicas are missing
    int &missing;
    // checks replica size
    bool checkSize;
    // verbose mode
    bool verbose;

public:
    RackAwareReplicationVerifier(int &s, int &u, int &m, int &z,
                                 int &ms, bool cs, bool v) :
        sameRack(s), underReplicated(u), mismatchSizes(m),
        zeroSizes(z), missing(ms), checkSize(cs), verbose(v) { }
    void verify (const map<chunkId_t, ChunkPlacementInfo >::value_type p, ofstream &ofs) {
        chunkId_t cid = p.first;
        ChunkPlacementInfo c = p.second;
        vector<size_t> chunkSizes = gLayoutEmulator.GetChunkSizes(cid);
        set<int> seenRacks;

        for (uint32_t i = 0; i < c.chunkServers.size(); i++) {
            if (seenRacks.find(c.chunkServers[i]->GetRack()) != seenRacks.end()) {
                sameRack++;
                if (verbose) {
                    cout << "Chunk " << cid << " (File " << c.fid << " " << metatree.getPathname(c.fid) << ")";
                    cout << " Multiple copies on same rack" << endl;
                }
                break;
            }
            seenRacks.insert(c.chunkServers[i]->GetRack());
        }

        MetaFattr *fa = metatree.getFattr(c.fid);
        if ((fa == NULL) || (c.chunkServers.size() == 0)) {
            string fileName = metatree.getPathname(c.fid);
            cout << "Chunk " << cid << " (File " << c.fid << " " << fileName << ")";
            cout << " No copies" << endl;
            ofs << fileName << endl;
            missing++;
        } else if ((int) c.chunkServers.size() < fa->numReplicas) {
            underReplicated++;
            cout << "Chunk " << cid << " (File " << c.fid << " " << metatree.getPathname(c.fid) << ")";
            cout << " Under replicated chunk with only " << c.chunkServers.size() << " replicas" << endl;
        }

        if (chunkSizes.size() == 0)
            return;

        for (uint32_t i = 0; i < c.chunkServers.size(); i++) {
            for (uint32_t j = i + 1; j < c.chunkServers.size(); j++) {
                if (c.chunkServers[i] == c.chunkServers[j]) {
                    cout << "Chunk " << cid << " (File " << c.fid << " " << metatree.getPathname(c.fid) << ")";
                    cout << "Two copies of chunk on same node"<< endl;
                }
            }
        }

        if (checkSize) {
            bool isMisMatch = false;
            for (uint32_t i =1 ; i < chunkSizes.size(); i++) {
                if (chunkSizes[i-1] != chunkSizes[i]) {
                    cout << "Chunk " << cid << " (File " << c.fid << " " << metatree.getPathname(c.fid) << ")";
                    cout << " Mismatch in replica sizes" <<  endl;
                    isMisMatch = true;
                }
            }
            if (isMisMatch)
                mismatchSizes++;

            bool isZeroSize = false;
            for (uint32_t i = 0 ; i < chunkSizes.size(); i++) {
                if (chunkSizes[i] == 0) {
                    cout << "Chunk " << cid << " (File " << c.fid << " " << metatree.getPathname(c.fid) << ")";
                    cout << " Replica with size 0" << endl;
                    isZeroSize = true;
                }
            }
            if (isZeroSize)
                zeroSizes++;

            bool missingAll = true;
            for (uint32_t i = 0 ; i < chunkSizes.size(); i++) {
                if (chunkSizes[i] == 0) {
                    missingAll = false;
                }
            }
            if (missingAll) {
                cout << "Chunk " << cid << " (File " << c.fid << " " << metatree.getPathname(c.fid) << ")";
                cout << " is 0-sized; potentially missing chunk " << endl;
                missing++;
            }
        }
    }
};

int
LayoutEmulator::VerifyRackAwareReplication(bool checkSize, bool verbose)
{
    int sameRack = 0, underReplicated = 0, mismatchSize = 0;
    int zeroSize = 0, missing = 0;
    ofstream ofs;
    string missingFileName = "file_missing_blocks.txt";
    RackAwareReplicationVerifier rackVerify =
        RackAwareReplicationVerifier(sameRack, underReplicated,
                                     mismatchSize, zeroSize,
                                     missing, checkSize, verbose);
    ofs.open(missingFileName.c_str());
    cout << "************************************************" << endl;
    cout << " KFS Replica Checker " << endl;
    cout << "************************************************" << endl;
    CSMapIter iter;
    for (iter = mChunkToServerMap.begin(); iter != mChunkToServerMap.end(); iter++) {
    	rackVerify.verify(*iter, ofs);
    }
    ofs.flush();
    ofs.close();
    cout << "************************************************" << endl;
    cout << " Total chunks : " << mChunkToServerMap.size() << endl;
    cout << " Total chunks missing : " << missing << endl;
    cout << " Total chunks on same rack : " << sameRack << endl;
    cout << " Total chunks under replicated : " << underReplicated << endl;
    cout << " Total chunks mismatch replica sizes : " << mismatchSize << endl;
    cout << " Total chunks with zero sizes : " << zeroSize << endl;
    if (missing)
        cout << " Status : KFS CORRUPT" << endl;
    else {
    	cout << " Status : KFS HEALTHY" << endl;
    	unlink(missingFileName.c_str());
    }
    cout << "************************************************" << endl;

    return sameRack;
}
