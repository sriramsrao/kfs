//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: rebalancer_main.cc 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2010/05/03
//
// Copyright 2010 Quantcast Corporation.  All rights reserved.
// Quantcast PROPRIETARY and CONFIDENTIAL.
//
// Rebalancer for KFS: 
// Inputs:
//  1. nodes.def --- that lists what the nodes are, what racks they
// are on, and what their utilizations are. 
//  2. For each node, a file that lists the blocks on that node, what
// drives they are on
//
// What this tool does: From the nodes.def file, the tool works out
// what the average node utilization should be; what nodes are above
// the utilization threshold.  Then, for the under-utilized nodes, we
// load in the blocks stored on those node. Next, for each
// over-utilized nodes, we work out which blocks need to be moved:
//  --- we compute the # of blocks that have to be moved
//  --- from each of the drives, we determine how many blocks have to
// be moved (do a weighted average based on the # of blocks on the
// drive)
//  --- for each block that has to be moved, we see which
// under-utilized node has space AND is currently not hosting the
// block; we put it there and update view.  
//  --- if an under-utilized node ever becomes "full", we take it out
// of the candidate list.
//  Complexity: 
//     for each overloaded node, O(# of blocks that need to be moved off that node)
//
//----------------------------------------------------------------------------

#include "rebalancer.h"
#include "common/kfsdecls.h"
#include <string.h>
#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include <fstream>
#include <sstream>
#include <boost/lexical_cast.hpp>

using namespace KFS;
using std::string;
using std::vector;
using std::ifstream;
using std::istringstream;
using std::cout;
using std::endl;

void
ChunkServer::printStats() 
{
    cout << mLocation.hostname << ' ' << mNumChunks << ' ' 
         << mUsedSpace << ' ' << mTotalSpace << ' ' << getUtilization() << endl;
}

void split(const string &str, vector<string> &parts, char sep)
{
    string::size_type start = 0;
    string::size_type slash = 0;

    while (slash != string::npos) {
        assert(slash == 0 || str[slash] == sep);
        slash = str.find(sep, start);
        string nextc = str.substr(start, slash - start);
        start = slash + 1;
        parts.push_back(nextc);
    }
}

void 
ChunkServer::loadChunks(string &dataDir, bool splitByDrive)
{
    string fn = dataDir + "/" + mLocation.hostname + ".list";
    ifstream file(fn.c_str());
    string driveName;
    int numFiles = 0;

    cout << "Loading data from: " << fn << "...";

    mNumChunks = 0;
    mUsedSpace = 0;
    while (!file.eof()) {
        // read the drive and # of files that follow
        file >> driveName;
        file >> numFiles;
        if (file.eof())
            break;
        DriveInfo drive;
        drive.usedSpace = 0;
        for (int i = 0; i < numFiles; i++) {
            vector<string> parts;
            ChunkIdSz c;

            // output is from ls -1sS: <size in 1k blocks> <filename>
            file >> c.chunksz;
            file >> fn;
            if (c.chunksz == 0)
                continue;
            split(fn, parts, '.');
            c.cid = boost::lexical_cast<kfsChunkId_t>(parts[1]);
            if (!splitByDrive) {
                addChunk(c.cid, c.chunksz);
                continue;
            }
            mNumChunks++;
            mUsedSpace += c.chunksz;
            drive.usedSpace += c.chunksz;
            drive.chunks.push_back(c);
        }
        if (splitByDrive) {
            mDrives.push_back(drive);
        }
    }
    cout << "Done" << endl;
}

void
Rebalancer::readNodesFn(const string nodesFn)
{
    ifstream file(nodesFn.c_str());
    const int MAXLINE = 4096;
    char line[MAXLINE];

    if (file.fail()) {
        cout << "Unable to open: " << nodesFn << endl;
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
        // in GB
        double usedSpace;
        double freeSpace;

        ist >> loc.hostname;
        ist >> loc.port;
        ist >> rack;
        ist >> usedSpace;
        ist >> freeSpace;

        ChunkServerPtr csp;

        csp.reset(new ChunkServer(loc, rack, (off_t) (usedSpace * 1024 * 1024), 
                                  (off_t) (freeSpace * 1024 * 1024)));
        mChunkServers.push_back(csp);
        mAvgSpaceUtil += csp->getUtilization();
        csp->printStats();
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

    for (uint32_t i = 0; i < mChunkServers.size(); i++) {
        double util = mChunkServers[i]->getUtilization();
        if (util < mMinRebalanceSpaceUtilThreshold)
            mUnderLoadedServers.push_back(mChunkServers[i]);
        else if (util > mMaxRebalanceSpaceUtilThreshold)
            mOverLoadedServers.push_back(mChunkServers[i]);
    }
}

void
Rebalancer::loadChunks(string dataDir)
{
    for (uint32_t i = 0; i < mUnderLoadedServers.size(); i++) 
        mUnderLoadedServers[i]->loadChunks(dataDir);
        
    // for the overloaded servers, split the chunks by drive so that
    // we can do the rebalancing that also evens out the utilization
    // amongst the N drives on the box
    for (uint32_t i = 0; i < mOverLoadedServers.size(); i++)
        mOverLoadedServers[i]->loadChunks(dataDir, true);

}

void
Rebalancer::rebalance(string outputFn)
{
    mOutFd = open(outputFn.c_str(), O_CREAT|O_WRONLY, S_IRUSR|S_IWUSR);
    if (mOutFd < 0) {
        perror("open: ");
        exit(-1);
    }
    for (uint32_t i = 0; i < mOverLoadedServers.size(); i++)
        rebalance(mOverLoadedServers[i]);
    close(mOutFd);
}

void
Rebalancer::rebalance(ChunkServerPtr &source)
{
    uint32_t nextServerIdx = 0;
    off_t targetUsedSpace;
    off_t targetSpaceToBeFreed;
    off_t usedSpace = source->getUsedSpace();

    // work out how many chunks need to be moved
    
    // Ideally, the space used on the box should be:
    targetUsedSpace = mMaxRebalanceSpaceUtilThreshold * source->getTotalSpace();
    
    targetSpaceToBeFreed = usedSpace - targetUsedSpace;

    vector<DriveInfo> &drives = source->getDrives();

    cout << "Rebalancing: " << source->ToString() << ' '
         << "used space: " << usedSpace << ' '
         << " target to be freed: " << targetSpaceToBeFreed << ' '
         << " target utilization: " << targetUsedSpace / (double) source->getTotalSpace()
         << endl;
        
    for (uint32_t i = 0; i < drives.size(); i++) {
        if (drives[i].usedSpace == 0)
            continue;
        // do a weighted average: weight the amount of data to be
        // moved of the drive based on how much space is used on the
        // drive proportional to space used on the node.
        off_t targetSpaceToFreeOnDrive = 
            targetSpaceToBeFreed * (drives[i].usedSpace / (double) usedSpace);

        cout << "Rebalancing: " << source->ToString()
             << " working on drive: " << i
             << " used space on drive: " << drives[i].usedSpace << ' '
             << " target to be freed: " << targetSpaceToFreeOnDrive << ' '
             << endl;
    
        uint32_t nextChunk = 0;
        int numChunksMoved = 0;
        while (1) {
            if (targetSpaceToFreeOnDrive <= 0) {
                break;
            }
            if (mUnderLoadedServers.size() == 0)
                break;
            if (nextChunk >= drives[i].chunks.size())
                break;

            kfsChunkId_t cid = drives[i].chunks[nextChunk].cid;
            off_t chunksize = drives[i].chunks[nextChunk].chunksz;
            nextChunk++;
            for (int retryCount = 0; retryCount < 5; retryCount++) {
                nextServerIdx %= mUnderLoadedServers.size();
                if (mUnderLoadedServers[nextServerIdx]->isHostingChunk(cid)) {
                    nextServerIdx++;
                    continue;
                }
                RebalancePlanInfo_t rpi;
                ChunkServerPtr dst = mUnderLoadedServers[nextServerIdx];
                rpi.chunkId = cid;
                
                strncpy(rpi.dst, dst->getLocation().ToString().c_str(),
                        RebalancePlanInfo_t::hostnamelen);
                strncpy(rpi.src, source->getLocation().ToString().c_str(),
                        RebalancePlanInfo_t::hostnamelen);
                write(mOutFd, &rpi, sizeof(RebalancePlanInfo_t));


                // cout << "Moving chunk: " << cid << " from: " << source->ToString() << " -> " 
                // << mUnderLoadedServers[nextServerIdx]->ToString() << endl;
                mUnderLoadedServers[nextServerIdx]->addChunk(cid, chunksize);
                if (mUnderLoadedServers[nextServerIdx]->getUtilization() > mMaxRebalanceSpaceUtilThreshold) {
                    uint32_t lastIdx = mUnderLoadedServers.size() - 1;
                    mUnderLoadedServers[nextServerIdx] = mUnderLoadedServers[lastIdx];
                    mUnderLoadedServers.pop_back();
                    nextServerIdx = 0;
                } else {
                    nextServerIdx++;
                }
                numChunksMoved++;
                targetSpaceToFreeOnDrive -= chunksize;
                source->removeChunk(cid, chunksize);
                break;
            }
            if (source->getUtilization() < mMaxRebalanceSpaceUtilThreshold)
                break;
        }
        cout << "Rebalancing: " << source->ToString()
             << " done working on drive: " << i 
             << " moved: " << numChunksMoved
             << endl;

    }
}

void
Rebalancer::printStats()
{
    for (uint32_t i = 0; i < mChunkServers.size(); i++)
        mChunkServers[i]->printStats();
}

int
main(int argc, char **argv)
{
    Rebalancer rebalancer;

    if (argc < 3) {
        cout << "Usage: " << argv[0] << " <network defn> <data dir> <outputFn> " << endl;
        exit(0);
    }

    rebalancer.readNodesFn(argv[1]);
    rebalancer.loadChunks(argv[2]);
    cout << "Status before rebalance: " << endl;
    rebalancer.printStats();
    rebalancer.rebalance(argv[3]);
    cout << "Status after rebalance: " << endl;
    rebalancer.printStats();
    
}
