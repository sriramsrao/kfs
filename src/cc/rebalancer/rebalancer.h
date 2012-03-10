//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: rebalancer.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2010/05/04
//
// Copyright 2010 Quantcast Corporation.  All rights reserved.
// Quantcast PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef REBALANCER_REBALANCER_H
#define REBALANCER_REBALANCER_H
#include <stdint.h>
#include <vector>
#include <string>
#include <set>
#include <boost/shared_ptr.hpp>
#include "common/kfstypes.h"
#include "common/kfsdecls.h"

namespace KFS
{
    struct ChunkIdSz {
        kfsChunkId_t cid;
        int  chunksz;
    };

    struct DriveInfo {
        off_t usedSpace;
        std::vector<ChunkIdSz> chunks;
    };

    class ChunkServer {
    public:
        ChunkServer(const ServerLocation &loc, int rack, off_t usedSpace, off_t freeSpace) :
            mLocation(loc), mRack(rack), mNumChunks(0),
            mUsedSpace(usedSpace) {
            mTotalSpace = usedSpace + freeSpace;
        }
        bool isHostingChunk(kfsChunkId_t cid) {
            return mChunks.find(cid) != mChunks.end();
        }
        void addChunk(kfsChunkId_t cid, size_t chunksize) {
            mChunks.insert(cid);
            mNumChunks++;
            mUsedSpace += chunksize;
        }
        void removeChunk(kfsChunkId_t cid, size_t chunksize) {
            mChunks.erase(cid);
            mNumChunks--;
            mUsedSpace -= chunksize;
        }
        double getUtilization() const {
            return ((double) mUsedSpace) / ((double) mTotalSpace);
        }
        ServerLocation &getLocation() {
            return mLocation;
        }
        std::vector<DriveInfo> &getDrives() {
            return mDrives;
        }
        void loadChunks(std::string &dataDir, bool splitByDrive = false);
        off_t getTotalSpace() const {
            return mTotalSpace;
        }
        off_t getUsedSpace() const {
            return mUsedSpace;
        }
        std::string ToString() {
            return mLocation.ToString();
        }
        void printStats();
        
    private:
        std::set<kfsChunkId_t> mChunks;
        // aggregate the chunks on a node based on the drive on which
        // it is stored
        std::vector<DriveInfo> mDrives;
        ServerLocation mLocation;
        int     mRack;
        int     mNumChunks;
        // These are in K units
        off_t	mUsedSpace;
        off_t   mTotalSpace;
    };

    typedef boost::shared_ptr<ChunkServer> ChunkServerPtr;
    
    class Rebalancer {
    public:
        Rebalancer() : mPercentVariationFromMean(0.1) { };
        void readNodesFn(const std::string nodesFn);
        void loadChunks(const std::string dataDir);
        void rebalance(std::string outputFn);
        void printStats();
    private:
        std::vector<ChunkServerPtr> mChunkServers;
        std::vector<ChunkServerPtr> mUnderLoadedServers;
        std::vector<ChunkServerPtr> mOverLoadedServers;
        int mOutFd;
        double mAvgSpaceUtil;
        double mMinRebalanceSpaceUtilThreshold;
        double mMaxRebalanceSpaceUtilThreshold;
        double mPercentVariationFromMean;

        void rebalance(ChunkServerPtr &source);
    };

    // When the rebalance planner it works out a plan that specifies
    // which chunk has to be moved from src->dst
    struct RebalancePlanInfo_t {
        static const int hostnamelen = 256;
        chunkId_t chunkId;
        char dst[hostnamelen];
        char src[hostnamelen];
    };

}

#endif // REBALANCER_REBALANCER_H
