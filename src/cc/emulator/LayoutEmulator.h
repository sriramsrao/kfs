//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: LayoutEmulator.h 1552 2011-01-06 22:21:54Z sriramr $
//
// Created 2008/08/08
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

#ifndef EMULATOR_LAYOUTEMULATOR_H
#define EMULATOR_LAYOUTEMULATOR_H

#include <string>
#include <map>
#include <tr1/unordered_map>
#include "meta/LayoutManager.h"

namespace KFS
{

    class LayoutEmulator : public LayoutManager {
    public:
        LayoutEmulator() : mPercentVariationFromMean(0.1), mNumBlksRebalanced(0) {
            SetMinChunkserversToExitRecovery(0);
            ToggleRebalancing(true);
        };

        // Given a chunk->location data in a file, rebuild the chunk->location map.
        //
        int LoadChunkmap(const std::string &chunkLocationFn, bool addChunksToReplicationChecker = false);

        void AddServer(const ServerLocation &loc, int rack, uint64_t totalSpace, uint64_t usedSpace);

        void SetupForRebalancePlanning(int utilVariationFromMean) {
            mDoingRebalancePlanning = true;
            mPercentVariationFromMean = utilVariationFromMean / 100.0;
        }
        int SetRebalancePlanOutFile(const std::string &rebalancePlanFn);
        int BuildRebalancePlan();

        void ChunkReplicationDone(MetaChunkReplicate *req);
        
        void ExecuteRebalancePlan();

        void PrintChunkserverBlockCount();

        void ReadNetworkDefn(const std::string &networkFn);

        int VerifyRackAwareReplication(bool checkSize, bool verbose);

        seq_t GetChunkversion(fid_t fid, chunkId_t cid);

        size_t GetChunkSize(chunkId_t cid);

        vector<size_t> GetChunkSizes(chunkId_t cid);

        void MarkServerDown(const ServerLocation &loc);

        int GetNumBlksRebalanced() const {
            return mNumBlksRebalanced;
        }
    private:
        void Parse(const char *line, bool addChunksToReplicationChecker);
        bool mDoingRebalancePlanning;

        // for the purposes of rebalancing, we compute the cluster
        // wide average space utilization; then we take into the
        // desired variation from mean to compute thresholds that determine
        // which nodes are candidates for migration.
        float mPercentVariationFromMean;
        double mAvgSpaceUtil;

        int mNumBlksRebalanced;

        struct ChunkIdHash
            : public std::unary_function<chunkId_t, std::size_t>
        {
            std::size_t operator()(chunkId_t v) const
            {
                // two >> to get rid of compiler warning
                // when sizeof(v) == sizeof(size_t)
                const std::size_t vs(v >> (sizeof(std::size_t) * 8 - 1));
                return (std::size_t(v) ^ std::size_t((vs >> 1)));
            }
        };
        typedef std::tr1::unordered_map<chunkId_t, std::vector<size_t>, ChunkIdHash > ChunkSizeMap;

        ChunkSizeMap mChunkSize;
    };

    extern LayoutEmulator gLayoutEmulator;
}

#endif // EMULATOR_LAYOUTEMULATOR_H
