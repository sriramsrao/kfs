//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: workbuilder.cc 3830 2012-03-05 19:54:45Z sriramr $
//
// Created 2011/01/04
//
// Copyright 2011 Yahoo Corporation.  All rights reserved.
// This file is part of the Sailfish project.
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
//
//----------------------------------------------------------------------------

#include "util.h"
#include "workbuilder.h"
#include "clientmgr.h"

#include "libkfsIO/Globals.h"
#include "common/log.h"

#include <unistd.h>
#include <sys/stat.h>
#include <sstream>
#include <map>
#include <string.h> /* PAUSING */
#include <boost/lexical_cast.hpp>

using namespace KFS;
using namespace workbuilder;
using std::string;
using std::vector;
using std::map;
using std::pair;
using std::set;

ClientManager workbuilder::gClientManager;
Workbuilder workbuilder::gWorkbuilder;

map<string, WorkbuilderOp_t> gWorkbuilderStrToOp;

void Workbuilder::mainLoop(const string &indexDir, const string &iFileBasedir, const string &logdir,
    int clientAcceptPort)
{
    mIFileBasedir = iFileBasedir;
    mLogdir = logdir;

    if (indexDir != "") {
        mJob->loadIndexFromDir(indexDir);
        //delete mJob;
        return;
    }

    gClientManager.StartAcceptor(clientAcceptPort);

    // Every 30 secs pull in the indexes...
    SetTimeoutInterval(30 * 1000);


    // add a timeout handler
    libkfsio::globalNetManager().RegisterTimeoutHandler(this);

    KFS_LOG_INFO("Starting workbuilder...");
    libkfsio::globalNetManager().MainLoop();
}

void Workbuilder::restart()
{
    KFS_LOG_INFO("Workbuilder asked to exit...");
    if (mJob != NULL) {
        // mJob->finalize();
        delete mJob;
    }
    MsgLogger::Stop();
    exit(0);
}

void Workbuilder::handleOp(WorkbuilderOp &op)
{
    int res = 0;

    KFS_LOG_VA_DEBUG("received op, type %i",op.opType);

    switch (op.opType) {
        case kRestart:
            restart();
            break;
        case kMapperStart:
            res = mapperStart(op.params);
            if (res < 0) {
                op.responseCode = 403;
                op.responseLen = strlen("We are running another job");
                op.response = new char[op.responseLen + 1];
                strncpy(op.response, "We are running another job", op.responseLen);
                break;
            }
            op.responseCode = 200;
            break;
        case kChunksAppended:
            res = chunksAppended(op.params, op.contentLen, op.contentData);
            if (res < 0) {
                op.responseCode = 403;
                op.responseLen = strlen("We are running another job");
                op.response = new char[op.responseLen + 1];
                strncpy(op.response, "We are running another job", op.responseLen);
                break;
            }
            op.responseCode = 200;
            break;
        case kBuildPlan:
            res = initBuildPlan(op.params);
            if (res < 0) {
                op.responseCode = 404;
                break;
            }
            op.responseCode = 200;
            break;
        case kChunkLost:
            res = chunkLost(op.params, op.responseCode);
            if (res < 0) {
                op.responseCode = 403;
                op.responseLen = strlen("We are running another job");
                op.response = new char[op.responseLen + 1];
                strncpy(op.response, "We are running another job", op.responseLen);
                break;
            }
            break;
        case kReducerStart:
            if (mAlwaysReplyWithRetry) { //try again response
                op.responseCode = 201;
                break;
            }
            res = reducerStart(op.params, &op.response, op.responseLen,
                op.responseCode);
            KFS_LOG_STREAM_INFO << "Response to reducer start: " << op.responseCode <<
                " retval = " << res << KFS_LOG_EOM;
            break;
        case kReducerRecordCount:
            res = reducerRecordCount(op.params);
            op.responseCode = 200;
            break;
        case kHealthCheck:
            op.responseLen = strlen("workbuilder is alive");
            op.response = new char[op.responseLen + 1];
            strncpy(op.response, "workbuilder is alive", op.responseLen);
            op.responseCode = 200;
            break;
        case kReducerUpdate:            
            res = reducerUpdate(op.params, op.contentLen, op.contentData);
            if(res == 0) {
                op.responseLen = strlen("OK");
                op.response = new char[op.responseLen + 1];
                strncpy(op.response, "OK", op.responseLen);
            }
            else {
                op.responseLen = strlen("WIND_UP");
                op.response = new char[op.responseLen + 1];
                strncpy(op.response, "WIND_UP", op.responseLen);
            }
            op.responseCode = 200;
            break;
        default:
            break;
    }
}

int Workbuilder::reducerUpdate(Properties &kv, int contentLen,
    const char *contentData)
{
    int reducerId = kv.getValue("reducerid", 0);
    uint32_t recordCountDelta = kv.getValue("delta", (uint32_t) 0);
    uint32_t recordCount = kv.getValue("count", (uint32_t) 0);
    
    if (!mJob) {
        // ignore the stale update and ask the merger to exit.  we aren't
        // running any job.  
        return 1;
    }

    KFS_LOG_STREAM_INFO << "**PAUSING: " << "Got update from reducer " << reducerId 
        << ", next key is " << contentData << KFS_LOG_EOM;

    mJob->reduceTaskHeartbeat(reducerId, recordCountDelta, recordCount);

    if (mJob->shouldPreemptReduceTask()) {
        // Add a new task to JT	
        if (mJob->isInTestMode())
            // in test mode...keep going
            return 0;


        KFS_LOG_STREAM_INFO << "**PAUSING: " << "Reducer " << reducerId 
            << " winds up, next key is " << contentData << KFS_LOG_EOM;

        // Remainder reduce task
        IFileKey_t remainderKey((const unsigned char*) contentData, contentLen);
        mJob->addRemainderReduceTask(reducerId, remainderKey);

        // Notify JT		
        JobStatus notifyResult =  mJob->getJTNotifier().setNumReducers(mJob->getNumReducers());
        
        // End: Add a new task to JT	
        KFS_LOG_STREAM_INFO << "**PAUSING: " << "Added new reducer, result is " 
            << notifyResult << KFS_LOG_EOM;

        return 1;
    }
    return 0;		
}

int Workbuilder::reducerRecordCount(Properties &kv)
{
    string job = kv.getValue("jobid", "");
    KFS_LOG_VA_DEBUG("reducer record count, jobid %s",job.c_str());

    if (mJob == NULL)
        return 0;

    // write out the counter only if it is the job we are running
    bool sameJob = mJob->isRunningJob(job);

    if (!sameJob)
        return 0;

    int reducerId = kv.getValue("reducerid", 0);
    int partition = kv.getValue("partition", 0);
    int actual = kv.getValue("actual", 0);
    int expected = kv.getValue("expected", 0);
    mJob->storeReduceTaskRecordCount(reducerId, partition, actual, expected);
    return 0;
}

// A mapper calls in notifying a new job
int Workbuilder::mapperStart(Properties &kv)
{
    string job = kv.getValue("jobid", "");
    string debugJobId = kv.getValue("debugid", "");
    KFS_LOG_VA_DEBUG("mapper start, jobid %s",job.c_str());

    if (mJob != NULL) {
        // if we aren't running THE job that we are called with, need
        // to get the status of the job we thought we should be running.
        bool sameJob = mJob->isRunningJob(job);

        JobStatus js = mJob->checkStatus(!sameJob);
        if (js == kRunning) {
            string s = mJob->getJobName();

            if (!sameJob)
                KFS_LOG_VA_WARN("Ignoring mapper start request from job %s, since %s is running",
                                 job.c_str(), s.c_str());
            // duplicate registrations for the same job are non-fatal
            return sameJob ? 0 : -1;
        }
        // mJob->finalize();

        KFS_LOG_STREAM_INFO << "Deleting old job: " << mJob->getJobName()
            << KFS_LOG_EOM;

        delete mJob;
    }
    int nMaps   = kv.getValue("m", 1);
    int nIfiles = kv.getValue("r", 1);
    vector<string> parts;

    // format: <jobtracker>-<jobid>
    splitPair(job, parts, '-');
    if (debugJobId == "")
        debugJobId = parts[1];
    mJob = new Job(this, mIFileBasedir, mLogdir, parts[0], parts[1],
        debugJobId, nMaps, nIfiles);

    mJob->setTaskWorkLimit(mTaskWorkLimitBytes);

    KFS_LOG_VA_INFO("Creating new job for %s, debug id = %s", 
        mJob->getJobName().c_str(), debugJobId.c_str());

    return 0;
}

// A mapper calls at the end of the task notifying us of the set of
// chunks to which it appended data
int Workbuilder::chunksAppended(Properties &kv, int contentLen, const char *contentData)
{
    string job = kv.getValue("jobid", "");
    int mapperId = kv.getValue("mapperid", -1);
    uint32_t numRecords = kv.getValue("numrecords", (uint32_t) 0);
    int64_t numBytes = kv.getValue("numbytes", (int64_t) 0);

    if (mJob == NULL) {
        KFS_LOG_VA_WARN("Ignoring chunks appended from job %s, since nothing is running",
            job.c_str());
        return -1;
    }

    if (mJob != NULL) {
        // if we aren't running THE job that we are called with, need
        // to get the status of the job we thought we should be running.
        bool sameJob = mJob->isRunningJob(job);

        JobStatus js = mJob->checkStatus(!sameJob);
        if (js == kRunning) {
            string s = mJob->getJobName();

            if (!sameJob) {
                KFS_LOG_VA_WARN("Ignoring chunks appended from job %s, since %s is running",
                                 job.c_str(), s.c_str());
                return -1;
            }
        }
    }

    if (contentLen == 0) {
        KFS_LOG_STREAM_INFO << "Jobid: " << job << " notification of chunks appended from task = "
            << mapperId << " , but no chunkids? " << KFS_LOG_EOM;
        return 0;
    }

    string c(contentData, contentLen);
    vector<string> chunkIds;

    KFS_LOG_STREAM_INFO << "Jobid: " << job << " notification of chunks appended from task = "
        << mapperId << " , chunkIds: " << c << KFS_LOG_EOM;

    // Write out a line per mapper that we can grep out
    mJob->getLog() << "Task stats: " << mapperId << ' '
        << numRecords << ' ' << numBytes << std::endl;

    // format: <chunkId>,<chunkId>
    split(c, chunkIds, ',');
    if (chunkIds.size() == 0)
        chunkIds.push_back(c);
    for (uint32_t i = 0; i < chunkIds.size(); i++) {
        if (chunkIds[i] == "")
            continue;
        try {
            // we can't let a client sending bogus data crash the workbuilder.
            // if we can't figure out what the client is saying...move on
            kfsChunkId_t cid = boost::lexical_cast<kfsChunkId_t>(chunkIds[i]);
            mJob->addChunkAppender(mapperId, cid);
        } catch (boost::bad_lexical_cast) { }
    }

    return 0;
}

// A merger calls in notifying us of a missing chunk in an I-file.
// Notify the JT to re-run those map tasks that generated data for the lost chunk.
int Workbuilder::chunkLost(Properties &kv, int &responseCode)
{
    string job = kv.getValue("jobid", "");
    int reducerId = kv.getValue("reducerid", -1);
    kfsChunkId_t lostChunkId = kv.getValue("chunkid", (kfsChunkId_t) -1);
    kfsFileId_t iFileId = kv.getValue("ifileid", (kfsFileId_t) -1);

    KFS_LOG_VA_INFO("notification of lost chunk (<%lld, %lld> from task = %d, jobid %s",
        iFileId, lostChunkId, reducerId, job.c_str());

    if (mJob == NULL) {
        KFS_LOG_VA_WARN("Ignoring lost chunk notification from job %s, since nothing is running",
            job.c_str());
        return -1;
    }

    if (mJob != NULL) {
        // if we aren't running THE job that we are called with, need
        // to get the status of the job we thought we should be running.
        bool sameJob = mJob->isRunningJob(job);

        JobStatus js = mJob->checkStatus(!sameJob);
        if (js == kRunning) {
            string s = mJob->getJobName();

            if (!sameJob) {
                KFS_LOG_VA_WARN("Ignoring lost chunk notification from job %s, since %s is running",
                                 job.c_str(), s.c_str());
                return -1;
            }
        }
    }

    // start on fixing the failure handling...
    if (1) {
        // Need to robustify this handling a bit.  Until then...
        KFS_LOG_STREAM_INFO << "Short-circuiting the failure handling path for now..."
            << " in job: " << job
            << KFS_LOG_EOM;
        responseCode = 200;
        return 0;
    }

    // With respect to merger: if the merger has handed at least 1
    // record from the lost chunk to the Java process, merger should
    // exit.  if not, the merger can wait, and then start its work
    // after the data (re) generation is complete

    if (!mJob->isChunkLost(lostChunkId)) {
        // we are hearing this for the first time.
        // the request has been accepted
        mJob->handleLostChunk(iFileId, lostChunkId);
        // tell the merger to wait/exit while this chunk is being regenerated
        responseCode = 201;
        return 0;
    }
    if (mJob->isChunkRecoveryComplete(lostChunkId)) {
        // tell the merger to ignore this chunk and continue
        // processing other chunks
        responseCode = 200;
    } else {
        // tell the merger that chunk is being re-generated and it has
        // to wait/exit
        responseCode = 201;
    }
    return 0;
}

int
Workbuilder::dummyMapperStart(const string &iFileBasedir,
    const string &logdir, string job, int ifileId)
{
    vector<string> parts;
    splitPair(job, parts, '-');

    mIFileBasedir = iFileBasedir;
    mLogdir = logdir;

    mJob = new Job(this, mIFileBasedir, mLogdir, parts[0], parts[1], parts[1], 1, 0);
    mJob->setTestMode(ifileId);
    return 0;
}

// Called when the first reducer starts up and contacts us
int
Workbuilder::initBuildPlan(Properties &kv)
{
    string job = kv.getValue("jobid", "");
    KFS_LOG_VA_DEBUG("buildPlan, jobid %s",job.c_str());
    if ((mJob == NULL) || (!mJob->isRunningJob(job))) {
        KFS_LOG_VA_INFO("Notification of init build plan from unknown job %s", job.c_str());
        return -1;
    }

    if (mJob->isPlanDone())
        return 0;

    KFS_LOG_VA_INFO("Notification of init build plan from job %s", job.c_str());

    mJob->setPlanInProgress();

    // notify the job and wait for the index readers to finish up.
    mJob->reducerStart();
    return 0;
}

// This method is called "internally" once the index reading is done.
// The caller is from Job.cc's indexReaderDone().
//
void
Workbuilder::buildPlan()
{
    //if reduce task has notifed, assign work+notify
    int numReducers = mJob->assignReduceTaskWork();

    KFS_LOG_STREAM_INFO << "# of reducers for job: " << mJob->getJobId()
        << " is: " << numReducers << KFS_LOG_EOM;

    // From this point onwards, whenever a reducer contacts the
    // workbuilder, we have work ready to hand out.
    mJob->setPlanDone();

    if (mJob->isInTestMode())
        return;

    JobStatus retCode = mJob->getJTNotifier().setNumReducers(numReducers);
    if (retCode == kRetry)
        mRetrySetNumReducers = true;

}

int Workbuilder::reducerStart(Properties &kv, char **response, int &responseLen, int &responseCode)
{
    int reducerId = kv.getValue("reducerid", 0);
    string job = kv.getValue("jobid", "");
    KFS_LOG_VA_DEBUG("reducer start, job %s, taskid %i",job.c_str(), reducerId);
    if ((mJob == NULL) || (!mJob->isRunningJob(job))) {
        KFS_LOG_VA_INFO("Notification of reduce start from unknown job %s", job.c_str());
        responseCode = 405;
        return -1;
    }
    if (mJob->isPlanInProgress()) {
        KFS_LOG_INFO("Plan in progress, returning retry");
        responseCode = 201;
        return -1;
    }
    // 1. if the reducer corresponds to an I-file for which a chunk
    // was lost, and mappers that wrote to the lost chunk are being
    // re-run, need to check with the JT if those tasks have finished;
    //  -- This is done in job.cc where we pull out the work
    // 2. If those tasks have finished, we need to read the index to
    // validate that the chunk sorting is done.
    //  -- This is done in job.cc where we pull out the work
    // If no recovery is going on for the I-file, then we can hand reducer the work.
    // otherwise, we should return a 201 to force the reducer to retry
    responseCode = mJob->getReduceTaskWork(reducerId, response, responseLen);
    return 0;
}

void
Workbuilder::setJobParams(uint64_t taskWorkLimit)
{
    mTaskWorkLimitBytes = taskWorkLimit;
    if (mJob) {
        mJob->setTaskWorkLimit(taskWorkLimit);
    }
}



void
Workbuilder::Timeout()
{
    if (mJob == NULL)
        return;

    if (mRetrySetNumReducers) {
        int numReducers = mJob->getNumReducers();
        JobStatus retCode = mJob->getJTNotifier().setNumReducers(numReducers);

        mRetrySetNumReducers = (retCode == kRetry);
    }

    JobStatus js = mJob->checkStatus();
    if (js == kRunning) {
        mJob->getNumReducersToPreempt();
        return;
    }

    delete mJob;
    mJob = NULL;
}

void
workbuilder::initWorkbuilderOpMap()
{
    gWorkbuilderStrToOp["restart"] = kRestart;
    gWorkbuilderStrToOp["mapperstart"] = kMapperStart;
    gWorkbuilderStrToOp["chunksappended"] = kChunksAppended;
    gWorkbuilderStrToOp["buildplan"] = kBuildPlan;
    gWorkbuilderStrToOp["chunklost"] = kChunkLost;
    gWorkbuilderStrToOp["reducerstart"] = kReducerStart;
    gWorkbuilderStrToOp["recordcount"] = kReducerRecordCount;
    gWorkbuilderStrToOp["buildplan"] = kBuildPlan;
    gWorkbuilderStrToOp["status"] = kHealthCheck;
    /* PAUSING BEGIN */
    gWorkbuilderStrToOp["reducerupdate"] = kReducerUpdate;
    /* PAUSING END */
}

WorkbuilderOp_t 
workbuilder::getWorkbuilderOp(const string &query)
{
    if (query == "")
        return kHealthCheck;

    if (gWorkbuilderStrToOp.find(query) == gWorkbuilderStrToOp.end())
        return kHealthCheck;
    return gWorkbuilderStrToOp[query];
}

