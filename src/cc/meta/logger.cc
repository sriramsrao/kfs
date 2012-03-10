/*!
 * $Id: logger.cc 1552 2011-01-06 22:21:54Z sriramr $
 *
 * Copyright 2008 Quantcast Corp.
 * Copyright 2006-2008 Kosmix Corp.
 *
 * This file is part of Kosmos File System (KFS).
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * \file logger.cc
 * \brief thread for logging metadata updates
 */

#include <csignal>

#include "logger.h"
#include "queue.h"
#include "checkpoint.h"
#include "util.h"
#include "replay.h"
#include "common/log.h"
#include "libkfsIO/Globals.h"
#include "NetDispatch.h"

using namespace KFS;
using namespace libkfsio;

// default values
string KFS::LOGDIR("./kfslog");
string KFS::LASTLOG(LOGDIR + "/last");

Logger KFS::oplog(LOGDIR);

static KFS::LogRotater logRotater;

void
LogRotater::Timeout()
{
	oplog.finishLog();
}

void
Logger::dispatch(MetaRequest *r)
{
	r->seqno = ++nextseq;
	if (r->mutation && r->status == 0) {
		log(r);
		cp.note_mutation();
	}
	gNetDispatch.Dispatch(r);
}

/*!
 * \brief log the request and flush the result to the fs buffer.
*/
int
Logger::log(MetaRequest *r)
{
	int res = r->log(file);
	if (res >= 0)
		flushResult(r);
	return res;
}

/*!
 * \brief flush log entries to disk
 *
 * Make sure that all of the log entries are on disk and
 * update the highest sequence number logged.
 */
void
Logger::flushLog()
{
	seq_t last = nextseq;

	file.flush();
	if (file.fail())
		panic("Logger::flushLog", true);

	committed = last;
}

/*!
 * \brief set the log filename/log # to seqno
 * \param[in] seqno	the next log sequence number (lognum)
 */
void
Logger::setLog(int seqno)
{
	assert(seqno >= 0);
	lognum = seqno;
	logname = logfile(lognum);
}

/*!
 * \brief open a new log file for writing
 * \param[in] seqno	the next log sequence number (lognum)
 * \return		0 if successful, negative on I/O error
 */
int
Logger::startLog(int seqno)
{
	assert(seqno >= 0);
	lognum = seqno;
	logname = logfile(lognum);
	if (file_exists(logname)) {
		// following log replay, until the next CP, we
		// should continue to append to the logfile that we replayed.
		// seqno will be set to the value we got from the chkpt file.
		// So, don't overwrite the log file.
		KFS_LOG_VA_DEBUG("Opening %s in append mode", logname.c_str());
		file.open(logname.c_str(), std::ios_base::app);
		return (file.fail()) ? -EIO : 0;
	}
	file.open(logname.c_str());
	file << "version/" << VERSION << '\n';

	// for debugging, record when the log was opened
	time_t t = time(NULL);

	file << "time/" << ctime(&t);
	file.flush();
	return (file.fail()) ? -EIO : 0;
}

/*!
 * \brief close current log file and begin a new one
 */
int
Logger::finishLog()
{
	// if there has been no update to the log since the last roll, don't
	// roll the file over; otherwise, we'll have a file every N mins
	if (incp == committed)
		return 0;

	// for debugging, record when the log was closed
	time_t t = time(NULL);

	file << "time/" << ctime(&t);
	file.flush();
	file.close();
	link_latest(logname, LASTLOG);
	if (file.fail())
		warn("link_latest", true);
	incp = committed;
	int status = startLog(lognum + 1);
	cp.resetMutationCount();
	return status;
}

/*!
 * \brief make sure result is on disk
 * \param[in] r	the result of interest
 *
 * If this result has a higher sequence number than what is
 * currently known to be on disk, flush the log to disk.
 */
void
Logger::flushResult(MetaRequest *r)
{
	if (r->seqno > committed) {
		flushLog();
		assert(r->seqno <= committed);
	}
}

void
KFS::logger_setup_paths(const string &logdir)
{
	if (logdir != "") {
		LOGDIR = logdir;
		LASTLOG = LOGDIR + "/last";
		oplog.setLogDir(LOGDIR);
	}
}

void
KFS::logger_init()
{
	if (oplog.startLog(replayer.logno()) < 0)
		panic("KFS::logger_init, startLog", true);
	logRotater.SetInterval(LOG_ROLLOVER_MAXSEC);
	globalNetManager().RegisterTimeoutHandler(&logRotater);
}
