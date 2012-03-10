#/bin/bash
#
# $Id: kfsrun.sh 387 2010-05-27 16:16:44Z sriramsrao $
#
# Copyright 2006 Kosmix Corp.
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
# Script to start/stop a memory pinner process on a node.
# 
#
startServer()
{
    if [ -e $SERVER_PID_FILE ];
    then
	PROCID=`cat $SERVER_PID_FILE`
	PROC_COUNT=`ps -ef | awk '{print $2}'  | grep -c $PROCID`
	if [[ $PROC_COUNT -gt  0 ]]; 
	    then
	    echo "$server is already running..."
	    exit 0
        fi;
	# No such process; so, restart the server
	rm -f $PID_FILE
    fi

    echo "Starting $server..."
    export LD_LIBRARY_PATH=/homes/sriramr/openssl/lib:/homes/sriramr/lib:${LD_LIBRARY_PATH}
    bin/$server $memoryToPin > /dev/null 2>&1 &
    echo $! > $SERVER_PID_FILE

    RETVAL=$?
    echo 
    return $RETVAL
}

stopServer()
{
    echo -n "Stopping $PROG: "

    if [ ! -e $PID_FILE ]; 
	then
	echo "ERROR: No PID file ( $PID_FILE )"
	return -1;
    fi;

    PROCID=`cat $PID_FILE`
    if [ -z $PROCID ]; 
	then
	echo ERROR: No PID value in file
	return -2;
    fi

    PROC_COUNT=`ps -ef | awk '{print $2}'  | grep -c $PROCID`
    if [[ $PROC_COUNT -gt  0 ]]; 
	then
	echo -n $"Stopping $prog ( $PROCID )"
	kill -TERM $PROCID
    fi;

    rm -f $PID_FILE

    echo
    RETVAL=$?
    return $RETVAL
}

# Process any command line arguments
TEMP=`getopt -o m:sSh -l memory: \
	-n memory_pin.sh -- "$@"`
eval set -- "$TEMP"

server="memory_pinner"
while true
do
	case "$1" in
	-m|--memory) memoryToPin=$2; shift;;
	-s|--start) mode="start";;
	-S|--stop) mode="stop";;
	-h|--help) echo "usage: $0 [--memory <value>]"; exit;;
	--) break ;;
	esac
	shift
done

[ -f bin/$server ] || exit 0
LOGS_DIR="logs"
SERVER_LOG_FILE=$LOGS_DIR/$server.log
SERVER_PID_FILE=$LOGS_DIR/$server.pid

case $mode in
    "start")
	startServer
	;;
    "stop")
	PROG=$server
	PID_FILE=$SERVER_PID_FILE
	stopServer
	;;
    *)
	echo "Need to specify server"
	exit 1
esac


exit $RETVAL
