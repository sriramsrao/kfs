#!/usr/bin/env bash
#
# $Id: kfsrun.sh 36 2007-11-12 02:43:36Z sriramsrao $
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
# Script to start/stop a meta/chunk server on a node
# 
#
startServer()
{
    if [ "$server" = "metaserver" ];
	then
	if [ ! -e $WEBUI_PID_FILE ];
	    then
	    webui/kfsstatus.py webui/server.conf > webui/tmp.txt < /dev/null 2>&1 &
	    echo $! > $WEBUI_PID_FILE
	fi
    fi

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

    if [ ! -f $config ];
    then
	echo "No config file...Not starting $server"
	exit -1
    fi
    echo "Starting $server..."
    export LD_LIBRARY_PATH=`pwd`/lib:$LD_LIBRARY_PATH
#    export UMEM_DEBUG=default
#    export UMEM_LOGGING=transaction
    bin/$server $config $SERVER_LOG_FILE > /dev/null 2>&1 &
    echo $! > $SERVER_PID_FILE

    # no need to start cleaner on chunkservers
    if [ "$server" = "chunkserver" ];
	then
	RETVAL=$?
	echo
	return $RETVAL
    fi

    if [ ! -e $CLEANER_PID_FILE ];
	then
	echo "Starting cleaner..."
	if [ -n "$backup_node" ];
	    then
	    # Once an hour, clean/backup stuff
	    cleaner_args="-b $backup_node -p $backup_path -s 3600"
	else
	    cleaner_args="-s 3600"
	fi
	    
	scripts/kfsclean.sh $cleaner_args > $CLEANER_LOG_FILE < /dev/null 2>&1 &
	echo $! > $CLEANER_PID_FILE
    else
	echo "cleaner is already running..."
    fi

    RETVAL=$?
    echo 
    return $RETVAL
}

stopServer()
{
    echo -n $"Stopping $PROG: "

    if [[ ! -e $PID_FILE ]]; 
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
# TEMP=`getopt -o f:b:sSmch -l file:,backup:,start,stop,meta,chunk,help \
# 	-n kfsrun.sh -- "$@"`
# eval set -- "$TEMP"

backup_node=
backup_path=

set -- `getopt f:b:p:sSmch $*`

#while true
for i in $*
do
	case "$i" in
	-s|--start) mode="start";;
	-S|--stop) mode="stop";;
	-m|--meta) server="metaserver";;
	-c|--chunk) server="chunkserver";;
	-f|--file) config=$2;;
	-b|--backup_node) backup_node=$2;;
	-p|--backup_path) backup_path=$2;;
	-h|--help) echo "usage: $0 [-s | -S] [-m | -c] [-f <config>]"; 
		echo " -s: start";
		echo " -S: stop";
		exit;;
	--) break ;;
	esac
	shift
done

[ -f bin/$server ] || exit 0
LOGS_DIR="logs"
SERVER_LOG_FILE=$LOGS_DIR/$server.log
SERVER_PID_FILE=$LOGS_DIR/$server.pid

CLEANER_LOG_FILE=$LOGS_DIR/$server.cleaner.log
CLEANER_PID_FILE=$LOGS_DIR/$server.cleaner.pid

WEBUI_PID_FILE=$LOGS_DIR/$server.webui.pid

case $mode in
    "start")
	startServer
	;;
    "stop")
	PROG=$server
	PID_FILE=$SERVER_PID_FILE
	stopServer
	if [ "$server" = "metaserver" ];
	then
	    PROG=$server.cleaner
	    PID_FILE=$CLEANER_PID_FILE
	    stopServer
	    PROG=$server.webui
	    PID_FILE=$WEBUI_PID_FILE
	    stopServer
	fi
	;;
    *)
	echo "Need to specify server"
	exit 1
esac


exit $RETVAL
