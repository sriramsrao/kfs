#!/usr/bin/bash
#
# $Id: kfsinstall.sh 36 2007-11-12 02:43:36Z sriramsrao $
#
# Copyright 2007 Kosmix Corp.
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
# Script to install/upgrade a server package on a machine
#

tarProg="gtar"

installServer()
{
    # if everything exists, return
    if [ -d $serverDir ] && [ -f $serverDir/bin/$serverBinary ] ;
	then
	    echo "$serverBinary exists...skipping"
	    return 0
    fi

    if [ ! -d $serverDir ]; 
	then
	mkdir -p $serverDir
    fi

    if [ ! -d $serverDir ]; 
	then
	echo "Unable to create $serverDir"
	exit -1
    fi

    cd $serverDir; $tarProg -zxf /tmp/kfspkg.tgz 

    # Make a logs dir for the startup script
    mkdir -p $serverDir/logs

    case $serverType in
	-m|--meta) 
	    mv tmp/fn.* $serverDir/bin/MetaServer.prp 
	    mkdir -p $serverDir/bin/kfscp
	    mkdir -p $serverDir/bin/kfslog
	    ;;
	-c|--chunk)
	    mv /tmp/ChunkServer.prp $serverDir/bin/ChunkServer.prp 
	    mkdir -p $serverDir/bin/kfslog
	    mkdir -p $chunkDir
	    ;;
	*)
	    echo "Unknown server"
	    ;;
    esac
    rm -rf tmp
    RETVAL=0
    return $RETVAL
}

upgradeServer()
{
    if [ ! -d $serverDir ]; 
	then
	echo "Can't upgrade: No $serverDir"
	exit -1
    fi

    cd $serverDir; $tarProg -zxf /tmp/kfspkg.tgz 
    case $serverType in
	-m|--meta) 
	    mv tmp/fn.* $serverDir/bin/MetaServer.prp 
	    ;;
	-c|--chunk)
	    mv /tmp/ChunkServer.prp $serverDir/bin/ChunkServer.prp 
	    ;;
	*)
	    echo "Unknown server"
	    ;;
    esac

    rm -rf tmp
    RETVAL=0
    return $RETVAL
}

uninstallServer()
{
    $serverDir/scripts/kfsrun.sh -S $serverType 

    case $serverType in
	-m|--meta) 
	    rm -rf $serverDir
	    ;;
	-c|--chunk)
	    rm -rf $chunkDir
	    rm -rf $serverDir
	    ;;
	*)
	    echo "Unknown server"
	    ;;
    esac

}

# Process any command line arguments
# TEMP=`getopt -o d:mc:r:hiuU -l dir:,meta,chunk:,tar:help,install,upgrade,uninstall \
# 	-n kfsinstall.sh -- "$@"`
# eval set -- "$TEMP"

set -- `getopt d:mc:r:hiuU $*`

# while true
for i in $*
  do
  case "$i" in
      -d|--dir) serverDir=$2;;
      -m|--meta) serverType="$i"; serverBinary="metaserver";;
      -c|--chunk) 
	  serverType="$i"; 
	  chunkDir=$2;
	  serverBinary="chunkserver";;
      -i|--install) mode="install";;
      -r|--tar) tarProg="$2";;
      -u|--upgrade) mode="upgrade";;
      -U|--uninstall) mode="uninstall";;
      -h|--help) 
	  echo -n "usage: $0 [-d <serverDir>] [-m | -chunk <chunk dir>] [-i | -u | -U ]";
	  echo " -i: install";
	  echo " -u: upgrade";
	  echo " -U: uninstall";
	  echo " [-r: <tar|gtar> ]";
	  exit;;
      --) break ;;
  esac
  shift
done

case $mode in
    "install")
	installServer
	;;
    "upgrade")
	upgradeServer
	;;
    "uninstall")
	uninstallServer
	;;
    *)
	echo "Need to specify mode"
	exit 1
esac


exit $RETVAL
