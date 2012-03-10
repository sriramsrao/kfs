#!/bin/bash
#
# $Id: kfsinstall.sh 387 2010-05-27 16:16:44Z sriramsrao $
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
    sh $serverDir/scripts/kfsrun.sh --stop $serverType 

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
TEMP=`getopt -o d:mc:r:hiuU -l dir:,meta,chunk:,tar:help,install,upgrade,uninstall \
	-n kfsinstall.sh -- "$@"`
eval set -- "$TEMP"

while true
  do
  case "$1" in
      -d|--dir) serverDir=$2; shift;;
      -m|--meta) serverType="$1"; serverBinary="metaserver";;
      -c|--chunk) 
	  serverType="$1"; 
	  chunkDir=$2;
	  serverBinary="chunkserver";
	  shift;;
      -i|--install) mode="install";;
      -r|--tar) tarProg="$2"; shift;;
      -u|--upgrade) mode="upgrade";;
      -U|--uninstall) mode="uninstall";;
      -h|--help) 
	  echo -n "usage: $0 [--serverDir <dir>] [--meta | --chunk <chunk dir>]";
	  echo " [--install | --upgrade | --uninstall ]";
	  echo " [--tar <tar|gtar> ] ";
	  exit;;
      --) break ;;
  esac
  shift
done

# try user's suggestion, then try gtar then tar
if which $tarProg > /dev/null]; then
    $tarProg=$tarProg
else
  if which "gtar" > /dev/null; then
    tarProg="gtar"
  else
    tarProg="tar"
  fi
fi

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
