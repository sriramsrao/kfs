#!/bin/bash
#
# $Id: metabkup.sh 94 2008-07-22 17:29:02Z sriramsrao $
#
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
# Script to copy the metaserver checkpoint files to a remote node.
# The same script can also be used to restore the checkpoint files
# from remote path to local.
# 

# process any command-line arguments
TEMP=`getopt -o d:b:p:R:h -l dir:,backup_node:,backup_path:,recover:help -n metabkup.sh -- "$@"`
eval set -- "$TEMP"

recover=0
backup_node=
backup_path=
while true
do
	case "$1" in
	-d|--dir) kfs_dir=$2; shift 2;;
	-b|--backup_node) backup_node=$2; shift 2;;	    
	-p|--backup_path) backup_path=$2; shift 2;;
	-R|--recover) recover=1; shift;;
	-h|--help) echo "usage: $0 [-d cpdir] [-b backup_node] [-p backup_path] {-recover}"; exit ;;
	--) shift; break ;;
	esac
done

cpdir="$kfs_dir/bin/kfscp"
logdir="$kfs_dir/bin/kfslog"
if [ ! -d $cpdir ];
    then
    echo "$cpdir is non-existent"
    exit -1
fi

if [ $recover -eq 0 ];
    then
    rsync -avz --delete $cpdir $backup_node:$backup_path
    rsync -avz $logdir $backup_node:$backup_path
else
    # Restore the checkpoint files from remote node
    rsync -avz $backup_node:$backup_path/"kfscp" .
    rsync -avz $backup_node:$backup_path/"kfslog" .
fi    
