#
# $Id: CMakeLists.txt 400 2010-08-22 06:07:05Z sriramsrao $
#
# Created 2006/10/20
# Author: Sriram Rao (Kosmix Corp)
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
#

project (KFS)

cmake_minimum_required(VERSION 2.4.6)

# Locate Boost
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
# set(Boost_LIB_DIAGNOSTIC_DEFINITIONS "-DBOOST_LIB_DIAGNOSTIC")
set(Boost_USE_STATIC_LIBS   ON)
set(Boost_USE_MULTITHREADED ON)
find_package(Boost COMPONENTS regex program_options REQUIRED)
message(STATUS "Boost-includes = ${Boost_INCLUDE_DIRS}")
message(STATUS "Boost-libs = ${Boost_LIBRARIES}")
# Locate the path to jni.h
find_package(JNI)

# Use the IntelIPP package
set (CMAKE_IPPBASE "/usr/local/share/include/ippinclude")
if (EXISTS ${CMAKE_IPPBASE} AND CMAKE_SIZEOF_VOID_P MATCHES "8" AND NOT KFS_NO_IPP)
    message (STATUS "Using IntelIPP = ${CMAKE_IPPBASE}")
    set (CMAKE_KFS_USE_INTEL_IPP ON)
#    set (CMAKE_IPP_INCLUDE_DIR ${CMAKE_IPPBASE}/include)
    set (CMAKE_IPP_INCLUDE_DIR ${CMAKE_IPPBASE})
#    set (CMAKE_IPP_LIB_DIR     ${CMAKE_IPPBASE}/lib)
    set (CMAKE_IPP_LIB_DIR      "/usr/local/share/libs/ipplibs/lib")
    set (CMAKE_IPP_LIBS 
        ${CMAKE_IPP_LIB_DIR}/libippdcemergedem64t.a
        ${CMAKE_IPP_LIB_DIR}/libippdcmergedem64t.a
        ${CMAKE_IPP_LIB_DIR}/libippsemergedem64t.a
        ${CMAKE_IPP_LIB_DIR}/libippsmergedem64t.a
        ${CMAKE_IPP_LIB_DIR}/libippcoreem64t.a
    )
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUSE_INTEL_IPP")
endif (EXISTS ${CMAKE_IPPBASE} AND CMAKE_SIZEOF_VOID_P MATCHES "8" AND NOT KFS_NO_IPP)

# Change this to where the install directory is located
if (CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  set(CMAKE_INSTALL_PREFIX "." CACHE PATH "installation directory prefix" FORCE)
endif (CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)

# Build with statically linked libraries; the value for this variable has to be defined here
# overwriting whatever is in the cache.
# When set to ON, we build with statically linked libraries; when off we 
# link with dynamically linked libs

IF (CMAKE_SYSTEM_NAME STREQUAL "SunOS")
   IF (BUILD_CPU_MODE STREQUAL "32")
      message (STATUS "Building 32-bit mode on Solaris")
      # If we are asked to build 32 bit mode
      add_definitions (-D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE -D_LARGE_FILES)
   ELSE (BUILD_CPU_MODE STREQUAL "32")
      # On solaris, use 64-bit mode
      set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -m64")
      set(CMAKE_C_FLAGS  "${CMAKE_C_FLAGS} -m64")
      set(CMAKE_SHARED_LINKER_FLAGS  "${CMAKE_SHARED_LINKER_FLAGS} -m64")
   ENDIF (BUILD_CPU_MODE STREQUAL "32")
   # Statically linked binaries don't work on solaris
   set (USE_STATIC_LIB_LINKAGE OFF CACHE BOOL "Build binaries with statically linked libraries" FORCE)
   # Cmake does whacky relink on solaris and messes things up; avoid this
   set (CMAKE_SKIP_RPATH ON)
ELSE (CMAKE_SYSTEM_NAME STREQUAL "SunOS")
   set (USE_STATIC_LIB_LINKAGE ON CACHE BOOL "Build binaries with statically linked libraries" FORCE)
    IF (CMAKE_SIZEOF_VOID_P MATCHES "4")
       message (STATUS "Enabling largefile source flags")
       add_definitions (-D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_LARGE_FILES)
    ENDIF (CMAKE_SIZEOF_VOID_P MATCHES "4")
ENDIF (CMAKE_SYSTEM_NAME STREQUAL "SunOS")

IF (ENABLE_PROFILING)
   message (STATUS "Enabling profiling with gprof")
   set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -pg")
   set(CMAKE_C_FLAGS  "${CMAKE_C_FLAGS} -pg")
   set(CMAKE_SHAREDBoost_USE_MULTITHREADED_LINKER_FLAGS  "${CMAKE_SHARED_LINKER_FLAGS} -pg")
   set(CMAKE_EXE_FLAGS  "${CMAKE_EXE_FLAGS} -pg")
ENDIF (ENABLE_PROFILING)

# Darwin compilers need to be told about ports
IF (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
   set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I/opt/local/include -L/opt/local/lib")
ENDIF (CMAKE_SYSTEM_NAME STREQUAL "Darwin")


# Change the line to Release to build release binaries
# For servers, build with debugging info; for tools, build Release
#

IF (NOT CMAKE_BUILD_TYPE)
   message (STATUS "Setting build type to Debug")
   set (CMAKE_BUILD_TYPE "Debug")
ENDIF (NOT CMAKE_BUILD_TYPE)

IF (CMAKE_BUILD_TYPE STREQUAL "Release")
   message(STATUS "Enabling -D NDEBUG flag")
   set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -D NDEBUG -g3")
ENDIF(CMAKE_BUILD_TYPE STREQUAL "Release")

message (STATUS "Setting build type to ${CMAKE_BUILD_TYPE}")

set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -Wall -Wno-multichar -DBOOST_SP_USE_QUICK_ALLOCATOR")
string(TOUPPER KFS_OS_NAME_${CMAKE_SYSTEM_NAME} KFS_OS_NAME)
add_definitions (-D${KFS_OS_NAME})

#
# Find the path to libfuse.so
#

# IF (EXISTS "/lib64/libfuse.so")
#   SET(Fuse_LIBRARY_DIR "/lib64")
# ELSE (EXISTS "/lib64/libfuse.so")
#   SET(Fuse_LIBRARY_DIR "")
# ENDIF (EXISTS "/lib64/libfuse.so")


# Fill in the path to fuse library
SET(Fuse_LIBRARY_DIR "")
SET(Fuse_INCLUDE_DIR "")

# Find libedit
# Install from: http://www.thrysoee.dk/editline/
FIND_PACKAGE(libedit)
#
# On Linux machines install xfs, xfs-devel RPMs; chunkserver code can
# benefit from xfs space reservations and get contiguous allocation.
#
FIND_PACKAGE(xfs)

if(COMMAND cmake_policy)
    cmake_policy(SET CMP0003 NEW)
endif(COMMAND cmake_policy)

# include dirs
include_directories( ${Boost_INCLUDE_DIRS} src/cc)
if (CMAKE_KFS_USE_INTEL_IPP)
    include_directories( ${CMAKE_IPP_INCLUDE_DIR} )
endif (CMAKE_KFS_USE_INTEL_IPP)

# get the subdirs we want
add_subdirectory (src/cc/common)
add_subdirectory (src/cc/sailfish/sorter)
add_subdirectory (src/cc/sailfish/workbuilder)
add_subdirectory (src/cc/sailfish/mrtools)
add_subdirectory (src/cc/rebalancer)
add_subdirectory (src/cc/meta)
add_subdirectory (src/cc/chunk)
add_subdirectory (src/cc/libkfsClient)
add_subdirectory (src/cc/libkfsIO)
add_subdirectory (src/cc/tools)
add_subdirectory (src/cc/tests)
add_subdirectory (src/cc/emulator)
add_subdirectory (src/cc/telemetry)
#add_subdirectory (src/test-scripts)
add_subdirectory (src/cc/qcdio)

IF (NOT ${JAVA_INCLUDE_PATH} STREQUAL "")
   message(STATUS "Found JNI...building kfs_access")
   include_directories ( ${JAVA_INCLUDE_PATH} ${JAVA_INCLUDE_PATH2} )
   add_subdirectory (src/cc/access)
ENDIF (NOT ${JAVA_INCLUDE_PATH} STREQUAL "")

IF (NOT ${Fuse_LIBRARY_DIR} STREQUAL "")
   message(STATUS "Found fuse")
   include_directories ( ${Fuse_INCLUDE_DIR} )
   add_subdirectory (src/cc/fuse)
ENDIF (NOT ${Fuse_LIBRARY_DIR} STREQUAL "")
