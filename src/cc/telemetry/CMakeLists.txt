#
# $Id: CMakeLists.txt 392 2010-05-27 20:11:26Z sriramsrao $
#
# Created 2008/09/13
#
# Copyright 2008 Quantcast Corporation.
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

set (exe_files
telemetry_server
telemetry_repeater
)

#
# Every executable depends on its namesake source with _main.cc
#
foreach (exe_file ${exe_files})
        add_executable (${exe_file} ${exe_file}_main.cc)
        if (USE_STATIC_LIB_LINKAGE)
           target_link_libraries (${exe_file} kfsCommon qcdio pthread)
           if (CMAKE_SYSTEM_NAME STREQUAL "SunOS")
                      target_link_libraries (${exe_file} socket nsl)              
           endif (CMAKE_SYSTEM_NAME STREQUAL "SunOS")
           add_dependencies (${exe_file} kfsCommon qcdio)
        else (USE_STATIC_LIB_LINKAGE)
           target_link_libraries (${exe_file} kfsCommon-shared  qcdio-shared pthread)
           if (CMAKE_SYSTEM_NAME STREQUAL "SunOS")
                      target_link_libraries (${exe_file} socket nsl)              
           endif (CMAKE_SYSTEM_NAME STREQUAL "SunOS")
           add_dependencies (${exe_file} kfsCommon-shared qcdio-shared)
        endif (USE_STATIC_LIB_LINKAGE)
        if (NOT APPLE)
          target_link_libraries(${exe_file} rt)
        endif (NOT APPLE)

endforeach (exe_file)

#
install (TARGETS ${exe_files}
        RUNTIME DESTINATION bin)


