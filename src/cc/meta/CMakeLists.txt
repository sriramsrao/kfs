
#
# For the library take everything except the *_main.cc files
#
set (lib_srcs
checkpoint.cc
ChunkReplicator.cc
ChunkServer.cc
ChunkServerFactory.cc
ChildProcessTracker.cc
ClientSM.cc
entry.cc
kfsops.cc
kfstree.cc
LayoutManager.cc
LeaseCleaner.cc
logger.cc
meta.cc
NetDispatch.cc
replay.cc
request.cc
restore.cc
startup.cc
util.cc
)

add_library (kfsMeta STATIC ${lib_srcs})
add_library (kfsMeta-shared SHARED ${lib_srcs})
set_target_properties (kfsMeta-shared PROPERTIES OUTPUT_NAME "kfsMeta")
set_target_properties (kfsMeta PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (kfsMeta-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)

set (exe_files metaserver logcompactor filelister kfsfsck)
foreach (exe_file ${exe_files})
        add_executable (${exe_file} ${exe_file}_main.cc)
        if (USE_STATIC_LIB_LINKAGE)
           target_link_libraries (${exe_file} kfsMeta kfsIO kfsCommon qcdio pthread crypto)
           add_dependencies (${exe_file} kfsCommon kfsIO kfsMeta qcdio)
        else (USE_STATIC_LIB_LINKAGE)
           target_link_libraries (${exe_file} kfsMeta-shared kfsIO-shared kfsCommon-shared qcdio-shared pthread crypto)
           add_dependencies (${exe_file} kfsCommon-shared kfsIO-shared kfsMeta-shared qcdio-shared)
        endif (USE_STATIC_LIB_LINKAGE)
endforeach (exe_file)

if (APPLE)
  target_link_libraries(kfsMeta-shared kfsCommon-shared kfsIO-shared crypto)
endif (APPLE)

if (NOT APPLE)
  target_link_libraries(kfsMeta rt)
  target_link_libraries(metaserver rt)
endif (NOT APPLE)

if (CMAKE_SYSTEM_NAME STREQUAL "SunOS")
   # mtmalloc seemed to worsen metaserver startup time; it took
   # 4 mins for fsck to load checkpoint from WORM, where as 30 for metaserver.
   # So, switch to umem
   target_link_libraries(kfsMeta umem)
   target_link_libraries(metaserver umem)
endif (CMAKE_SYSTEM_NAME STREQUAL "SunOS")

#
# Install them
#
install (TARGETS ${exe_files} kfsMeta kfsMeta-shared
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib/static)

