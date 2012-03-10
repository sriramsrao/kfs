
# build libchunk
set (lib_srcs
    AtomicRecordAppender.cc
    BufferManager.cc
    ChunkSortHelper.cc
    ChunkManager.cc
    ChunkServer.cc
    ClientManager.cc
    ClientSM.cc
    DiskIo.cc
    KfsOps.cc
    LeaseClerk.cc
    Logger.cc
    MetaServerSM.cc
    RemoteSyncSM.cc
    Replicator.cc
    Utils.cc
)

add_library (kchunklib STATIC ${lib_srcs})
add_library (kchunklib-shared SHARED ${lib_srcs})
set_target_properties (kchunklib-shared PROPERTIES OUTPUT_NAME "kchunklib")
set_target_properties (kchunklib PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (kchunklib-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)

add_executable (chunkserver ChunkServer_main.cc)
add_executable (chunkscrubber chunkscrubber_main.cc)
#add_executable (chunktrimmer chunktrimmer_main.cc)

set (exe_files chunkserver chunkscrubber) #chunktrimmer)

foreach (exe_file ${exe_files})
        if (USE_STATIC_LIB_LINKAGE)
           target_link_libraries (${exe_file} kchunklib kfsIO kfsCommon qcdio pthread crypto lzo2)
           add_dependencies (${exe_file} kfsCommon kfsIO qcdio)
        else (USE_STATIC_LIB_LINKAGE)
           target_link_libraries (${exe_file} kchunklib-shared kfsIO-shared kfsCommon-shared qcdio-shared pthread crypto lzo2)
           add_dependencies (${exe_file} kfsCommon-shared kfsIO-shared qcdio-shared)
        endif (USE_STATIC_LIB_LINKAGE)
endforeach (exe_file)

if (APPLE)
   target_link_libraries(kchunklib-shared kfsIO-shared  kfsCommon-shared qcdio-shared crypto lzo2)
endif(APPLE)

if (NOT APPLE)
  target_link_libraries(chunkserver rt)
endif (NOT APPLE)

if (CMAKE_SYSTEM_NAME STREQUAL "SunOS")
   target_link_libraries(chunkserver umem)
endif (CMAKE_SYSTEM_NAME STREQUAL "SunOS")

#
# Install them
#
install (TARGETS ${exe_files} kchunklib-shared
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib/static)

