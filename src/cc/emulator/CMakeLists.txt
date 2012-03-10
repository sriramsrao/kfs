
#
# For the library take everything except the *_main.cc files
#
set (lib_srcs
ChunkServerEmulator.cc
LayoutEmulator.cc
EmulatorSetup.cc
)

add_library (kfsEmulator STATIC ${lib_srcs})
add_library (kfsEmulator-shared SHARED ${lib_srcs})
set_target_properties (kfsEmulator-shared PROPERTIES OUTPUT_NAME "kfsEmulator")
set_target_properties (kfsEmulator PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (kfsEmulator-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)

set (exe_files rebalanceplanner rebalanceexecutor replicachecker rereplicator)
foreach (exe_file ${exe_files})
        add_executable (${exe_file} ${exe_file}_main.cc)
        if (USE_STATIC_LIB_LINKAGE)
           target_link_libraries (${exe_file} kfsEmulator kfsMeta kfsIO kfsCommon qcdio pthread crypto)
           add_dependencies (${exe_file} kfsEmulator kfsCommon kfsIO qcdio kfsMeta)
        else (USE_STATIC_LIB_LINKAGE)
           target_link_libraries (${exe_file} kfsEmulator-shared kfsMeta-shared kfsIO-shared kfsCommon-shared qcdio-shared pthread crypto)
           add_dependencies (${exe_file} kfsEmulator-shared kfsCommon-shared kfsIO-shared kfsMeta-shared qcdio-shared)
        endif (USE_STATIC_LIB_LINKAGE)
endforeach (exe_file)

if (APPLE)
  target_link_libraries(kfsEmulator-shared kfsMeta-shared kfsCommon-shared kfsIO-shared)
endif (APPLE)

if (NOT APPLE)
  target_link_libraries(kfsEmulator rt)
endif (NOT APPLE)

if (CMAKE_SYSTEM_NAME STREQUAL "SunOS")
   target_link_libraries(kfsEmulator mtmalloc)
endif (CMAKE_SYSTEM_NAME STREQUAL "SunOS")

#
# Install them
#
install (TARGETS ${exe_files} kfsEmulator kfsEmulator-shared
        RUNTIME DESTINATION bin/emulator
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib/static)

