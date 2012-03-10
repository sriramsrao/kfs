
# Build the kappender library
set (lib_srcs
  chunksorter.cc
  util.cc
)

add_library (kcslib STATIC ${lib_srcs})
add_library (kcslib-shared SHARED ${lib_srcs})
set_target_properties (kcslib-shared PROPERTIES OUTPUT_NAME "kcslib")
set_target_properties (kcslib PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (kcslib-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)

add_dependencies (kcslib kchunklib)
add_dependencies (kcslib-shared kchunklib-shared)

set (exe_files
   chunksorter
   chunksort_test
   sortertest
)

IF (APPLE)
  target_link_libraries(kcslib-shared kchunklib-shared  kfsCommon-shared)
ENDIF (APPLE)

set (COMPRESS_LIBS crypto lzo2)
add_definitions(-DKFS_COMPRESSION_ENABLED)

#
# Every executable depends on its namesake source with _main.cc
#
foreach (exe_file ${exe_files})
        add_executable (${exe_file} ${exe_file}_main.cc)
        if (USE_STATIC_LIB_LINKAGE)
                add_dependencies (${exe_file} kcslib)
                target_link_libraries (${exe_file} kcslib kchunklib kfsIO kfsCommon qcdio crypto)
		target_link_libraries(${exe_file} ${COMPRESS_LIBS})
        else (USE_STATIC_LIB_LINKAGE)
                add_dependencies (${exe_file} kcslib-shared)
                target_link_libraries (${exe_file} kcslib-shared kchunklib-shared kfsIO-shared qcdio-shared)
		target_link_libraries(${exe_file} ${COMPRESS_LIBS})
        endif (USE_STATIC_LIB_LINKAGE)
endforeach (exe_file)

#
install (TARGETS ${exe_files} kcslib-shared
        RUNTIME DESTINATION bin/sailfish
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib/static)
