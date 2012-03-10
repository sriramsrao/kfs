
# Build the kappender library
set (lib_srcs
    appender.cc
    input_handler.cc
    workgetter.cc
    imerger.cc
   util.cc
)

add_library (kapplib STATIC ${lib_srcs})
add_library (kapplib-shared SHARED ${lib_srcs})
set_target_properties (kapplib-shared PROPERTIES OUTPUT_NAME "kapplib")
set_target_properties (kapplib PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (kapplib-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)

add_dependencies (kapplib kfsClient)
add_dependencies (kapplib-shared kfsClient-shared)

set (exe_files
    kappender
    imerger
    kdatagen
   ifilereader
   icomparer
   merge_validator
)

IF (APPLE)
  target_link_libraries(kapplib-shared kfsClient-shared  kfsCommon-shared)
ENDIF (APPLE)
set (COMPRESS_LIBS crypto lzo2)
add_definitions(-DKFS_COMPRESSION_ENABLED)

#
# Every executable depends on its namesake source with _main.cc
#
foreach (exe_file ${exe_files})
        add_executable (${exe_file} ${exe_file}_main.cc)
        if (USE_STATIC_LIB_LINKAGE)
                add_dependencies (${exe_file} kapplib)
                target_link_libraries (${exe_file} kapplib kfsClient)
		target_link_libraries(${exe_file} ${COMPRESS_LIBS})
        else (USE_STATIC_LIB_LINKAGE)
                add_dependencies (${exe_file} kapplib-shared)
                target_link_libraries (${exe_file} kapplib-shared kfsClient-shared)
		target_link_libraries(${exe_file} ${COMPRESS_LIBS})
        endif (USE_STATIC_LIB_LINKAGE)
endforeach (exe_file)

#
install (TARGETS ${exe_files} kapplib-shared
        RUNTIME DESTINATION bin/sailfish
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib/static)
