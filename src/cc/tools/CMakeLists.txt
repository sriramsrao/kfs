
# Build the tools library
set (lib_srcs
    MonUtils.cc
    KfsCd.cc
    KfsChangeReplication.cc
    KfsCp.cc
    KfsFstat.cc
    KfsLs.cc
    KfsMkdirs.cc
    KfsMv.cc
    KfsRm.cc
    KfsRmdir.cc
    KfsPwd.cc
    KfsAppend.cc
    KfsToolsCommon.cc
    utils.cc
)

add_library (tools STATIC ${lib_srcs})
add_library (tools-shared SHARED ${lib_srcs})
set_target_properties (tools-shared PROPERTIES OUTPUT_NAME "tools")
set_target_properties (tools PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (tools-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)

add_dependencies (tools kfsClient)
add_dependencies (tools-shared kfsClient-shared)

set (exe_files
cptokfs
cpfromkfs
kfsshell
kfsping
kfstoggleworm
kfsput
kfsstats
kfsfileenum
kfsdataverify
kfsretire
kfscp
kfsls
)

if (APPLE)
   target_link_libraries(tools-shared kfsClient-shared kfsCommon-shared)
endif (APPLE)

#
# Every executable depends on its namesake source with _main.cc
#
foreach (exe_file ${exe_files})
        add_executable (${exe_file} ${exe_file}_main.cc)
        if (USE_STATIC_LIB_LINKAGE)
                add_dependencies (${exe_file} tools)
                target_link_libraries (${exe_file} tools kfsClient)
        else (USE_STATIC_LIB_LINKAGE)
                add_dependencies (${exe_file} tools-shared)
                target_link_libraries (${exe_file} tools-shared kfsClient-shared)
        endif (USE_STATIC_LIB_LINKAGE)
endforeach (exe_file)

if (LIBEDIT_FOUND)
    target_link_libraries(kfsshell ${LIBEDIT_LIBRARY})
endif (LIBEDIT_FOUND)

#
install (TARGETS ${exe_files} tools tools-shared
        RUNTIME DESTINATION bin/tools
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib/static)
