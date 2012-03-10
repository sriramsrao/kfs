
#
# For the library take everything except the *_main.cc files
#

set (lib_srcs
clientsm.cc
job.cc
indexreader.cc
statuschecker.cc
setnumreducer.cc
workbuilder.cc
util.cc
)

#
# build both static and dynamic
#
add_library (kworkb STATIC ${lib_srcs})
add_library (kworkb-shared SHARED ${lib_srcs})
set_target_properties (kworkb-shared PROPERTIES OUTPUT_NAME "kworkb")
set_target_properties (kworkb PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (kworkb-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)

add_dependencies (kworkb kfsCommon)
add_dependencies (kworkb-shared kfsCommon-shared)

add_executable (kworkbuilder workbuilder_main.cc)

set (exe_files kworkbuilder)

foreach (exe_file ${exe_files})
        if (USE_STATIC_LIB_LINKAGE)
           add_dependencies (${exe_file} kworkb kfsClient kfsCommon kfsIO)
           target_link_libraries (${exe_file} kworkb kfsClient kfsIO  kfsCommon  ssl crypto z)
        else (USE_STATIC_LIB_LINKAGE)
           add_dependencies (${exe_file} kworkb-shared kfsClient-shared kfsCommon-shared  kfsIO-shared)
           target_link_libraries (${exe_file} kworkb-shared   kfsClient-shared kfsIO-shared kfsCommon-shared  ssl crypto  z)
        endif (USE_STATIC_LIB_LINKAGE)
endforeach (exe_file)

if (APPLE OR CYGWIN)
  target_link_libraries(kworkb-shared kfsClient-shared kfsIO-shared  kfsCommon-shared  ssl crypto z)
endif (APPLE OR CYGWIN)

#
# Install them
#
install (TARGETS ${exe_files} kworkb kworkb-shared
        RUNTIME DESTINATION bin/sailfish
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib/static)

