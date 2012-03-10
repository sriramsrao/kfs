
# Take all the .cc files and build a library out of them
file (GLOB sources *.cc)

#
# Build a static and a dynamically linked libraries.  Both libraries
# should have the same root name, but installed in different places
#
add_library (kfsClient STATIC ${sources})
add_library (kfsClient-shared SHARED ${sources})
set_target_properties (kfsClient-shared PROPERTIES OUTPUT_NAME "kfsClient")

#
# Since the objects have to be built twice, set this up so they don't
# clobber each other.

set_target_properties (kfsClient PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (kfsClient-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)

add_dependencies (kfsClient kfsCommon kfsIO qcdio)
# get everthing into one
target_link_libraries (kfsClient kfsCommon kfsIO qcdio crypto lzo2)
target_link_libraries (kfsClient-shared kfsCommon-shared kfsIO-shared qcdio-shared crypto lzo2)
#
install (TARGETS kfsClient kfsClient-shared
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib/static)

install (FILES KfsAttr.h KfsClient.h DESTINATION include/kfs)
