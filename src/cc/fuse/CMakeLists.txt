
#
# Build the fuse binary; build it only on demand
#
add_executable (kfs_fuse kfs_fuse_main.cc)
link_directories(${Fuse_LIBRARY_DIR})
target_link_libraries (kfs_fuse kfsClient fuse pthread rt)

#
install (TARGETS kfs_fuse
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib-static)
