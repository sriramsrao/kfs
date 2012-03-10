
# Take all the .cc files and build a library out of them
add_library (kfs_access SHARED KfsAccessJni.cc)
add_dependencies (kfs_access kfsClient-shared)
target_link_libraries (kfs_access kfsClient-shared)
if (NOT APPLE)
  target_link_libraries(kfs_access rt)
endif (NOT APPLE)

#
install (TARGETS kfs_access
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib-static)
