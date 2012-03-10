
#
# For the library take everything except the *_main.cc files
#

add_executable (rebalancer rebalancer_main.cc)

set (exe_files rebalancer)

#
# Install them
#
install (TARGETS ${exe_files} 
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib/static)

