
# Take all the .cc files and build a library out of them
set (sources
    Acceptor.cc
    BufferedSocket.cc
    Checksum.cc
    Counter.cc
    EventManager.cc
    Globals.cc
    IOBuffer.cc
    NetConnection.cc
    NetErrorSimulator.cc
    NetManager.cc
    TcpSocket.cc
    TelemetryClient.cc
)

add_library (kfsIO STATIC ${sources})
add_library (kfsIO-shared SHARED ${sources})
set_target_properties (kfsIO-shared PROPERTIES OUTPUT_NAME "kfsIO")

set_target_properties (kfsIO PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (kfsIO-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)

add_dependencies (kfsIO qcdio kfsCommon)
add_dependencies (kfsIO-shared qcdio-shared kfsCommon-shared)
target_link_libraries (kfsIO qcdio kfsCommon pthread ${Boost_LIBRARIES})
target_link_libraries (kfsIO-shared qcdio-shared kfsCommon-shared pthread)
if (NOT APPLE)
   target_link_libraries (kfsIO rt)
   target_link_libraries (kfsIO-shared rt)
endif(NOT APPLE)

if (CMAKE_KFS_USE_INTEL_IPP)
    target_link_libraries(kfsIO ${CMAKE_IPP_LIBS})
    target_link_libraries(kfsIO-shared ${CMAKE_IPP_LIBS})
else (CMAKE_KFS_USE_INTEL_IPP)
    target_link_libraries(kfsIO z)
    target_link_libraries(kfsIO-shared z)
endif (CMAKE_KFS_USE_INTEL_IPP)

if (CMAKE_SYSTEM_NAME STREQUAL "SunOS")
  target_link_libraries (kfsIO nsl socket resolv)
  target_link_libraries (kfsIO-shared nsl socket resolv)
endif (CMAKE_SYSTEM_NAME STREQUAL "SunOS")

install (TARGETS kfsIO kfsIO-shared
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib/static)
