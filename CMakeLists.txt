cmake_minimum_required(VERSION 2.8)
project(Reddit)
set(CMAKE_CXX_STANDARD 17)

set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR})

find_package(Snap REQUIRED)
if (NOT Snap_FOUND)
    message(FATAL_ERROR "Package Snap not found")
else()
    message(STATUS "Snap Found.")
endif (NOT Snap_FOUND)
add_definitions(${Snap_DEFINITIONS})
include_directories(${Snap_INCLUDE_DIRS})
message("Snap Include: " ${Snap_INCLUDE_DIRS})
set(LIBRARIES ${LIBRARIES} ${Snap_LIBRARIES})

set(SOURCE Snap/main.cpp)

add_executable(split-reddit ${SOURCE})
target_link_libraries(split-reddit ${LIBRARIES})


#
#include_directories(src include)
#find_package(Boost 1.54 REQUIRED)
#if (Boost_FOUND)
#    include_directories(${Boost_INCLUDE_DIR})
#    add_definitions("-DHAS_BOOST")
#
#    set(Boost_USE_STATIC ON)
#    set(Boost_USE_STATIC_LIBS ON)
#    set(Boost_USE_MULTITHREADED ON)
#    set(Boost_USE_STATIC_RUNTIME ON)
#endif()
#
## cmake -DCMAKE_BUILD_TYPE=Debug ..
#if(CMAKE_BUILD_TYPE STREQUAL "Debug")
#    message(STATUS "Mode: Debug")
#    SET(CMAKE_CXX_FLAGS "-std=gnu++0x -g -O0 -Wall -fpermissive -Wextra -pedantic -DBOOST_LOG_DYN_LINK")
#else()
#    message(STATUS "Mode: Release")
#    SET(CMAKE_CXX_FLAGS "-std=gnu++0x -O3 -fpermissive -DBOOST_LOG_DYN_LINK")
#endif()
#
#set(SOURCE src/reddit-join.cpp
#        include/joiner.hpp              src/joiner.cpp
#        include/logger.hpp src/logger.cpp src/user-data.cpp include/user-data.hpp
#        include/csv.h Snap/main.cpp)
#
#set(TARGET reddit-join)
#add_executable(${TARGET} ${SOURCE})
#if(APPLE OR WIN32)
#    target_link_libraries(${TARGET}
#            pthread
#            boost_thread-mt
#            boost_system-mt
#            boost_filesystem-mt
#            boost_log-mt
#            boost_log_setup-mt
#            boost_chrono-mt
#            boost_date_time-mt
#            boost_program_options-mt
#            boost_regex-mt)
#else()
#    target_link_libraries(${TARGET}
#            pthread
#            boost_thread
#            boost_system
#            boost_filesystem
#            boost_log
#            boost_log_setup
#            boost_chrono
#            boost_date_time
#            boost_program_options
#            boost_regex)
#endif()
