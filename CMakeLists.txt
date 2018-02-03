cmake_minimum_required(VERSION 3.9)
project(Reddit)
set(CMAKE_CXX_STANDARD 17)

find_package(Boost 1.54 REQUIRED)
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIR})
    link_directories(${Boost_LIBRARY_DIR})
endif()


# cmake -DCMAKE_BUILD_TYPE=Debug ..
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    message(STATUS "Mode: Debug")
    SET(CMAKE_CXX_FLAGS "-std=c++11 -g -O0 -Wall -fpermissive -Wextra -pedantic")
else()
    message(STATUS "Mode: Release")
    SET(CMAKE_CXX_FLAGS "-std=c++11 -Ofast -fpermissive -DBOOST_LOG_DYN_LINK")
endif()


include_directories(src include)
set(SOURCE
        src/reddit-joiner.cpp   include/reddit-joiner.hpp include/logger.hpp
        include/logger.hpp)

add_executable(reddit-join src/reddit-join.cpp ${SOURCE})
target_link_libraries(reddit-join ${Boost_LIBRARIES} Boost:program_options boost_log-mt)
