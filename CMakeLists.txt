cmake_minimum_required(VERSION 2.8)
project(Reddit)
set(CMAKE_CXX_STANDARD 11)
set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/cmake)

find_package(Snap REQUIRED)

add_definitions(${Snap_DEFINITIONS})
include_directories(${Snap_INCLUDE_DIRS})
message("Snap Include: " ${Snap_INCLUDE_DIRS})
set(LIBRARIES ${LIBRARIES} ${Snap_LIBRARIES})

# cmake -DCMAKE_BUILD_TYPE=Debug ..
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    message(STATUS "Mode: Debug")
    SET(CMAKE_CXX_FLAGS "-std=c++11 -g -O0")
else()
    message(STATUS "Mode: Release")
    SET(CMAKE_CXX_FLAGS "-std=c++11 -Ofast")
endif()



include_directories(src include)
set(SOURCE
        include/splitter.h              src/splitter.cpp
        include/user-data.hpp           src/user-data.cpp
        include/reddit-parser.h         src/reddit-parser.cpp
        include/table-splitter.hpp      src/table-splitter.cpp)

set(TEST test/test.cpp)

add_executable(split-reddit src/main.cpp  ${SOURCE})
target_link_libraries(split-reddit ${LIBRARIES})

add_executable(test-reddit ${TEST})
target_link_libraries(test-reddit ${LIBRARIES})