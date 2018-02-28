cmake_minimum_required(VERSION 2.8)
project(Reddit)
set(CMAKE_CXX_STANDARD 11)
set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/cmake)


find_package(SNAP REQUIRED)
if (NOT SNAP_FOUND)
    message(FATAL_ERROR "Package SNAP not found")
else()
    message(STATUS "SNAP Found: " ${SNAP_INCLUDE_DIRS})
endif (NOT SNAP_FOUND)

add_definitions(${SNAP_DEFINITIONS})
include_directories(${SNAP_INCLUDE_DIRS})
message("Snap Include: " ${SNAP_INCLUDE_DIRS})
set(LIBRARIES ${LIBRARIES} ${SNAP_LIBRARIES})

include_directories(src include)

set(SOURCE
        include/splitter.h              src/splitter.cpp
        include/user-data.hpp           src/user-data.cpp
        include/reddit-parser.h         src/reddit-parser.cpp
        src/table-splitter.cpp include/table-splitter.hpp)

set(OLD src/reddit-join.cpp
        include/joiner.hpp              src/joiner.cpp)


set(TEST test/test.cpp)

add_executable(split-reddit src/main.cpp  ${SOURCE})
target_link_libraries(split-reddit ${LIBRARIES})

add_executable(test-reddit ${TEST})
target_link_libraries(test-reddit ${LIBRARIES})