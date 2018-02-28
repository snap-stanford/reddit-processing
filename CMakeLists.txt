cmake_minimum_required(VERSION 2.8)
project(Reddit)
set(CMAKE_CXX_STANDARD 11)
set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/cmake)


find_package(Snap REQUIRED)
if (NOT Snap_FOUND)
    message(FATAL_ERROR "Package Snap not found")
else()
    message(STATUS "Snap Found: " ${Snap_INCLUDE_DIRS})
endif (NOT Snap_FOUND)

add_definitions(${Snap_DEFINITIONS})
include_directories(${Snap_INCLUDE_DIRS})
message("Snap Include: " ${Snap_INCLUDE_DIRS})
set(LIBRARIES ${LIBRARIES} ${Snap_LIBRARIES})

include_directories(src include)

set(SOURCE
        include/splitter.h              src/splitter.cpp
        include/user-data.hpp           src/user-data.cpp
        include/reddit-parser.h         src/reddit-parser.cpp
        src/table-splitter.cpp          include/table-splitter.hpp)

set(OLD src/reddit-join.cpp
        include/joiner.hpp              src/joiner.cpp)


set(TEST test/test.cpp)

add_executable(split-reddit src/main.cpp  ${SOURCE})
target_link_libraries(split-reddit ${LIBRARIES})

add_executable(test-reddit ${TEST})
target_link_libraries(test-reddit ${LIBRARIES})