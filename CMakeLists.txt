cmake_minimum_required(VERSION 2.8)
project(Reddit)
set(CMAKE_CXX_STANDARD 11)
set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/cmake)

if (APPLE)
    set(CMAKE_C_COMPILER /usr/local/Cellar/gcc@5/5.5.0_2/bin/gcc-5 CACHE STRING "C compiler" FORCE)
    set(CMAKE_CXX_COMPILER /usr/local/Cellar/gcc@5/5.5.0_2/bin/g++-5 CACHE STRING "C++ compiler" FORCE)
endif()

find_package(Snap REQUIRED)
add_definitions(${Snap_DEFINITIONS})
include_directories(${Snap_INCLUDE_DIRS})
set(LIBRARIES ${LIBRARIES} ${Snap_LIBRARIES})

find_package(OpenMP)
if (OPENMP_FOUND)
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
endif()

# cmake -DCMAKE_BUILD_TYPE=Debug ..
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    message(STATUS "Mode: Debug")
    SET(CMAKE_CXX_FLAGS "-std=c++11 -g -O0")
else()
    message(STATUS "Mode: Release")
    SET(CMAKE_CXX_FLAGS "-std=c++11 -Ofast")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp")

include_directories(src include)
set(SOURCE
        include/splitter.hpp src/splitter.cpp
        include/reddit-parser.hpp       src/reddit-parser.cpp
        include/table-splitter.hpp      src/table-splitter.cpp)

set(TEST test/test.cpp)

add_executable(split-reddit src/main.cpp  ${SOURCE})
target_link_libraries(split-reddit ${LIBRARIES})

add_executable(test-reddit ${TEST} ${SOURCE})
target_link_libraries(test-reddit ${LIBRARIES})

add_executable(problem test/problem.cpp ${SOURCE})
target_link_libraries(problem ${LIBRARIES})