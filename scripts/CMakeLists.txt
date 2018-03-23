cmake_minimum_required(VERSION 3.9)
project(boost_python_test)
set(CMAKE_CXX_STANDARD 17)

if(APPLE)
    set(CMAKE_SHARED_LIBRARY_SUFFIX ".so")
endif(APPLE)

find_package( PythonLibs 2.7 REQUIRED)
include_directories( ${PYTHON_INCLUDE_DIRS} )

find_package(Boost COMPONENTS python REQUIRED )
include_directories(${Boost_INCLUDE_DIR} )

# Build our library
add_library(dict SHARED libdict.cpp)
target_link_libraries(dict ${Boost_LIBRARIES} ${PYTHON_LIBRARIES})
