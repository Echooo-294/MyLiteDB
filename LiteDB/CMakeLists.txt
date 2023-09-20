cmake_minimum_required(VERSION 3.8)
project(LiteDB)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_COMPILER "g++")
set(CMAKE_CXX_FLAGS "-g -Wall -Werror -std=c++11")
set(CMAKE_CXX_FLAGS_DEBUG "-O0")
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG ")

set(CMAKE_INSTALL_PREFIX "install")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY
    ${CMAKE_BINARY_DIR}/build/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY
    ${CMAKE_BINARY_DIR}/build/bin)

include_directories(${CMAKE_SOURCE_DIR}/src/parser/include)
include_directories(${CMAKE_SOURCE_DIR}/src)

add_subdirectory(src)
