cmake_minimum_required(VERSION 3.5)
project(cjoin)
set(CMAKE_CXX_STANDARD 17)

include_directories(cppsrc/include)
include_directories("node_modules/node-addon-api")

set(SOURCE_FILES cppsrc/main2.cpp cppsrc/main.cpp)

add_executable(cjoin ${SOURCE_FILES})