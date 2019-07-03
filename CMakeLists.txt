cmake_minimum_required(VERSION 3.5)
project(cjoin)
set(CMAKE_CXX_STANDARD 17)

include_directories(cppsrc/include)
include_directories("node_modules/node-addon-api")
#target_link_libraries(main ${CMAKE_SOURCE_DIR}/libbingitup.a)
#add_subdirectory("node_modules/node-addon-api")

set(SOURCE_FILES cppsrc/main.cpp cppsrc/Join.cpp cppsrc/Util.cpp cppsrc/sample.cpp)


add_executable(cjoin ${SOURCE_FILES})