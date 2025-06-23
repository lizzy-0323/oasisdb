cmake_minimum_required(VERSION 3.10)
project(vector_search_engine)

# Add o3 flag
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3")

# Set build type
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(hnsw_c_api SHARED
    c_api/hnsw/hnsw_c_api.cpp
)

target_include_directories(hnsw_c_api PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/c_api/hnsw
)

set_target_properties(hnsw_c_api PROPERTIES
    PREFIX "lib"
    OUTPUT_NAME "hnsw"
)

if(APPLE)
    set_target_properties(hnsw_c_api PROPERTIES SUFFIX ".dylib")
endif()
