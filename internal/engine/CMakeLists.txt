cmake_minimum_required(VERSION 3.10)
project(vector_search_engine)

# Add o3 flag
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3")

# Set build type
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# OpenMP
if(APPLE)
    if(CMAKE_C_COMPILER_ID MATCHES "Clang")
        set(OpenMP_C_FLAGS "-Xpreprocessor -fopenmp -I/opt/homebrew/opt/libomp/include")
        set(OpenMP_C_LIB_NAMES "omp")
        set(OpenMP_omp_LIBRARY "/opt/homebrew/opt/libomp/lib/libomp.dylib")
    endif()

    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        set(OpenMP_CXX_FLAGS "-Xpreprocessor -fopenmp -I/opt/homebrew/opt/libomp/include")
        set(OpenMP_CXX_LIB_NAMES "omp")
        set(OpenMP_omp_LIBRARY "/opt/homebrew/opt/libomp/lib/libomp.dylib")
    endif()
endif()

find_package(OpenMP)

# 设置 C++ 标准
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 设置生成动态库
add_library(hnsw_c_api SHARED
    c_api/hnsw/hnsw_c_api.cpp
)

add_library(ivf_c_api SHARED
    c_api/ivf/ivf_c_api.cpp
)

# 设置头文件包含目录
target_include_directories(hnsw_c_api PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/c_api/hnsw
)

target_include_directories(ivf_c_api PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/c_api/ivf
)

# 设置库的输出名称
set_target_properties(hnsw_c_api PROPERTIES
    PREFIX "lib"
    OUTPUT_NAME "hnsw"
)

set_target_properties(ivf_c_api PROPERTIES
    PREFIX "lib"
    OUTPUT_NAME "ivf"
)

# 如果是 MacOS，设置 .dylib 后缀
if(APPLE)
    set_target_properties(hnsw_c_api PROPERTIES SUFFIX ".dylib")
    set_target_properties(ivf_c_api PROPERTIES SUFFIX ".dylib")
endif()

# 链接 OpenMP
if(OpenMP_CXX_FOUND)
    message("OPENMP FOUND")
    target_link_libraries(ivf_c_api PUBLIC OpenMP::OpenMP_CXX)
endif()
