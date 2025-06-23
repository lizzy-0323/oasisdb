cmake_minimum_required(VERSION 3.10...3.26)
project(oasisdb_engine LANGUAGES CXX)

include(GNUInstallDirs)
include(CheckCXXCompilerFlag)

# Add o3 flag
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3")
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Configure OpenMP for macOS
if(APPLE)
    # Find Homebrew's libomp installation
    execute_process(
        COMMAND brew --prefix libomp
        OUTPUT_VARIABLE HOMEBREW_LIBOMP_PREFIX
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    
    if(HOMEBREW_LIBOMP_PREFIX)
        # Set OpenMP flags for macOS with Clang
        set(OpenMP_CXX_FLAGS "-Xpreprocessor -fopenmp")
        set(OpenMP_CXX_LIB_NAMES "omp")
        set(OpenMP_omp_LIBRARY "${HOMEBREW_LIBOMP_PREFIX}/lib/libomp.dylib")
        set(OpenMP_CXX_INCLUDE_DIRS "${HOMEBREW_LIBOMP_PREFIX}/include")
        
        # Add to global flags
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
        include_directories(${OpenMP_CXX_INCLUDE_DIRS})
        link_directories(${HOMEBREW_LIBOMP_PREFIX}/lib)
        
        message(STATUS "Using OpenMP from Homebrew: ${HOMEBREW_LIBOMP_PREFIX}")
    else()
        message(WARNING "Homebrew's libomp not found. OpenMP might not work correctly.")
    endif()
endif()

find_package(FAISS QUIET)
if(NOT FAISS_FOUND)
    message(STATUS "FAISS not found, building from source")
    include(FetchContent)
    
    set(FAISS_ENABLE_GPU OFF CACHE BOOL "Disable GPU support" FORCE)
    set(FAISS_ENABLE_PYTHON OFF CACHE BOOL "Disable Python bindings" FORCE)
    set(FAISS_ENABLE_RAFT OFF CACHE BOOL "Disable RAFT" FORCE)
    set(BUILD_TESTING OFF CACHE BOOL "Disable tests" FORCE)
    
    FetchContent_Declare(
        faiss
        GIT_REPOSITORY https://github.com/facebookresearch/faiss.git
        GIT_TAG v1.7.4  # 使用稳定版本
    )
    
    FetchContent_GetProperties(faiss)
    if(NOT faiss_POPULATED)
        FetchContent_Populate(faiss)
        add_subdirectory(${faiss_SOURCE_DIR} ${faiss_BINARY_DIR} EXCLUDE_FROM_ALL)
    endif()
    
    set(FAISS_INCLUDE_DIRS 
        $<BUILD_INTERFACE:${faiss_SOURCE_DIR}>
        $<BUILD_INTERFACE:${faiss_BINARY_DIR}>
        $<INSTALL_INTERFACE:include>
    )
    set(FAISS_LIBRARIES faiss)
    message(STATUS "Using FAISS from source")
else()
    message(STATUS "Found FAISS: ${FAISS_LIBRARIES}")
endif()

add_library(hnsw_c_api SHARED
    c_api/hnsw/hnsw_c_api.cpp
)

add_library(ivf_c_api SHARED
    c_api/ivf/ivf_c_api.cpp
)

target_include_directories(hnsw_c_api PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/c_api/hnsw
)

target_include_directories(ivf_c_api PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/c_api/ivf
    ${FAISS_INCLUDE_DIRS}
)

set_target_properties(hnsw_c_api PROPERTIES
    PREFIX "lib"
    OUTPUT_NAME "hnsw"
)

set_target_properties(ivf_c_api PROPERTIES
    PREFIX "lib"
    OUTPUT_NAME "ivf"
)

if(APPLE)
    set_target_properties(hnsw_c_api PROPERTIES SUFFIX ".dylib")
    set_target_properties(ivf_c_api PROPERTIES SUFFIX ".dylib")
endif()

target_link_libraries(ivf_c_api PUBLIC ${FAISS_LIBRARIES})

# Link OpenMP if on macOS
if(APPLE)
    if(DEFINED OpenMP_omp_LIBRARY)
        target_link_libraries(ivf_c_api PUBLIC ${OpenMP_omp_LIBRARY})
    endif()
endif()
