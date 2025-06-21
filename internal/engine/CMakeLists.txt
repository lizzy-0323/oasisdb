cmake_minimum_required(VERSION 3.10)
project(vector_search_engine)

# Add o3 flag
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3")
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
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
