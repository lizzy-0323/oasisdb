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

# Find FAISS
if(APPLE)
    set(FAISS_ROOT "/opt/homebrew")
    set(FAISS_INCLUDE_DIR "${FAISS_ROOT}/include")
    set(FAISS_LIBRARY "${FAISS_ROOT}/lib/libfaiss.dylib")
endif()

find_path(FAISS_INCLUDE_DIRS faiss/Index.h
    PATHS ${FAISS_INCLUDE_DIR}
    DOC "FAISS include directory")
find_library(FAISS_LIBRARIES faiss
    PATHS ${FAISS_LIBRARY}
    DOC "FAISS library")

if(NOT FAISS_INCLUDE_DIRS OR NOT FAISS_LIBRARIES)
    message(FATAL_ERROR "FAISS library not found. Please install it using 'brew install faiss'")
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
