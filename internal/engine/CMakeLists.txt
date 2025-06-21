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
    # macOS - try to find FAISS installed via Homebrew
    set(FAISS_ROOT "/opt/homebrew")
    set(FAISS_INCLUDE_DIR "${FAISS_ROOT}/include")
    set(FAISS_LIBRARY "${FAISS_ROOT}/lib/libfaiss.dylib")
elseif(UNIX AND NOT APPLE)
    # Linux - try common installation paths
    set(FAISS_INCLUDE_DIR "/usr/include" "/usr/local/include")
    set(FAISS_LIBRARY "/usr/lib" "/usr/lib64" "/usr/local/lib" "/usr/local/lib64")
endif()

find_path(FAISS_INCLUDE_DIRS faiss/Index.h
    PATHS ${FAISS_INCLUDE_DIR}
    DOC "FAISS include directory")
find_library(FAISS_LIBRARIES faiss
    PATHS ${FAISS_LIBRARY}
    DOC "FAISS library")

if(NOT FAISS_INCLUDE_DIRS OR NOT FAISS_LIBRARIES)
    message(STATUS "FAISS library not found. Attempting to install it automatically...")
    
    if(APPLE)
        # Try to install FAISS using Homebrew on macOS
        message(STATUS "Installing FAISS using Homebrew...")
        execute_process(
            COMMAND brew install faiss
            RESULT_VARIABLE BREW_RESULT
            OUTPUT_VARIABLE BREW_OUTPUT
            ERROR_VARIABLE BREW_ERROR
        )
        
        if(NOT BREW_RESULT EQUAL 0)
            message(FATAL_ERROR "Failed to install FAISS using Homebrew: ${BREW_ERROR}")
        endif()
        
        # After installation, set paths again
        set(FAISS_ROOT "/opt/homebrew")
        set(FAISS_INCLUDE_DIR "${FAISS_ROOT}/include")
        set(FAISS_LIBRARY "${FAISS_ROOT}/lib/libfaiss.dylib")
        
    elseif(UNIX AND NOT APPLE)
        # Try to install FAISS on Linux using apt or yum
        find_program(APT_PROGRAM apt-get)
        find_program(YUM_PROGRAM yum)
        
        if(APT_PROGRAM)
            # Debian/Ubuntu
            message(STATUS "Installing FAISS using apt-get...")
            execute_process(
                COMMAND ${CMAKE_COMMAND} -E env DEBIAN_FRONTEND=noninteractive
                ${APT_PROGRAM} update
                RESULT_VARIABLE APT_UPDATE_RESULT
            )
            
            if(NOT APT_UPDATE_RESULT EQUAL 0)
                message(WARNING "Failed to update apt repositories")
            endif()
            
            execute_process(
                COMMAND ${CMAKE_COMMAND} -E env DEBIAN_FRONTEND=noninteractive
                ${APT_PROGRAM} install -y libfaiss-dev
                RESULT_VARIABLE APT_INSTALL_RESULT
                OUTPUT_VARIABLE APT_OUTPUT
                ERROR_VARIABLE APT_ERROR
            )
            
            if(NOT APT_INSTALL_RESULT EQUAL 0)
                message(FATAL_ERROR "Failed to install FAISS using apt-get: ${APT_ERROR}")
            endif()
            
        elseif(YUM_PROGRAM)
            # CentOS/RHEL/Fedora
            message(STATUS "Installing FAISS using yum...")
            execute_process(
                COMMAND ${YUM_PROGRAM} install -y faiss-devel
                RESULT_VARIABLE YUM_RESULT
                OUTPUT_VARIABLE YUM_OUTPUT
                ERROR_VARIABLE YUM_ERROR
            )
            
            if(NOT YUM_RESULT EQUAL 0)
                message(FATAL_ERROR "Failed to install FAISS using yum: ${YUM_ERROR}")
            endif()
            
        else()
            message(FATAL_ERROR "Could not find apt-get or yum to install FAISS. Please install FAISS manually.")
        endif()
        
        # After installation, set paths again for Linux
        set(FAISS_INCLUDE_DIR "/usr/include" "/usr/local/include")
        set(FAISS_LIBRARY "/usr/lib" "/usr/lib64" "/usr/local/lib" "/usr/local/lib64")
    else()
        message(FATAL_ERROR "Automatic installation is only supported on macOS and Linux. Please install FAISS manually.")
    endif()
    
    # Try to find FAISS again after installation
    find_path(FAISS_INCLUDE_DIRS faiss/Index.h
        PATHS ${FAISS_INCLUDE_DIR}
        DOC "FAISS include directory")
    find_library(FAISS_LIBRARIES faiss
        PATHS ${FAISS_LIBRARY}
        DOC "FAISS library")
    
    if(NOT FAISS_INCLUDE_DIRS OR NOT FAISS_LIBRARIES)
        message(FATAL_ERROR "Failed to find FAISS after installation attempt. Please install it manually.")
    endif()
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
