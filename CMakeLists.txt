# SimpleGameTemplate cmake build script.
cmake_minimum_required(VERSION 3.12)
project(Coal)

# Check the build type
if (CMAKE_BUILD_TYPE STREQUAL "")
	# CMake defaults to leaving CMAKE_BUILD_TYPE empty. This screws up
	# differentiation between debug and release builds.
	set(CMAKE_BUILD_TYPE "RelWithDebInfo" CACHE STRING "Choose the type of build, options are: None (CMAKE_CXX_FLAGS or CMAKE_C_FLAGS used) Debug Release RelWithDebInfo MinSizeRel." FORCE)
endif ()

set_property(GLOBAL PROPERTY USE_FOLDERS ON)

# Turn warnings and use C++ 2017.
if (${CMAKE_CXX_COMPILER_ID} STREQUAL MSVC)
	# using Visual Studio C++
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /W4")
else()
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -std=c++17 -Wno-unknown-pragmas")
	if(UNIX)
		if(NOT ON_EMSCRIPTEN)
        	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
		endif()
    endif()
endif()

##find_package(LLVM REQUIRED CONFIG)

# Set output dir.
set(EXECUTABLE_OUTPUT_PATH "${Coal_BINARY_DIR}/dist")
set(LIBRARY_OUTPUT_PATH "${Coal_BINARY_DIR}/dist")

# Add the current directory.
include_directories(
.
"${PROJECT_SOURCE_DIR}/include"
"${PROJECT_SOURCE_DIR}/third-party/unittestpp"
)

# Build the library
add_subdirectory(lib)

# Build the tests
enable_testing()
add_subdirectory(tests)

# Build the samples
add_subdirectory(samples)

# Build the tools
add_subdirectory(tools)

