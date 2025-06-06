cmake_minimum_required(VERSION 3.16)
project(SimpleStorage LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

file(GLOB_RECURSE source_files CONFIGURE_DEPENDS src/*.cpp src/*.h)
add_library(simplestorage_lib ${source_files} "src/utils.cpp")

# =======================
# GoogleTest via FetchContent
# =======================
include(FetchContent)
FetchContent_Declare(
  googletest
  DOWNLOAD_EXTRACT_TIMESTAMP ON
  URL https://github.com/google/googletest/archive/refs/tags/v1.17.0.zip
)
set(INSTALL_GTEST OFF CACHE BOOL "Disable installation of googletest")
FetchContent_MakeAvailable(googletest)

# =======================
# nlohmann/json via FetchContent
# =======================
FetchContent_Declare(
  nlohmann_json
  URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
)
FetchContent_MakeAvailable(nlohmann_json)

target_link_libraries(simplestorage_lib PUBLIC nlohmann_json::nlohmann_json)

# =======================
# FetchContent Boost.Interprocess for file_lock support
# =======================
FetchContent_Declare(
    Boost
    URL https://github.com/boostorg/boost/releases/download/boost-1.88.0/boost-1.88.0-cmake.tar.gz
    DOWNLOAD_EXTRACT_TIMESTAMP ON
    EXCLUDE_FROM_ALL
)
FetchContent_MakeAvailable(Boost)

# Make sure Boost include directories are visible
target_include_directories(simplestorage_lib PUBLIC ${Boost_INCLUDE_DIRS})
# Link against Boost.Interprocess
target_link_libraries(simplestorage_lib PUBLIC Boost::interprocess)

enable_testing()

# =======================
# Tests
# =======================
include(GoogleTest)
file(GLOB TEST_SOURCES tests/*.cpp)

foreach(test_src ${TEST_SOURCES})
    get_filename_component(test_name ${test_src} NAME_WE)
    add_executable(${test_name} ${test_src} "src/utils.cpp")
    target_link_libraries(${test_name} PRIVATE simplestorage_lib gtest_main)
    if(test_name STREQUAL "performance_test")
        gtest_discover_tests(
            ${test_name}
            PROPERTIES LABELS "perf" EXCLUDE_FROM_DEFAULT TRUE
        )
    else()
        gtest_discover_tests(${test_name})
    endif()
    message(${test_name})
endforeach()
