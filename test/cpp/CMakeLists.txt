# GCS Extension Unit Tests

# Set the test target name
set(TEST_TARGET gcs_unit_tests)

# Find required packages
find_package(google_cloud_cpp_storage REQUIRED)

# Collect test source files
set(TEST_SOURCES test_main.cpp test_cache_behavior.cpp test_file_operations.cpp
                 test_glob_operations.cpp)

# Add benchmarks only on Clang/GNU
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  list(APPEND TEST_SOURCES benchmark_large_file_reads.cpp)
endif()

# Create the test executable
add_executable(${TEST_TARGET} ${TEST_SOURCES})

# Include directories
target_include_directories(
  ${TEST_TARGET}
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../src/include
          ${CMAKE_CURRENT_SOURCE_DIR}/../../duckdb/test/include
          ${CMAKE_CURRENT_SOURCE_DIR}/../../duckdb/third_party/catch)

# Link against the extension library and required dependencies
target_link_libraries(${TEST_TARGET} PRIVATE gcs_extension duckdb_static
                                             google-cloud-cpp::storage)

# Enable testing
enable_testing()

# Add test
add_test(NAME ${TEST_TARGET} COMMAND ${TEST_TARGET})

# Set C++ standard
set_target_properties(${TEST_TARGET} PROPERTIES CXX_STANDARD 17
                                                CXX_STANDARD_REQUIRED ON)

# Add compiler flags
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(
    ${TEST_TARGET} PRIVATE -Wall -Wextra -DCATCH_CONFIG_ENABLE_BENCHMARKING=1)
endif()
