cmake_minimum_required(VERSION 3.5...3.29)

# Set extension name here
set(TARGET_NAME gcs)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/gcs_extension.cpp
    src/gcs_filesystem.cpp
    src/gcs_parsed_url.cpp
    src/gcs_secret.cpp)

set(PARAMETERS "-warnings")
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})

find_package(google_cloud_cpp_storage REQUIRED)

# Static lib
target_include_directories(${EXTENSION_NAME} PUBLIC google-cloud-cpp::storage)
target_link_libraries(${EXTENSION_NAME} google-cloud-cpp::storage)

# Loadable binary
target_link_libraries(${TARGET_NAME}_loadable_extension google-cloud-cpp::storage)
target_include_directories(${TARGET_NAME}_loadable_extension PRIVATE google-cloud-cpp::storage)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

# Add unit tests subdirectory if building tests
if(BUILD_UNITTESTS)
  add_subdirectory(test/cpp)
endif()
