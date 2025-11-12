PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
EXT_NAME=gcs
EXT_CONFIG=${PROJ_DIR}extension_config.cmake
EXT_FLAGS=CMAKE_EXPORT_COMPILE_COMMANDS=1

# Require VCPKG_ROOT to be set
ifeq ("${VCPKG_ROOT}", "")
  $(warning VCPKG_ROOT is not set. Please set it to your vcpkg installation directory, e.g., export VCPKG_ROOT=~/dev/vcpkg)
endif

# Set VCPKG_TOOLCHAIN_PATH if not already set
ifeq ("${VCPKG_TOOLCHAIN_PATH}", "")
  VCPKG_TOOLCHAIN_PATH := ${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake
  export VCPKG_TOOLCHAIN_PATH
endif

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Unit test targets
.PHONY: unittest unittest_release unittest_debug unittest_verbose

# Build and run unit tests (release)
unittest: unittest_release

benchmark: benchmark_release

# Build unit tests (release)
buildunittest_release:
	@echo "Building extension with unit tests (release)..."
	BUILD_UNITTESTS=1 $(MAKE) release

# Build unit tests (debug)
buildunittest_debug:
	@echo "Building extension with unit tests (debug)..."
	BUILD_UNITTESTS=1 $(MAKE) debug

# Build with unit tests enabled and run them (release)
unittest_release: buildunittest_release
	@echo "Running unit tests..."
	build/release/extension/gcs/test/cpp/gcs_unit_tests

benchmark_release: buildunittest_release
	@echo "Running benchmarks..."
	build/release/extension/gcs/test/cpp/gcs_unit_tests [!benchmark]

# Build with unit tests enabled and run them (debug)
unittest_debug: buildunittest_debug
	@echo "Running unit tests..."
	build/debug/extension/gcs/test/cpp/gcs_unit_tests

# Run unit tests with verbose output
unittest_verbose: buildunittest_debug
	@echo "Running unit tests..."
	build/release/extension/gcs/test/cpp/gcs_unit_tests -s
