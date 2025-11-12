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

# Build with unit tests enabled and run them (release)
unittest_release: BUILD_UNITTESTS=1
unittest_release:
	@echo "Building extension with unit tests (release)..."
	BUILD_UNITTESTS=1 $(MAKE) release
	@echo "Running unit tests..."
	@if [ -f build/release/extension/gcs/test/cpp/gcs_unit_tests ]; then \
		build/release/extension/gcs/test/cpp/gcs_unit_tests; \
	else \
		echo "Error: Unit test binary not found. Make sure BUILD_UNITTESTS=1 was set during build."; \
		exit 1; \
	fi

# Build with unit tests enabled and run them (debug)
unittest_debug: BUILD_UNITTESTS=1
unittest_debug:
	@echo "Building extension with unit tests (debug)..."
	BUILD_UNITTESTS=1 $(MAKE) debug
	@echo "Running unit tests..."
	@if [ -f build/debug/extension/gcs/test/cpp/gcs_unit_tests ]; then \
		build/debug/extension/gcs/test/cpp/gcs_unit_tests; \
	else \
		echo "Error: Unit test binary not found. Make sure BUILD_UNITTESTS=1 was set during build."; \
		exit 1; \
	fi

# Run unit tests with verbose output
unittest_verbose:
	@echo "Running unit tests with verbose output..."
	@if [ -f build/release/extension/gcs/test/cpp/gcs_unit_tests ]; then \
		build/release/extension/gcs/test/cpp/gcs_unit_tests -s; \
	else \
		echo "Error: Unit test binary not found. Run 'make unittest' first."; \
		exit 1; \
	fi
