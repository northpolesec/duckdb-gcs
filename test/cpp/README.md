# GCS Extension Unit Tests

This directory contains C++ unit tests for the DuckDB GCS extension.

## Test Coverage

The unit tests cover:

1. **Cache Behavior** (`test_cache_behavior.cpp`)
   - Metadata cache TTL expiration
   - List cache TTL expiration
   - LRU eviction for metadata cache
   - LRU eviction for list cache
   - Cache size limits
   - Cache enable/disable functionality

2. **File Operations** (`test_file_operations.cpp`)
   - Integer overflow protection in `SafeMaxRead`
   - Parallel cache reads and writes
   - Large file handling (up to 5TB)
   - Buffer boundary conditions
   - Edge cases (zero-sized files, seeks, etc.)

3. **Glob Operations** (`test_glob_operations.cpp`)
   - Large result set caching (up to 10,000 files)
   - Hierarchical prefix matching
   - Hive-style partitioned datasets
   - Performance with many small vs few large globs
   - Edge cases (empty results, special characters, long prefixes)

## Building and Running Tests

### Build with Unit Tests

From the repository root:

```bash
# Configure with unit tests enabled
BUILD_UNITTESTS=1 make

# Or using CMake directly
mkdir -p build/unittest
cd build/unittest
cmake -DBUILD_UNITTESTS=ON -DVCPKG_TOOLCHAIN_PATH=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake ../..
make
```

### Run Unit Tests

```bash
# From the build directory
./test/cpp/gcs_unit_tests

# Run specific test cases
./test/cpp/gcs_unit_tests "[cache]"
./test/cpp/gcs_unit_tests "[file]"
./test/cpp/gcs_unit_tests "[glob]"

# Run with verbose output
./test/cpp/gcs_unit_tests -s
```

### Run via CTest

```bash
# From the build directory
ctest -R gcs_unit_tests -V
```

## Using Mock GCS Clients

The tests use Google Cloud Storage mocking capabilities where possible. See the
[GCS C++ Testing documentation](https://cloud.google.com/cpp/docs/reference/storage/latest/storage-mocking)
for more information on creating and using mock clients.

## Writing New Tests

When adding new tests:

1. Add test file to `test/cpp/`
2. Update `test/cpp/CMakeLists.txt` to include the new source file
3. Use `gcs_test_helper.hpp` for common test utilities
4. Follow the Catch2 test framework conventions
5. Tag tests appropriately (e.g., `[gcs]`, `[cache]`, `[file]`, `[glob]`)

## Test Tags

- `[gcs]` - All GCS extension tests
- `[cache]` - Cache behavior tests
- `[file]` - File operation tests
- `[glob]` - Glob/listing operation tests
- `[parallel]` - Tests involving parallel operations
- `[performance]` - Performance-related tests
- `[large]` - Tests with large files or datasets
