#include "catch.hpp"
#include "gcs_test_helper.hpp"
#include "gcs_filesystem.hpp"
#include <thread>
#include <vector>
#include <atomic>

using namespace duckdb;

TEST_CASE("SafeMaxRead: Integer overflow protection", "[gcs][file]") {
	SECTION("Normal case - offset < length") {
		idx_t offset = 1000;
		idx_t length = 5000;
		int64_t result = SafeMaxRead(offset, length);
		REQUIRE(result == 4000);
	}

	SECTION("Edge case - offset == length (at EOF)") {
		idx_t offset = 5000;
		idx_t length = 5000;
		int64_t result = SafeMaxRead(offset, length);
		REQUIRE(result == 0);
	}

	SECTION("Edge case - offset > length (past EOF)") {
		idx_t offset = 6000;
		idx_t length = 5000;
		int64_t result = SafeMaxRead(offset, length);
		REQUIRE(result == 0);
	}

	SECTION("Large file - normal read") {
		idx_t offset = 0;
		idx_t length = static_cast<idx_t>(5) * 1024 * 1024 * 1024; // 5GB
		int64_t result = SafeMaxRead(offset, length);
		REQUIRE(result == static_cast<int64_t>(5) * 1024 * 1024 * 1024);
	}

	SECTION("Very large difference within INT64_MAX") {
		idx_t offset = 0;
		idx_t length = static_cast<idx_t>(1) << 62; // Large but < INT64_MAX
		int64_t result = SafeMaxRead(offset, length);
		REQUIRE(result > 0);
		REQUIRE(result == static_cast<int64_t>(length));
	}

	SECTION("Maximum safe signed value") {
		idx_t offset = 0;
		idx_t length = static_cast<idx_t>(NumericLimits<int64_t>::Maximum());
		int64_t result = SafeMaxRead(offset, length);
		REQUIRE(result == NumericLimits<int64_t>::Maximum());
	}

	SECTION("Difference exceeds INT64_MAX") {
		idx_t offset = 0;
		idx_t length = static_cast<idx_t>(NumericLimits<int64_t>::Maximum()) + 1000;
		int64_t result = SafeMaxRead(offset, length);
		// Should be capped at INT64_MAX
		REQUIRE(result == NumericLimits<int64_t>::Maximum());
	}

	SECTION("Near-boundary offset and length") {
		idx_t offset = static_cast<idx_t>(NumericLimits<int64_t>::Maximum()) - 100;
		idx_t length = static_cast<idx_t>(NumericLimits<int64_t>::Maximum());
		int64_t result = SafeMaxRead(offset, length);
		REQUIRE(result == 100);
	}
}

TEST_CASE("GCSFileHandle: Parallel reads", "[gcs][file][parallel]") {
	// This test would ideally use a mock GCS client
	// For now, we test the thread safety of the cache and file handle operations

	GCSReadOptions options;
	options.enable_caches = true;
	options.max_metadata_cache_entries = 1000;
	options.metadata_cache_ttl_seconds = 300;

	auto credentials = google::cloud::MakeInsecureCredentials();
	auto client = gcs::Client(google::cloud::Options {}.set<google::cloud::UnifiedCredentialsOption>(credentials));

	auto context = make_shared_ptr<GCSContextState>(client, options);

	SECTION("Parallel cache writes and reads") {
		std::atomic<int> errors {0};
		const int num_threads = 10;
		const int operations_per_thread = 100;

		std::vector<std::thread> threads;

		for (int t = 0; t < num_threads; t++) {
			threads.emplace_back([&, t]() {
				try {
					for (int i = 0; i < operations_per_thread; i++) {
						auto key = "object-" + std::to_string(t) + "-" + std::to_string(i);
						auto metadata = gcs::ObjectMetadata()
						                    .set_bucket("test-bucket")
						                    .set_name(key)
						                    .set_size(1024 * i)
						                    .set_generation(1);

						// Write to cache
						context->SetCachedMetadata("test-bucket", key, metadata);

						// Read from cache
						auto cached = context->GetCachedMetadata("test-bucket", key);
						if (!cached.has_value() || cached->size() != 1024 * i) {
							errors++;
						}
					}
				} catch (...) {
					errors++;
				}
			});
		}

		for (auto &thread : threads) {
			thread.join();
		}

		REQUIRE(errors == 0);
	}

	SECTION("Parallel list cache operations") {
		std::atomic<int> errors {0};
		const int num_threads = 10;
		const int operations_per_thread = 50;

		std::vector<std::thread> threads;

		for (int t = 0; t < num_threads; t++) {
			threads.emplace_back([&, t]() {
				try {
					for (int i = 0; i < operations_per_thread; i++) {
						auto prefix = "prefix-" + std::to_string(t) + "-" + std::to_string(i) + "/";
						vector<OpenFileInfo> results;
						results.push_back(OpenFileInfo("gs://test-bucket/" + prefix + "file.parquet"));

						// Write to cache
						context->SetCachedList("test-bucket", prefix, results);

						// Read from cache
						auto cached = context->GetCachedList("test-bucket", prefix);
						if (!cached.has_value() || cached->size() != 1) {
							errors++;
						}
					}
				} catch (...) {
					errors++;
				}
			});
		}

		for (auto &thread : threads) {
			thread.join();
		}

		REQUIRE(errors == 0);
	}

	SECTION("Concurrent cache eviction") {
		GCSReadOptions small_cache_options;
		small_cache_options.enable_caches = true;
		small_cache_options.max_metadata_cache_entries = 50; // Small cache to trigger evictions
		small_cache_options.metadata_cache_ttl_seconds = 300;

		auto small_context = make_shared_ptr<GCSContextState>(client, small_cache_options);

		std::atomic<int> errors {0};
		const int num_threads = 10;
		const int operations_per_thread = 20;

		std::vector<std::thread> threads;

		for (int t = 0; t < num_threads; t++) {
			threads.emplace_back([&, t]() {
				try {
					for (int i = 0; i < operations_per_thread; i++) {
						auto key = "object-" + std::to_string(t * operations_per_thread + i);
						auto metadata = gcs::ObjectMetadata()
						                    .set_bucket("test-bucket")
						                    .set_name(key)
						                    .set_size(1024)
						                    .set_generation(1);

						small_context->SetCachedMetadata("test-bucket", key, metadata);

						// Try to read it back (might be evicted)
						auto cached = small_context->GetCachedMetadata("test-bucket", key);
						// We don't require it to be cached since it might have been evicted
					}
				} catch (...) {
					errors++;
				}
			});
		}

		for (auto &thread : threads) {
			thread.join();
		}

		// Should not crash or throw exceptions
		REQUIRE(errors == 0);
	}
}

TEST_CASE("GCSFileHandle: Large file handling", "[gcs][file][large]") {
	SECTION("Buffer boundaries") {
		// Test reading at various buffer boundaries
		idx_t buffer_size = 1024 * 1024; // 1MB buffer

		struct TestCase {
			idx_t file_size;
			idx_t offset;
			idx_t read_size;
			idx_t expected_max_read;
		};

		std::vector<TestCase> test_cases = {
		    // Normal read within file
		    {10 * 1024 * 1024, 0, buffer_size, buffer_size},
		    // Read at end of file
		    {10 * 1024 * 1024, 10 * 1024 * 1024 - 100, buffer_size, 100},
		    // Read exactly at EOF
		    {10 * 1024 * 1024, 10 * 1024 * 1024, buffer_size, 0},
		    // Read past EOF
		    {10 * 1024 * 1024, 10 * 1024 * 1024 + 100, buffer_size, 0},
		    // Large file, read near end
		    {static_cast<idx_t>(5) * 1024 * 1024 * 1024, static_cast<idx_t>(5) * 1024 * 1024 * 1024 - 1000, buffer_size,
		     1000},
		};

		for (const auto &test : test_cases) {
			int64_t max_read = SafeMaxRead(test.offset, test.file_size);
			int64_t actual_read = MinValue<int64_t>(max_read, static_cast<int64_t>(test.read_size));
			REQUIRE(actual_read == static_cast<int64_t>(test.expected_max_read));
		}
	}

	SECTION("Multiple buffer-sized reads") {
		// Simulate reading a large file in chunks
		idx_t file_size = 100 * 1024 * 1024; // 100MB
		idx_t buffer_size = 1024 * 1024;     // 1MB
		idx_t offset = 0;
		idx_t total_read = 0;

		while (offset < file_size) {
			int64_t max_read = SafeMaxRead(offset, file_size);
			int64_t to_read = MinValue<int64_t>(max_read, static_cast<int64_t>(buffer_size));

			REQUIRE(to_read > 0);
			REQUIRE(to_read <= static_cast<int64_t>(buffer_size));

			offset += to_read;
			total_read += to_read;
		}

		REQUIRE(total_read == file_size);
		REQUIRE(offset == file_size);

		// Next read should return 0
		int64_t final_read = SafeMaxRead(offset, file_size);
		REQUIRE(final_read == 0);
	}

	SECTION("Very large file simulation (5TB)") {
		// GCS supports files up to 5TB
		idx_t file_size = static_cast<idx_t>(5) * 1024 * 1024 * 1024 * 1024; // 5TB
		idx_t offset = 0;

		// Read from start
		int64_t max_read_start = SafeMaxRead(offset, file_size);
		REQUIRE(max_read_start > 0);

		// Read from middle
		idx_t middle_offset = file_size / 2;
		int64_t max_read_middle = SafeMaxRead(middle_offset, file_size);
		REQUIRE(max_read_middle > 0);

		// Read near end
		idx_t near_end_offset = file_size - 1000;
		int64_t max_read_near_end = SafeMaxRead(near_end_offset, file_size);
		REQUIRE(max_read_near_end == 1000);
	}
}

TEST_CASE("GCSFileHandle: Edge cases", "[gcs][file]") {
	SECTION("Zero-sized file") {
		idx_t file_size = 0;
		idx_t offset = 0;

		int64_t max_read = SafeMaxRead(offset, file_size);
		REQUIRE(max_read == 0);
	}

	SECTION("Single byte file") {
		idx_t file_size = 1;

		// Read from start
		int64_t max_read = SafeMaxRead(0, file_size);
		REQUIRE(max_read == 1);

		// Read from EOF
		int64_t max_read_eof = SafeMaxRead(1, file_size);
		REQUIRE(max_read_eof == 0);
	}

	SECTION("Buffer larger than file") {
		idx_t file_size = 1024;
		idx_t buffer_size = 1024 * 1024; // 1MB buffer for 1KB file

		int64_t max_read = SafeMaxRead(0, file_size);
		int64_t actual_read = MinValue<int64_t>(max_read, static_cast<int64_t>(buffer_size));
		REQUIRE(actual_read == 1024);
	}

	SECTION("Sequential reads with seeks") {
		idx_t file_size = 1000;

		// Read first 100 bytes
		idx_t offset = 0;
		int64_t read1 = SafeMaxRead(offset, file_size);
		REQUIRE(read1 == 1000);

		// Seek to middle
		offset = 500;
		int64_t read2 = SafeMaxRead(offset, file_size);
		REQUIRE(read2 == 500);

		// Seek back to start
		offset = 0;
		int64_t read3 = SafeMaxRead(offset, file_size);
		REQUIRE(read3 == 1000);

		// Seek to end
		offset = 1000;
		int64_t read4 = SafeMaxRead(offset, file_size);
		REQUIRE(read4 == 0);
	}
}
