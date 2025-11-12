#include "catch.hpp"

#include "gcs_filesystem.hpp"
#include <thread>
#include <vector>
#include <atomic>
#include <random>

using namespace duckdb;

TEST_CASE("GCS Benchmark: Large-scale file metadata operations", "[gcs][!benchmark]") {
	GCSReadOptions options;
	options.enable_caches = true;
	options.max_metadata_cache_entries = 10000;
	options.max_list_cache_entries = 1000;
	options.metadata_cache_ttl_seconds = 300;
	options.list_cache_ttl_seconds = 300;

	auto credentials = google::cloud::MakeInsecureCredentials();
	auto client = gcs::Client(google::cloud::Options {}.set<google::cloud::UnifiedCredentialsOption>(credentials));

	auto context = make_shared_ptr<GCSContextState>(client, options);

	SECTION("Benchmark: Cache 10,000 file metadata entries") {
		BENCHMARK("Cache 10,000 metadata entries") {
			for (int i = 0; i < 10000; i++) {
				auto key = "data/year=2024/month=01/file_" + std::to_string(i) + ".parquet";
				auto metadata = gcs::ObjectMetadata()
				                    .set_bucket("benchmark-bucket")
				                    .set_name(key)
				                    .set_size(1024 * 1024 * 10) // 10MB files
				                    .set_generation(1);

				context->SetCachedMetadata("benchmark-bucket", key, metadata);
			}
		};

		// Verify cache is populated
		auto cached = context->GetCachedMetadata("benchmark-bucket", "data/year=2024/month=01/file_5000.parquet");
		REQUIRE(cached.has_value());
	}

	SECTION("Benchmark: Read 10,000 cached metadata entries") {
		// Pre-populate cache
		for (int i = 0; i < 10000; i++) {
			auto key = "data/file_" + std::to_string(i) + ".parquet";
			auto metadata = gcs::ObjectMetadata()
			                    .set_bucket("benchmark-bucket")
			                    .set_name(key)
			                    .set_size(1024 * 1024)
			                    .set_generation(1);

			context->SetCachedMetadata("benchmark-bucket", key, metadata);
		}

		BENCHMARK("Read 10,000 cached metadata entries") {
			int hits = 0;
			for (int i = 0; i < 10000; i++) {
				auto key = "data/file_" + std::to_string(i) + ".parquet";
				auto cached = context->GetCachedMetadata("benchmark-bucket", key);
				if (cached.has_value()) {
					hits++;
				}
			}
			REQUIRE(hits == 10000);
		};
	}

	SECTION("Benchmark: Parallel metadata cache access with 1000 files") {
		// Pre-populate cache with 1000 files
		for (int i = 0; i < 1000; i++) {
			auto key = "data/parallel_file_" + std::to_string(i) + ".parquet";
			auto metadata = gcs::ObjectMetadata()
			                    .set_bucket("benchmark-bucket")
			                    .set_name(key)
			                    .set_size(1024 * 1024 * 5)
			                    .set_generation(1);

			context->SetCachedMetadata("benchmark-bucket", key, metadata);
		}

		BENCHMARK("Parallel access to 1000 cached files (10 threads)") {
			std::atomic<int> total_reads {0};
			std::vector<std::thread> threads;
			const int num_threads = 10;
			const int reads_per_thread = 100;

			for (int t = 0; t < num_threads; t++) {
				threads.emplace_back([&, t]() {
					for (int i = 0; i < reads_per_thread; i++) {
						// Each thread reads random files from the cache
						int file_idx = (t * reads_per_thread + i) % 1000;
						auto key = "data/parallel_file_" + std::to_string(file_idx) + ".parquet";
						auto cached = context->GetCachedMetadata("benchmark-bucket", key);
						if (cached.has_value()) {
							total_reads++;
						}
					}
				});
			}

			for (auto &thread : threads) {
				thread.join();
			}

			REQUIRE(total_reads == num_threads * reads_per_thread);
		};
	}
}

TEST_CASE("GCS Benchmark: Large-scale list cache operations", "[gcs][!benchmark]") {
	GCSReadOptions options;
	options.enable_caches = true;
	options.max_list_cache_entries = 1000;
	options.list_cache_ttl_seconds = 300;

	auto credentials = google::cloud::MakeInsecureCredentials();
	auto client = gcs::Client(google::cloud::Options {}.set<google::cloud::UnifiedCredentialsOption>(credentials));

	auto context = make_shared_ptr<GCSContextState>(client, options);

	SECTION("Benchmark: Cache glob results for 100 partitions with 100 files each") {
		BENCHMARK("Cache 100 partitions with 100 files each (10,000 total files)") {
			// Simulate a partitioned dataset with 100 partitions, each containing 100 files
			for (int partition = 0; partition < 100; partition++) {
				auto prefix = "data/partition_" + std::to_string(partition) + "/";
				vector<OpenFileInfo> files;

				for (int file = 0; file < 100; file++) {
					files.push_back(
					    OpenFileInfo("gs://benchmark-bucket/" + prefix + "file_" + std::to_string(file) + ".parquet"));
				}

				context->SetCachedList("benchmark-bucket", prefix, files);
			}

			// Verify a random partition
			auto cached = context->GetCachedList("benchmark-bucket", "data/partition_50/");
			REQUIRE(cached.has_value());
			REQUIRE(cached->size() == 100);
		};
	}

	SECTION("Benchmark: Read cached glob results for 100 partitions") {
		// Pre-populate cache with 100 partitions
		for (int partition = 0; partition < 100; partition++) {
			auto prefix = "data/partition_" + std::to_string(partition) + "/";
			vector<OpenFileInfo> files;

			for (int file = 0; file < 100; file++) {
				files.push_back(
				    OpenFileInfo("gs://benchmark-bucket/" + prefix + "file_" + std::to_string(file) + ".parquet"));
			}

			context->SetCachedList("benchmark-bucket", prefix, files);
		}

		BENCHMARK("Read cached glob results for 100 partitions") {
			int total_files = 0;
			for (int partition = 0; partition < 100; partition++) {
				auto prefix = "data/partition_" + std::to_string(partition) + "/";
				auto cached = context->GetCachedList("benchmark-bucket", prefix);
				if (cached.has_value()) {
					total_files += cached->size();
				}
			}

			REQUIRE(total_files == 10000);
		};
	}

	SECTION("Benchmark: Hierarchical partitioned dataset (Hive-style)") {
		BENCHMARK("Cache 36 month partitions across 3 years (3,600 files)") {

			// Simulate 3 years of monthly partitions with 100 files each
			std::vector<std::string> years = {"2022", "2023", "2024"};
			std::vector<std::string> months = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"};

			for (const auto &year : years) {
				for (const auto &month : months) {
					auto prefix = "data/year=" + year + "/month=" + month + "/";
					vector<OpenFileInfo> files;

					for (int file = 0; file < 100; file++) {
						files.push_back(OpenFileInfo("gs://benchmark-bucket/" + prefix + "data_" +
						                             std::to_string(file) + ".parquet"));
					}

					context->SetCachedList("benchmark-bucket", prefix, files);
				}
			}

			// Query all partitions for 2024
			int files_2024 = 0;
			for (const auto &month : months) {
				auto prefix = "data/year=2024/month=" + month + "/";
				auto cached = context->GetCachedList("benchmark-bucket", prefix);
				if (cached.has_value()) {
					files_2024 += cached->size();
				}
			}

			REQUIRE(files_2024 == 1200); // 12 months * 100 files
		};
	}
}

TEST_CASE("GCS Benchmark: Simulated parallel file reads", "[gcs][!benchmark]") {
	GCSReadOptions options;
	options.enable_caches = true;
	options.transfer_concurrency = 8;
	options.buffer_size = 1024 * 1024; // 1MB buffer

	SECTION("Benchmark: SafeMaxRead calculations for 1000 files") {
		BENCHMARK("SafeMaxRead calculations for 1000 files") {
			// Simulate computing read ranges for 1000 files of varying sizes
			std::vector<idx_t> file_sizes;
			for (int i = 0; i < 1000; i++) {
				// Files ranging from 1MB to 1GB
				file_sizes.push_back(1024 * 1024 * (1 + (i % 1000)));
			}

			idx_t total_computed = 0;
			for (const auto &file_size : file_sizes) {
				// Simulate reading each file in chunks
				idx_t offset = 0;
				while (offset < file_size) {
					int64_t max_read = SafeMaxRead(offset, file_size);
					idx_t to_read = MinValue<int64_t>(max_read, 1024 * 1024);
					offset += to_read;
					total_computed++;
				}
			}

			REQUIRE(total_computed > 0);
		};
	}

	SECTION("Benchmark: Parallel buffer operations simulation") {
		BENCHMARK("Parallel buffer operations for 500 files") {
			std::atomic<int> files_processed {0};
			std::vector<std::thread> threads;
			const int num_threads = 8;
			const int total_files = 500;
			const int files_per_thread = (total_files + num_threads - 1) / num_threads; // Ceiling division

			for (int t = 0; t < num_threads; t++) {
				threads.emplace_back([&]() {
					for (int i = 0; i < files_per_thread; i++) {
						// Simulate file metadata and buffer calculations
						idx_t file_size = 10 * 1024 * 1024; // 10MB
						idx_t buffer_size = 1024 * 1024;    // 1MB
						idx_t offset = 0;

						int reads = 0;
						while (offset < file_size) {
							int64_t max_read = SafeMaxRead(offset, file_size);
							idx_t to_read = MinValue<int64_t>(max_read, buffer_size);
							offset += to_read;
							reads++;
						}

						files_processed++;
					}
				});
			}

			for (auto &thread : threads) {
				thread.join();
			}

			REQUIRE(files_processed >= 500);
		};
	}
}

TEST_CASE("GCS Benchmark: Cache eviction under load", "[gcs][!benchmark]") {
	SECTION("Benchmark: LRU eviction with 1000 entries and 10,000 accesses") {
		GCSReadOptions options;
		options.enable_caches = true;
		options.max_metadata_cache_entries = 1000; // Limited cache size
		options.metadata_cache_ttl_seconds = 300;

		auto credentials = google::cloud::MakeInsecureCredentials();
		auto client = gcs::Client(google::cloud::Options {}.set<google::cloud::UnifiedCredentialsOption>(credentials));

		auto context = make_shared_ptr<GCSContextState>(client, options);

		BENCHMARK("LRU eviction test: 10,000 file accesses with 1000-entry cache") {
			// Simulate accessing 10,000 different files with a cache that holds only 1000
			std::random_device rd;
			std::mt19937 gen(42); // Fixed seed for reproducibility
			std::uniform_int_distribution<> distrib(0, 9999);

			int cache_hits = 0;
			int cache_misses = 0;

			for (int access = 0; access < 10000; access++) {
				// Zipfian-like access pattern (some files accessed more frequently)
				int file_id = (access < 5000) ? (distrib(gen) % 500) : distrib(gen);
				auto key = "data/file_" + std::to_string(file_id) + ".parquet";

				auto cached = context->GetCachedMetadata("benchmark-bucket", key);
				if (cached.has_value()) {
					cache_hits++;
				} else {
					cache_misses++;
					// Simulate fetching from GCS and caching
					auto metadata = gcs::ObjectMetadata()
					                    .set_bucket("benchmark-bucket")
					                    .set_name(key)
					                    .set_size(1024 * 1024 * 10)
					                    .set_generation(1);

					context->SetCachedMetadata("benchmark-bucket", key, metadata);
				}
			}

			REQUIRE(cache_hits + cache_misses == 10000);
		};
	}

	SECTION("Benchmark: Concurrent cache operations with eviction") {
		GCSReadOptions options;
		options.enable_caches = true;
		options.max_metadata_cache_entries = 500; // Small cache to force evictions
		options.metadata_cache_ttl_seconds = 300;

		auto credentials = google::cloud::MakeInsecureCredentials();
		auto client = gcs::Client(google::cloud::Options {}.set<google::cloud::UnifiedCredentialsOption>(credentials));

		auto context = make_shared_ptr<GCSContextState>(client, options);

		BENCHMARK("Concurrent cache with eviction (10 threads, 500-entry cache)") {
			std::atomic<int> operations {0};
			std::vector<std::thread> threads;
			const int num_threads = 10;
			const int operations_per_thread = 200;

			for (int t = 0; t < num_threads; t++) {
				threads.emplace_back([&, t]() {
					std::mt19937 gen(t); // Different seed per thread
					std::uniform_int_distribution<> distrib(0, 1999);

					for (int i = 0; i < operations_per_thread; i++) {
						int file_id = distrib(gen);
						auto key = "data/thread_" + std::to_string(t) + "_file_" + std::to_string(file_id) + ".parquet";

						auto cached = context->GetCachedMetadata("benchmark-bucket", key);
						if (!cached.has_value()) {
							// Cache miss, add to cache
							auto metadata = gcs::ObjectMetadata()
							                    .set_bucket("benchmark-bucket")
							                    .set_name(key)
							                    .set_size(1024 * 1024)
							                    .set_generation(1);

							context->SetCachedMetadata("benchmark-bucket", key, metadata);
						}

						operations++;
					}
				});
			}

			for (auto &thread : threads) {
				thread.join();
			}

			REQUIRE(operations == num_threads * operations_per_thread);
		};
	}
}

TEST_CASE("GCS Benchmark: Realistic workload simulation", "[gcs][!benchmark]") {
	SECTION("Benchmark: Data lake query - scan 1000 partitioned files") {
		GCSReadOptions options;
		options.enable_caches = true;
		options.max_metadata_cache_entries = 5000;
		options.max_list_cache_entries = 500;
		options.metadata_cache_ttl_seconds = 300;
		options.list_cache_ttl_seconds = 300;

		auto credentials = google::cloud::MakeInsecureCredentials();
		auto client = gcs::Client(google::cloud::Options {}.set<google::cloud::UnifiedCredentialsOption>(credentials));

		auto context = make_shared_ptr<GCSContextState>(client, options);

		BENCHMARK("Data lake query simulation: 1000 files across 50 partitions") {
			// Phase 1: Glob to discover files (50 partitions with 20 files each)
			vector<vector<OpenFileInfo>> all_partitions;
			for (int partition = 0; partition < 50; partition++) {
				auto prefix = "datalake/table/year=2024/partition_" + std::to_string(partition) + "/";
				vector<OpenFileInfo> files;

				for (int file = 0; file < 20; file++) {
					files.push_back(
					    OpenFileInfo("gs://datalake-bucket/" + prefix + "data_" + std::to_string(file) + ".parquet"));
				}

				context->SetCachedList("datalake-bucket", prefix, files);
				all_partitions.push_back(files);
			}

			// Phase 2: Get metadata for all discovered files
			int total_files = 0;
			idx_t total_size = 0;
			for (const auto &partition : all_partitions) {
				for (const auto &file_info : partition) {
					// Extract object key from path
					size_t bucket_end = file_info.path.find('/', 6); // After "gs://"
					if (bucket_end != std::string::npos) {
						std::string object_key = file_info.path.substr(bucket_end + 1);

						// Simulate fetching metadata
						auto metadata = gcs::ObjectMetadata()
						                    .set_bucket("datalake-bucket")
						                    .set_name(object_key)
						                    .set_size(1024 * 1024 * 128) // 128MB files
						                    .set_generation(1);

						context->SetCachedMetadata("datalake-bucket", object_key, metadata);

						auto cached = context->GetCachedMetadata("datalake-bucket", object_key);
						if (cached.has_value()) {
							total_size += cached->size();
							total_files++;
						}
					}
				}
			}

			REQUIRE(total_files == 1000);
		};
	}

	SECTION("Benchmark: Multi-user concurrent access simulation") {
		GCSReadOptions options;
		options.enable_caches = true;
		options.max_metadata_cache_entries = 2000;
		options.max_list_cache_entries = 200;

		auto credentials = google::cloud::MakeInsecureCredentials();
		auto client = gcs::Client(google::cloud::Options {}.set<google::cloud::UnifiedCredentialsOption>(credentials));

		auto context = make_shared_ptr<GCSContextState>(client, options);

		BENCHMARK("Multi-user simulation: 20 users, 50 queries each") {
			std::atomic<int> total_operations {0};
			std::vector<std::thread> users;
			const int num_users = 20;
			const int queries_per_user = 50;

			for (int user = 0; user < num_users; user++) {
				users.emplace_back([&, user]() {
					for (int query = 0; query < queries_per_user; query++) {
						// Each user queries different partitions
						int partition_id = (user * queries_per_user + query) % 100;
						auto prefix = "data/partition_" + std::to_string(partition_id) + "/";

						// Check if list is cached
						auto cached_list = context->GetCachedList("shared-bucket", prefix);
						if (!cached_list.has_value()) {
							// Simulate glob operation
							vector<OpenFileInfo> files;
							for (int f = 0; f < 10; f++) {
								files.push_back(OpenFileInfo("gs://shared-bucket/" + prefix + "file_" +
								                             std::to_string(f) + ".parquet"));
							}
							context->SetCachedList("shared-bucket", prefix, files);
						}

						// Access metadata for each file
						auto list = context->GetCachedList("shared-bucket", prefix);
						if (list.has_value()) {
							for (const auto &file_info : *list) {
								std::string object_key = prefix + "file.parquet";
								auto cached_meta = context->GetCachedMetadata("shared-bucket", object_key);
								if (!cached_meta.has_value()) {
									auto metadata = gcs::ObjectMetadata()
									                    .set_bucket("shared-bucket")
									                    .set_name(object_key)
									                    .set_size(1024 * 1024 * 10)
									                    .set_generation(1);

									context->SetCachedMetadata("shared-bucket", object_key, metadata);
								}
							}
						}

						total_operations++;
					}
				});
			}

			for (auto &user : users) {
				user.join();
			}

			REQUIRE(total_operations == num_users * queries_per_user);
		};
	}
}
