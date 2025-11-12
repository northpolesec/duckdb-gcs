#include "catch.hpp"
#include "gcs_test_helper.hpp"
#include "gcs_filesystem.hpp"
#include <chrono>
#include <thread>

using namespace duckdb;
namespace gcs = ::google::cloud::storage;

TEST_CASE("GCSContextState: Metadata cache TTL expiration", "[gcs][cache]") {
	GCSReadOptions options;
	options.metadata_cache_ttl_seconds = 1; // 1 second TTL for testing
	options.enable_caches = true;

	auto credentials = google::cloud::MakeInsecureCredentials();
	auto client = gcs::Client(google::cloud::Options {}.set<google::cloud::UnifiedCredentialsOption>(credentials));

	GCSContextState context(client, options);

	// Create test metadata
	auto metadata =
	    gcs::ObjectMetadata().set_bucket("test-bucket").set_name("test-object").set_size(1024).set_generation(1);

	SECTION("Metadata is cached and retrievable within TTL") {
		context.SetCachedMetadata("test-bucket", "test-object", metadata);

		auto cached = context.GetCachedMetadata("test-bucket", "test-object");
		REQUIRE(cached.has_value());
		REQUIRE(cached->name() == "test-object");
		REQUIRE(cached->size() == 1024);
	}

	SECTION("Metadata expires after TTL") {
		context.SetCachedMetadata("test-bucket", "test-object", metadata);

		// Verify it's cached
		auto cached = context.GetCachedMetadata("test-bucket", "test-object");
		REQUIRE(cached.has_value());

		// Wait for TTL to expire
		std::this_thread::sleep_for(std::chrono::seconds(2));

		// Should be expired now
		auto expired = context.GetCachedMetadata("test-bucket", "test-object");
		REQUIRE(!expired.has_value());
	}

	SECTION("Multiple entries with different keys") {
		auto metadata2 =
		    gcs::ObjectMetadata().set_bucket("test-bucket").set_name("test-object-2").set_size(2048).set_generation(1);

		context.SetCachedMetadata("test-bucket", "test-object", metadata);
		context.SetCachedMetadata("test-bucket", "test-object-2", metadata2);

		auto cached1 = context.GetCachedMetadata("test-bucket", "test-object");
		auto cached2 = context.GetCachedMetadata("test-bucket", "test-object-2");

		REQUIRE(cached1.has_value());
		REQUIRE(cached2.has_value());
		REQUIRE(cached1->size() == 1024);
		REQUIRE(cached2->size() == 2048);
	}
}

TEST_CASE("GCSContextState: List cache TTL expiration", "[gcs][cache]") {
	GCSReadOptions options;
	options.list_cache_ttl_seconds = 1; // 1 second TTL for testing
	options.enable_caches = true;

	auto credentials = google::cloud::MakeInsecureCredentials();
	auto client = gcs::Client(google::cloud::Options {}.set<google::cloud::UnifiedCredentialsOption>(credentials));

	GCSContextState context(client, options);

	vector<OpenFileInfo> test_results;
	test_results.push_back(OpenFileInfo("gs://test-bucket/file1.parquet"));
	test_results.push_back(OpenFileInfo("gs://test-bucket/file2.parquet"));

	SECTION("List results are cached and retrievable within TTL") {
		context.SetCachedList("test-bucket", "prefix/", test_results);

		auto cached = context.GetCachedList("test-bucket", "prefix/");
		REQUIRE(cached.has_value());
		REQUIRE(cached->size() == 2);
	}

	SECTION("List results expire after TTL") {
		context.SetCachedList("test-bucket", "prefix/", test_results);

		// Verify it's cached
		auto cached = context.GetCachedList("test-bucket", "prefix/");
		REQUIRE(cached.has_value());

		// Wait for TTL to expire
		std::this_thread::sleep_for(std::chrono::seconds(2));

		// Should be expired now
		auto expired = context.GetCachedList("test-bucket", "prefix/");
		REQUIRE(!expired.has_value());
	}
}

TEST_CASE("GCSContextState: LRU metadata cache eviction", "[gcs][cache]") {
	GCSReadOptions options;
	options.max_metadata_cache_entries = 10; // Small cache for testing
	options.metadata_cache_ttl_seconds = 300;
	options.enable_caches = true;

	auto credentials = google::cloud::MakeInsecureCredentials();
	auto client = gcs::Client(google::cloud::Options {}.set<google::cloud::UnifiedCredentialsOption>(credentials));

	GCSContextState context(client, options);

	SECTION("Cache evicts LRU entry when full") {
		// Fill the cache
		for (int i = 0; i < 10; i++) {
			auto metadata = gcs::ObjectMetadata()
			                    .set_bucket("test-bucket")
			                    .set_name("object-" + std::to_string(i))
			                    .set_size(1024)
			                    .set_generation(1);
			context.SetCachedMetadata("test-bucket", "object-" + std::to_string(i), metadata);
		}

		// Access all entries except the first one to make it LRU
		for (int i = 1; i < 10; i++) {
			auto cached = context.GetCachedMetadata("test-bucket", "object-" + std::to_string(i));
			REQUIRE(cached.has_value());
		}

		// Add one more entry, should evict object-0 (the LRU)
		auto new_metadata =
		    gcs::ObjectMetadata().set_bucket("test-bucket").set_name("object-10").set_size(1024).set_generation(1);
		context.SetCachedMetadata("test-bucket", "object-10", new_metadata);

		// object-0 should be evicted
		auto evicted = context.GetCachedMetadata("test-bucket", "object-0");
		REQUIRE(!evicted.has_value());

		// object-10 should be present
		auto new_entry = context.GetCachedMetadata("test-bucket", "object-10");
		REQUIRE(new_entry.has_value());

		// Other entries should still be present
		auto other = context.GetCachedMetadata("test-bucket", "object-5");
		REQUIRE(other.has_value());
	}

	SECTION("Updating existing entry doesn't trigger eviction") {
		// Fill the cache
		for (int i = 0; i < 10; i++) {
			auto metadata = gcs::ObjectMetadata()
			                    .set_bucket("test-bucket")
			                    .set_name("object-" + std::to_string(i))
			                    .set_size(1024)
			                    .set_generation(1);
			context.SetCachedMetadata("test-bucket", "object-" + std::to_string(i), metadata);
		}

		// Update an existing entry
		auto updated_metadata = gcs::ObjectMetadata()
		                            .set_bucket("test-bucket")
		                            .set_name("object-5")
		                            .set_size(2048) // Different size
		                            .set_generation(2);
		context.SetCachedMetadata("test-bucket", "object-5", updated_metadata);

		// All entries should still be present
		for (int i = 0; i < 10; i++) {
			auto cached = context.GetCachedMetadata("test-bucket", "object-" + std::to_string(i));
			REQUIRE(cached.has_value());
		}

		// Verify the update
		auto updated = context.GetCachedMetadata("test-bucket", "object-5");
		REQUIRE(updated->size() == 2048);
	}
}

TEST_CASE("GCSContextState: LRU list cache eviction", "[gcs][cache]") {
	GCSReadOptions options;
	options.max_list_cache_entries = 5; // Small cache for testing
	options.list_cache_ttl_seconds = 300;
	options.enable_caches = true;

	auto credentials = google::cloud::MakeInsecureCredentials();
	auto client = gcs::Client(google::cloud::Options {}.set<google::cloud::UnifiedCredentialsOption>(credentials));

	GCSContextState context(client, options);

	SECTION("List cache evicts LRU entry when full") {
		// Fill the cache with different prefixes
		for (int i = 0; i < 5; i++) {
			vector<OpenFileInfo> results;
			results.push_back(OpenFileInfo("gs://test-bucket/prefix-" + std::to_string(i) + "/file.parquet"));
			context.SetCachedList("test-bucket", "prefix-" + std::to_string(i) + "/", results);
		}

		// Access all entries except the first one
		for (int i = 1; i < 5; i++) {
			auto cached = context.GetCachedList("test-bucket", "prefix-" + std::to_string(i) + "/");
			REQUIRE(cached.has_value());
		}

		// Add one more entry, should evict prefix-0 (the LRU)
		vector<OpenFileInfo> new_results;
		new_results.push_back(OpenFileInfo("gs://test-bucket/prefix-5/file.parquet"));
		context.SetCachedList("test-bucket", "prefix-5/", new_results);

		// prefix-0 should be evicted
		auto evicted = context.GetCachedList("test-bucket", "prefix-0/");
		REQUIRE(!evicted.has_value());

		// prefix-5 should be present
		auto new_entry = context.GetCachedList("test-bucket", "prefix-5/");
		REQUIRE(new_entry.has_value());
	}
}

TEST_CASE("GCSContextState: Cache size limits", "[gcs][cache]") {
	GCSReadOptions options;
	options.max_metadata_cache_entries = 100;
	options.max_list_cache_entries = 50;
	options.metadata_cache_ttl_seconds = 300;
	options.list_cache_ttl_seconds = 60;
	options.enable_caches = true;

	auto credentials = google::cloud::MakeInsecureCredentials();
	auto client = gcs::Client(google::cloud::Options {}.set<google::cloud::UnifiedCredentialsOption>(credentials));

	GCSContextState context(client, options);

	SECTION("Metadata cache respects max size") {
		// Add entries up to the limit
		for (int i = 0; i < 100; i++) {
			auto metadata = gcs::ObjectMetadata()
			                    .set_bucket("test-bucket")
			                    .set_name("object-" + std::to_string(i))
			                    .set_size(1024)
			                    .set_generation(1);
			context.SetCachedMetadata("test-bucket", "object-" + std::to_string(i), metadata);
		}

		// All should be cached
		for (int i = 0; i < 100; i++) {
			auto cached = context.GetCachedMetadata("test-bucket", "object-" + std::to_string(i));
			REQUIRE(cached.has_value());
		}

		// Adding one more should trigger eviction
		auto new_metadata =
		    gcs::ObjectMetadata().set_bucket("test-bucket").set_name("object-100").set_size(1024).set_generation(1);
		context.SetCachedMetadata("test-bucket", "object-100", new_metadata);

		// New entry should be present
		auto new_entry = context.GetCachedMetadata("test-bucket", "object-100");
		REQUIRE(new_entry.has_value());
	}

	SECTION("List cache respects max size") {
		// Add entries up to the limit
		for (int i = 0; i < 50; i++) {
			vector<OpenFileInfo> results;
			results.push_back(OpenFileInfo("gs://test-bucket/prefix-" + std::to_string(i) + "/file.parquet"));
			context.SetCachedList("test-bucket", "prefix-" + std::to_string(i) + "/", results);
		}

		// All should be cached
		for (int i = 0; i < 50; i++) {
			auto cached = context.GetCachedList("test-bucket", "prefix-" + std::to_string(i) + "/");
			REQUIRE(cached.has_value());
		}

		// Adding one more should trigger eviction
		vector<OpenFileInfo> new_results;
		new_results.push_back(OpenFileInfo("gs://test-bucket/prefix-50/file.parquet"));
		context.SetCachedList("test-bucket", "prefix-50/", new_results);

		// New entry should be present
		auto new_entry = context.GetCachedList("test-bucket", "prefix-50/");
		REQUIRE(new_entry.has_value());
	}
}

TEST_CASE("GCSContextState: Cache can be disabled", "[gcs][cache]") {
	GCSReadOptions options;
	options.enable_caches = false;

	auto credentials = google::cloud::MakeInsecureCredentials();
	auto client = gcs::Client(google::cloud::Options {}.set<google::cloud::UnifiedCredentialsOption>(credentials));

	GCSContextState context(client, options);

	SECTION("Metadata is not cached when disabled") {
		auto metadata =
		    gcs::ObjectMetadata().set_bucket("test-bucket").set_name("test-object").set_size(1024).set_generation(1);

		context.SetCachedMetadata("test-bucket", "test-object", metadata);

		// Should not be cached
		auto cached = context.GetCachedMetadata("test-bucket", "test-object");
		REQUIRE(!cached.has_value());
	}

	SECTION("List results are not cached when disabled") {
		vector<OpenFileInfo> test_results;
		test_results.push_back(OpenFileInfo("gs://test-bucket/file1.parquet"));

		context.SetCachedList("test-bucket", "prefix/", test_results);

		// Should not be cached
		auto cached = context.GetCachedList("test-bucket", "prefix/");
		REQUIRE(!cached.has_value());
	}
}
