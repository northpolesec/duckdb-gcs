#include "catch.hpp"
#include "gcs_test_helper.hpp"
#include "gcs_filesystem.hpp"

using namespace duckdb;
namespace gcs = ::google::cloud::storage;

TEST_CASE("Generation: Metadata cache preserves generation number", "[gcs][generation]") {
	GCSReadOptions options;
	options.enable_caches = true;
	options.metadata_cache_ttl_seconds = 300;

	auto credentials = google::cloud::MakeInsecureCredentials();
	auto client = gcs::Client(google::cloud::Options {}.set<google::cloud::UnifiedCredentialsOption>(credentials));

	GCSContextState context(client, options);

	SECTION("Generation number is stored and retrieved from cache") {
		auto metadata =
		    gcs::ObjectMetadata().set_bucket("test-bucket").set_name("test-object").set_size(1024).set_generation(42);

		context.SetCachedMetadata("test-bucket", "test-object", metadata);

		auto cached = context.GetCachedMetadata("test-bucket", "test-object");
		REQUIRE(cached.has_value());
		REQUIRE(cached->generation() == 42);
	}

	SECTION("Different objects have different generation numbers") {
		auto metadata1 =
		    gcs::ObjectMetadata().set_bucket("test-bucket").set_name("object-a").set_size(1024).set_generation(100);

		auto metadata2 =
		    gcs::ObjectMetadata().set_bucket("test-bucket").set_name("object-b").set_size(2048).set_generation(200);

		context.SetCachedMetadata("test-bucket", "object-a", metadata1);
		context.SetCachedMetadata("test-bucket", "object-b", metadata2);

		auto cached1 = context.GetCachedMetadata("test-bucket", "object-a");
		auto cached2 = context.GetCachedMetadata("test-bucket", "object-b");

		REQUIRE(cached1.has_value());
		REQUIRE(cached2.has_value());
		REQUIRE(cached1->generation() == 100);
		REQUIRE(cached2->generation() == 200);
	}

	SECTION("Updated metadata replaces generation in cache") {
		auto metadata_v1 =
		    gcs::ObjectMetadata().set_bucket("test-bucket").set_name("test-object").set_size(1024).set_generation(1);

		context.SetCachedMetadata("test-bucket", "test-object", metadata_v1);

		auto cached_v1 = context.GetCachedMetadata("test-bucket", "test-object");
		REQUIRE(cached_v1.has_value());
		REQUIRE(cached_v1->generation() == 1);

		// Simulate object being overwritten (new generation)
		auto metadata_v2 =
		    gcs::ObjectMetadata().set_bucket("test-bucket").set_name("test-object").set_size(2048).set_generation(2);

		context.SetCachedMetadata("test-bucket", "test-object", metadata_v2);

		auto cached_v2 = context.GetCachedMetadata("test-bucket", "test-object");
		REQUIRE(cached_v2.has_value());
		REQUIRE(cached_v2->generation() == 2);
		REQUIRE(cached_v2->size() == 2048);
	}

	SECTION("Large generation numbers are preserved") {
		// GCS generation numbers can be very large
		std::int64_t large_gen = 1700000000000000LL;
		auto metadata = gcs::ObjectMetadata()
		                    .set_bucket("test-bucket")
		                    .set_name("test-object")
		                    .set_size(1024)
		                    .set_generation(large_gen);

		context.SetCachedMetadata("test-bucket", "test-object", metadata);

		auto cached = context.GetCachedMetadata("test-bucket", "test-object");
		REQUIRE(cached.has_value());
		REQUIRE(cached->generation() == large_gen);
	}
}

TEST_CASE("Generation: GCSFileHandle stores generation", "[gcs][generation]") {
	GCSReadOptions options;
	options.enable_caches = true;
	options.metadata_cache_ttl_seconds = 300;

	auto credentials = google::cloud::MakeInsecureCredentials();
	auto client = gcs::Client(google::cloud::Options {}.set<google::cloud::UnifiedCredentialsOption>(credentials));

	auto context = make_shared_ptr<GCSContextState>(client, options);

	SECTION("Default generation is zero") {
		GCSFileSystem fs("");
		OpenFileInfo info("gs://test-bucket/test-object");
		FileOpenFlags flags(FileOpenFlags::FILE_FLAGS_READ);

		GCSFileHandle handle(fs, info, flags, options, "test-bucket", "test-object", context);
		REQUIRE(handle.generation == 0);
	}

	SECTION("Generation is set from cached metadata") {
		auto metadata = gcs::ObjectMetadata()
		                    .set_bucket("test-bucket")
		                    .set_name("test-object")
		                    .set_size(4096)
		                    .set_generation(12345);

		context->SetCachedMetadata("test-bucket", "test-object", metadata);

		// Verify that cached metadata has the generation
		auto cached = context->GetCachedMetadata("test-bucket", "test-object");
		REQUIRE(cached.has_value());
		REQUIRE(cached->generation() == 12345);

		// Simulate what LoadRemoteFileInfo does with cached metadata
		GCSFileSystem fs("");
		OpenFileInfo info("gs://test-bucket/test-object");
		FileOpenFlags flags(FileOpenFlags::FILE_FLAGS_READ);

		GCSFileHandle handle(fs, info, flags, options, "test-bucket", "test-object", context);
		// Manually apply what LoadRemoteFileInfo does
		handle.length = cached->size();
		handle.generation = cached->generation();

		REQUIRE(handle.generation == 12345);
		REQUIRE(handle.length == 4096);
	}
}

TEST_CASE("Generation: Helper creates metadata with generation", "[gcs][generation]") {
	auto metadata = GCSTestHelper::CreateTestObjectMetadata("test-bucket", "test-object", 1024);

	REQUIRE(metadata.generation() == 1);
	REQUIRE(metadata.bucket() == "test-bucket");
	REQUIRE(metadata.name() == "test-object");
	REQUIRE(metadata.size() == 1024);
}

TEST_CASE("Generation: Eviction preserves generation on remaining entries", "[gcs][generation]") {
	GCSReadOptions options;
	options.enable_caches = true;
	options.max_metadata_cache_entries = 3;
	options.metadata_cache_ttl_seconds = 300;

	auto credentials = google::cloud::MakeInsecureCredentials();
	auto client = gcs::Client(google::cloud::Options {}.set<google::cloud::UnifiedCredentialsOption>(credentials));

	GCSContextState context(client, options);

	// Fill cache with entries having distinct generations
	for (int i = 0; i < 3; i++) {
		auto metadata = gcs::ObjectMetadata()
		                    .set_bucket("test-bucket")
		                    .set_name("object-" + std::to_string(i))
		                    .set_size(1024)
		                    .set_generation(100 + i);
		context.SetCachedMetadata("test-bucket", "object-" + std::to_string(i), metadata);
	}

	// Access object-1 and object-2 to make object-0 the LRU
	auto cached1 = context.GetCachedMetadata("test-bucket", "object-1");
	auto cached2 = context.GetCachedMetadata("test-bucket", "object-2");
	REQUIRE(cached1.has_value());
	REQUIRE(cached2.has_value());

	// Add a new entry to trigger eviction of object-0
	auto new_metadata =
	    gcs::ObjectMetadata().set_bucket("test-bucket").set_name("object-3").set_size(1024).set_generation(999);
	context.SetCachedMetadata("test-bucket", "object-3", new_metadata);

	// object-0 should be evicted
	auto evicted = context.GetCachedMetadata("test-bucket", "object-0");
	REQUIRE(!evicted.has_value());

	// Surviving entries should still have correct generation numbers
	auto surviving1 = context.GetCachedMetadata("test-bucket", "object-1");
	auto surviving2 = context.GetCachedMetadata("test-bucket", "object-2");
	auto new_entry = context.GetCachedMetadata("test-bucket", "object-3");

	REQUIRE(surviving1.has_value());
	REQUIRE(surviving1->generation() == 101);

	REQUIRE(surviving2.has_value());
	REQUIRE(surviving2->generation() == 102);

	REQUIRE(new_entry.has_value());
	REQUIRE(new_entry->generation() == 999);
}
