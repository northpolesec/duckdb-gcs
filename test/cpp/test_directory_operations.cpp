#include "catch.hpp"
#include "gcs_filesystem.hpp"
#include "gcs_parsed_url.hpp"

using namespace duckdb;

namespace {

static shared_ptr<GCSContextState> CreateTestContext() {
	GCSReadOptions options;
	options.enable_caches = true;
	auto credentials = google::cloud::MakeInsecureCredentials();
	auto client = gcs::Client(google::cloud::Options {}.set<google::cloud::UnifiedCredentialsOption>(credentials));
	return make_shared_ptr<GCSContextState>(client, options);
}

} // namespace

TEST_CASE("GCSDirectoryPrefix: object key to directory prefix", "[gcs][directory]") {
	SECTION("Empty key maps to bucket root") {
		REQUIRE(GCSDirectoryPrefix("") == "");
	}

	SECTION("Simple key gets a trailing slash") {
		REQUIRE(GCSDirectoryPrefix("data") == "data/");
	}

	SECTION("Nested key gets a trailing slash") {
		REQUIRE(GCSDirectoryPrefix("a/b/c") == "a/b/c/");
	}

	SECTION("Key that already ends in slash is left unchanged") {
		REQUIRE(GCSDirectoryPrefix("data/") == "data/");
	}

	SECTION("Hive-style partition key") {
		REQUIRE(GCSDirectoryPrefix("out/year=2024/month=01") == "out/year=2024/month=01/");
	}
}

TEST_CASE("GCSDirectoryPrefix: matches GCSParsedUrl normalization", "[gcs][directory]") {
	// GCSParsedUrl strips trailing slashes from the object key, so the directory prefix derived from
	// "gs://bucket/out" and "gs://bucket/out/" must be identical.
	GCSParsedUrl without_slash;
	without_slash.ParseUrl("gs://bucket/out");
	GCSParsedUrl with_slash;
	with_slash.ParseUrl("gs://bucket/out/");

	REQUIRE(GCSDirectoryPrefix(without_slash.object_key) == "out/");
	REQUIRE(GCSDirectoryPrefix(with_slash.object_key) == "out/");
	REQUIRE(GCSDirectoryPrefix(without_slash.object_key) == GCSDirectoryPrefix(with_slash.object_key));
}

TEST_CASE("GCSContextState: InvalidateCachedList drops only the target bucket", "[gcs][directory][cache]") {
	auto context = CreateTestContext();

	vector<OpenFileInfo> results;
	results.push_back(OpenFileInfo("gs://bucket-a/data/file.parquet"));

	// Populate several listings across two buckets.
	context->SetCachedList("bucket-a", "data/", results);
	context->SetCachedList("bucket-a", "data/year=2024/", results);
	context->SetCachedList("bucket-a", "other/", results);
	context->SetCachedList("bucket-b", "data/", results);

	// Sanity check that everything is cached.
	REQUIRE(context->GetCachedList("bucket-a", "data/").has_value());
	REQUIRE(context->GetCachedList("bucket-a", "data/year=2024/").has_value());
	REQUIRE(context->GetCachedList("bucket-a", "other/").has_value());
	REQUIRE(context->GetCachedList("bucket-b", "data/").has_value());

	context->InvalidateCachedList("bucket-a");

	// All listings for bucket-a are gone...
	REQUIRE_FALSE(context->GetCachedList("bucket-a", "data/").has_value());
	REQUIRE_FALSE(context->GetCachedList("bucket-a", "data/year=2024/").has_value());
	REQUIRE_FALSE(context->GetCachedList("bucket-a", "other/").has_value());
	// ...but the unrelated bucket is untouched.
	REQUIRE(context->GetCachedList("bucket-b", "data/").has_value());
}

TEST_CASE("GCSContextState: InvalidateCachedList does not match bucket-name prefixes", "[gcs][directory][cache]") {
	auto context = CreateTestContext();

	vector<OpenFileInfo> results;
	results.push_back(OpenFileInfo("gs://bucket/data/file.parquet"));

	// "bucket" is a prefix of "bucket-2"; invalidating "bucket" must not evict "bucket-2".
	context->SetCachedList("bucket", "data/", results);
	context->SetCachedList("bucket-2", "data/", results);

	context->InvalidateCachedList("bucket");

	REQUIRE_FALSE(context->GetCachedList("bucket", "data/").has_value());
	REQUIRE(context->GetCachedList("bucket-2", "data/").has_value());
}

TEST_CASE("GCSContextState: InvalidateCachedList on empty cache is a no-op", "[gcs][directory][cache]") {
	auto context = CreateTestContext();
	// Must not crash or throw when there is nothing cached.
	context->InvalidateCachedList("bucket-a");
	REQUIRE_FALSE(context->GetCachedList("bucket-a", "data/").has_value());
}
