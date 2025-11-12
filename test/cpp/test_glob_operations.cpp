#include "catch.hpp"
#include "gcs_filesystem.hpp"
#include "gcs_parsed_url.hpp"

using namespace duckdb;

TEST_CASE("GCS Glob: List cache with large result sets", "[gcs][glob][cache]") {
	GCSReadOptions options;
	options.enable_caches = true;
	options.list_cache_ttl_seconds = 300;
	options.max_list_cache_entries = 100;

	auto credentials = google::cloud::MakeInsecureCredentials();
	auto client = gcs::Client(google::cloud::Options {}.set<google::cloud::UnifiedCredentialsOption>(credentials));

	auto context = make_shared_ptr<GCSContextState>(client, options);

	SECTION("Cache large glob result set") {
		// Simulate a glob that returns 1000 files
		vector<OpenFileInfo> large_results;
		for (int i = 0; i < 1000; i++) {
			large_results.push_back(
			    OpenFileInfo("gs://test-bucket/data/year=2024/file_" + std::to_string(i) + ".parquet"));
		}

		context->SetCachedList("test-bucket", "data/year=2024/", large_results);

		// Retrieve and verify
		auto cached = context->GetCachedList("test-bucket", "data/year=2024/");
		REQUIRE(cached.has_value());
		REQUIRE(cached->size() == 1000);

		// Verify some entries
		REQUIRE((*cached)[0].path == "gs://test-bucket/data/year=2024/file_0.parquet");
		REQUIRE((*cached)[500].path == "gs://test-bucket/data/year=2024/file_500.parquet");
		REQUIRE((*cached)[999].path == "gs://test-bucket/data/year=2024/file_999.parquet");
	}

	SECTION("Multiple large glob results cached separately") {
		// Cache multiple large result sets with different prefixes
		for (int prefix_id = 0; prefix_id < 10; prefix_id++) {
			vector<OpenFileInfo> results;
			auto prefix = "prefix_" + std::to_string(prefix_id) + "/";

			for (int i = 0; i < 500; i++) {
				results.push_back(
				    OpenFileInfo("gs://test-bucket/" + prefix + "file_" + std::to_string(i) + ".parquet"));
			}

			context->SetCachedList("test-bucket", prefix, results);
		}

		// Verify all are cached correctly
		for (int prefix_id = 0; prefix_id < 10; prefix_id++) {
			auto prefix = "prefix_" + std::to_string(prefix_id) + "/";
			auto cached = context->GetCachedList("test-bucket", prefix);

			REQUIRE(cached.has_value());
			REQUIRE(cached->size() == 500);
		}
	}

	SECTION("Cache handles 10,000 file result set") {
		// Test with MaxResults=10000 which is the limit in the code
		vector<OpenFileInfo> huge_results;
		for (int i = 0; i < 10000; i++) {
			huge_results.push_back(OpenFileInfo("gs://test-bucket/bigdata/file_" + std::to_string(i) + ".parquet"));
		}

		context->SetCachedList("test-bucket", "bigdata/", huge_results);

		auto cached = context->GetCachedList("test-bucket", "bigdata/");
		REQUIRE(cached.has_value());
		REQUIRE(cached->size() == 10000);
	}
}

TEST_CASE("GCS Glob: Prefix matching", "[gcs][glob]") {
	SECTION("Parse glob patterns correctly") {
		// Test various glob patterns and their expected prefixes
		struct TestCase {
			std::string path;
			std::string expected_bucket;
			std::string expected_prefix;
		};

		std::vector<TestCase> test_cases = {
		    {"gs://bucket/path/to/files/*.parquet", "bucket", "path/to/files/"},
		    {"gs://bucket/year=2024/**/*.parquet", "bucket", "year=2024/"},
		    {"gs://bucket/prefix", "bucket", "prefix"},
		    {"gs://bucket/", "bucket", ""},
		};

		for (const auto &test : test_cases) {
			GCSParsedUrl parsed;
			parsed.ParseUrl(test.path);
			REQUIRE(parsed.bucket == test.expected_bucket);
		}
	}
}

TEST_CASE("GCS Glob: Hierarchical prefixes", "[gcs][glob][cache]") {
	GCSReadOptions options;
	options.enable_caches = true;
	options.list_cache_ttl_seconds = 300;

	auto credentials = google::cloud::MakeInsecureCredentials();
	auto client = gcs::Client(google::cloud::Options {}.set<google::cloud::UnifiedCredentialsOption>(credentials));

	auto context = make_shared_ptr<GCSContextState>(client, options);

	SECTION("Hierarchical data layout") {
		// Simulate a Hive-style partitioned dataset
		std::vector<std::string> years = {"2022", "2023", "2024"};
		std::vector<std::string> months = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"};

		// Cache results for each partition
		for (const auto &year : years) {
			for (const auto &month : months) {
				auto prefix = "data/year=" + year + "/month=" + month + "/";
				vector<OpenFileInfo> results;

				// Each partition has 100 files
				for (int i = 0; i < 100; i++) {
					results.push_back(
					    OpenFileInfo("gs://test-bucket/" + prefix + "data_" + std::to_string(i) + ".parquet"));
				}

				context->SetCachedList("test-bucket", prefix, results);
			}
		}

		// Verify we can retrieve specific partitions
		auto cached_2024_01 = context->GetCachedList("test-bucket", "data/year=2024/month=01/");
		REQUIRE(cached_2024_01.has_value());
		REQUIRE(cached_2024_01->size() == 100);

		auto cached_2023_12 = context->GetCachedList("test-bucket", "data/year=2023/month=12/");
		REQUIRE(cached_2023_12.has_value());
		REQUIRE(cached_2023_12->size() == 100);
	}

	SECTION("Deeply nested hierarchy") {
		// Test with deeply nested paths
		auto deep_prefix = "level1/level2/level3/level4/level5/";
		vector<OpenFileInfo> results;

		for (int i = 0; i < 50; i++) {
			results.push_back(OpenFileInfo(std::string("gs://test-bucket/") + deep_prefix + "file_" +
			                               std::to_string(i) + ".parquet"));
		}

		context->SetCachedList("test-bucket", deep_prefix, results);

		auto cached = context->GetCachedList("test-bucket", deep_prefix);
		REQUIRE(cached.has_value());
		REQUIRE(cached->size() == 50);
	}
}

TEST_CASE("GCS Glob: Performance with large result sets", "[gcs][glob][performance]") {
	GCSReadOptions options;
	options.enable_caches = true;
	options.list_cache_ttl_seconds = 300;
	options.max_list_cache_entries = 1000;

	auto credentials = google::cloud::MakeInsecureCredentials();
	auto client = gcs::Client(google::cloud::Options {}.set<google::cloud::UnifiedCredentialsOption>(credentials));

	auto context = make_shared_ptr<GCSContextState>(client, options);

	SECTION("Repeated access to cached large result set") {
		// Create a large result set
		vector<OpenFileInfo> large_results;
		for (int i = 0; i < 5000; i++) {
			large_results.push_back(OpenFileInfo("gs://test-bucket/bigdata/file_" + std::to_string(i) + ".parquet"));
		}

		context->SetCachedList("test-bucket", "bigdata/", large_results);

		// Access it multiple times
		for (int access = 0; access < 10; access++) {
			auto cached = context->GetCachedList("test-bucket", "bigdata/");
			REQUIRE(cached.has_value());
			REQUIRE(cached->size() == 5000);
		}
	}

	SECTION("Many small globs vs few large globs") {
		// Cache 100 small result sets (10 files each)
		for (int i = 0; i < 100; i++) {
			vector<OpenFileInfo> small_results;
			auto prefix = "small_prefix_" + std::to_string(i) + "/";

			for (int j = 0; j < 10; j++) {
				small_results.push_back(
				    OpenFileInfo("gs://test-bucket/" + prefix + "file_" + std::to_string(j) + ".parquet"));
			}

			context->SetCachedList("test-bucket", prefix, small_results);
		}

		// Cache 10 large result sets (100 files each)
		for (int i = 0; i < 10; i++) {
			vector<OpenFileInfo> large_results;
			auto prefix = "large_prefix_" + std::to_string(i) + "/";

			for (int j = 0; j < 100; j++) {
				large_results.push_back(
				    OpenFileInfo("gs://test-bucket/" + prefix + "file_" + std::to_string(j) + ".parquet"));
			}

			context->SetCachedList("test-bucket", prefix, large_results);
		}

		// Verify small results
		for (int i = 0; i < 100; i++) {
			auto prefix = "small_prefix_" + std::to_string(i) + "/";
			auto cached = context->GetCachedList("test-bucket", prefix);
			REQUIRE(cached.has_value());
			REQUIRE(cached->size() == 10);
		}

		// Verify large results
		for (int i = 0; i < 10; i++) {
			auto prefix = "large_prefix_" + std::to_string(i) + "/";
			auto cached = context->GetCachedList("test-bucket", prefix);
			REQUIRE(cached.has_value());
			REQUIRE(cached->size() == 100);
		}
	}
}

TEST_CASE("GCS Glob: Edge cases", "[gcs][glob]") {
	GCSReadOptions options;
	options.enable_caches = true;
	options.list_cache_ttl_seconds = 300;

	auto credentials = google::cloud::MakeInsecureCredentials();
	auto client = gcs::Client(google::cloud::Options {}.set<google::cloud::UnifiedCredentialsOption>(credentials));

	auto context = make_shared_ptr<GCSContextState>(client, options);

	SECTION("Empty result set") {
		vector<OpenFileInfo> empty_results;
		context->SetCachedList("test-bucket", "empty/", empty_results);

		auto cached = context->GetCachedList("test-bucket", "empty/");
		REQUIRE(cached.has_value());
		REQUIRE(cached->size() == 0);
	}

	SECTION("Single file result") {
		vector<OpenFileInfo> single_result;
		single_result.push_back(OpenFileInfo("gs://test-bucket/path/single.parquet"));

		context->SetCachedList("test-bucket", "path/", single_result);

		auto cached = context->GetCachedList("test-bucket", "path/");
		REQUIRE(cached.has_value());
		REQUIRE(cached->size() == 1);
		REQUIRE((*cached)[0].path == "gs://test-bucket/path/single.parquet");
	}

	SECTION("Files with special characters in names") {
		vector<OpenFileInfo> special_results;
		special_results.push_back(OpenFileInfo("gs://test-bucket/path/file-with-dash.parquet"));
		special_results.push_back(OpenFileInfo("gs://test-bucket/path/file_with_underscore.parquet"));
		special_results.push_back(OpenFileInfo("gs://test-bucket/path/file.with.dots.parquet"));
		special_results.push_back(OpenFileInfo("gs://test-bucket/path/file with spaces.parquet"));

		context->SetCachedList("test-bucket", "path/", special_results);

		auto cached = context->GetCachedList("test-bucket", "path/");
		REQUIRE(cached.has_value());
		REQUIRE(cached->size() == 4);
	}

	SECTION("Very long prefix") {
		std::string long_prefix = "a/very/long/prefix/that/goes/on/and/on/";
		for (int i = 0; i < 10; i++) {
			long_prefix += "level" + std::to_string(i) + "/";
		}

		vector<OpenFileInfo> results;
		results.push_back(OpenFileInfo("gs://test-bucket/" + long_prefix + "file.parquet"));

		context->SetCachedList("test-bucket", long_prefix, results);

		auto cached = context->GetCachedList("test-bucket", long_prefix);
		REQUIRE(cached.has_value());
		REQUIRE(cached->size() == 1);
	}

	SECTION("Prefixes that are substrings of each other") {
		// Test that caching distinguishes between similar prefixes
		vector<OpenFileInfo> results1;
		results1.push_back(OpenFileInfo("gs://test-bucket/data/file.parquet"));

		vector<OpenFileInfo> results2;
		results2.push_back(OpenFileInfo("gs://test-bucket/data2/file.parquet"));
		results2.push_back(OpenFileInfo("gs://test-bucket/data2/file2.parquet"));

		vector<OpenFileInfo> results3;
		results3.push_back(OpenFileInfo("gs://test-bucket/data/subfolder/file.parquet"));

		context->SetCachedList("test-bucket", "data/", results1);
		context->SetCachedList("test-bucket", "data2/", results2);
		context->SetCachedList("test-bucket", "data/subfolder/", results3);

		auto cached1 = context->GetCachedList("test-bucket", "data/");
		auto cached2 = context->GetCachedList("test-bucket", "data2/");
		auto cached3 = context->GetCachedList("test-bucket", "data/subfolder/");

		REQUIRE(cached1.has_value());
		REQUIRE(cached1->size() == 1);

		REQUIRE(cached2.has_value());
		REQUIRE(cached2->size() == 2);

		REQUIRE(cached3.has_value());
		REQUIRE(cached3->size() == 1);
	}
}
