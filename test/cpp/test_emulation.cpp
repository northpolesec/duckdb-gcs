#include "catch.hpp"
#include "gcs_filesystem.hpp"
#include "duckdb/common/exception.hpp"
#include "gcs_filesystem.hpp"
#include <cstdlib>

using namespace duckdb;

namespace {

struct ScopedEnvVar {
	std::string name;
	bool was_set;
	std::string old_val;

	ScopedEnvVar(const std::string &name, const char *val) : name(name) {
		const char *curr = std::getenv(name.c_str());
		was_set = (curr != nullptr);
		if (was_set) {
			old_val = curr;
		}
		if (val) {
			setenv(name.c_str(), val, 1);
		} else {
			unsetenv(name.c_str());
		}
	}

	~ScopedEnvVar() {
		if (was_set) {
			setenv(name.c_str(), old_val.c_str(), 1);
		} else {
			unsetenv(name.c_str());
		}
	}
};

class TestableGCSFileSystem : public GCSFileSystem {
public:
	TestableGCSFileSystem() : GCSFileSystem("") {
	}

	shared_ptr<GCSContextState> CallCreateStorageContext(const string &path, const GCSParsedUrl &parsed_url) {
		return CreateStorageContext(nullptr, path, parsed_url);
	}
};
} // namespace

TEST_CASE("GCS Emulator: Context creation and credential bypass", "[gcs][emulator]") {
	TestableGCSFileSystem fs;
	GCSParsedUrl parsed_url;
	parsed_url.bucket = "test-bucket";
	parsed_url.object_key = "test.parquet";

	SECTION("Fails when no credentials and no emulator are set") {
		ScopedEnvVar env1("STORAGE_EMULATOR_HOST", nullptr);
		ScopedEnvVar env2("CLOUD_STORAGE_EMULATOR_ENDPOINT", nullptr);

		REQUIRE_THROWS_AS(fs.CallCreateStorageContext("gcs://test-bucket/test.parquet", parsed_url), IOException);
	}

	SECTION("Succeeds without credentials when STORAGE_EMULATOR_HOST is set") {
		ScopedEnvVar env1("STORAGE_EMULATOR_HOST", "localhost:9000");
		ScopedEnvVar env2("CLOUD_STORAGE_EMULATOR_ENDPOINT", nullptr);

		shared_ptr<GCSContextState> context;
		REQUIRE_NOTHROW(context = fs.CallCreateStorageContext("gcs://test-bucket/test.parquet", parsed_url));
		REQUIRE(context != nullptr);
	}

	SECTION("Succeeds without credentials when CLOUD_STORAGE_EMULATOR_ENDPOINT is set") {
		ScopedEnvVar env1("STORAGE_EMULATOR_HOST", nullptr);
		ScopedEnvVar env2("CLOUD_STORAGE_EMULATOR_ENDPOINT", "http://127.0.0.1:4443");

		shared_ptr<GCSContextState> context;
		REQUIRE_NOTHROW(context = fs.CallCreateStorageContext("gcs://test-bucket/test.parquet", parsed_url));
		REQUIRE(context != nullptr);
	}

	SECTION("Ignores empty environment variables") {
		ScopedEnvVar env1("STORAGE_EMULATOR_HOST", "");
		ScopedEnvVar env2("CLOUD_STORAGE_EMULATOR_ENDPOINT", "");

		REQUIRE_THROWS_AS(fs.CallCreateStorageContext("gcs://test-bucket/test.parquet", parsed_url), IOException);
	}
}
