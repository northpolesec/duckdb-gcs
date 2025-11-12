#pragma once

#include <google/cloud/storage/client.h>
#include <memory>

#include "gcs_filesystem.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {
namespace gcs = ::google::cloud::storage;

// Helper class for creating test GCS objects
class GCSTestHelper {
public:
	// Helper to create a test ObjectMetadata
	static gcs::ObjectMetadata CreateTestObjectMetadata(const std::string &bucket, const std::string &object_name,
	                                                    std::size_t size,
	                                                    const std::string &content_type = "application/octet-stream") {
		return gcs::ObjectMetadata()
		    .set_bucket(bucket)
		    .set_name(object_name)
		    .set_size(size)
		    .set_content_type(content_type)
		    .set_generation(1)
		    .set_metageneration(1);
	}

	// Helper to create a list of test objects for Glob testing
	static std::vector<gcs::ObjectMetadata> CreateTestObjectList(const std::string &bucket, const std::string &prefix,
	                                                             int count) {
		std::vector<gcs::ObjectMetadata> objects;
		for (int i = 0; i < count; i++) {
			auto name = prefix + "file_" + std::to_string(i) + ".parquet";
			objects.push_back(CreateTestObjectMetadata(bucket, name, 1024 * (i + 1)));
		}
		return objects;
	}
};

// Helper class for testing with DuckDB database
class GCSTestFixture {
public:
	GCSTestFixture() : db(nullptr), con(nullptr) {
		db = make_uniq<DuckDB>(nullptr);
		con = make_uniq<Connection>(*db);

		// Load the GCS extension
		con->Query("LOAD 'gcs'");
	}

	~GCSTestFixture() = default;

	Connection &GetConnection() {
		return *con;
	}

	DuckDB &GetDatabase() {
		return *db;
	}

	// Helper to execute query and get result
	unique_ptr<MaterializedQueryResult> Query(const std::string &query) {
		return con->Query(query);
	}

private:
	unique_ptr<DuckDB> db;
	unique_ptr<Connection> con;
};

} // namespace duckdb
