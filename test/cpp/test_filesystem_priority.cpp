#include "catch.hpp"
#include "gcs_extension.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"

using namespace duckdb;

namespace {

// Stands in for httpfs, which also claims gs:// and gcs:// and is often loaded after this extension
// (for instance when DuckLake autoloads it for its data path).
class CompetingFileSystem : public FileSystem {
public:
	std::string GetName() const override {
		return "CompetingFileSystem";
	}

	bool CanHandleFile(const string &path) override {
		return StringUtil::StartsWith(path, "gs://") || StringUtil::StartsWith(path, "gcs://") ||
		       StringUtil::StartsWith(path, "s3://");
	}

	string PathSeparator(const string &) override {
		return "#";
	}
};

} // namespace

TEST_CASE("GCS filesystem keeps its URLs when another filesystem is registered later", "[gcs][filesystem]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<GcsExtension>();
	REQUIRE(db.ExtensionIsLoaded("gcs"));

	// The virtual filesystem forwards PathSeparator to the filesystem it resolves for the path
	auto &fs = db.GetFileSystem();
	fs.RegisterSubSystem(make_uniq<CompetingFileSystem>());

	REQUIRE(fs.PathSeparator("gs://bucket/file.parquet") == "/");
	REQUIRE(fs.PathSeparator("gcs://bucket/file.parquet") == "/");
	REQUIRE(fs.PathSeparator("gcss://bucket/file.parquet") == "/");
	// Paths the GCS filesystem does not handle still reach the competing filesystem
	REQUIRE(fs.PathSeparator("s3://bucket/file.parquet") == "#");
}
