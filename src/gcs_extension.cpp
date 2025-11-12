#include "gcs_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/config.hpp"
#include "gcs_filesystem.hpp"
#include "gcs_secret.hpp"

#include <sys/stat.h>
#include <string>

namespace duckdb {

// Google Cloud C++ SDK compiles in libcurl, which means the cert file location
// of the build machine is the place where it will look by default. But different
// distros put this file in different locations, so we need to configure the GCS
// client with the correct location. To avoid requiring the user to configure
// this, we search a number of common locations and use the first one we find.
// If no file is found (such as on macOS/Windows), we don't set the option on
// the client, so the default system store will be used.
static const std::string certFileLocations[] = {
    // Arch, Debian-based, Gentoo
    "/etc/ssl/certs/ca-certificates.crt",
    // RedHat 7 based
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
    // Redhat 6 based
    "/etc/pki/tls/certs/ca-bundle.crt",
    // OpenSUSE
    "/etc/ssl/ca-bundle.pem",
    // Alpine
    "/etc/ssl/cert.pem",
};

std::string FindCACertFile(DatabaseInstance &db) {
	for (auto &location : certFileLocations) {
		struct stat buf;
		if (stat(location.c_str(), &buf) == 0) {
			DUCKDB_LOG_DEBUG(db, "gcs.FindCACertFile", "CA path: %s", location.c_str());
			return location;
		}
	}
	return "";
}

static void LoadInternal(ExtensionLoader &loader) {
	auto &instance = loader.GetDatabaseInstance();

	// Try to find a CA cert file.
	auto caFile = FindCACertFile(loader.GetDatabaseInstance());

	// Register GCS filesystem - it will handle [gs,gcs,gcss]:// URLs via CanHandle()
	auto &fs = instance.GetFileSystem();
	fs.RegisterSubSystem(make_uniq<GCSFileSystem>(caFile));

	// Register secrets
	CreateGCSSecretFunctions::Register(loader);

	// Register extension options
	auto &config = DBConfig::GetConfig(instance);
	GCSReadOptions default_read_options;

	config.AddExtensionOption("gcs_enable_metadata_cache",
	                          "Enable caching of object metadata (size, modification time) to reduce API calls. "
	                          "Set to false to disable caching for debugging or when files change frequently.",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(default_read_options.enable_caches));

	config.AddExtensionOption("gcs_metadata_cache_ttl",
	                          "Time-to-live in seconds for cached object metadata. Default is 300 seconds (5 minutes). "
	                          "Increase for stable files, decrease if files change frequently.",
	                          LogicalType::INTEGER, Value::INTEGER(default_read_options.metadata_cache_ttl_seconds));

	config.AddExtensionOption(
	    "gcs_list_cache_ttl",
	    "Time-to-live in seconds for cached object listing results (used in glob operations). "
	    "Default is 60 seconds. Increase for stable directories, decrease if objects are added/removed frequently.",
	    LogicalType::INTEGER, Value::INTEGER(default_read_options.list_cache_ttl_seconds));

	config.AddExtensionOption("gcs_max_metadata_cache_entries",
	                          "Maximum number of metadata cache entries to prevent unbounded memory growth. "
	                          "Default is 10000. When limit is reached, least recently used entries are evicted.",
	                          LogicalType::UBIGINT, Value::UBIGINT(default_read_options.max_metadata_cache_entries));

	config.AddExtensionOption("gcs_max_list_cache_entries",
	                          "Maximum number of list cache entries to prevent unbounded memory growth. "
	                          "Default is 1000. When limit is reached, least recently used entries are evicted.",
	                          LogicalType::UBIGINT, Value::UBIGINT(default_read_options.max_list_cache_entries));

	config.AddExtensionOption("gcs_transfer_concurrency",
	                          "Number of concurrent worker threads to use when reading. "
	                          "Default is 5.",
	                          LogicalType::INTEGER, Value::INTEGER(default_read_options.transfer_concurrency));
}

void GcsExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string GcsExtension::Name() {
	return "gcs";
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(gcs, loader) {
	duckdb::LoadInternal(loader);
}
}
