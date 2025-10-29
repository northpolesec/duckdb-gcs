#include "gcs_parsed_url.hpp"
#include "duckdb/common/exception.hpp"
#include <regex>

namespace duckdb {

void GCSParsedUrl::ParseUrl(const std::string &url) {
	// Support gs://, gcs://, and gcss:// prefixes
	std::regex url_regex("^(gs|gcs|gcss)://([^/]+)/(.*)$");
	std::smatch matches;

	if (!std::regex_match(url, matches, url_regex)) {
		throw InvalidInputException(
		    "Invalid GCS URL format. Expected gs://bucket/path, gcs://bucket/path, or gcss://bucket/path");
	}

	prefix = matches[1];
	bucket = matches[2];
	object_key = matches[3];

	// Remove trailing slashes from object_key if present
	while (!object_key.empty() && object_key.back() == '/') {
		object_key.pop_back();
	}
}

} // namespace duckdb
