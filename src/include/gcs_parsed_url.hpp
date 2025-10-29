#pragma once

#include <string>

namespace duckdb {

struct GCSParsedUrl {
	std::string bucket;
	std::string object_key;
	std::string endpoint;
	std::string prefix;

	void ParseUrl(const std::string &url);
};

} // namespace duckdb
