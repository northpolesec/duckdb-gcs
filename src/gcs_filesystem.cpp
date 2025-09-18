#include "gcs_filesystem.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "gcs_secret.hpp"

#include <fstream>

namespace duckdb {

namespace gcs = ::google::cloud::storage;

// GCPContextState implementation
GCPContextState::GCPContextState(const GCPReadOptions &read_options) : read_options(read_options), is_valid(true) {
}

bool GCPContextState::IsValid() const {
	return is_valid;
}

void GCPContextState::QueryEnd() {
	is_valid = false;
}

// GCSFileHandle implementation
GCSFileHandle::GCSFileHandle(GCSFileSystem &fs, const OpenFileInfo &info, FileOpenFlags flags,
                             const GCPReadOptions &read_options, const std::string &bucket,
                             const std::string &object_key, google::cloud::storage::Client client)
    : FileHandle(fs, info.path, flags), flags(flags), length(0), last_modified(0), buffer_available(0), buffer_idx(0),
      file_offset(0), buffer_start(0), buffer_end(0), read_options(read_options), bucket(bucket),
      object_key(object_key), client(client) {
	if (flags.RequireParallelAccess()) {
		read_buffer = duckdb::unique_ptr<data_t[]>(new data_t[read_options.buffer_size]);
	}
}

bool GCSFileHandle::PostConstruct() {
	return true;
}

// GCSFileSystem implementation
duckdb::unique_ptr<FileHandle> GCSFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                       optional_ptr<FileOpener> opener) {
	OpenFileInfo info(path);
	return OpenFileExtended(info, flags, opener);
}

unique_ptr<FileHandle> GCSFileSystem::OpenFileExtended(const OpenFileInfo &info, FileOpenFlags flags,
                                                       optional_ptr<FileOpener> opener) {
	// DirectIO is not applicable for cloud storage, so we just ignore it
	// Cloud storage always goes through network buffers anyway
	if (flags.DirectIO()) {
		// Create new flags without DirectIO
		idx_t new_flags = flags.GetFlagsInternal() & ~FileOpenFlags::FILE_FLAGS_DIRECT_IO;
		flags = FileOpenFlags(new_flags, flags.Lock(), flags.Compression());
	}

	if (flags.ReturnNullIfNotExists()) {
		// Check if file exists first
		if (!FileExists(info.path, opener)) {
			return nullptr;
		}
	}

	auto handle = CreateHandle(info, flags, opener);
	if (!handle->PostConstruct()) {
		return nullptr;
	}
	return std::move(handle);
}

void GCSFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &gcp_handle = static_cast<GCSFileHandle &>(handle);
	idx_t bytes_read = 0;
	idx_t to_read = nr_bytes;
	idx_t file_offset = location;
	char *buffer_ptr = static_cast<char *>(buffer);

	if (gcp_handle.flags.RequireParallelAccess()) {
		// Parallel access - use buffering
		while (to_read > 0) {
			idx_t buffer_end = gcp_handle.buffer_start + gcp_handle.buffer_available;

			// Check if requested data is in buffer
			if (file_offset >= gcp_handle.buffer_start && file_offset < buffer_end) {
				idx_t buffer_offset = file_offset - gcp_handle.buffer_start;
				idx_t available_in_buffer = std::min(to_read, buffer_end - file_offset);

				memcpy(buffer_ptr, gcp_handle.read_buffer.get() + buffer_offset, available_in_buffer);

				buffer_ptr += available_in_buffer;
				file_offset += available_in_buffer;
				to_read -= available_in_buffer;
				bytes_read += available_in_buffer;
			} else {
				// Need to refill buffer
				gcp_handle.buffer_start = file_offset;
				gcp_handle.buffer_available =
				    std::min(gcp_handle.read_options.buffer_size, gcp_handle.length - file_offset);

				if (gcp_handle.buffer_available == 0) {
					break; // EOF
				}

				ReadRange(gcp_handle, file_offset, reinterpret_cast<char *>(gcp_handle.read_buffer.get()),
				          gcp_handle.buffer_available);
			}
		}
	} else {
		// Direct read without buffering
		ReadRange(gcp_handle, location, buffer_ptr, nr_bytes);
		bytes_read = nr_bytes;
	}

	if (bytes_read != static_cast<idx_t>(nr_bytes)) {
		throw IOException("Failed to read complete data from GCS");
	}
}

int64_t GCSFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &gcp_handle = static_cast<GCSFileHandle &>(handle);
	Read(handle, buffer, nr_bytes, gcp_handle.file_offset);
	gcp_handle.file_offset += nr_bytes;
	return nr_bytes;
}

int64_t GCSFileSystem::GetFileSize(FileHandle &handle) {
	auto &gcp_handle = static_cast<GCSFileHandle &>(handle);
	return gcp_handle.length;
}

timestamp_t GCSFileSystem::GetLastModifiedTime(FileHandle &handle) {
	auto &gcp_handle = static_cast<GCSFileHandle &>(handle);
	return gcp_handle.last_modified;
}

void GCSFileSystem::Seek(FileHandle &handle, idx_t location) {
	auto &gcp_handle = static_cast<GCSFileHandle &>(handle);
	gcp_handle.file_offset = location;
}

void GCSFileSystem::FileSync(FileHandle &handle) {
	// No-op for read-only filesystem
}

bool GCSFileSystem::LoadFileInfo(GCSFileHandle &handle) {
	if (handle.length == 0) {
		LoadRemoteFileInfo(handle);
	}
	return handle.length > 0;
}

shared_ptr<GCPContextState> GCSFileSystem::GetOrCreateStorageContext(optional_ptr<FileOpener> opener,
                                                                     const string &path,
                                                                     const GCPParsedUrl &parsed_url) {
	// Simplified - just create a new context each time
	// In production, we'd cache these per bucket
	return CreateStorageContext(opener, path, parsed_url);
}

GCPReadOptions GCSFileSystem::ParseGCPReadOptions(optional_ptr<FileOpener> opener) {
	GCPReadOptions options;

	if (!opener) {
		return options;
	}

	Value value;
	if (FileOpener::TryGetCurrentSetting(opener, "gcp_transfer_concurrency", value)) {
		options.transfer_concurrency = value.GetValue<int32_t>();
	}
	if (FileOpener::TryGetCurrentSetting(opener, "gcp_transfer_chunk_size", value)) {
		options.transfer_chunk_size = value.GetValue<int64_t>();
	}
	if (FileOpener::TryGetCurrentSetting(opener, "gcp_buffer_size", value)) {
		options.buffer_size = value.GetValue<idx_t>();
	}

	return options;
}

// GCSFileSystem implementation
vector<OpenFileInfo> GCSFileSystem::Glob(const string &path, FileOpener *opener) {
	vector<OpenFileInfo> results;

	GCPParsedUrl parsed_url;
	parsed_url.ParseUrl(path);

	auto context = GetOrCreateStorageContext(opener, path, parsed_url);
	if (!context) {
		throw IOException("Failed to create GCS context for globbing");
	}

	auto &gcs_context = context->As<GCSContextState>();

	// Convert glob pattern to prefix and suffix
	std::string prefix = parsed_url.object_key;
	std::string pattern = "";

	// Find the first wildcard
	size_t wildcard_pos = prefix.find_first_of("*?[");
	if (wildcard_pos != std::string::npos) {
		// Split at the last directory separator before the wildcard
		size_t last_sep = prefix.rfind('/', wildcard_pos);
		if (last_sep != std::string::npos) {
			pattern = prefix.substr(last_sep + 1);
			prefix = prefix.substr(0, last_sep + 1);
		} else {
			pattern = prefix;
			prefix = "";
		}
	}

	// List objects with prefix
	try {
		// Add a timeout for the list operation
		auto list_request =
		    gcs_context.client.ListObjects(parsed_url.bucket, gcs::Prefix(prefix),
		                                   gcs::MaxResults(1000) // Limit results to prevent hanging on large buckets
		    );

		for (auto &&object_metadata : list_request) {
			if (!object_metadata) {
				auto status = object_metadata.status();

				if (status.code() == google::cloud::StatusCode::kUnauthenticated) {
					throw IOException("GCS Authentication failed. Please run: gcloud auth application-default login");
				} else if (status.code() == google::cloud::StatusCode::kPermissionDenied) {
					throw IOException("GCS Permission denied. Check bucket permissions for: " + parsed_url.bucket);
				} else if (status.code() == google::cloud::StatusCode::kNotFound) {
					throw IOException("GCS bucket not found: " + parsed_url.bucket);
				} else if (status.code() == google::cloud::StatusCode::kUnavailable ||
				           status.code() == google::cloud::StatusCode::kDeadlineExceeded) {
					throw IOException("GCS request timed out or service unavailable. Check network connectivity.");
				}

				// Log but continue for other errors
				continue;
			}

			std::string full_path = parsed_url.prefix + "://" + parsed_url.bucket + "/" + object_metadata->name();

			// If we have a pattern, check if the object name matches using proper glob matching
			if (!pattern.empty()) {
				std::string name_part = object_metadata->name().substr(prefix.length());
				// Use DuckDB's Glob function for proper pattern matching
				if (duckdb::Glob(name_part.c_str(), name_part.length(), pattern.c_str(), pattern.length())) {
					results.push_back(OpenFileInfo(full_path));
				}
			} else {
				results.push_back(OpenFileInfo(full_path));
			}
		}
	} catch (const std::exception &e) {
		throw IOException("Failed to list GCS objects. Error: " + std::string(e.what()) +
		                  "\nPlease check:\n"
		                  "  1. Authentication: gcloud auth application-default login\n"
		                  "  2. Bucket exists and is accessible: " +
		                  parsed_url.bucket +
		                  "\n"
		                  "  3. Network connectivity to Google Cloud Storage");
	}

	return results;
}

bool GCSFileSystem::FileExists(const std::string &filename, optional_ptr<FileOpener> opener) {
	GCPParsedUrl parsed_url;
	parsed_url.ParseUrl(filename);

	auto context = GetOrCreateStorageContext(opener, filename, parsed_url);
	if (!context) {
		return false;
	}

	auto &gcs_context = context->As<GCSContextState>();
	auto object_metadata = gcs_context.client.GetObjectMetadata(parsed_url.bucket, parsed_url.object_key);

	return object_metadata.ok();
}

duckdb::unique_ptr<GCSFileHandle> GCSFileSystem::CreateHandle(const OpenFileInfo &info, FileOpenFlags flags,
                                                              optional_ptr<FileOpener> opener) {
	GCPParsedUrl parsed_url;
	parsed_url.ParseUrl(info.path);

	auto context = GetOrCreateStorageContext(opener, info.path, parsed_url);
	if (!context) {
		throw IOException("Failed to create GCS context");
	}

	auto read_options = ParseGCPReadOptions(opener);
	auto handle = make_uniq<GCSFileHandle>(*this, info, flags, read_options, parsed_url.bucket, parsed_url.object_key,
	                                       context->As<GCSContextState>().client);

	// Load file metadata
	LoadFileInfo(*handle);

	return handle;
}

void GCSFileSystem::ReadRange(GCSFileHandle &handle, idx_t file_offset, char *buffer_out, idx_t buffer_out_len) {
	GCPParsedUrl parsed_url;
	parsed_url.bucket = handle.bucket;
	parsed_url.object_key = handle.object_key;

	// Read the specified range from GCS
	auto reader = handle.client.ReadObject(handle.bucket, handle.object_key,
	                                       gcs::ReadRange(file_offset, file_offset + buffer_out_len));

	if (!reader) {
		throw IOException("Failed to read from GCS: " + reader.status().message());
	}

	reader.read(buffer_out, buffer_out_len);
	if (!reader) {
		throw IOException("Failed to read data from GCS");
	}
}

const std::string &GCSFileSystem::GetContextPrefix() const {
	return context_prefix;
}

const SecretMatch LookupSecret(optional_ptr<FileOpener> opener, const std::string &path) {
	auto secret_manager = FileOpener::TryGetSecretManager(opener);
	auto transaction = FileOpener::TryGetCatalogTransaction(opener);
	return secret_manager->LookupSecret(*transaction, path, CreateGCSSecretFunctions::GetGCSSecretType());
}

shared_ptr<GCPContextState> GCSFileSystem::CreateStorageContext(optional_ptr<FileOpener> opener,
                                                                const std::string &path,
                                                                const GCPParsedUrl &parsed_url) {
	auto secret_match = LookupSecret(opener, path);
	if (secret_match.HasMatch()) {
		auto read_options = ParseGCPReadOptions(opener);
		auto &kv_secret = dynamic_cast<const KeyValueSecret &>(secret_match.GetSecret());
		auto provider = kv_secret.TryGetValue("provider");

		auto options = google::cloud::Options {};
		if (!ca_roots_path.empty()) {
			options.set<google::cloud::CARootsFilePathOption>(ca_roots_path);
		}

		if (provider == GCSSecretProvider::ACCESS_TOKEN) {
			auto access_token = kv_secret.TryGetValue("access_token");
			if (!access_token.IsNull() && !access_token.ToString().empty()) {
				auto credentials = google::cloud::MakeAccessTokenCredentials(
				    access_token.ToString(), std::chrono::system_clock::now() + std::chrono::hours(1));
				options.set<google::cloud::UnifiedCredentialsOption>(credentials);
				auto client = gcs::Client(options);
				return make_shared_ptr<GCSContextState>(client, read_options);
			}
		} else if (provider == GCSSecretProvider::SERVICE_ACCOUNT) {
			auto key_path = kv_secret.TryGetValue("service_account_key_path");
			if (!key_path.IsNull() && !key_path.ToString().empty()) {
				std::ifstream key_file(key_path.ToString());
				if (key_file.is_open()) {
					std::string json_contents((std::istreambuf_iterator<char>(key_file)),
					                          std::istreambuf_iterator<char>());
					auto credentials = google::cloud::MakeServiceAccountCredentials(json_contents);
					options.set<google::cloud::UnifiedCredentialsOption>(credentials);
					auto client = gcs::Client(options);
					return make_shared_ptr<GCSContextState>(client, read_options);
				}
			}
		} else if (provider == GCSSecretProvider::CREDENTIAL_CHAIN) {
			auto credentials = google::cloud::MakeGoogleDefaultCredentials();
			options.set<google::cloud::UnifiedCredentialsOption>(credentials);
			auto client = gcs::Client(options);
			return make_shared_ptr<GCSContextState>(client, read_options);
		}
	}

	// Provide helpful error message
	std::string error_msg = "No valid GCP credentials found.\n\n";
	error_msg += "The Google Cloud Storage extension requires authentication.\n";
	error_msg += "Please use one of these methods:\n\n";
	error_msg += "1. Set up Application Default Credentials:\n";
	error_msg += "   gcloud auth application-default login\n\n";
	error_msg += "   Note: Regular 'gcloud auth login' is NOT sufficient.\n";
	error_msg += "   You must use 'gcloud auth application-default login'.\n\n";
	error_msg += "2. Use a service account key file:\n";
	error_msg += "   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json\n\n";
	error_msg += "3. Run in a GCP environment with default service account\n\n";
	error_msg += "4. Create a secret in secret manager, one of:\n";
	error_msg += "   duckdb> CREATE SECRET gcp (TYPE gcp, PROVIDER credential_chain);\n";
	error_msg += "   duckdb> CREATE SECRET gcp (TYPE gcp, PROVIDER service_account, service_account_key_path = "
	             "'/path/to/key.json');\n";
	error_msg +=
	    "   duckdb> CREATE SECRET gcp (TYPE gcp, PROVIDER access_token, access_token = 'your_access_token');\n\n";

	throw IOException(error_msg);
}

void GCSFileSystem::LoadRemoteFileInfo(GCSFileHandle &handle) {
	GCPParsedUrl parsed_url;
	parsed_url.bucket = handle.bucket;
	parsed_url.object_key = handle.object_key;

	auto object_metadata = handle.client.GetObjectMetadata(handle.bucket, handle.object_key);
	if (!object_metadata) {
		throw IOException("Failed to get object metadata: " + object_metadata.status().message());
	}

	handle.length = object_metadata->size();

	// Convert time_point to time_t
	auto time_point = object_metadata->updated();
	auto duration = time_point.time_since_epoch();
	handle.last_modified = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
}

} // namespace duckdb
