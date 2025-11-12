#include "gcs_filesystem.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "gcs_secret.hpp"

#include <atomic>
#include <cstddef>
#include <fstream>
#include <future>
#include <mutex>
#include <optional>
#include <thread>

namespace duckdb {

namespace gcs = ::google::cloud::storage;

google::cloud::Options BuildOptimizedClientOptions(std::shared_ptr<google::cloud::Credentials> credentials,
                                                   const std::string &ca_roots_path,
                                                   const GCSReadOptions &read_options) {
	auto options = google::cloud::Options {};
	options.set<google::cloud::UnifiedCredentialsOption>(std::move(credentials));
	options.set<gcs::DownloadBufferSizeOption>(read_options.buffer_size);
	options.set<gcs::RetryPolicyOption>(gcs::LimitedErrorCountRetryPolicy(3).clone());
	options.set<gcs::BackoffPolicyOption>(
	    google::cloud::ExponentialBackoffPolicy(std::chrono::milliseconds(100), std::chrono::seconds(5), 2.0).clone());

	if (!ca_roots_path.empty()) {
		options.set<google::cloud::CARootsFilePathOption>(ca_roots_path);
	}

	return options;
}

void GCSContextState::QueryEnd() {
}

std::optional<gcs::ObjectMetadata> GCSContextState::GetCachedMetadata(const std::string &bucket,
                                                                      const std::string &object_key) {
	if (!read_options.enable_caches) {
		return std::nullopt;
	}

	auto key = MakeMetadataKey(bucket, object_key);

	std::scoped_lock lock(cache_mutex);
	auto it = metadata_cache.find(key);
	if (it == metadata_cache.end()) {
		return std::nullopt;
	}

	auto age = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - it->second.cached_at)
	               .count();
	if (age > read_options.metadata_cache_ttl_seconds) {
		metadata_cache.erase(it);
		return std::nullopt;
	}

	// Update last accessed time for LRU tracking
	it->second.last_accessed = std::chrono::steady_clock::now();
	return it->second.metadata;
}

void GCSContextState::SetCachedMetadata(const std::string &bucket, const std::string &object_key,
                                        const gcs::ObjectMetadata &metadata) {
	if (!read_options.enable_caches) {
		return;
	}

	auto key = MakeMetadataKey(bucket, object_key);

	std::scoped_lock lock(cache_mutex);

	// Check if we need to evict before inserting
	if (metadata_cache.size() >= read_options.max_metadata_cache_entries &&
	    metadata_cache.find(key) == metadata_cache.end()) {
		EvictLRUMetadataEntryLocked();
	}

	auto now = std::chrono::steady_clock::now();
	metadata_cache[key] = {metadata, now, now};
}

std::optional<vector<OpenFileInfo>> GCSContextState::GetCachedList(const std::string &bucket,
                                                                   const std::string &prefix) {
	if (!read_options.enable_caches) {
		return std::nullopt;
	}

	auto key = MakeListKey(bucket, prefix);

	std::scoped_lock lock(cache_mutex);
	auto it = list_cache.find(key);
	if (it == list_cache.end()) {
		return std::nullopt;
	}

	auto age = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - it->second.cached_at)
	               .count();
	if (age > read_options.list_cache_ttl_seconds) {
		it = list_cache.erase(it);
		return std::nullopt;
	}

	// Update last accessed time for LRU tracking
	it->second.last_accessed = std::chrono::steady_clock::now();
	return it->second.results;
}

void GCSContextState::SetCachedList(const std::string &bucket, const std::string &prefix,
                                    const vector<OpenFileInfo> &results) {
	if (!read_options.enable_caches) {
		return;
	}

	auto key = MakeListKey(bucket, prefix);

	std::scoped_lock lock(cache_mutex);

	// Check if we need to evict before inserting
	if (list_cache.size() >= read_options.max_list_cache_entries && list_cache.find(key) == list_cache.end()) {
		EvictLRUListEntryLocked();
	}

	auto now = std::chrono::steady_clock::now();
	list_cache[key] = {results, now, now};
}

void GCSContextState::EvictLRUMetadataEntryLocked() {
	if (metadata_cache.empty()) {
		return;
	}

	// Find the least recently accessed entry
	auto lru_it = metadata_cache.begin();
	auto oldest_time = lru_it->second.last_accessed;

	for (auto it = metadata_cache.begin(); it != metadata_cache.end(); ++it) {
		if (it->second.last_accessed < oldest_time) {
			oldest_time = it->second.last_accessed;
			lru_it = it;
		}
	}

	metadata_cache.erase(lru_it);
}

void GCSContextState::EvictLRUListEntryLocked() {
	if (list_cache.empty()) {
		return;
	}

	// Find the least recently accessed entry
	auto lru_it = list_cache.begin();
	auto oldest_time = lru_it->second.last_accessed;

	for (auto it = list_cache.begin(); it != list_cache.end(); ++it) {
		if (it->second.last_accessed < oldest_time) {
			oldest_time = it->second.last_accessed;
			lru_it = it;
		}
	}

	list_cache.erase(lru_it);
}

// GCSFileHandle implementation
GCSFileHandle::GCSFileHandle(GCSFileSystem &fs, const OpenFileInfo &info, FileOpenFlags flags,
                             const GCSReadOptions &read_options, const std::string &bucket,
                             const std::string &object_key, shared_ptr<GCSContextState> context)
    : FileHandle(fs, info.path, flags), flags(flags), length(0), last_modified(0), buffer_available(0), buffer_idx(0),
      file_offset(0), buffer_start(0), buffer_end(0), read_options(read_options), bucket(bucket),
      object_key(object_key), context(std::move(context)) {

	if (!flags.RequireParallelAccess()) {
		read_buffer = duckdb::unique_ptr<data_t[]>(new data_t[read_options.buffer_size]);
	}
}

bool GCSFileHandle::PostConstruct() {
	return true;
}

void GCSFileHandle::TryAddLogger(FileOpener &opener) {
	auto context = opener.TryGetClientContext();
	if (context) {
		logger = context->logger;
	}
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
	auto &hfh = handle.Cast<GCSFileHandle>();

	idx_t to_read = nr_bytes;
	idx_t buffer_offset = 0;

	// Don't buffer when DirectIO is set.
	if (hfh.flags.DirectIO() || hfh.flags.RequireParallelAccess()) {
		if (to_read == 0) {
			return;
		}
		ReadRange(hfh, location, (char *)buffer, to_read);
		hfh.buffer_available = 0;
		hfh.buffer_idx = 0;
		hfh.file_offset = location + nr_bytes;
		return;
	}

	if (location >= hfh.buffer_start && location < hfh.buffer_end) {
		hfh.file_offset = location;
		hfh.buffer_idx = location - hfh.buffer_start;
		hfh.buffer_available = (hfh.buffer_end - hfh.buffer_start) - hfh.buffer_idx;
	} else {
		// reset buffer
		hfh.buffer_available = 0;
		hfh.buffer_idx = 0;
		hfh.file_offset = location;
	}
	while (to_read > 0) {
		auto buffer_read_len = MinValue<idx_t>(hfh.buffer_available, to_read);
		if (buffer_read_len > 0) {
			D_ASSERT(hfh.buffer_start + hfh.buffer_idx + buffer_read_len <= hfh.buffer_end);
			memcpy((char *)buffer + buffer_offset, hfh.read_buffer.get() + hfh.buffer_idx, buffer_read_len);

			buffer_offset += buffer_read_len;
			to_read -= buffer_read_len;

			hfh.buffer_idx += buffer_read_len;
			hfh.buffer_available -= buffer_read_len;
			hfh.file_offset += buffer_read_len;
		}

		if (to_read > 0 && hfh.buffer_available == 0) {
			auto new_buffer_available = MinValue<idx_t>(hfh.read_options.buffer_size, hfh.length - hfh.file_offset);

			// Bypass buffer if we read more than buffer size
			if (to_read > new_buffer_available) {
				ReadRange(hfh, location + buffer_offset, (char *)buffer + buffer_offset, to_read);
				hfh.buffer_available = 0;
				hfh.buffer_idx = 0;
				hfh.file_offset += to_read;
				break;
			} else {
				ReadRange(hfh, hfh.file_offset, (char *)hfh.read_buffer.get(), new_buffer_available);
				hfh.buffer_available = new_buffer_available;
				hfh.buffer_idx = 0;
				hfh.buffer_start = hfh.file_offset;
				hfh.buffer_end = hfh.buffer_start + new_buffer_available;
			}
		}
	}
}

int64_t SafeMaxRead(idx_t offset, idx_t length) {
	if (offset >= length) {
		// Already at or past EOF
		return 0;
	}

	// Compute difference in unsigned space to avoid overflow
	idx_t unsigned_diff = length - offset;
	// Check if the difference exceeds the maximum signed 64-bit value
	if (unsigned_diff > static_cast<idx_t>(NumericLimits<int64_t>::Maximum())) {
		return NumericLimits<int64_t>::Maximum();
	} else {
		return static_cast<int64_t>(unsigned_diff);
	}
}

int64_t GCSFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &gcp_handle = handle.Cast<GCSFileHandle>();

	// Calculate max_read safely to avoid integer overflow/underflow
	int64_t max_read = SafeMaxRead(gcp_handle.file_offset, gcp_handle.length);

	nr_bytes = MinValue<int64_t>(max_read, nr_bytes);
	Read(handle, buffer, nr_bytes, gcp_handle.file_offset);
	gcp_handle.file_offset += nr_bytes;
	return nr_bytes;
}

int64_t GCSFileSystem::GetFileSize(FileHandle &handle) {
	auto &gcp_handle = handle.Cast<GCSFileHandle>();
	return gcp_handle.length;
}

timestamp_t GCSFileSystem::GetLastModifiedTime(FileHandle &handle) {
	auto &gcp_handle = handle.Cast<GCSFileHandle>();
	return gcp_handle.last_modified;
}

void GCSFileSystem::Seek(FileHandle &handle, idx_t location) {
	auto &gcp_handle = handle.Cast<GCSFileHandle>();
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

shared_ptr<GCSContextState> GCSFileSystem::GetOrCreateStorageContext(optional_ptr<FileOpener> opener,
                                                                     const string &path,
                                                                     const GCSParsedUrl &parsed_url) {
	auto client_context = FileOpener::TryGetClientContext(opener);
	if (!client_context) {
		return nullptr;
	}

	auto context_key = GetContextPrefix() + parsed_url.bucket;
	auto &registered_state = client_context->registered_state;
	auto result = registered_state->Get<GCSContextState>(context_key);
	if (!result) {
		result = CreateStorageContext(opener, path, parsed_url);
		registered_state->Insert(context_key, result);
	}

	return result;
}

GCSReadOptions GCSFileSystem::ParseGCSReadOptions(optional_ptr<FileOpener> opener) {
	GCSReadOptions options;

	if (!opener) {
		return options;
	}

	Value value;
	if (FileOpener::TryGetCurrentSetting(opener, "gcs_metadata_cache_ttl", value)) {
		auto ttl = value.GetValue<int32_t>();
		if (ttl < 0) {
			throw InvalidInputException("gcs_metadata_cache_ttl must be non-negative (got %d)", ttl);
		}
		options.metadata_cache_ttl_seconds = ttl;
	}
	if (FileOpener::TryGetCurrentSetting(opener, "gcs_list_cache_ttl", value)) {
		auto ttl = value.GetValue<int32_t>();
		if (ttl < 0) {
			throw InvalidInputException("gcs_list_cache_ttl must be non-negative (got %d)", ttl);
		}
		options.list_cache_ttl_seconds = ttl;
	}
	if (FileOpener::TryGetCurrentSetting(opener, "gcs_enable_metadata_cache", value)) {
		options.enable_caches = value.GetValue<bool>();
	}
	if (FileOpener::TryGetCurrentSetting(opener, "gcs_max_metadata_cache_entries", value)) {
		auto max_entries = value.GetValue<idx_t>();
		if (max_entries < 10) {
			throw InvalidInputException("gcs_max_metadata_cache_entries must be at least 10 (got %llu)", max_entries);
		}
		if (max_entries > 10000000) {
			throw InvalidInputException("gcs_max_metadata_cache_entries cannot exceed 10,000,000 (got %llu)",
			                            max_entries);
		}
		options.max_metadata_cache_entries = max_entries;
	}
	if (FileOpener::TryGetCurrentSetting(opener, "gcs_max_list_cache_entries", value)) {
		auto max_entries = value.GetValue<idx_t>();
		if (max_entries < 10) {
			throw InvalidInputException("gcs_max_list_cache_entries must be at least 10 (got %llu)", max_entries);
		}
		if (max_entries > 1000000) {
			throw InvalidInputException("gcs_max_list_cache_entries cannot exceed 1,000,000 (got %llu)", max_entries);
		}
		options.max_list_cache_entries = max_entries;
	}
	if (FileOpener::TryGetCurrentSetting(opener, "gcs_transfer_concurrency", value)) {
		auto concurrency = value.GetValue<int32_t>();
		if (concurrency < 1) {
			throw InvalidInputException("gcs_transfer_concurrency must be at least 1 (got %d)", concurrency);
		}
		if (concurrency > 1000) {
			throw InvalidInputException("gcs_transfer_concurrency cannot exceed 1000 (got %d)", concurrency);
		}
		options.transfer_concurrency = concurrency;
	}

	return options;
}

// GCSFileSystem implementation
vector<OpenFileInfo> GCSFileSystem::Glob(const string &path, FileOpener *opener) {
	vector<OpenFileInfo> results;

	GCSParsedUrl parsed_url;
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

	// Check cache first
	auto cached_results = gcs_context.GetCachedList(parsed_url.bucket, prefix);
	if (cached_results.has_value()) {
		if (pattern.empty()) {
			return cached_results.value();
		}

		// Filter cached results by pattern if needed
		for (const auto &info : cached_results.value()) {
			// Extract object name from full path
			std::string full_path = info.path;
			size_t bucket_pos = full_path.find("://");

			if (bucket_pos == std::string::npos) {
				continue;
			}

			size_t path_start = full_path.find('/', bucket_pos + 3);

			if (path_start == std::string::npos) {
				continue;
			}

			std::string name_part = full_path.substr(path_start + 1);
			if (name_part.length() < prefix.length()) {
				continue;
			}

			name_part = name_part.substr(prefix.length());
			if (duckdb::Glob(name_part.c_str(), name_part.length(), pattern.c_str(), pattern.length())) {
				results.push_back(info);
			}
		}
		if (!results.empty()) {
			return results;
		}
	}

	// List objects with prefix
	try {
		// TODO: Add pagination if the number of results is > the max specified here
		auto list_request = gcs_context.GetClient().ListObjects(
		    parsed_url.bucket, gcs::Prefix(prefix),
		    gcs::MaxResults(10000) // Limit results to prevent hanging on large buckets
		);

		for (auto &&object_metadata : list_request) {
			if (!object_metadata) {
				auto &status = object_metadata.status();

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

		// Cache the results
		gcs_context.SetCachedList(parsed_url.bucket, prefix, results);
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
	GCSParsedUrl parsed_url;
	parsed_url.ParseUrl(filename);

	auto context = GetOrCreateStorageContext(opener, filename, parsed_url);
	if (!context) {
		return false;
	}

	auto &gcs_context = context->As<GCSContextState>();
	auto object_metadata = gcs_context.GetClient().GetObjectMetadata(parsed_url.bucket, parsed_url.object_key);

	return object_metadata.ok();
}

duckdb::unique_ptr<GCSFileHandle> GCSFileSystem::CreateHandle(const OpenFileInfo &info, FileOpenFlags flags,
                                                              optional_ptr<FileOpener> opener) {
	GCSParsedUrl parsed_url;
	parsed_url.ParseUrl(info.path);

	auto context = GetOrCreateStorageContext(opener, info.path, parsed_url);
	if (!context) {
		throw IOException("Failed to create GCS context");
	}

	auto read_options = ParseGCSReadOptions(opener);
	auto handle =
	    make_uniq<GCSFileHandle>(*this, info, flags, read_options, parsed_url.bucket, parsed_url.object_key, context);
	handle->TryAddLogger(*opener);

	// Load file metadata
	LoadFileInfo(*handle);

	return handle;
}

void GCSFileSystem::ReadRange(GCSFileHandle &handle, idx_t file_offset, char *buffer_out, idx_t buffer_out_len) {
	auto opts = handle.read_options;

	idx_t parallel_threshold = opts.buffer_size * 2;
	bool use_parallel = buffer_out_len > parallel_threshold && opts.transfer_concurrency > 1;

	if (!use_parallel) {
		// Single-threaded read for small reads
		auto reader = handle.GetClient().ReadObject(handle.bucket, handle.object_key,
		                                            gcs::ReadRange(file_offset, file_offset + buffer_out_len));
		if (!reader) {
			throw IOException("Failed to read from GCS: " + reader.status().message());
		}

		reader.read(buffer_out, buffer_out_len);
		if (!reader) {
			throw IOException("Failed to read data from GCS");
		}
		return;
	}

	// Parallel chunked read
	idx_t chunk_size = std::max(opts.transfer_chunk_size, static_cast<int64_t>(opts.buffer_size));
	idx_t num_chunks = (buffer_out_len + chunk_size - 1) / chunk_size;
	idx_t max_concurrent = std::min(static_cast<idx_t>(opts.transfer_concurrency), num_chunks);

	// Shared flag to signal when an error has occurred, allowing early termination
	std::atomic<bool> error_occurred {false};

	// Structure to hold chunk information
	struct ChunkInfo {
		idx_t chunk_offset;
		idx_t chunk_size;
		char *chunk_buffer;
	};

	std::vector<ChunkInfo> chunks(num_chunks);
	for (idx_t i = 0; i < num_chunks; i++) {
		chunks[i].chunk_offset = file_offset + i * chunk_size;
		chunks[i].chunk_size = std::min(chunk_size, buffer_out_len - i * chunk_size);
		chunks[i].chunk_buffer = buffer_out + i * chunk_size;
	}

	// Launch async tasks respecting max_concurrent limit
	std::vector<std::future<void>> futures;
	futures.reserve(max_concurrent);
	idx_t next_chunk = 0;

	// Helper to launch a chunk read
	auto launch_chunk = [&](idx_t chunk_idx) -> std::future<void> {
		const auto &chunk = chunks[chunk_idx];
		return std::async(std::launch::async, [&handle, chunk, &error_occurred]() {
			// Check flag before doing expensive network operation
			if (error_occurred.load(std::memory_order_acquire)) {
				return;
			}

			auto reader = handle.GetClient().ReadObject(
			    handle.bucket, handle.object_key,
			    gcs::ReadRange(chunk.chunk_offset, chunk.chunk_offset + chunk.chunk_size));

			if (!reader) {
				error_occurred.store(true, std::memory_order_release);
				throw IOException("Failed to read chunk from GCS: " + reader.status().message());
			}

			// Check flag again before reading data
			if (error_occurred.load(std::memory_order_acquire)) {
				return;
			}

			reader.read(chunk.chunk_buffer, chunk.chunk_size);
			if (!reader) {
				error_occurred.store(true, std::memory_order_release);
				throw IOException("Failed to read chunk data from GCS");
			}
		});
	};

	// Launch initial batch of futures up to max_concurrent
	while (next_chunk < num_chunks && futures.size() < max_concurrent) {
		if (error_occurred.load(std::memory_order_acquire)) {
			break;
		}
		futures.push_back(launch_chunk(next_chunk++));
	}

	// Process futures as they complete, launching new ones to maintain max_concurrent
	while (!futures.empty()) {
		// Wait for at least one future to complete
		for (auto it = futures.begin(); it != futures.end();) {
			if (it->wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
				// This will throw if the task encountered an exception
				it->get();
				it = futures.erase(it);

				// Launch next chunk if available and no error occurred
				if (next_chunk < num_chunks && !error_occurred.load(std::memory_order_acquire)) {
					futures.push_back(launch_chunk(next_chunk++));
				}
			} else {
				++it;
			}
		}

		// Small sleep to avoid busy-waiting
		if (!futures.empty()) {
			std::this_thread::sleep_for(std::chrono::microseconds(100));
		}
	}
}

const std::string &GCSFileSystem::GetContextPrefix() const {
	return context_prefix;
}

const SecretMatch LookupSecret(optional_ptr<FileOpener> opener, const std::string &path) {
	if (!opener) {
		return SecretMatch();
	}
	auto secret_manager = FileOpener::TryGetSecretManager(opener);
	auto transaction = FileOpener::TryGetCatalogTransaction(opener);
	if (!secret_manager || !transaction) {
		return SecretMatch();
	}
	return secret_manager->LookupSecret(*transaction, path, CreateGCSSecretFunctions::GetGCSSecretType());
}

shared_ptr<GCSContextState> GCSFileSystem::CreateStorageContext(optional_ptr<FileOpener> opener,
                                                                const std::string &path,
                                                                const GCSParsedUrl &parsed_url) {
	auto secret_match = LookupSecret(opener, path);
	if (secret_match.HasMatch()) {
		auto read_options = ParseGCSReadOptions(opener);
		auto &kv_secret = dynamic_cast<const KeyValueSecret &>(secret_match.GetSecret());
		auto provider = kv_secret.TryGetValue("provider");

		if (provider == GCSSecretProvider::ACCESS_TOKEN) {
			auto access_token = kv_secret.TryGetValue("access_token");
			if (!access_token.IsNull() && !access_token.ToString().empty()) {
				auto credentials = google::cloud::MakeAccessTokenCredentials(
				    access_token.ToString(), std::chrono::system_clock::now() + std::chrono::hours(1));
				auto options = BuildOptimizedClientOptions(credentials, ca_roots_path, read_options);
				auto client = gcs::Client(options);
				return make_shared_ptr<GCSContextState>(std::move(client), read_options);
			}
		} else if (provider == GCSSecretProvider::SERVICE_ACCOUNT) {
			auto key_path = kv_secret.TryGetValue("service_account_key_path");
			if (!key_path.IsNull() && !key_path.ToString().empty()) {
				std::ifstream key_file(key_path.ToString());
				if (key_file.is_open()) {
					std::string json_contents((std::istreambuf_iterator<char>(key_file)),
					                          std::istreambuf_iterator<char>());
					auto credentials = google::cloud::MakeServiceAccountCredentials(json_contents);
					auto options = BuildOptimizedClientOptions(credentials, ca_roots_path, read_options);
					auto client = gcs::Client(options);
					return make_shared_ptr<GCSContextState>(std::move(client), read_options);
				}
			}
		} else if (provider == GCSSecretProvider::CREDENTIAL_CHAIN) {
			auto credentials = google::cloud::MakeGoogleDefaultCredentials();
			auto options = BuildOptimizedClientOptions(credentials, ca_roots_path, read_options);
			auto client = gcs::Client(options);
			return make_shared_ptr<GCSContextState>(std::move(client), read_options);
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
	auto &gcs_context = handle.context->As<GCSContextState>();

	// Check cache first
	auto cached_metadata = gcs_context.GetCachedMetadata(handle.bucket, handle.object_key);
	if (cached_metadata.has_value()) {
		handle.length = cached_metadata->size();
		auto time_point = cached_metadata->updated();
		auto duration = time_point.time_since_epoch();
		handle.last_modified = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
		return;
	}

	auto object_metadata = gcs_context.GetClient().GetObjectMetadata(handle.bucket, handle.object_key);
	if (!object_metadata) {
		throw IOException("Failed to get object metadata: " + object_metadata.status().message());
	}

	// Cache the metadata
	gcs_context.SetCachedMetadata(handle.bucket, handle.object_key, *object_metadata);

	handle.length = object_metadata->size();

	// Convert time_point to time_t
	auto time_point = object_metadata->updated();
	auto duration = time_point.time_since_epoch();
	handle.last_modified = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
}

} // namespace duckdb
