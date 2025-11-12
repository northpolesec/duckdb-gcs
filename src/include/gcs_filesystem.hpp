#pragma once

#include <google/cloud/storage/client.h>
#include <google/cloud/storage/object_metadata.h>
#include <unordered_map>
#include <mutex>
#include <chrono>
#include <optional>

#include "gcs_parsed_url.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context_state.hpp"
#include <cstddef>
#include <ctime>
#include <cstdint>

namespace duckdb {

namespace gcs = ::google::cloud::storage;

struct GCSReadOptions {
	int32_t transfer_concurrency = 5;
	int64_t transfer_chunk_size = static_cast<int64_t>(1 * 1024 * 1024);

	idx_t buffer_size = static_cast<idx_t>(1 * 1024 * 1024);

	int32_t metadata_cache_ttl_seconds = 300;
	int32_t list_cache_ttl_seconds = 60;
	bool enable_caches = true;

	// Cache size limits to prevent unbounded memory growth
	idx_t max_metadata_cache_entries = 10000;
	idx_t max_list_cache_entries = 1000;
};

struct MetadataCacheEntry {
	gcs::ObjectMetadata metadata;
	std::chrono::steady_clock::time_point cached_at;
	mutable std::chrono::steady_clock::time_point last_accessed;
};

struct ListCacheEntry {
	vector<OpenFileInfo> results;
	std::chrono::steady_clock::time_point cached_at;
	mutable std::chrono::steady_clock::time_point last_accessed;
};

class GCSFileSystem;

class GCSContextState : public ClientContextState {
public:
	GCSContextState(gcs::Client client, const GCSReadOptions &read_options)
	    : read_options(read_options), client(std::move(client)) {
	}

	void QueryEnd() override;

	std::optional<gcs::ObjectMetadata> GetCachedMetadata(const std::string &bucket, const std::string &object_key);
	void SetCachedMetadata(const std::string &bucket, const std::string &object_key,
	                       const gcs::ObjectMetadata &metadata);
	std::optional<vector<OpenFileInfo>> GetCachedList(const std::string &bucket, const std::string &prefix);
	void SetCachedList(const std::string &bucket, const std::string &prefix, const vector<OpenFileInfo> &results);

	inline std::string MakeMetadataKey(const std::string &bucket, const std::string &object_key) {
		return "metadata:" + bucket + ":" + object_key;
	}
	inline std::string MakeListKey(const std::string &bucket, const std::string &prefix) {
		return "list:" + bucket + ":" + prefix;
	}

	inline gcs::Client GetClient() {
		return client;
	}

	template <class TARGET>
	TARGET &As() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &As() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}

protected:
	const GCSReadOptions read_options;

	gcs::Client client;

private:
	std::mutex cache_mutex;
	std::unordered_map<std::string, MetadataCacheEntry> metadata_cache;
	std::unordered_map<std::string, ListCacheEntry> list_cache;

	void EvictLRUMetadataEntryLocked();
	void EvictLRUListEntryLocked();
};

class GCSFileHandle : public FileHandle {
public:
	GCSFileHandle(GCSFileSystem &fs, const OpenFileInfo &info, FileOpenFlags flags, const GCSReadOptions &read_options,
	              const std::string &bucket, const std::string &object_key, shared_ptr<GCSContextState> context);

	bool PostConstruct();
	void TryAddLogger(FileOpener &opener);
	void Close() override {
		// No explicit cleanup needed.
	}

	inline gcs::Client GetClient() {
		return context->GetClient();
	}

	FileOpenFlags flags;

	// File info
	idx_t length;
	timestamp_t last_modified;

	// Read buffer
	duckdb::unique_ptr<data_t[]> read_buffer;
	// Read info
	idx_t buffer_available;
	idx_t buffer_idx;
	idx_t file_offset;
	idx_t buffer_start;
	idx_t buffer_end;

	const GCSReadOptions read_options;

	// GCS-specific fields
	std::string bucket;
	std::string object_key;
	// Context is owned by the ClientContext's registered_state, not by this handle.
	// This prevents circular references since the context never holds references to handles.
	shared_ptr<GCSContextState> context;
};

class GCSFileSystem : public FileSystem {
public:
	GCSFileSystem(std::string ca_roots_path) : ca_roots_path(ca_roots_path) {
	}
	~GCSFileSystem() override = default;

	std::string GetName() const override {
		return "GCSFileSystem";
	}

	bool CanHandleFile(const string &path) override {
		return path.rfind("gs://", 0) == 0 || path.rfind("gcs://", 0) == 0 || path.rfind("gcss://", 0) == 0;
	}

	// FS methods
	duckdb::unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener = nullptr) override;

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	bool CanSeek() override {
		return true;
	}
	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}
	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		return false;
	}
	int64_t GetFileSize(FileHandle &handle) override;
	timestamp_t GetLastModifiedTime(FileHandle &handle) override;
	void Seek(FileHandle &handle, idx_t location) override;
	void FileSync(FileHandle &handle) override;

	bool LoadFileInfo(GCSFileHandle &handle);

	string PathSeparator(const string &path) override {
		return "/";
	}

	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;
	bool FileExists(const std::string &filename, optional_ptr<FileOpener> opener = nullptr) override;

protected:
	unique_ptr<FileHandle> OpenFileExtended(const OpenFileInfo &info, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener) override;

	bool SupportsOpenFileExtended() const override {
		return true;
	}

	duckdb::unique_ptr<GCSFileHandle> CreateHandle(const OpenFileInfo &info, FileOpenFlags flags,
	                                               optional_ptr<FileOpener> opener);
	void ReadRange(GCSFileHandle &handle, idx_t file_offset, char *buffer_out, idx_t buffer_out_len);

	const std::string &GetContextPrefix() const;
	shared_ptr<GCSContextState> GetOrCreateStorageContext(optional_ptr<FileOpener> opener, const string &path,
	                                                      const GCSParsedUrl &parsed_url);
	shared_ptr<GCSContextState> CreateStorageContext(optional_ptr<FileOpener> opener, const std::string &path,
	                                                 const GCSParsedUrl &parsed_url);

	void LoadRemoteFileInfo(GCSFileHandle &handle);
	static GCSReadOptions ParseGCSReadOptions(optional_ptr<FileOpener> opener);

private:
	std::string context_prefix = "gcs_context_";

	std::string ca_roots_path;
};

// Helper function to safely calculate max read size, preventing integer overflow
int64_t SafeMaxRead(idx_t offset, idx_t length);

} // namespace duckdb
