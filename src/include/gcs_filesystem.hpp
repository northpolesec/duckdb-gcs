#pragma once

#include <google/cloud/storage/client.h>

#include "gcs_parsed_url.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context_state.hpp"
#include <ctime>
#include <cstdint>

namespace duckdb {

struct GCPReadOptions {
	int32_t transfer_concurrency = 5;
	int64_t transfer_chunk_size = 1 * 1024 * 1024;
	idx_t buffer_size = 1 * 1024 * 1024;
};

class GCPContextState : public ClientContextState {
public:
	const GCPReadOptions read_options;

public:
	virtual bool IsValid() const;
	void QueryEnd() override;

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
	GCPContextState(const GCPReadOptions &read_options);

protected:
	bool is_valid;
};

class GCSFileSystem;

class GCSContextState : public GCPContextState {
public:
	GCSContextState(const google::cloud::storage::Client &client, const GCPReadOptions &read_options)
	    : GCPContextState(read_options), client(client) {
	}

	google::cloud::storage::Client client;
};

class GCSFileHandle : public FileHandle {
public:
	GCSFileHandle(GCSFileSystem &fs, const OpenFileInfo &info, FileOpenFlags flags, const GCPReadOptions &read_options,
	              const std::string &bucket, const std::string &object_key, google::cloud::storage::Client client);

	virtual bool PostConstruct();
	void Close() override {
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

	const GCPReadOptions read_options;

	// GCS-specific fields
	std::string bucket;
	std::string object_key;
	google::cloud::storage::Client client;
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
	shared_ptr<GCPContextState> GetOrCreateStorageContext(optional_ptr<FileOpener> opener, const string &path,
	                                                      const GCPParsedUrl &parsed_url);
	shared_ptr<GCPContextState> CreateStorageContext(optional_ptr<FileOpener> opener, const std::string &path,
	                                                 const GCPParsedUrl &parsed_url);

	void LoadRemoteFileInfo(GCSFileHandle &handle);
	static GCPReadOptions ParseGCPReadOptions(optional_ptr<FileOpener> opener);

private:
	std::string context_prefix = "gcs_context_";

	std::string ca_roots_path;
};

} // namespace duckdb
