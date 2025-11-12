# duckdb-gcs

---

This DuckDB extension allows you to read files from Google Cloud Storage,
natively, using the Google Cloud C++ SDK.

DuckDB's core httpfs extension allows reading files from GCS, but it does so
using the S3 compatibility API. This is not as fast as the native API and it
requires using HMAC keys. HMAC keys are, by default, disabled by organization
policy for service accounts.

Note: Because the core `httpfs` extension registers itself as a handler for
`gs://` and `gcs://` URLs, this extension also supports `gcss://` as a way to
force its usage.

## Building

### Requirements

- CMake 3.5+
- C++14 compiler
- vcpkg (C++ package manager)
- Google Cloud SDK (`gcloud` CLI) - optional, only needed for testing with real GCS buckets

### Checkout

```bash
# Clone the repository with submodules (REQUIRED)
git clone --recurse-submodules https://github.com/northpolesec/duckdb-gcs.git
cd duckdb-gcs
```

```bash
# If you already cloned without submodules, initialize them:
# git submodule update --init --recursive
```

**Note:** The `--recurse-submodules` flag is essential. This project requires:

- DuckDB source code (via submodule)
- Extension CI tools (via submodule)

### Managing dependencies

DuckDB extensions uses VCPKG for dependency management. Enabling VCPKG is very
simple: follow the
[installation instructions](https://vcpkg.io/en/getting-started) or just run the
following:

```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```

### Build steps

Now to build the extension, run:

```sh
make
```

The main binaries that will be built are:

```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/gcs/gcs.duckdb_extension
```

- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `gcs.duckdb_extension` is the loadable binary as it would be distributed.

### Tests

The extension has unit tests and a very basic DuckDB integration test.

#### DuckDB Integration Test

DuckDB extensions, by default, have a test that launches DuckDB and runs queries
to see if the extension is working properly. To run this test:

```bash
make test
```

#### Unit Tests

The extension also has extensive unit tests using mocks for GCS itself, to ensure
globbing, caching, threading, etc. are working correctly.

To run the unit tests:

```
make unittest
```

The unit test target doesn't run the benchmark tests by default. To run the benchmarks:

```
make benchmark
```

### Quick Start

```bash
# 1. Set up authentication
gcloud auth application-default login

# 2. Test the extension
./build/release/duckdb -c "
  LOAD 'build/release/extension/gcs/gcs.duckdb_extension';
  SELECT * FROM read_parquet('gs://your-bucket/file.parquet') LIMIT 5;
"
```

## Usage

### Loading the Extension

```sql
-- Load the extension (for development/testing)
LOAD 'build/release/extension/gcs/gcs.duckdb_extension';

-- Or if installed
INSTALL gcs;
LOAD gcs;
```

### Authentication

DuckDB's secret management provides secure, scoped authentication with multiple
provider options:

```sql
-- Load the extension
LOAD gcs;

-- Option 1: Credential Chain (uses ADC, service accounts, etc.)
CREATE SECRET my_gcp_secret (
    TYPE GCP,
    PROVIDER credential_chain,
    PROJECT_ID 'your-project-id'  -- optional
);

-- Option 2: Service Account Key File
CREATE SECRET my_service_account (
    TYPE GCP,
    PROVIDER service_account,
    SERVICE_ACCOUNT_KEY_PATH '/path/to/service-account.json',
    PROJECT_ID 'your-project-id'  -- optional
);

-- Option 3: Access Token (for programmatic access)
CREATE SECRET my_access_token (
    TYPE GCP,
    PROVIDER access_token,
    ACCESS_TOKEN 'ya29.your_access_token_here',
    PROJECT_ID 'your-project-id'
);

-- Option 4: Scoped Secret (restricts access to specific bucket/path)
CREATE SECRET my_scoped_secret (
    TYPE GCP,
    PROVIDER credential_chain,
    PROJECT_ID 'your-project-id',
    SCOPE 'gcs://my-specific-bucket'
);
```

### Basic File Reading

```sql
-- Read a Parquet file from GCS
SELECT * FROM read_parquet('gs://my-bucket/data.parquet');

-- Read a CSV file
SELECT * FROM read_csv('gs://my-bucket/data.csv');

-- Query with filters (pushdown to GCS)
SELECT * FROM read_parquet('gs://my-bucket/data.parquet')
WHERE year = 2024;

-- Query multiple files with glob patterns
SELECT * FROM read_parquet('gs://my-bucket/data/*.parquet');

-- Aggregate queries
SELECT COUNT(*), AVG(price), MAX(price)
FROM read_parquet('gs://my-bucket/sales.parquet');
```

### Example: Real Data

```sql
-- Load extension
LOAD 'build/release/extension/gcs/gcs.duckdb_extension';

-- Query a parquet file in GCS
SELECT * FROM read_parquet('gs://my-bucket/house-prices.parquet')
WHERE bedrooms >= 3
ORDER BY price DESC
LIMIT 10;

-- Get file metadata
SELECT * FROM glob('gs://my-bucket/*.parquet');
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This extension is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
````
