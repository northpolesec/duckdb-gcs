#pragma once

#include "duckdb/main/extension.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

struct GCSSecretType {
	static constexpr const char *TYPE_NAME = "GCP";
};

struct GCSSecretProvider {
	// Application Default Credentials
	static constexpr const char *CREDENTIAL_CHAIN = "credential_chain";
	// Service Account with JSON key file
	static constexpr const char *SERVICE_ACCOUNT = "service_account";
	// OAuth2 access token
	static constexpr const char *ACCESS_TOKEN = "access_token";
};

class CreateGCSSecretFunctions {
public:
	static void Register(ExtensionLoader &instance);

	// Secret creation functions
	static unique_ptr<BaseSecret> CreateGCSSecretFromServiceAccount(ClientContext &context, CreateSecretInput &input);
	static unique_ptr<BaseSecret> CreateGCSSecretFromCredentialChain(ClientContext &context, CreateSecretInput &input);
	static unique_ptr<BaseSecret> CreateGCSSecretFromAccessToken(ClientContext &context, CreateSecretInput &input);

	// Helper functions
	static string GetGCSSecretType() {
		return GCSSecretType::TYPE_NAME;
	}
};

} // namespace duckdb
