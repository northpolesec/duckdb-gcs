#include "gcs_secret.hpp"

#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

static unique_ptr<CreateSecretFunction> GetGCPCreateSecretFunction() {
	// Create the secret create function for GCP
	SecretType secret_type;
	secret_type.name = CreateGCSSecretFunctions::GetGCSSecretType();
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = GCSSecretProvider::CREDENTIAL_CHAIN;

	CreateSecretFunction function = {secret_type.name, "gcp",
	                                 CreateGCSSecretFunctions::CreateGCSSecretFromCredentialChain};
	function.named_parameters["service_account_key_path"] = LogicalType::VARCHAR;
	function.named_parameters["service_account_email"] = LogicalType::VARCHAR;
	function.named_parameters["access_token"] = LogicalType::VARCHAR;
	function.named_parameters["project_id"] = LogicalType::VARCHAR;

	return make_uniq<CreateSecretFunction>(function);
}

void CreateGCSSecretFunctions::Register(ExtensionLoader &loader) {
	// Register the secret type
	SecretType secret_type;
	secret_type.name = GetGCSSecretType();
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = GCSSecretProvider::CREDENTIAL_CHAIN;
	loader.RegisterSecretType(secret_type);

	// Service Account provider
	CreateSecretFunction service_account_function = {secret_type.name, GCSSecretProvider::SERVICE_ACCOUNT,
	                                                 CreateGCSSecretFromServiceAccount};
	service_account_function.named_parameters["service_account_key_path"] = LogicalType::VARCHAR;
	service_account_function.named_parameters["service_account_email"] = LogicalType::VARCHAR;
	service_account_function.named_parameters["project_id"] = LogicalType::VARCHAR;
	loader.RegisterFunction(service_account_function);

	// Credential Chain provider (default)
	CreateSecretFunction credential_chain_function = {secret_type.name, GCSSecretProvider::CREDENTIAL_CHAIN,
	                                                  CreateGCSSecretFromCredentialChain};
	credential_chain_function.named_parameters["project_id"] = LogicalType::VARCHAR;
	loader.RegisterFunction(credential_chain_function);

	// Access Token provider
	CreateSecretFunction access_token_function = {secret_type.name, GCSSecretProvider::ACCESS_TOKEN,
	                                              CreateGCSSecretFromAccessToken};
	access_token_function.named_parameters["access_token"] = LogicalType::VARCHAR;
	access_token_function.named_parameters["project_id"] = LogicalType::VARCHAR;
	loader.RegisterFunction(access_token_function);
}

unique_ptr<BaseSecret> CreateGCSSecretFunctions::CreateGCSSecretFromServiceAccount(ClientContext &context,
                                                                                   CreateSecretInput &input) {
	auto scope = input.scope;
	auto secret = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);

	// Service account key path (required for this provider)
	auto service_account_key_path = input.options.find("service_account_key_path");
	if (service_account_key_path == input.options.end()) {
		throw InvalidInputException("SERVICE_ACCOUNT provider requires 'service_account_key_path' parameter");
	}
	secret->secret_map["service_account_key_path"] = service_account_key_path->second;

	// Optional service account email
	auto service_account_email = input.options.find("service_account_email");
	if (service_account_email != input.options.end()) {
		secret->secret_map["service_account_email"] = service_account_email->second;
	}

	// Optional project ID
	auto project_id = input.options.find("project_id");
	if (project_id != input.options.end()) {
		secret->secret_map["project_id"] = project_id->second;
	}

	// Set provider type
	secret->secret_map["provider"] = GCSSecretProvider::SERVICE_ACCOUNT;

	// Mark sensitive keys for redaction
	secret->redact_keys = {"service_account_key_path"};

	return std::move(secret);
}

unique_ptr<BaseSecret> CreateGCSSecretFunctions::CreateGCSSecretFromCredentialChain(ClientContext &context,
                                                                                    CreateSecretInput &input) {
	auto scope = input.scope;
	auto secret = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);

	// Optional project ID
	auto project_id = input.options.find("project_id");
	if (project_id != input.options.end()) {
		secret->secret_map["project_id"] = project_id->second;
	}

	// Set provider type
	secret->secret_map["provider"] = GCSSecretProvider::CREDENTIAL_CHAIN;

	return std::move(secret);
}

unique_ptr<BaseSecret> CreateGCSSecretFunctions::CreateGCSSecretFromAccessToken(ClientContext &context,
                                                                                CreateSecretInput &input) {
	auto scope = input.scope;
	auto secret = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);

	// Access token (required for this provider)
	auto access_token = input.options.find("access_token");
	if (access_token == input.options.end()) {
		throw InvalidInputException("ACCESS_TOKEN provider requires 'access_token' parameter");
	}
	secret->secret_map["access_token"] = access_token->second;

	// Optional project ID
	auto project_id = input.options.find("project_id");
	if (project_id != input.options.end()) {
		secret->secret_map["project_id"] = project_id->second;
	}

	// Set provider type
	secret->secret_map["provider"] = GCSSecretProvider::ACCESS_TOKEN;

	// Mark sensitive keys for redaction
	secret->redact_keys = {"access_token"};

	return std::move(secret);
}

} // namespace duckdb
