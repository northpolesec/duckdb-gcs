#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "test_helpers.hpp"

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

int main(int argc, char *argv[]) {
	return Catch::Session().run(argc, argv);
}
