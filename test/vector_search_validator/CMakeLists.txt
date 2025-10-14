set(VALIDATOR_BIN "${CMAKE_BINARY_DIR}/vector-search-validator/bin/vector-search-validator")
set(VECTOR_STORE_BIN "${CMAKE_BINARY_DIR}/vector-search-validator/bin/vector-store")

add_custom_command(
    OUTPUT ${VALIDATOR_BIN}
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/build-validator ${CMAKE_BINARY_DIR}
    DEPENDS
        build-validator
        Cargo.toml
        crates/validator/Cargo.toml
        crates/validator/src/main.rs
        crates/validator-scylla/Cargo.toml
        crates/validator-scylla/src/lib.rs
        crates/validator-scylla/src/cql.rs
)

add_custom_command(
    OUTPUT ${VECTOR_STORE_BIN}
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/build-vector-store ${CMAKE_BINARY_DIR}
    DEPENDS
        build-env
        build-vector-store
)

add_custom_target(vector_search_validator DEPENDS ${VALIDATOR_BIN} ${VECTOR_STORE_BIN})
add_dependencies(tests vector_search_validator)

