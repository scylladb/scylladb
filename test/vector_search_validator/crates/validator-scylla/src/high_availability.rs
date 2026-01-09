/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::common;
use async_backtrace::framed;
use tracing::info;
use vector_search_validator_tests::ScyllaClusterExt;
use vector_search_validator_tests::ScyllaNodeConfig;
use vector_search_validator_tests::TestActors;
use vector_search_validator_tests::TestCase;
use vector_search_validator_tests::VectorStoreNodeConfig;

#[framed]
pub(crate) async fn new() -> TestCase {
    let timeout = common::DEFAULT_TEST_TIMEOUT;
    TestCase::empty()
        .with_cleanup(timeout, common::cleanup)
        .with_test(
            "secondary_uri_works_correctly",
            timeout,
            test_secondary_uri_works_correctly,
        )
}

#[framed]
async fn test_secondary_uri_works_correctly(actors: TestActors) {
    info!("started");

    let vs_urls = common::get_default_vs_urls(&actors).await;
    let vs_url = &vs_urls[0];

    let scylla_configs: Vec<ScyllaNodeConfig> = vec![
        ScyllaNodeConfig {
            db_ip: actors.services_subnet.ip(common::DB_OCTET_1),
            primary_vs_uris: vec![vs_url.clone()],
            secondary_vs_uris: vec![],
        },
        ScyllaNodeConfig {
            db_ip: actors.services_subnet.ip(common::DB_OCTET_2),
            primary_vs_uris: vec![],
            secondary_vs_uris: vec![vs_url.clone()],
        },
        ScyllaNodeConfig {
            db_ip: actors.services_subnet.ip(common::DB_OCTET_3),
            primary_vs_uris: vec![],
            secondary_vs_uris: vec![vs_url.clone()],
        },
    ];
    let vs_configs = vec![VectorStoreNodeConfig {
        vs_ip: actors.services_subnet.ip(common::VS_OCTET_1),
        db_ip: actors.services_subnet.ip(common::DB_OCTET_1),
        envs: Default::default(),
    }];
    common::init_with_config(actors.clone(), scylla_configs, vs_configs).await;

    let vs_ips = vec![actors.services_subnet.ip(common::VS_OCTET_1)];
    let (session, clients) = common::prepare_connection_with_custom_vs_ips(&actors, vs_ips).await;

    let keyspace = common::create_keyspace(&session).await;
    let table =
        common::create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;

    // Insert vectors
    for i in 0..100 {
        let embedding = vec![i as f32, (i * 2) as f32, (i * 3) as f32];
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, v) VALUES (?, ?)"),
                (i, &embedding),
            )
            .await
            .expect("failed to insert data");
    }

    let index = common::create_index(&session, &clients, &table, "v").await;

    for client in &clients {
        let index_status = common::wait_for_index(&client, &index).await;

        assert_eq!(
            index_status.count, 100,
            "Expected 100 vectors to be indexed"
        );
    }

    // Down the first node with primary URI
    let first_node_ip = actors.services_subnet.ip(common::DB_OCTET_1);
    info!("Bringing down node {first_node_ip}");
    actors.db.down_node(first_node_ip).await;

    // Should work via secondary URIs
    let results = common::get_query_results(
        format!("SELECT pk FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10"),
        &session,
    )
    .await;

    let rows = results
        .rows::<(i32,)>()
        .expect("failed to get rows after node down");
    assert!(
        rows.rows_remaining() <= 10,
        "Expected at most 10 results from ANN query after node down"
    );

    // Drop keyspace
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}
