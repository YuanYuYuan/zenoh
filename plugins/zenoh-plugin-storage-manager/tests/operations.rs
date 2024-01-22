//
// Copyright (c) 2023 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

// Test wild card updates -
// 1. normal case, just some wild card puts and deletes on existing keys and ensure it works
// 2. check for dealing with out of order updates

use std::str::FromStr;
use std::thread::sleep;

use zenoh::prelude::r#async::*;
use zenoh::query::Reply;
use zenoh::{prelude::Config, time::Timestamp};
use zenoh_plugin_trait::Plugin;

async fn put_data(session: &zenoh::Session, key_expr: &str, value: &str, _timestamp: Timestamp) {
    println!("Putting Data ('{key_expr}': '{value}')...");
    //  @TODO: how to add timestamp metadata with put, not manipulating sample...
    session.put(key_expr, value).res().await.unwrap();
}

async fn delete_data(session: &zenoh::Session, key_expr: &str, _timestamp: Timestamp) {
    println!("Deleting Data '{key_expr}'...");
    //  @TODO: how to add timestamp metadata with delete, not manipulating sample...
    session.delete(key_expr).res().await.unwrap();
}

async fn get_data(session: &zenoh::Session, key_expr: &str) -> Vec<Sample> {
    let replies: Vec<Reply> = session
        .get(key_expr)
        .res()
        .await
        .unwrap()
        .into_iter()
        .collect();
    println!("Getting replies on '{key_expr}': '{replies:?}'...");
    let mut samples = Vec::new();
    for reply in replies {
        if let Ok(sample) = reply.sample {
            samples.push(sample);
        }
    }
    println!("Getting Data on '{key_expr}': '{samples:?}'...");
    samples
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_updates_in_order() {
    let mut config = Config::default();
    config
        .insert_json5(
            "plugins/storage-manager",
            r#"{
                    storages: {
                        operation_test: {
                            key_expr: "operation/test/**",
                            volume: {
                                id: "memory"
                            }
                        }
                    }
                }"#,
        )
        .unwrap();

    let runtime = zenoh::runtime::Runtime::new(config).await.unwrap();
    let storage =
        zenoh_plugin_storage_manager::StoragesPlugin::start("storage-manager", &runtime).unwrap();

    let session = zenoh::init(runtime).res().await.unwrap();

    sleep(std::time::Duration::from_secs(1));

    put_data(
        &session,
        "operation/test/a",
        "1",
        Timestamp::from_str("2022-01-17T10:42:10.418555997Z/BC779A06D7E049BD88C3FF3DB0C17FCC")
            .unwrap(),
    )
    .await;

    sleep(std::time::Duration::from_millis(10));

    // expects exactly one sample
    let data = get_data(&session, "operation/test/a").await;
    assert_eq!(data.len(), 1);
    assert_eq!(format!("{}", data[0].value), "1");

    put_data(
        &session,
        "operation/test/b",
        "2",
        Timestamp::from_str("2022-01-17T10:43:10.418555997Z/BC779A06D7E049BD88C3FF3DB0C17FCC")
            .unwrap(),
    )
    .await;

    sleep(std::time::Duration::from_millis(10));

    // expects exactly one sample
    let data = get_data(&session, "operation/test/b").await;
    assert_eq!(data.len(), 1);
    assert_eq!(format!("{}", data[0].value), "2");

    delete_data(
        &session,
        "operation/test/a",
        Timestamp::from_str("2022-01-17T10:43:10.418555997Z/BC779A06D7E049BD88C3FF3DB0C17FCC")
            .unwrap(),
    )
    .await;

    sleep(std::time::Duration::from_millis(10));

    // expects zero sample
    let data = get_data(&session, "operation/test/a").await;
    assert_eq!(data.len(), 0);

    // expects exactly one sample
    let data = get_data(&session, "operation/test/b").await;
    assert_eq!(data.len(), 1);
    assert_eq!(format!("{}", data[0].value), "2");
    assert_eq!(data[0].key_expr.as_str(), "operation/test/b");

    drop(storage);
}
