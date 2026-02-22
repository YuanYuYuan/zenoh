// bench_pong: pong server (async recv loop, avoids async_lock deadlock)
// Usage: bench_pong <listen_endpoint>
// Example: bench_pong tcp/127.0.0.1:7447

use zenoh::{key_expr::keyexpr, qos::CongestionControl, Config};

#[tokio::main]
async fn main() {
    zenoh::init_log_from_env_or("error");

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: bench_pong <endpoint>");
        eprintln!("Example: bench_pong tcp/127.0.0.1:7447");
        std::process::exit(1);
    }
    let endpoint = &args[1];

    let mut config = Config::default();
    config.insert_json5("listen/endpoints", &format!("[\"{endpoint}\"]")).unwrap();
    config.insert_json5("scouting/multicast/enabled", "false").unwrap();
    config.insert_json5("mode", "\"peer\"").unwrap();

    let session = zenoh::open(config).await.unwrap();

    let key_ping = keyexpr::new("test/ping").unwrap();
    let key_pong = keyexpr::new("test/pong").unwrap();

    let pub_ = session
        .declare_publisher(key_pong)
        .congestion_control(CongestionControl::Block)
        .await
        .unwrap();

    let mut sub = session.declare_subscriber(key_ping).await.unwrap();

    eprintln!("bench_pong listening on {endpoint}");

    while let Ok(sample) = sub.recv_async().await {
        pub_.put(sample.payload().clone()).await.unwrap();
    }
}
