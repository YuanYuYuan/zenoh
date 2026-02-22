// bench_ping: TCP loopback ping client (async)
// Usage: bench_ping <connect_endpoint> <payload_bytes> <samples> <warmup_secs>
// Example: bench_ping tcp/127.0.0.1:7447 64 1000 5

use std::time::{Duration, Instant};
use zenoh::{bytes::ZBytes, key_expr::keyexpr, qos::CongestionControl, Config};

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    zenoh::init_log_from_env_or("error");

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 5 {
        eprintln!("Usage: bench_ping <endpoint> <payload_bytes> <samples> <warmup_secs>");
        eprintln!("Example: bench_ping tcp/127.0.0.1:7447 64 1000 5");
        std::process::exit(1);
    }
    let endpoint  = &args[1];
    let payload: usize   = args[2].parse().expect("payload_bytes");
    let samples: usize   = args[3].parse().expect("samples");
    let warmup_s: f64    = args[4].parse().expect("warmup_secs");

    let mut config = Config::default();
    config.insert_json5("connect/endpoints", &format!("[\"{endpoint}\"]")).unwrap();
    config.insert_json5("scouting/multicast/enabled", "false").unwrap();
    config.insert_json5("mode", "\"peer\"").unwrap();

    let session = zenoh::open(config).await.unwrap();

    let key_ping = keyexpr::new("test/ping").unwrap();
    let key_pong = keyexpr::new("test/pong").unwrap();

    let mut sub = session.declare_subscriber(key_pong).await.unwrap();
    let pub_ = session
        .declare_publisher(key_ping)
        .congestion_control(CongestionControl::Block)
        .await
        .unwrap();

    let data: ZBytes = (0..payload).map(|i| (i % 10) as u8).collect::<Vec<u8>>().into();

    // Warmup
    eprintln!("Warming up for {warmup_s}s...");
    let until = Instant::now() + Duration::from_secs_f64(warmup_s);
    while Instant::now() < until {
        pub_.put(data.clone()).await.unwrap();
        let _ = sub.recv_async().await;
    }

    // Measure
    let mut rtts = Vec::with_capacity(samples);
    for _ in 0..samples {
        let t0 = Instant::now();
        pub_.put(data.clone()).await.unwrap();
        let _ = sub.recv_async().await;
        rtts.push(t0.elapsed().as_micros());
    }

    for (i, rtt) in rtts.iter().enumerate() {
        println!("{payload} bytes: seq={i} rtt={rtt}µs lat={}µs", rtt / 2);
    }
}
