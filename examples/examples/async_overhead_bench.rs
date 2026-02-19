// Async Overhead Benchmark: Compare pure tokio vs Zenoh patterns
//
// Measures round-trip time for 64-byte payloads at 200 Hz
//
// Usage:
//   cargo run --release --example async_overhead_bench tokio-server
//   cargo run --release --example async_overhead_bench tokio-client
//   cargo run --release --example async_overhead_bench zenoh-callback-pong
//   cargo run --release --example async_overhead_bench zenoh-callback-ping
//   cargo run --release --example async_overhead_bench zenoh-loop-pong
//   cargo run --release --example async_overhead_bench zenoh-loop-ping

use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::sleep;

const PAYLOAD_SIZE: usize = 64;
const FREQUENCY: u64 = 200; // Hz
const WARMUP_SAMPLES: usize = 200;  // 1 second warmup
const TEST_SAMPLES: usize = 1000;   // 5 seconds test

// ============================================================================
// Pure Tokio TCP Baseline
// ============================================================================

async fn tokio_tcp_server() {
    let listener = TcpListener::bind("127.0.0.1:7447").await.unwrap();
    println!("Tokio TCP server listening on 127.0.0.1:7447");

    let (mut socket, _) = listener.accept().await.unwrap();
    let mut buf = vec![0u8; PAYLOAD_SIZE];

    loop {
        // Read ping
        match socket.read_exact(&mut buf).await {
            Ok(_) => {
                // Echo pong
                if socket.write_all(&buf).await.is_err() {
                    break;
                }
            }
            Err(_) => break,
        }
    }
}

async fn tokio_tcp_client() {
    // Wait for server
    sleep(Duration::from_millis(500)).await;

    let mut stream = TcpStream::connect("127.0.0.1:7447").await.unwrap();
    let payload = vec![42u8; PAYLOAD_SIZE];
    let mut buf = vec![0u8; PAYLOAD_SIZE];

    println!("=== Pure Tokio TCP Benchmark ===");
    println!("Payload: {} bytes, Frequency: {} Hz", PAYLOAD_SIZE, FREQUENCY);

    // Warmup
    println!("Warming up ({} samples)...", WARMUP_SAMPLES);
    for _ in 0..WARMUP_SAMPLES {
        stream.write_all(&payload).await.unwrap();
        stream.read_exact(&mut buf).await.unwrap();
    }

    // Benchmark
    println!("Running benchmark ({} samples)...", TEST_SAMPLES);
    let mut samples = Vec::with_capacity(TEST_SAMPLES);
    let interval = Duration::from_micros(1_000_000 / FREQUENCY);

    for _ in 0..TEST_SAMPLES {
        let start = Instant::now();

        stream.write_all(&payload).await.unwrap();
        stream.read_exact(&mut buf).await.unwrap();

        let rtt = start.elapsed();
        samples.push(rtt);

        // Rate limiting
        sleep(interval).await;
    }

    print_stats("Pure Tokio TCP", &samples);
}

// ============================================================================
// Zenoh Callback + Spawn Pattern
// ============================================================================

/// Build a zenoh config for the pong side: listen on explicit TCP address.
fn pong_config() -> zenoh::Config {
    let mut config = zenoh::Config::default();
    config
        .insert_json5("listen/endpoints", r#"["tcp/127.0.0.1:7448"]"#)
        .unwrap();
    config
        .insert_json5("scouting/multicast/enabled", "false")
        .unwrap();
    config
}

/// Build a zenoh config for the ping side: connect to pong's explicit TCP address.
fn ping_config() -> zenoh::Config {
    let mut config = zenoh::Config::default();
    config
        .insert_json5("connect/endpoints", r#"["tcp/127.0.0.1:7448"]"#)
        .unwrap();
    config
        .insert_json5("scouting/multicast/enabled", "false")
        .unwrap();
    config
}

async fn zenoh_callback_pong() {
    use std::sync::{atomic::{AtomicU64, Ordering}, Arc};

    println!("Starting Zenoh callback+spawn pong server...");
    let session = zenoh::open(pong_config()).await.unwrap();

    let publisher = Arc::new(
        session
            .declare_publisher("test/pong")
            .congestion_control(zenoh::qos::CongestionControl::Block)
            .express(true)
            .await
            .unwrap()
    );

    let cb_count = Arc::new(AtomicU64::new(0));
    let cb_count2 = cb_count.clone();

    let _subscriber = session
        .declare_subscriber("test/ping")
        .callback(move |sample| {
            let publisher = publisher.clone();
            let payload = sample.payload().clone();
            let n = cb_count2.fetch_add(1, Ordering::Relaxed);
            eprintln!("[pong-cb] callback #{n} fired, spawning task");

            // This is the pattern we're testing: spawn inside callback
            tokio::spawn(async move {
                eprintln!("[pong-cb] task #{n}: calling publisher.put()");
                let res = publisher.put(payload).await;
                eprintln!("[pong-cb] task #{n}: publisher.put() returned {:?}", res.is_ok());
            });
            eprintln!("[pong-cb] callback #{n}: tokio::spawn returned, callback exiting");
        })
        .await
        .unwrap();

    println!("Zenoh callback+spawn pong server ready. Press Ctrl+C to exit.");
    // Keep running until killed
    std::future::pending::<()>().await;
}

async fn zenoh_callback_ping() {
    use zenoh::bytes::ZBytes;

    // Wait for pong server to start listening
    sleep(Duration::from_millis(500)).await;

    println!("=== Zenoh Callback+Spawn Benchmark ===");
    println!("Payload: {} bytes, Frequency: {} Hz", PAYLOAD_SIZE, FREQUENCY);

    let session = zenoh::open(ping_config()).await.unwrap();

    let mut subscriber = session.declare_subscriber("test/pong").await.unwrap();
    let publisher = session
        .declare_publisher("test/ping")
        .congestion_control(zenoh::qos::CongestionControl::Block)
        .express(true)
        .await
        .unwrap();

    // Allow time for declaration exchange to complete over the TCP link
    // before publishing the first message.
    sleep(Duration::from_secs(1)).await;

    let payload: ZBytes = vec![42u8; PAYLOAD_SIZE].into();

    // Warmup
    println!("Warming up ({} samples)...", WARMUP_SAMPLES);
    for i in 0..WARMUP_SAMPLES {
        eprintln!("[ping] warmup #{i}: publishing...");
        publisher.put(payload.clone()).await.unwrap();
        eprintln!("[ping] warmup #{i}: published, waiting recv...");
        let _ = subscriber.recv_async().await;
        eprintln!("[ping] warmup #{i}: recv complete");
        if i >= 3 { break; } // Only debug first 4 samples then run normally
    }
    // Remaining warmup without debug
    for _ in 4..WARMUP_SAMPLES {
        publisher.put(payload.clone()).await.unwrap();
        let _ = subscriber.recv_async().await;
    }

    // Benchmark
    println!("Running benchmark ({} samples)...", TEST_SAMPLES);
    let mut samples = Vec::with_capacity(TEST_SAMPLES);
    let interval = Duration::from_micros(1_000_000 / FREQUENCY);

    for _ in 0..TEST_SAMPLES {
        let start = Instant::now();

        publisher.put(payload.clone()).await.unwrap();
        let _ = subscriber.recv_async().await;

        let rtt = start.elapsed();
        samples.push(rtt);

        sleep(interval).await;
    }

    print_stats("Zenoh Callback+Spawn", &samples);
}

// ============================================================================
// Zenoh Async Receive Loop Pattern
// ============================================================================

async fn zenoh_loop_pong() {
    println!("Starting Zenoh async loop pong server...");
    let session = zenoh::open(pong_config()).await.unwrap();

    let publisher = session
        .declare_publisher("test/pong")
        .congestion_control(zenoh::qos::CongestionControl::Block)
        .express(true)
        .await
        .unwrap();

    let mut subscriber = session
        .declare_subscriber("test/ping")
        .await
        .unwrap();

    println!("Zenoh async loop pong server ready");

    println!("Zenoh async loop pong server ready. Press Ctrl+C to exit.");

    loop {
        match subscriber.recv_async().await {
            Ok(sample) => {
                let _ = publisher.put(sample.payload().clone()).await;
            }
            Err(_) => break,
        }
    }
}

async fn zenoh_loop_ping() {
    use zenoh::bytes::ZBytes;

    // Wait for pong server
    sleep(Duration::from_millis(500)).await;

    println!("=== Zenoh Async Loop Benchmark ===");
    println!("Payload: {} bytes, Frequency: {} Hz", PAYLOAD_SIZE, FREQUENCY);

    let session = zenoh::open(ping_config()).await.unwrap();

    let mut subscriber = session.declare_subscriber("test/pong").await.unwrap();
    let publisher = session
        .declare_publisher("test/ping")
        .congestion_control(zenoh::qos::CongestionControl::Block)
        .express(true)
        .await
        .unwrap();

    // Allow time for declaration exchange to complete over the TCP link.
    sleep(Duration::from_secs(1)).await;

    let payload: ZBytes = vec![42u8; PAYLOAD_SIZE].into();

    // Warmup
    println!("Warming up ({} samples)...", WARMUP_SAMPLES);
    for _ in 0..WARMUP_SAMPLES {
        publisher.put(payload.clone()).await.unwrap();
        let _ = subscriber.recv_async().await;
    }

    // Benchmark
    println!("Running benchmark ({} samples)...", TEST_SAMPLES);
    let mut samples = Vec::with_capacity(TEST_SAMPLES);
    let interval = Duration::from_micros(1_000_000 / FREQUENCY);

    for _ in 0..TEST_SAMPLES {
        let start = Instant::now();

        publisher.put(payload.clone()).await.unwrap();
        let _ = subscriber.recv_async().await;

        let rtt = start.elapsed();
        samples.push(rtt);

        sleep(interval).await;
    }

    print_stats("Zenoh Async Loop", &samples);
}

// ============================================================================
// Statistics
// ============================================================================

fn print_stats(name: &str, samples: &[Duration]) {
    let mut sorted: Vec<_> = samples.iter().map(|d| d.as_micros()).collect();
    sorted.sort_unstable();

    let min = sorted[0];
    let max = sorted[sorted.len() - 1];
    let p25 = sorted[sorted.len() / 4];
    let p50 = sorted[sorted.len() / 2];
    let p75 = sorted[sorted.len() * 3 / 4];
    let p95 = sorted[sorted.len() * 95 / 100];
    let p99 = sorted[sorted.len() * 99 / 100];
    let avg: u128 = sorted.iter().sum::<u128>() / sorted.len() as u128;

    println!("\n{} Results:", name);
    println!("  Samples: {}", sorted.len());
    println!("  Min:     {:6}µs", min);
    println!("  P25:     {:6}µs", p25);
    println!("  P50:     {:6}µs  (median)", p50);
    println!("  Avg:     {:6}µs", avg);
    println!("  P75:     {:6}µs", p75);
    println!("  P95:     {:6}µs", p95);
    println!("  P99:     {:6}µs", p99);
    println!("  Max:     {:6}µs", max);
}

// ============================================================================
// Single-process benchmark modes (reliable, no multicast scouting needed)
// ============================================================================

/// Run tokio TCP ping-pong in a single process.
/// Server runs as a spawned task, client runs in the main task.
async fn bench_tokio_tcp() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    // Start server in background task
    let server_task = tokio::spawn(async {
        let listener = TcpListener::bind("127.0.0.1:7449").await.unwrap();
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; PAYLOAD_SIZE];
        loop {
            match socket.read_exact(&mut buf).await {
                Ok(_) => {
                    if socket.write_all(&buf).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    sleep(Duration::from_millis(100)).await;

    let mut stream = TcpStream::connect("127.0.0.1:7449").await.unwrap();
    let payload = vec![42u8; PAYLOAD_SIZE];
    let mut buf = vec![0u8; PAYLOAD_SIZE];

    println!("=== Pure Tokio TCP Benchmark (single-process) ===");
    println!("Payload: {} bytes, Frequency: {} Hz", PAYLOAD_SIZE, FREQUENCY);

    println!("Warming up ({} samples)...", WARMUP_SAMPLES);
    for _ in 0..WARMUP_SAMPLES {
        stream.write_all(&payload).await.unwrap();
        stream.read_exact(&mut buf).await.unwrap();
    }

    println!("Running benchmark ({} samples)...", TEST_SAMPLES);
    let mut samples = Vec::with_capacity(TEST_SAMPLES);
    let interval = Duration::from_micros(1_000_000 / FREQUENCY);

    for _ in 0..TEST_SAMPLES {
        let start = Instant::now();
        stream.write_all(&payload).await.unwrap();
        stream.read_exact(&mut buf).await.unwrap();
        samples.push(start.elapsed());
        sleep(interval).await;
    }

    print_stats("Pure Tokio TCP", &samples);
    server_task.abort();
}

/// Run zenoh callback+spawn ping-pong in a single process.
/// Uses a single session for both pong and ping (in-process loopback routing).
async fn bench_zenoh_callback() {
    use std::sync::Arc;
    use zenoh::{bytes::ZBytes, qos::CongestionControl, Config};

    println!("=== Zenoh Callback+Spawn Benchmark (single-process) ===");
    println!("Payload: {} bytes, Frequency: {} Hz", PAYLOAD_SIZE, FREQUENCY);

    let session = zenoh::open(Config::default()).await.unwrap();

    // Pong side: declare publisher for pong, subscriber on ping with callback+spawn
    let pong_publisher = Arc::new(
        session
            .declare_publisher("test/pong")
            .congestion_control(CongestionControl::Block)
            .express(true)
            .await
            .unwrap(),
    );

    let _ping_sub = session
        .declare_subscriber("test/ping")
        .callback(move |sample| {
            let publisher = pong_publisher.clone();
            let payload = sample.payload().clone();
            tokio::spawn(async move {
                let _ = publisher.put(payload).await;
            });
        })
        .await
        .unwrap();

    // Ping side: declare subscriber on pong, publisher on ping
    let mut pong_sub = session.declare_subscriber("test/pong").await.unwrap();
    let ping_publisher = session
        .declare_publisher("test/ping")
        .congestion_control(CongestionControl::Block)
        .express(true)
        .await
        .unwrap();

    // Brief settling time for routing tables
    sleep(Duration::from_millis(100)).await;

    let payload: ZBytes = vec![42u8; PAYLOAD_SIZE].into();

    println!("Warming up ({} samples)...", WARMUP_SAMPLES);
    for _ in 0..WARMUP_SAMPLES {
        ping_publisher.put(payload.clone()).await.unwrap();
        let _ = pong_sub.recv_async().await;
    }

    println!("Running benchmark ({} samples)...", TEST_SAMPLES);
    let mut samples = Vec::with_capacity(TEST_SAMPLES);
    let interval = Duration::from_micros(1_000_000 / FREQUENCY);

    for _ in 0..TEST_SAMPLES {
        let start = Instant::now();
        ping_publisher.put(payload.clone()).await.unwrap();
        let _ = pong_sub.recv_async().await;
        samples.push(start.elapsed());
        sleep(interval).await;
    }

    print_stats("Zenoh Callback+Spawn", &samples);
}

/// Run zenoh async-loop ping-pong in a single process.
/// Pong runs as a spawned task using an async recv loop.
async fn bench_zenoh_loop() {
    use zenoh::{bytes::ZBytes, qos::CongestionControl, Config};

    println!("=== Zenoh Async Loop Benchmark (single-process) ===");
    println!("Payload: {} bytes, Frequency: {} Hz", PAYLOAD_SIZE, FREQUENCY);

    let session = zenoh::open(Config::default()).await.unwrap();

    // Pong side: declare subscriber on ping and publisher on pong
    let pong_publisher = session
        .declare_publisher("test/pong")
        .congestion_control(CongestionControl::Block)
        .express(true)
        .await
        .unwrap();

    let mut ping_sub = session.declare_subscriber("test/ping").await.unwrap();

    // Pong runs as an async recv loop in a spawned task
    let pong_task = tokio::spawn(async move {
        loop {
            match ping_sub.recv_async().await {
                Ok(sample) => {
                    let _ = pong_publisher.put(sample.payload().clone()).await;
                }
                Err(_) => break,
            }
        }
    });

    // Ping side: declare subscriber on pong, publisher on ping
    let mut pong_sub = session.declare_subscriber("test/pong").await.unwrap();
    let ping_publisher = session
        .declare_publisher("test/ping")
        .congestion_control(CongestionControl::Block)
        .express(true)
        .await
        .unwrap();

    // Brief settling time for routing tables
    sleep(Duration::from_millis(100)).await;

    let payload: ZBytes = vec![42u8; PAYLOAD_SIZE].into();

    println!("Warming up ({} samples)...", WARMUP_SAMPLES);
    for _ in 0..WARMUP_SAMPLES {
        ping_publisher.put(payload.clone()).await.unwrap();
        let _ = pong_sub.recv_async().await;
    }

    println!("Running benchmark ({} samples)...", TEST_SAMPLES);
    let mut samples = Vec::with_capacity(TEST_SAMPLES);
    let interval = Duration::from_micros(1_000_000 / FREQUENCY);

    for _ in 0..TEST_SAMPLES {
        let start = Instant::now();
        ping_publisher.put(payload.clone()).await.unwrap();
        let _ = pong_sub.recv_async().await;
        samples.push(start.elapsed());
        sleep(interval).await;
    }

    print_stats("Zenoh Async Loop", &samples);
    pong_task.abort();
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <mode>", args[0]);
        eprintln!("Modes (single-process, recommended):");
        eprintln!("  bench-tokio-tcp      - Pure tokio TCP ping-pong");
        eprintln!("  bench-callback       - Zenoh callback+spawn ping-pong");
        eprintln!("  bench-loop           - Zenoh async receive loop ping-pong");
        eprintln!("Modes (two-process, requires separate pong + ping invocations):");
        eprintln!("  tokio-server         - Pure tokio TCP pong server");
        eprintln!("  tokio-client         - Pure tokio TCP ping client");
        eprintln!("  zenoh-callback-pong  - Zenoh callback+spawn pong");
        eprintln!("  zenoh-callback-ping  - Zenoh callback+spawn ping");
        eprintln!("  zenoh-loop-pong      - Zenoh async loop pong");
        eprintln!("  zenoh-loop-ping      - Zenoh async loop ping");
        std::process::exit(1);
    }

    match args[1].as_str() {
        "bench-tokio-tcp" => bench_tokio_tcp().await,
        "bench-callback" => bench_zenoh_callback().await,
        "bench-loop" => bench_zenoh_loop().await,
        "tokio-server" => tokio_tcp_server().await,
        "tokio-client" => tokio_tcp_client().await,
        "zenoh-callback-pong" => zenoh_callback_pong().await,
        "zenoh-callback-ping" => zenoh_callback_ping().await,
        "zenoh-loop-pong" => zenoh_loop_pong().await,
        "zenoh-loop-ping" => zenoh_loop_ping().await,
        _ => {
            eprintln!("Unknown mode: {}", args[1]);
            std::process::exit(1);
        }
    }
}
