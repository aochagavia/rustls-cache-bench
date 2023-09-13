use std::cell::{Cell, RefCell};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use clap::Parser;
use fastrand::Rng;
use rustls::server::{NoServerSessionStorage, ServerSessionMemoryCache, StoresServerSessions};
use serde::Serialize;
use tokio::sync::mpsc::{Receiver, Sender};

/// The amount of tickets that are generated as part of a handshake
const TICKETS_PER_HANDSHAKE: u64 = 4; // rustls' default

/// A message from the client to one of the servers
enum ClientMessage {
    /// Shutdown the server thread
    Shutdown,
    /// Run a full handshake
    FullHandshake,
    /// Run a resumed handshake
    ResumedHandshake(Vec<u8>),
}

/// A message from the server to the client, meaning that the handshake has completed.
///
/// The vector contains tickets that can be used for the resumed handshake.
struct ServerMessage(Vec<Vec<u8>>);

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    /// The cache sizes to test (comma-separated)
    #[arg(long, value_delimiter(','))]
    cache_sizes: Vec<usize>,
    /// The total amount of "handshakes" that will be executed
    #[arg(long, default_value("100000"))]
    handshakes: u64,
    /// The ratio of resumed handshakes per full handshake (between 0 and 1)
    #[arg(long, default_value("0.5"))]
    resumed_handshake_ratio: f64,
    /// The output directory for the benchmark results
    #[arg(long, default_value("bench_out"))]
    output_dir: PathBuf,
    /// The amount of server threads that should be used (defaults to the amount of physical cores
    /// of the machine minus one)
    #[arg(long)]
    server_threads: Option<usize>,
    /// Whether a noop cache should be used instead of SessionMemoryCache (useful to check the
    /// maximum possible throughput for this setup)
    #[arg(long)]
    noop_cache: bool,
}

fn main() {
    let cli = Cli::parse();
    if cli.cache_sizes.is_empty() {
        println!("At least one value should be provided through --cache-sizes (run again with `--help` for details)");
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    // Run benchmarks, once for each provided cache size
    for &cache_size in &cli.cache_sizes {
        bench_run(&rt, cache_size, &cli);
    }
}

/// Executes a single benchmark run, for a specific cache size
fn bench_run(rt: &tokio::runtime::Runtime, cache_size: usize, cli: &Cli) {
    let server_threads = num_cpus::get_physical() - 1;
    if server_threads < 2 {
        panic!("These benchmarks only make sense on machines with at least 3 cores");
    }

    let cache: Arc<dyn StoresServerSessions> = if cli.noop_cache {
        Arc::new(NoServerSessionStorage {})
    } else {
        ServerSessionMemoryCache::new(cache_size)
    };

    // Generate initial tickets
    let mut initial_tickets = Vec::with_capacity(server_threads);
    let mut rng = Rng::new();
    for _ in 0..server_threads {
        let ticket = generate_ticket(&mut rng);
        cache.put(ticket.clone(), generate_value());
        initial_tickets.push(ticket);
    }

    // Start the server
    let server = MultiThreadedServer::start(cache, server_threads, cli);

    // Hammer the server from our client!
    let local = tokio::task::LocalSet::new();
    let client_stats = local.block_on(
        rt,
        run_client(
            cli.handshakes,
            cli.resumed_handshake_ratio,
            initial_tickets,
            server.channels,
        ),
    );

    // Shut down the server threads
    let server_stats = server
        .join_handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect::<Vec<_>>();

    // Save to file for plotting
    save_handshake_stats(cache_size, &client_stats, &server_stats, &cli.output_dir);
}

/// Save the handshake durations to a file for analysis
fn save_handshake_stats(
    cache_size: usize,
    client_stats: &ClientStats,
    server_stats: &[ServerStats],
    out_dir: &Path,
) {
    let base_dir = out_dir.join(cache_size.to_string());
    std::fs::create_dir_all(&base_dir).unwrap();

    // Timings
    let mut out = File::create(base_dir.join("timings")).unwrap();
    for duration in &client_stats.handshake_durations {
        write!(out, "{};", duration.as_micros()).unwrap();
    }

    // Throughput
    let handshake_server_total: Duration = server_stats
        .iter()
        .flat_map(|s| &s.handshake_durations)
        .sum();

    let total_handshakes = client_stats.handshake_durations.len();

    let throughput_stats = ThroughputStats {
        cache_size,
        requests_per_second: total_handshakes as f64 / client_stats.time_handshaking.as_secs_f64(),
        client_side_seconds: client_stats
            .handshake_durations
            .iter()
            .sum::<Duration>()
            .as_secs_f64(),
        server_side_seconds: handshake_server_total.as_secs_f64(),
    };

    let file = File::create(base_dir.join("throughput.json")).unwrap();
    serde_json::to_writer(file, &throughput_stats).unwrap();
}

/// Hammers the server with as many requests as possible, from a single thread
async fn run_client(
    handshakes: u64,
    resumed_handshake_ratio: f64,
    initial_tickets: Vec<Vec<u8>>,
    server_threads: Vec<(Sender<ClientMessage>, Receiver<ServerMessage>)>,
) -> ClientStats {
    let handshake_durations = Rc::new(RefCell::new(Vec::with_capacity(handshakes as usize)));
    let handshakes_left = Rc::new(Cell::new(handshakes));

    let start = Instant::now();

    // Spawn a recurring handshaking task against each server thread
    let mut handles = Vec::new();
    for (i, ((tx, mut rx), initial_ticket)) in server_threads
        .into_iter()
        .zip(initial_tickets)
        .enumerate()
    {
        let handshake_durations = handshake_durations.clone();
        let handshakes_left = handshakes_left.clone();

        let handle = tokio::task::spawn_local(async move {
            let mut rng = Rng::with_seed(i as u64 * 999);
            let mut prev_ticket = initial_ticket;

            loop {
                let left = handshakes_left.get();
                if left == 0 {
                    // No handshakes left, shutdown the server thread and finish the task
                    tx.send(ClientMessage::Shutdown).await.unwrap();
                    break;
                } else {
                    handshakes_left.set(left - 1);
                }

                prev_ticket = run_handshake(
                    &tx,
                    &mut rx,
                    &mut rng,
                    prev_ticket,
                    handshake_durations.clone(),
                    resumed_handshake_ratio,
                )
                .await;
            }
        });

        handles.push(handle);
    }

    // Wait until work for all threads has finished
    for handle in handles {
        handle.await.unwrap();
    }

    // This measurement is used to calculate throughput
    let time_handshaking = Instant::now() - start;

    // This measurement is used to plot a latency histogram and to calculate saturation (measures
    // each request separately; subject to the overhead of exchanging data between the client and
    // the server threads)
    let handshake_durations = Rc::try_unwrap(handshake_durations).ok().unwrap();
    let handshake_durations = handshake_durations.into_inner();

    ClientStats {
        time_handshaking,
        handshake_durations,
    }
}

/// Runs a handshake against one of the server threads, communicating through the provided channel.
///
/// Returns a ticket that can be used in a future handshake.
async fn run_handshake(
    tx: &Sender<ClientMessage>,
    rx: &mut Receiver<ServerMessage>,
    rng: &mut Rng,
    prev_ticket: Vec<u8>,
    handshake_durations: Rc<RefCell<Vec<Duration>>>,
    resumed_handshake_ratio: f64,
) -> Vec<u8> {
    // Decide between full and resumed handshake
    let message = if rng.f64() < resumed_handshake_ratio {
        // Because the ticket pool is initialized with as many tickets as server threads, and
        // every handshake makes sure to add at least one token, it is guaranteed the pool will
        // never be empty
        ClientMessage::ResumedHandshake(prev_ticket)
    } else {
        ClientMessage::FullHandshake
    };

    // Actually do the handshake
    let start_handshake = Instant::now();
    tx.send(message).await.unwrap();
    let tickets = rx.recv().await.unwrap().0;
    let handshake_duration = Instant::now() - start_handshake;

    handshake_durations.borrow_mut().push(handshake_duration);
    tickets.into_iter().next().unwrap()
}

/// A handshake server backed by multiple worker threads
struct MultiThreadedServer {
    /// The channels that a client can use to communicate with each thread
    channels: Vec<(Sender<ClientMessage>, Receiver<ServerMessage>)>,
    /// The worker threads' join handles
    join_handles: Vec<JoinHandle<ServerStats>>,
}

impl MultiThreadedServer {
    /// Start the server
    fn start(cache: Arc<dyn StoresServerSessions>, server_threads: usize, cli: &Cli) -> Self {
        let mut channels = Vec::new();
        let mut join_handles = Vec::new();

        for i in 0..server_threads {
            let rng = Rng::with_seed(i as u64 * 42);
            let (tx, rx, join_handle) =
                Self::start_thread(cache.clone(), rng, server_threads, cli);
            channels.push((tx, rx));
            join_handles.push(join_handle);
        }

        MultiThreadedServer {
            channels,
            join_handles,
        }
    }

    /// Starts a single server thread
    fn start_thread(
        cache: Arc<dyn StoresServerSessions>,
        mut rng: Rng,
        server_threads: usize,
        cli: &Cli,
    ) -> (
        Sender<ClientMessage>,
        Receiver<ServerMessage>,
        JoinHandle<ServerStats>,
    ) {
        let (server_tx, client_rx) = tokio::sync::mpsc::channel(1);
        let (client_tx, mut server_rx) = tokio::sync::mpsc::channel(1);

        let mut stats = ServerStats::with_enough_capacity(server_threads, cli.handshakes);
        let handle = std::thread::spawn(move || {
            loop {
                let request = server_rx.blocking_recv().unwrap();
                if let ClientMessage::Shutdown = request {
                    break;
                }

                let start = Instant::now();
                if let ClientMessage::ResumedHandshake(key) = &request {
                    // Resumed handshakes call `ServerSessionMemoryCache::take` (in TLS 1.3)
                    cache.take(key.as_slice());
                }

                // All handshakes generate new tickets
                let mut tickets = Vec::with_capacity(TICKETS_PER_HANDSHAKE as usize);
                for _ in 0..TICKETS_PER_HANDSHAKE {
                    let key = generate_ticket(&mut rng);
                    tickets.push(key.clone());
                    cache.put(key, generate_value());
                }
                let done = Instant::now();

                server_tx.blocking_send(ServerMessage(tickets)).unwrap();

                stats.handled_handshakes += 1;
                stats.handshake_durations.push(done - start);
            }

            stats
        });

        (client_tx, client_rx, handle)
    }
}

fn generate_ticket(rng: &mut Rng) -> Vec<u8> {
    // Keys are 32 bytes in TLS 1.3
    let mut key = vec![0; 32];
    rng.fill(&mut key);
    key
}

fn generate_value() -> Vec<u8> {
    // Values have variable length, but from the source code it looks like 80 is reasonable
    vec![0; 80]
}

#[derive(Serialize)]
struct ThroughputStats {
    cache_size: usize,
    requests_per_second: f64,
    client_side_seconds: f64,
    server_side_seconds: f64,
}

struct ClientStats {
    time_handshaking: Duration,
    handshake_durations: Vec<Duration>,
}

struct ServerStats {
    handled_handshakes: u64,
    handshake_durations: Vec<Duration>,
}

impl ServerStats {
    fn with_enough_capacity(server_threads: usize, handshakes: u64) -> Self {
        // Requests are almost evenly distributed (we overallocate as though there was one server
        // thread less)
        let handshake_durations = Vec::with_capacity(handshakes as usize / (server_threads - 1));

        Self {
            handled_handshakes: 0,
            handshake_durations,
        }
    }
}
