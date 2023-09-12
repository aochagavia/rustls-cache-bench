use std::fs::File;
use std::io::Write;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use clap::Parser;
use fastrand::Rng;
use rustls::server::{ServerSessionMemoryCache, StoresServerSessions};
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
    /// The amount of full "handshakes" that will be executed
    #[arg(long, default_value("40000"))]
    full_handshakes: u64,
    /// The amount of resumed "handshakes" that will be executed after a full one
    #[arg(long, default_value("2"))]
    resumed_handshakes_per_full_handshake: u64,
    /// The output directory for the benchmark results
    #[arg(long, default_value("bench_out"))]
    output_dir: PathBuf,
    /// The amount of server threads that should be used (defaults to the amount of physical cores
    /// of the machine minus one)
    #[arg(long)]
    server_threads: Option<usize>,
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
    let mut throughput_stats = Vec::new();
    for &cache_size in &cli.cache_sizes {
        let throughput = bench_run(&rt, cache_size, &cli);
        throughput_stats.push(throughput);
    }

    // Output the throughput stats
    let mut file = File::create(cli.output_dir.join("throughput.json")).unwrap();
    write!(
        file,
        "{}",
        serde_json::to_string(&throughput_stats).unwrap()
    )
    .unwrap();
}

/// Executes a single benchmark run, for a specific cache size
fn bench_run(rt: &tokio::runtime::Runtime, cache_size: usize, cli: &Cli) -> ThroughputStats {
    let server_threads = num_cpus::get_physical() - 1;
    if server_threads < 2 {
        panic!("These benchmarks only make sense on machines with at least 3 cores");
    }

    let mut server = MultiThreadedServer::start(cache_size, server_threads, cli);
    let local = tokio::task::LocalSet::new();

    // Hammer the server from our client!
    let client_stats = local.block_on(
        rt,
        run_client(
            cli.full_handshakes,
            cli.resumed_handshakes_per_full_handshake,
            server.client_tx.clone(),
            mem::take(&mut server.client_rx),
        ),
    );

    // Shutdown server threads
    let server_stats = server.shutdown();

    // Save client stats to file for plotting
    save_handshake_stats(
        "fh",
        &client_stats.full_handshake_durations,
        cache_size,
        &cli.output_dir,
    );
    save_handshake_stats(
        "rh",
        &client_stats.resumed_handshake_durations,
        cache_size,
        &cli.output_dir,
    );

    // Calculate throughput stats
    let full_handshake_server_total: Duration = server_stats
        .iter()
        .flat_map(|s| &s.full_handshake_durations)
        .sum();
    let resumed_handshake_server_total: Duration = server_stats
        .iter()
        .flat_map(|s| &s.resumed_handshake_durations)
        .sum();
    let total_handshakes =
        cli.full_handshakes + cli.full_handshakes * cli.resumed_handshakes_per_full_handshake;
    ThroughputStats {
        cache_size,
        requests_per_second: total_handshakes as f64 / client_stats.time_handshaking.as_secs_f64(),
        fh_client_side_seconds: client_stats
            .full_handshake_durations
            .iter()
            .sum::<Duration>()
            .as_secs_f64(),
        fh_server_side_seconds: full_handshake_server_total.as_secs_f64(),
        rh_client_side_seconds: client_stats
            .resumed_handshake_durations
            .iter()
            .sum::<Duration>()
            .as_secs_f64(),
        rh_server_side_seconds: resumed_handshake_server_total.as_secs_f64(),
    }
}

/// Save the handshake stats to a file for analysis
fn save_handshake_stats(
    handshake_file_name: &str,
    durations: &[Duration],
    cache_size: usize,
    out_dir: &Path,
) {
    let base_path = out_dir.join(cache_size.to_string());
    std::fs::create_dir_all(&base_path).unwrap();

    let mut out = File::create(base_path.join(handshake_file_name)).unwrap();
    for duration in durations {
        write!(out, "{};", duration.as_micros()).unwrap();
    }
}

/// Hammer the server with as many requests as possible, from a single thread.
///
/// For simplicity, resumed handshakes are executed right after full handshakes.
async fn run_client(
    full_handshakes: u64,
    resumed_handshakes_per_full_handshake: u64,
    client_tx: Arc<Vec<Sender<ClientMessage>>>,
    client_rx: Vec<Receiver<ServerMessage>>,
) -> ClientStats {
    let (mut scheduler, server_thread_done_tx) = ServerThreadScheduler::new(client_rx);

    let full_handshake_durations =
        Arc::new(Mutex::new(Vec::with_capacity(full_handshakes as usize)));
    let resumed_handshake_durations = Arc::new(Mutex::new(Vec::with_capacity(
        (full_handshakes * resumed_handshakes_per_full_handshake) as usize,
    )));

    let start = Instant::now();
    for _ in 0..full_handshakes {
        // We wait for an available server thread to avoid spawning thousands of idle tokio tasks
        let (server_thread_index, mut client_rx) = scheduler.next_idle_server_thread().await;

        // Clone so we can move into the closure
        let client_tx = client_tx.clone();
        let server_thread_done_tx = server_thread_done_tx.clone();
        let full_handshake_durations = full_handshake_durations.clone();
        let resumed_handshake_durations = resumed_handshake_durations.clone();
        tokio::task::spawn_local(async move {
            // Full handshake
            let start_handshake = Instant::now();
            client_tx[server_thread_index]
                .send(ClientMessage::FullHandshake)
                .await
                .unwrap();
            let tickets = client_rx.recv().await.unwrap().0;
            let full_handshake_duration = Instant::now() - start_handshake;

            // Resumed handshakes (sequential, against the same server thread)
            for ticket in tickets
                .into_iter()
                .take(resumed_handshakes_per_full_handshake as usize)
            {
                let start_handshake = Instant::now();
                client_tx[server_thread_index]
                    .send(ClientMessage::ResumedHandshake(ticket))
                    .await
                    .unwrap();
                client_rx.recv().await.unwrap();

                resumed_handshake_durations
                    .lock()
                    .unwrap()
                    .push(Instant::now() - start_handshake);
            }

            // Notify the server scheduler that the server thread is idle again
            server_thread_done_tx
                .send((server_thread_index, client_rx))
                .await
                .unwrap();

            // Track stats
            full_handshake_durations
                .lock()
                .unwrap()
                .push(full_handshake_duration);
        });
    }

    // Wait for all server threads to finish their work
    scheduler.wait_until_all_idle().await;

    let time_handshaking = Instant::now() - start;
    let full_handshake_durations = Arc::try_unwrap(full_handshake_durations)
        .unwrap()
        .into_inner()
        .unwrap();
    let resumed_handshake_durations = Arc::try_unwrap(resumed_handshake_durations)
        .unwrap()
        .into_inner()
        .unwrap();

    ClientStats {
        time_handshaking,
        full_handshake_durations,
        resumed_handshake_durations,
    }
}

/// A scheduler that keeps track of idle server threads.
///
/// When a client wants to communicate with the server, it retrieves an idle server thread from the
/// scheduler. It also retrieves a `Receiver` that can be used to receive messages from the thread
/// (remember that `tokio` channels are single consumer).
struct ServerThreadScheduler {
    /// The total number of server threads
    total_server_threads: usize,
    /// The number of idle server threads
    idle_server_threads: Vec<(usize, Receiver<ServerMessage>)>,
    /// When a client is done, it gives back (recycles) the thread's `Receiver` through this channel
    server_thread_done_rx: Receiver<(usize, Receiver<ServerMessage>)>,
    /// A rng, used to choose the server thread when there are multiple available
    rng: Rng,
}

/// A sender meant to recycle server channels, so they can be used again by new tasks
type ServerChannelRecycler = Arc<Sender<(usize, Receiver<ServerMessage>)>>;

impl ServerThreadScheduler {
    fn new(client_rx: Vec<Receiver<ServerMessage>>) -> (Self, ServerChannelRecycler) {
        let total_servers = client_rx.len();

        let (server_done_tx, server_done_rx) =
            tokio::sync::mpsc::channel::<(usize, Receiver<ServerMessage>)>(total_servers);
        let idle_servers: Vec<_> = (0..).zip(client_rx).collect();

        let server_done_tx = Arc::new(server_done_tx);
        let scheduler = Self {
            total_server_threads: total_servers,
            idle_server_threads: idle_servers,
            server_thread_done_rx: server_done_rx,
            rng: Rng::with_seed(42),
        };

        (scheduler, server_done_tx)
    }

    /// Returns the index of an idle server thread, and its corresponding `Receiver`.
    ///
    /// Blocks if there are no idle server threads, waiting until one becomes available.
    async fn next_idle_server_thread(&mut self) -> (usize, Receiver<ServerMessage>) {
        if self.idle_server_threads.is_empty() {
            // Need to wait for one server thread to become available
            let server = self.server_thread_done_rx.recv().await.unwrap();
            self.idle_server_threads.push(server);
        }

        // We have at least one server thread available, but maybe we have received more in the
        // meantime. Let's get them without blocking.
        while let Ok(server) = self.server_thread_done_rx.try_recv() {
            self.idle_server_threads.push(server);
        }

        // Pick a server thread
        let i = self.rng.usize(0..self.idle_server_threads.len());
        self.idle_server_threads.swap_remove(i)
    }

    /// Waits until all server threads become idle
    async fn wait_until_all_idle(&mut self) {
        while self.idle_server_threads.len() < self.total_server_threads {
            let server = self.server_thread_done_rx.recv().await.unwrap();
            self.idle_server_threads.push(server);
        }
    }
}

/// A handshake server backed by multiple worker threads
struct MultiThreadedServer {
    /// The senders that a client can use to communicate with each thread
    client_tx: Arc<Vec<Sender<ClientMessage>>>,
    /// The receivers that a client can use to communicate with each thread
    client_rx: Vec<Receiver<ServerMessage>>,
    /// The worker threads' join handles
    join_handles: Vec<JoinHandle<ServerStats>>,
}

impl MultiThreadedServer {
    /// Start the server
    fn start(cache_size: usize, server_threads: usize, cli: &Cli) -> Self {
        let cache = ServerSessionMemoryCache::new(cache_size);

        let mut client_tx = Vec::new();
        let mut client_rx = Vec::new();
        let mut join_handles = Vec::new();

        for i in 0..server_threads {
            let rng_seed = i as u64 * 42;
            let (tx, rx, join_handle) =
                Self::start_thread(cache.clone(), rng_seed, server_threads, cli);
            client_tx.push(tx);
            client_rx.push(rx);
            join_handles.push(join_handle);
        }

        MultiThreadedServer {
            client_tx: Arc::new(client_tx),
            client_rx,
            join_handles,
        }
    }

    /// Starts a single server thread
    fn start_thread(
        cache: Arc<ServerSessionMemoryCache>,
        rng_seed: u64,
        server_threads: usize,
        cli: &Cli,
    ) -> (
        Sender<ClientMessage>,
        Receiver<ServerMessage>,
        JoinHandle<ServerStats>,
    ) {
        let (server_tx, client_rx) = tokio::sync::mpsc::channel(1);
        let (client_tx, mut server_rx) = tokio::sync::mpsc::channel(1);

        let mut stats = ServerStats::with_enough_capacity(
            server_threads,
            cli.full_handshakes,
            cli.resumed_handshakes_per_full_handshake,
        );
        let handle = std::thread::spawn(move || {
            let mut rng = Rng::with_seed(rng_seed);

            loop {
                let request = server_rx.blocking_recv().unwrap();
                if let ClientMessage::Shutdown = request {
                    break;
                }

                let start_resumed = Instant::now();
                if let ClientMessage::ResumedHandshake(key) = &request {
                    // Resumed handshakes call `ServerSessionMemoryCache::take` (in TLS 1.3)
                    cache.take(key.as_slice());
                }

                // All handshakes generate new tickets
                let start_full = Instant::now();
                let mut tickets = Vec::with_capacity(TICKETS_PER_HANDSHAKE as usize);
                for _ in 0..TICKETS_PER_HANDSHAKE {
                    // Keys are 32 bytes in TLS 1.3
                    let mut key = vec![0; 32];
                    rng.fill(&mut key);
                    tickets.push(key.clone());

                    // Values have variable length, but from the source code it looks like 80 is reasonable
                    let value = vec![0; 80];
                    cache.put(key, value);
                }

                let done = Instant::now();
                server_tx.blocking_send(ServerMessage(tickets)).unwrap();

                stats.handled_handshakes += 1;
                match &request {
                    ClientMessage::FullHandshake => {
                        stats.full_handshake_durations.push(done - start_full)
                    }
                    ClientMessage::ResumedHandshake(_) => {
                        stats.resumed_handshake_durations.push(done - start_resumed)
                    }
                    _ => (),
                }
            }

            stats
        });

        (client_tx, client_rx, handle)
    }

    /// Shutdown the server
    fn shutdown(self) -> Vec<ServerStats> {
        for tx in self.client_tx.iter() {
            tx.blocking_send(ClientMessage::Shutdown).unwrap();
        }
        let mut server_stats = Vec::new();
        for handle in self.join_handles {
            server_stats.push(handle.join().unwrap());
        }
        server_stats
    }
}

#[derive(Serialize)]
struct ThroughputStats {
    cache_size: usize,
    requests_per_second: f64,
    fh_client_side_seconds: f64,
    fh_server_side_seconds: f64,
    rh_client_side_seconds: f64,
    rh_server_side_seconds: f64,
}

struct ClientStats {
    time_handshaking: Duration,
    full_handshake_durations: Vec<Duration>,
    resumed_handshake_durations: Vec<Duration>,
}

struct ServerStats {
    handled_handshakes: u64,
    full_handshake_durations: Vec<Duration>,
    resumed_handshake_durations: Vec<Duration>,
}

impl ServerStats {
    fn with_enough_capacity(
        server_threads: usize,
        full_handshakes: u64,
        resumed_handshakes_per_full_handshake: u64,
    ) -> Self {
        // Requests are almost evenly distributed
        let full_handshake_durations =
            Vec::with_capacity(full_handshakes as usize / (server_threads - 1));
        let resumed_handshake_durations = Vec::with_capacity(
            (full_handshakes * resumed_handshakes_per_full_handshake) as usize
                / (server_threads - 1),
        );

        Self {
            handled_handshakes: 0,
            full_handshake_durations,
            resumed_handshake_durations,
        }
    }
}
