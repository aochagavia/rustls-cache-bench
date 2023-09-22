use std::fs::File;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use clap::Parser;
use crossbeam::channel::{Receiver, Sender, TrySendError};
use fastrand::Rng;
use mio::unix::SourceFd;
use mio::{Events, Interest, Poll, Token};
use rustls::server::{NoServerSessionStorage, ServerSessionMemoryCache, StoresServerSessions};
use serde::Serialize;
use timerfd::{ClockId, SetTimeFlags, TimerFd, TimerState};

/// The amount of tickets that are generated as part of a handshake
const TICKETS_PER_HANDSHAKE: u64 = 4; // rustls' default

/// A message from the client to one of the servers
enum ClientMessage {
    /// Shutdown the server thread
    Shutdown,
    /// Simulate the execution of a handshake
    ///
    /// For each handshake, the server determines whether it should be full or resumed according to
    /// `Cli::resumed_handshake_ratio`.
    Handshake { request_id: usize },
}

/// A message from the server to the client, meaning that the handshake has completed
struct ServerMessage {
    request_id: usize,
}

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    /// The cache sizes to benchmark (comma-separated)
    #[arg(long, value_delimiter(','))]
    cache_sizes: Vec<usize>,
    /// The amount of handshakes to execute per second
    #[arg(long, default_value("10000"))]
    handshakes_per_second: u64,
    /// The duration of each benchmark in seconds
    #[arg(long, default_value("2"))]
    duration_seconds: u64,
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
    /// maximum possible throughput for the provided batch size)
    #[arg(long)]
    noop: bool,
}

fn main() {
    let cli = Cli::parse();
    if cli.cache_sizes.is_empty() {
        println!("At least one value should be provided through --cache-sizes (run again with `--help` for details)");
    }
    if cli.resumed_handshake_ratio < 0.0 || cli.resumed_handshake_ratio > 1.0 {
        println!("--resumed-handshake-ratio should be between 0 and 1 (run again with `--help` for details)");
        return;
    }
    if cli.duration_seconds < 1 || cli.duration_seconds > 60 {
        println!(
            "--duration-seconds should be between 1 and 60 (run again with `--help` for details)"
        );
        return;
    }

    // Run benchmarks, once for each provided cache size
    for &cache_size in &cli.cache_sizes {
        bench_run(cache_size, &cli);
    }
}

/// Executes a single benchmark run, for a specific cache size
fn bench_run(cache_size: usize, cli: &Cli) {
    let server_threads = num_cpus::get_physical() - 1;
    if server_threads < 2 {
        panic!("These benchmarks only make sense on machines with at least 3 cores");
    }

    let cache: Arc<dyn StoresServerSessions> = if cli.noop {
        Arc::new(NoServerSessionStorage {})
    } else {
        ServerSessionMemoryCache::new(cache_size)
    };
    let server = MultiThreadedServer::start(cache, server_threads, cli);

    // Hammer the server from our client!
    let mut poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(2);

    let request_interval_micros = (1_000 * 1_000) / cli.handshakes_per_second;
    let request_interval = interval(Duration::from_micros(request_interval_micros));
    let request_fd = request_interval.as_raw_fd();
    poll.registry()
        .register(&mut SourceFd(&request_fd), Token(0), Interest::READABLE)
        .unwrap();

    let server_io_interval = interval(Duration::from_micros(10));
    let server_io_fd = server_io_interval.as_raw_fd();
    poll.registry()
        .register(&mut SourceFd(&server_io_fd), Token(1), Interest::READABLE)
        .unwrap();

    let max_handshakes = max_handshakes(cli) as usize;
    let mut request_start = Vec::with_capacity(max_handshakes);
    let mut request_end = vec![None; max_handshakes];
    let mut next_request_id = 0;
    let mut concurrent_requests = 0;

    let bench_start = Instant::now();
    let finish = bench_start + Duration::from_secs(cli.duration_seconds);
    loop {
        poll.poll(&mut events, None).unwrap();
        let handshakes_to_make = request_interval.read();
        let do_server_io = server_io_interval.read() > 0;
        let now = Instant::now();

        if do_server_io {
            // Actually enqueue started requests to the server (if the channel has capacity)
            while next_request_id < request_start.len() {
                match server.tx.try_send(ClientMessage::Handshake {
                    request_id: next_request_id,
                }) {
                    Ok(_) => {
                        next_request_id += 1;
                        concurrent_requests += 1;
                    }
                    Err(TrySendError::Full(_)) => break,
                    Err(TrySendError::Disconnected(_)) => unreachable!(),
                }
            }

            // Receive responses from the server
            while let Ok(msg) = server.rx.try_recv() {
                request_end[msg.request_id] = Some(now);
                concurrent_requests -= 1;
            }
        }

        if now < finish {
            // Start new requests
            for _ in 0..handshakes_to_make {
                request_start.push(now);
            }
        } else if concurrent_requests == 0 {
            // We are done, shut down the servers
            for _ in 0..server_threads {
                server.tx.try_send(ClientMessage::Shutdown).unwrap();
            }

            break;
        }
    }

    poll.registry()
        .deregister(&mut SourceFd(&request_fd))
        .unwrap();
    poll.registry()
        .deregister(&mut SourceFd(&server_io_fd))
        .unwrap();

    // Wait for the server threads to complete
    let server_stats = server
        .join_handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect::<Vec<_>>();

    // Gather client stats
    let time_handshaking = Instant::now() - bench_start;
    let mut client_durations = Vec::with_capacity(request_start.len());
    for (start, end) in request_start.into_iter().zip(request_end) {
        let end = end.unwrap();
        client_durations.push(end - start);
    }
    let client_stats = ClientStats {
        time_handshaking,
        handshake_durations: client_durations,
    };

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

    let total_handshakes: u64 = server_stats.iter().map(|s| s.handled_handshakes).sum();
    let server_count = server_stats.len();
    let throughput_stats = ThroughputStats {
        cache_size,
        handshakes_per_second: total_handshakes as f64
            / client_stats.time_handshaking.as_secs_f64(),
        server_saturation: handshake_server_total.as_secs_f64()
            / (client_stats.time_handshaking.as_secs_f64() * server_count as f64),
    };

    let file = File::create(base_dir.join("throughput.json")).unwrap();
    serde_json::to_writer(file, &throughput_stats).unwrap();
}

/// A handshake server backed by multiple worker threads
struct MultiThreadedServer {
    /// The channels that a client can use to communicate with each thread
    tx: Sender<ClientMessage>,
    rx: Receiver<ServerMessage>,
    /// The worker threads' join handles
    join_handles: Vec<JoinHandle<ServerStats>>,
}

impl MultiThreadedServer {
    /// Start the server
    fn start(cache: Arc<dyn StoresServerSessions>, server_threads: usize, cli: &Cli) -> Self {
        let mut join_handles = Vec::new();

        let (tx, server_rx) = crossbeam::channel::bounded(server_threads * 8);
        let (server_tx, rx) = crossbeam::channel::bounded(server_threads * 8);

        for i in 0..server_threads {
            let rng = Rng::with_seed(i as u64 * 42);
            let join_handle = Self::start_thread(
                server_tx.clone(),
                server_rx.clone(),
                cache.clone(),
                rng,
                server_threads,
                cli,
            );
            join_handles.push(join_handle);
        }

        MultiThreadedServer {
            tx,
            rx,
            join_handles,
        }
    }

    /// Starts a single server thread
    fn start_thread(
        server_tx: Sender<ServerMessage>,
        server_rx: Receiver<ClientMessage>,
        cache: Arc<dyn StoresServerSessions>,
        mut rng: Rng,
        server_threads: usize,
        cli: &Cli,
    ) -> JoinHandle<ServerStats> {
        // Keep a ticket around for resumed handshakes
        let mut ticket = generate_ticket(&mut rng);
        cache.put(ticket.clone(), generate_value());

        let resumed_handshake_ratio = cli.resumed_handshake_ratio;
        let mut stats = ServerStats::with_enough_capacity(server_threads, max_handshakes(cli));
        std::thread::spawn(move || {
            loop {
                let request = server_rx.recv().unwrap();
                let request_id = match request {
                    ClientMessage::Shutdown => break,
                    ClientMessage::Handshake { request_id } => request_id,
                };

                let start = Instant::now();

                let should_resume = rng.f64() < resumed_handshake_ratio;
                if should_resume {
                    // Resumed handshakes call `ServerSessionMemoryCache::take` (in TLS 1.3)
                    cache.take(&ticket);
                }

                // Generate the same amount of new tickets as rustls (though without a
                // cryptographically secure generator)
                let mut tickets = Vec::with_capacity(TICKETS_PER_HANDSHAKE as usize);
                for _ in 0..TICKETS_PER_HANDSHAKE {
                    let key = generate_ticket(&mut rng);
                    tickets.push(key.clone());
                    cache.put(key, generate_value());
                }

                ticket = tickets.into_iter().next().unwrap();

                let done = Instant::now();
                stats.handled_handshakes += 1;
                stats.handshake_durations.push(done - start);
                server_tx.send(ServerMessage { request_id }).unwrap();
            }

            stats
        })
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
    handshakes_per_second: f64,
    server_saturation: f64,
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

fn interval(duration: Duration) -> TimerFd {
    let mut tfd = TimerFd::new_custom(ClockId::Monotonic, true, false).unwrap();
    tfd.set_state(
        TimerState::Periodic {
            current: duration,
            interval: duration,
        },
        SetTimeFlags::Default,
    );
    tfd
}

fn max_handshakes(cli: &Cli) -> u64 {
    cli.handshakes_per_second * cli.duration_seconds + 100
}
