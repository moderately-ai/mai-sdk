use anyhow::Result;
use clap::{Parser, ValueEnum};
use mai_sdk_core::handler::Startable;
use mai_sdk_runtime::state::RuntimeState;
use mai_sdk_runtime::state::RuntimeStateArgs;
use slog::Drain;
use slog::Logger;
use std::fs::OpenOptions;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, ValueEnum)]
enum LoggingMode {
    Stdout,
    File,
    None,
}

#[derive(Debug, Clone, ValueEnum)]
enum RuntimeVariant {
    /// This node will join the network but will not participate in task execution
    Relay,

    /// This node will join the network and will participate in task execution
    /// NOTE: the worker node will still be able to relay messages
    Worker,
}

#[derive(Parser, Debug, Clone)]
struct Args {
    /// List of listen addresses for the network to use
    #[clap(long, env)]
    pub gossip_listen_addrs: Vec<String>,

    /// List of bootstrap addresses for the network to use
    #[clap(long, env)]
    pub bootstrap_addrs: Vec<String>,

    /// Gossipsub heartbeat interval
    #[clap(long, default_value = "10", env)]
    pub gossipsub_heartbeat_interval: u64,

    /// Logging mode
    #[clap(long, default_value = "none", env)]
    pub log_mode: LoggingMode,

    /// Log level
    #[clap(long, default_value = "info", env)]
    pub log_level: String,

    /// Log path
    #[clap(long, default_value = "./debug.log", env)]
    pub log_path: String,

    /// Ping interval
    #[clap(long, default_value = "30", env)]
    pub ping_interval: u64,

    /// Runtime variant
    #[clap(long, default_value = "worker", env)]
    pub runtime_variant: RuntimeVariant,
}

fn get_logger(args: &Args) -> Result<Logger> {
    match args.log_mode {
        LoggingMode::File => {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&args.log_path)?;

            let decorator = slog_term::PlainSyncDecorator::new(file);
            let drain = slog_term::FullFormat::new(decorator).build().fuse();
            let drain = slog_async::Async::new(drain)
                .overflow_strategy(slog_async::OverflowStrategy::Block)
                .build()
                .fuse();
            Ok(slog::Logger::root(drain, slog::o!()))
        }
        LoggingMode::Stdout => {
            let decorator = slog_term::TermDecorator::new().build();
            let drain = slog_term::FullFormat::new(decorator).build().fuse();
            let drain = slog_async::Async::new(drain)
                .overflow_strategy(slog_async::OverflowStrategy::Block)
                .build()
                .fuse();
            Ok(slog::Logger::root(drain, slog::o!()))
        }
        LoggingMode::None => Ok(slog::Logger::root(slog::Discard, slog::o!())),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Setup signal handling
    let term = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&term))?;

    // Parse the command line arguments
    let args = Args::parse();

    // Setup the runtime
    let runtime = match args.runtime_variant {
        RuntimeVariant::Worker => RuntimeState::new_worker(RuntimeStateArgs {
            logger: get_logger(&args)?,
            listen_addrs: args.gossip_listen_addrs.clone(),
            bootstrap_addrs: args.bootstrap_addrs.clone(),
            ping_interval: Duration::from_secs(args.ping_interval),
            gossipsub_heartbeat_interval: Duration::from_secs(args.gossipsub_heartbeat_interval),
            psk: None,
        }),
        _ => todo!(),
    };

    // Start the runtime
    runtime.start().await?;

    Ok(())
}
