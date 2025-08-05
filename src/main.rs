use clap::{Parser, Subcommand};
use number_range::NumberRangeOptions;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;
use types::Network;

pub mod commands;
mod historical_checkpoint_reader;
pub mod types;
mod utils;

#[derive(Parser)]
#[command(name = "iota consistency check")]
#[command(about = "Verify data consistency between IOTA network services and checkpoint data", long_about = None)]
struct App {
    #[clap(long, default_value = "INFO", env = "LOG_LEVEL")]
    log_level: Level,

    #[arg(short, long, default_value_t = Network::Mainnet)]
    network: Network,
    /// Fetch a specific range of checkpoints from the kv store
    #[arg(
        short,
        long,
        value_name = "1,2,3-5,10",
        value_parser(number_range_parser)
    )]
    // We must enforce the complete path to the Vec to make the parse succeed:
    // https://github.com/clap-rs/clap/issues/4679#issuecomment-1419303347
    checkpoints: std::vec::Vec<usize>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Test consistency of the Key Value store
    KvStore {
        /// The instance ID of the BigTableDB [default: value provided by the
        /// `--network` argument]
        #[arg(long)]
        instance_id: Option<String>,
        /// The column family to use for the Key Value pairs
        #[arg(long, default_value_t = String::from("iota"))]
        column_family: String,
    },
    /// Test consistency of the Key value store trough REST API
    RestKv,
}

fn number_range_parser(num: &str) -> anyhow::Result<Vec<usize>> {
    let mut values = NumberRangeOptions::default()
        .with_range_sep('-')
        .parse(num)?
        .collect::<Vec<usize>>();

    if values.is_empty() {
        return Err(anyhow::anyhow!("No checkpoints specified"));
    }

    values.sort();

    Ok(values)
}

/// Initialize the tracing with custom subscribers
fn init_tracing(log_level: Level) {
    let subscriber = FmtSubscriber::builder().with_max_level(log_level).finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app = App::parse();

    init_tracing(app.log_level);

    match app.command {
        Command::KvStore {
            instance_id,
            column_family,
        } => {
            let instance_id = instance_id.unwrap_or_else(|| app.network.to_string());
            commands::kv_store::run(app.network, instance_id, column_family, app.checkpoints).await
        }
        Command::RestKv => commands::rest_kv::run(app.network, app.checkpoints).await,
    }
}
