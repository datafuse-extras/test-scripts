use anyhow::Result;

use clap::Parser;
use clap::Subcommand;
use env_logger::Env;
use log::info;

mod auto_vacuum;
mod change_tracking;
mod explict_txn;
mod multi_table_insert;
mod util;
mod vacuum2;

use auto_vacuum::Args as AutoVacuumArgs;
use change_tracking::Args as ChangeTrackingArgs;
use change_tracking::ChangeTrackingSuite;
use vacuum2::Args as Vacuum2Args;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    ChangeTracking(ChangeTrackingArgs),
    ExplicitTxn,
    MultiTableInsert,
    AutoVacuum(AutoVacuumArgs),
    Vacuum2(Vacuum2Args),
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    let dsn = std::env::var("DATABEND_DSN").unwrap_or(
        "databend://root:@localhost:8000/default?sslmode=disable&enable_experimental_merge_into=1"
            .to_owned(),
    );

    info!("using DSN {}", dsn);
    match args.command {
        Commands::ChangeTracking(cmd_args) => ChangeTrackingSuite::run(cmd_args, dsn).await,
        Commands::ExplicitTxn => explict_txn::run(dsn).await,
        Commands::MultiTableInsert => multi_table_insert::run(dsn).await,
        Commands::AutoVacuum(cmd_args) => auto_vacuum::run(cmd_args, dsn).await,
        Commands::Vacuum2(cmd_args) => vacuum2::run(cmd_args, dsn).await,
    }
}
