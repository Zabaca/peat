//! `storcon_records`: dump, compare and copy the storage controller's records between Postgres
//! and Kubernetes objects, and rebuild them from the copy in a bucket, with the controller
//! stopped. storage_controller/kubernetes/README.md, "The copy tool", is its manual.

use clap::{Parser, Subcommand};
use storage_controller::records_tool::{self, DEFAULT_GENERATION_MARGIN, Outcome, Store, Stores};
use utils::logging::{self, LogFormat};

#[derive(Parser)]
#[command(about = "Move the storage controller's records between its stores, with it stopped")]
struct Cli {
    /// The Postgres store's address (or DATABASE_URL), as the controller's `--database-url`.
    #[arg(long, env = "DATABASE_URL", hide_env_values = true, global = true)]
    database_url: Option<String>,
    /// The Kubernetes store's namespace; by default the kubeconfig's or the pod's own.
    #[arg(long, global = true)]
    records_namespace: Option<String>,
    /// The copy in the bucket, as the controller's `--records-copy-storage`.
    #[arg(long, global = true)]
    records_copy_storage: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Every record as one JSON document
    Dump {
        #[arg(long, value_enum)]
        from: Store,
        /// A file to write to, instead of standard output
        #[arg(long)]
        output: Option<std::path::PathBuf>,
    },
    /// Every difference between two stores, field by field
    Compare {
        #[arg(value_enum)]
        a: Store,
        #[arg(value_enum)]
        b: Store,
        /// The report as one JSON document
        #[arg(long)]
        json: bool,
    },
    /// Copy every record from one store into the other, then compare them
    Copy {
        #[arg(long, value_enum)]
        from: Store,
        #[arg(long, value_enum)]
        to: Store,
        /// Remove what the destination holds first
        #[arg(long)]
        replace: bool,
    },
    /// Rebuild the records from the copy in the bucket, every generation raised
    Restore {
        #[arg(long, value_enum)]
        to: Store,
        /// The pageservers' own remote_storage setting, to read the generations their store holds
        #[arg(long)]
        pageserver_storage: String,
        /// Remove what the destination holds first
        #[arg(long)]
        replace: bool,
        /// How far above the highest generation seen each shard's is set
        #[arg(long, default_value_t = DEFAULT_GENERATION_MARGIN)]
        generation_margin: u32,
        /// For safekeepers that need one
        #[arg(long, env = "SAFEKEEPER_JWT_TOKEN", hide_env_values = true)]
        safekeeper_jwt_token: Option<String>,
    },
}

#[tokio::main]
async fn main() {
    logging::init(
        LogFormat::Plain,
        logging::TracingErrorLayerEnablement::Disabled,
        logging::Output::Stderr,
    )
    .expect("logging");
    let cli = Cli::parse();
    let stores = Stores {
        kubeconfig: None,
        database_url: cli.database_url,
        records_namespace: cli.records_namespace,
        records_copy_storage: cli.records_copy_storage,
    };
    let code = match run(&stores, cli.command).await {
        Ok(outcome) => {
            match &outcome {
                Outcome::Done => {}
                Outcome::Refused(reason) => eprintln!("refused: {reason}"),
                Outcome::Differ(diff) => {
                    eprintln!("the stores differ in {} places:", diff.len());
                    for d in diff {
                        eprintln!("  {d}");
                    }
                }
            }
            outcome.exit_code()
        }
        Err(e) => {
            eprintln!("error: {e:#}");
            1
        }
    };
    std::process::exit(code);
}

async fn run(stores: &Stores, command: Command) -> anyhow::Result<Outcome> {
    match command {
        Command::Dump { from, output } => {
            let doc = records_tool::dump(stores, from).await?;
            match output {
                Some(path) => std::fs::write(path, doc + "\n")?,
                None => println!("{doc}"),
            }
            Ok(Outcome::Done)
        }
        Command::Compare { a, b, json } => {
            let (outcome, counts) = records_tool::compare(stores, a, b).await?;
            if json {
                let differences = match &outcome {
                    Outcome::Differ(d) => d.clone(),
                    _ => Vec::new(),
                };
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "equal": outcome == Outcome::Done,
                        "counts": counts,
                        "differences": differences,
                    }))?
                );
            } else {
                for (kind, (x, y)) in &counts {
                    println!("{kind}: {x} {y}");
                }
                if outcome == Outcome::Done {
                    println!("equal");
                }
            }
            Ok(outcome)
        }
        Command::Copy { from, to, replace } => {
            let outcome = records_tool::copy(stores, from, to, replace).await?;
            if outcome == Outcome::Done {
                println!("copied and compared: equal");
            }
            Ok(outcome)
        }
        Command::Restore {
            to,
            pageserver_storage,
            replace,
            generation_margin,
            safekeeper_jwt_token,
        } => {
            let (outcome, report) = records_tool::restore(
                stores,
                to,
                &pageserver_storage,
                safekeeper_jwt_token,
                replace,
                generation_margin,
            )
            .await?;
            if let Some(t) = report.copy_written_at {
                println!("the copy was written at {t}");
            }
            println!(
                "{} shards' generations raised; {} timelines' membership read from their safekeepers",
                report.shards_raised, report.timelines_read_from_safekeepers
            );
            for t in &report.timelines_no_safekeeper_answered {
                println!("no safekeeper answered for timeline {t}: its membership is the copy's");
            }
            for t in &report.tenants_in_store_not_in_copy {
                println!("tenant {t} has data in the pageservers' store but no record in the copy");
            }
            if outcome == Outcome::Done {
                println!("restored and compared: equal");
            }
            Ok(outcome)
        }
    }
}
