//! `storcon_records`: dump, compare and copy the storage controller's records between Postgres
//! and Kubernetes objects, scan the pageservers' store for the generations it holds, and rebuild
//! the records from the copy in a bucket, with the controller stopped.
//! storage_controller/kubernetes/README.md, "The copy tool", is its manual.

use std::collections::BTreeSet;

use clap::{Args, Parser, Subcommand};
use storage_controller::records_tool::{
    self, DEFAULT_GENERATION_MARGIN, Outcome, RestoreOptions, Running, Scan, Store, Stores,
};
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

/// The check that nothing still runs, for every command that writes.
#[derive(Args)]
struct RunningArgs {
    /// Where the controller is, when the records name no leader (no --address-for-peers)
    #[arg(long)]
    controller_address: Option<String>,
    /// Skip the check that the controller (and pageservers) are stopped, having checked by hand
    #[arg(long)]
    assume_stopped: bool,
}

impl RunningArgs {
    fn running(&self) -> Running {
        Running {
            controller_address: self.controller_address.clone(),
            assume_stopped: self.assume_stopped,
        }
    }
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
        /// With --replace, from a source with no pageserver: the destination is meant to be
        /// emptied (refused from a source with pageservers; name losses with --accept-older)
        #[arg(long, requires = "replace")]
        empty_destination: bool,
        /// With --replace: records (`<Kind>/<name>`: Tenant/<tenant id>, Timeline/<name>) the
        /// destination holds at higher generations than the source's, or that the source lacks,
        /// each meant to be lowered or dropped
        #[arg(long, requires = "replace", value_delimiter = ',')]
        accept_older: Vec<String>,
        /// From kubernetes into postgres: go without --records-copy-storage, and so without the
        /// check of the source against the copy's highest-ever values
        #[arg(long)]
        no_bucket_check: bool,
        /// Tenants and timelines (`<Kind>/<name>`) remade under their id, whose generations are
        /// below what the copy has seen
        #[arg(long, value_delimiter = ',')]
        accept_remade: Vec<String>,
        #[command(flatten)]
        running: RunningArgs,
    },
    /// Read the pageservers' store for the generation each tenant shard holds (with the
    /// pageservers' credentials, after every pageserver is stopped)
    ScanGenerations {
        /// The pageservers' own remote_storage setting
        #[arg(long)]
        pageserver_storage: String,
        /// The file to write, for `restore --generations`
        #[arg(long)]
        output: std::path::PathBuf,
    },
    /// Rebuild the records from the copy in the bucket, every generation raised
    Restore {
        #[arg(long, value_enum)]
        to: Store,
        /// The file `scan-generations` wrote
        #[arg(long)]
        generations: std::path::PathBuf,
        /// Remove what the destination holds first
        #[arg(long)]
        replace: bool,
        /// How far above the highest generation seen each shard's is set
        #[arg(long, default_value_t = DEFAULT_GENERATION_MARGIN)]
        generation_margin: u32,
        /// Records the store shows and the copy lacks (`<Kind>/<name>`), to go on without
        #[arg(long, value_delimiter = ',')]
        leave_out: Vec<String>,
        /// Go on for timelines a safekeeper did not answer for, their membership generation
        /// raised by the margin
        #[arg(long)]
        accept_silent_safekeepers: bool,
        /// For safekeepers that need one
        #[arg(long, env = "SAFEKEEPER_JWT_TOKEN", hide_env_values = true)]
        safekeeper_jwt_token: Option<String>,
        #[command(flatten)]
        running: RunningArgs,
    },
    /// The scale measure (storage_controller/kubernetes/measure.md): a storage server's restart
    /// with this many databases on it, on records of its own in a namespace it makes and deletes
    /// (and on --database-url, an empty Postgres database, when given)
    MeasureRestart {
        #[arg(long, default_value_t = 1000)]
        tenants: usize,
        /// Leave the measure's namespace and its records in place
        #[arg(long)]
        keep: bool,
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
        Command::MeasureRestart { tenants, keep } => {
            let lines = records_tool::measure::measure_restart(&records_tool::measure::Measure {
                tenants,
                kubeconfig: stores.kubeconfig.clone(),
                database_url: stores.database_url.clone(),
                keep,
            })
            .await?;
            for line in lines {
                println!("{line}");
            }
            Ok(Outcome::Done)
        }
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
        Command::Copy {
            from,
            to,
            replace,
            empty_destination,
            accept_older,
            no_bucket_check,
            accept_remade,
            running,
        } => {
            let accept: BTreeSet<String> = accept_remade.into_iter().collect();
            let options = records_tool::CopyOptions {
                replace,
                empty_destination,
                accept_older: accept_older.into_iter().collect(),
                no_bucket_check,
            };
            let outcome =
                records_tool::copy(stores, from, to, options, &running.running(), &accept).await?;
            if outcome == Outcome::Done {
                println!("copied and compared: equal");
            }
            Ok(outcome)
        }
        Command::ScanGenerations {
            pageserver_storage,
            output,
        } => {
            let scan = records_tool::scan_generations(&pageserver_storage).await?;
            std::fs::write(&output, serde_json::to_vec_pretty(&scan)?)?;
            println!(
                "{} tenant shards and {} tenants' timelines found in {}, at {}",
                scan.shards.len(),
                scan.timelines.len(),
                scan.storage,
                scan.scanned_at
            );
            Ok(Outcome::Done)
        }
        Command::Restore {
            to,
            generations,
            replace,
            generation_margin,
            leave_out,
            accept_silent_safekeepers,
            safekeeper_jwt_token,
            running,
        } => {
            let scan: Scan = serde_json::from_slice(&std::fs::read(&generations)?)?;
            println!(
                "generations from {}, scanned at {}",
                scan.storage, scan.scanned_at
            );
            let options = RestoreOptions {
                replace,
                margin: generation_margin,
                leave_out: leave_out.into_iter().collect(),
                accept_silent_safekeepers,
                safekeeper_jwt: safekeeper_jwt_token,
            };
            let (outcome, report) =
                records_tool::restore(stores, to, &scan, &options, &running.running()).await?;
            if let Some(t) = report.copy_written_at {
                println!("the copy was written at {t}");
            }
            println!(
                "{} shards' generations raised; {} timelines' membership read from every member",
                report.shards_raised, report.timelines_read_from_safekeepers
            );
            for t in &report.timelines_kept_silent {
                println!(
                    "timeline {t}: a safekeeper did not answer; its membership is the copy's, its generation raised"
                );
            }
            for t in &report.left_out {
                println!("left out (data in the store, no record): {t}");
            }
            if outcome == Outcome::Done {
                println!("restored and compared: equal");
            }
            Ok(outcome)
        }
    }
}
