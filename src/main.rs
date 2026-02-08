// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::Result;
use std::path::PathBuf;

use log::{debug, error, info, warn};
use structopt::StructOpt;

use tpctools::tpcds::TpcDs;
use tpctools::tpch::TpcH;
use tpctools::{convert_to_parquet, Tpc};

/// Raise the file-descriptor soft limit to the hard limit.
///
/// DataFusion's hive-partitioned parquet writer opens one file per unique
/// partition value simultaneously. TPC-DS fact tables can have ~2,200+
/// distinct date surrogate keys, which exceeds the macOS default soft limit
/// of 256. Raising to the hard limit (typically 10,240+) avoids "Too many
/// open files" errors without requiring user-side OS tuning.
#[cfg(unix)]
fn raise_fd_limit() {
    use libc::{getrlimit, rlimit, setrlimit, RLIMIT_NOFILE};
    unsafe {
        let mut rlim = rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if getrlimit(RLIMIT_NOFILE, &mut rlim) != 0 {
            warn!("failed to query file descriptor limit");
            return;
        }
        if rlim.rlim_cur >= rlim.rlim_max {
            return;
        }
        let old = rlim.rlim_cur;
        rlim.rlim_cur = rlim.rlim_max;
        if setrlimit(RLIMIT_NOFILE, &rlim) == 0 {
            info!("raised file descriptor limit from {} to {}", old, rlim.rlim_max);
        } else {
            warn!("failed to raise file descriptor limit");
        }
    }
}

#[cfg(not(unix))]
fn raise_fd_limit() {
    // Windows default is 8,192+ handles — no adjustment needed.
}

#[derive(Debug, StructOpt)]
struct GenerateOpt {
    /// TPC benchmark to use (tpcds or tpch)
    #[structopt(short, long)]
    benchmark: String,

    /// Scale factor
    #[structopt(short, long)]
    scale: usize,

    /// Number of partitions to generate in parallel
    #[structopt(short, long)]
    partitions: usize,

    /// Path to tpcds-kit
    #[structopt(short, long, parse(from_os_str))]
    generator_path: PathBuf,

    /// Output path
    #[structopt(short, long, parse(from_os_str))]
    output: PathBuf,
}

#[derive(Debug, StructOpt)]
struct ConvertOpt {
    /// TPC benchmark to use (tpcds or tpch)
    #[structopt(short, long)]
    benchmark: String,

    /// Path to csv files
    #[structopt(parse(from_os_str), required = true, short = "i", long = "input")]
    input_path: PathBuf,

    /// Output path
    #[structopt(parse(from_os_str), required = true, short = "o", long = "output")]
    output_path: PathBuf,

    /// Write fact tables as Hive-partitioned parquet (column=value/ subdirectories)
    #[structopt(long)]
    hive_partition: bool,

    /// Number of parallel threads for conversion (0 = auto-detect based on available CPUs)
    #[structopt(short = "j", long, default_value = "0")]
    concurrency: usize,

    /// Batch size for reading CSV files
    #[structopt(long, default_value = "8192")]
    batch_size: usize,

    /// Compression codec for parquet output (snappy, zstd, none)
    #[structopt(long, default_value = "snappy")]
    compression: String,

    /// Enable dictionary encoding for parquet columns (default: off for faster conversion)
    #[structopt(long)]
    dictionary: bool,
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = "tpctools",
    about = "Tools for generating and converting TPC data sets."
)]
struct Opt {
    /// Enable verbose (debug-level) logging
    #[structopt(short, long, global = true)]
    verbose: bool,

    #[structopt(subcommand)]
    cmd: Command,
}

#[derive(Debug, StructOpt)]
enum Command {
    Generate(GenerateOpt),
    Convert(ConvertOpt),
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::from_args();
    if std::env::var("RUST_LOG").is_err() {
        let level = if opt.verbose { "debug" } else { "info" };
        std::env::set_var("RUST_LOG", level);
    }
    env_logger::init();

    raise_fd_limit();
    match opt.cmd {
        Command::Generate(opt) => {
            let scale = opt.scale;
            let partitions = opt.partitions;

            if !opt.generator_path.exists() {
                error!(
                    "generator path does not exist: {}",
                    opt.generator_path.display()
                );
                std::process::exit(1);
            }

            if !opt.output.exists() {
                error!("output path does not exist: {}", opt.output.display());
                std::process::exit(1);
            }

            let generator_path = format!("{}", opt.generator_path.display());
            let output_path_str = format!("{}", opt.output.display());

            let tpc = create_benchmark(&opt.benchmark);

            tpc.generate(scale, partitions, &generator_path, &output_path_str)?;
        }
        Command::Convert(opt) => {
            let tpc = create_benchmark(&opt.benchmark);
            let concurrency = if opt.concurrency == 0 {
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(1)
            } else {
                opt.concurrency
            };
            debug!(
                "convert settings: concurrency={}, batch_size={}, compression={}, hive_partition={}, dictionary={}",
                concurrency, opt.batch_size, opt.compression, opt.hive_partition, opt.dictionary
            );
            match convert_to_parquet(
                tpc.as_ref(),
                opt.input_path.as_path().to_str().unwrap(),
                opt.output_path.as_path().to_str().unwrap(),
                opt.hive_partition,
                concurrency,
                opt.batch_size,
                &opt.compression,
                opt.dictionary,
            )
            .await
            {
                Ok(_) => {}
                Err(e) => error!("conversion failed: {:?}", e),
            }
        }
    }

    Ok(())
}

fn create_benchmark(name: &str) -> Box<dyn Tpc> {
    match name {
        "tpcds" | "tpc-ds" => Box::new(TpcDs::new()),
        "tpch" | "tpc-h" => Box::new(TpcH::new()),
        _ => panic!("invalid benchmark name"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_benchmark_tpch() {
        let b = create_benchmark("tpch");
        assert_eq!(b.get_table_ext(), "tbl");
        assert_eq!(b.get_table_names().len(), 8);
    }

    #[test]
    fn create_benchmark_tpc_h_alias() {
        let b = create_benchmark("tpc-h");
        assert_eq!(b.get_table_ext(), "tbl");
        assert_eq!(b.get_table_names().len(), 8);
    }

    #[test]
    fn create_benchmark_tpcds() {
        let b = create_benchmark("tpcds");
        assert_eq!(b.get_table_ext(), "dat");
        assert_eq!(b.get_table_names().len(), 24);
    }

    #[test]
    fn create_benchmark_tpc_ds_alias() {
        let b = create_benchmark("tpc-ds");
        assert_eq!(b.get_table_ext(), "dat");
        assert_eq!(b.get_table_names().len(), 24);
    }

    #[test]
    #[should_panic(expected = "invalid benchmark name")]
    fn create_benchmark_invalid_name_panics() {
        create_benchmark("invalid");
    }

    #[test]
    #[should_panic(expected = "invalid benchmark name")]
    fn create_benchmark_empty_string_panics() {
        create_benchmark("");
    }

    #[test]
    #[cfg(unix)]
    fn raise_fd_limit_succeeds() {
        use libc::{getrlimit, rlimit, RLIMIT_NOFILE};
        raise_fd_limit();
        unsafe {
            let mut rlim = rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            assert_eq!(getrlimit(RLIMIT_NOFILE, &mut rlim), 0);
            assert_eq!(rlim.rlim_cur, rlim.rlim_max);
        }
    }
}
