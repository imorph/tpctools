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

use structopt::StructOpt;
use tpctools::PartitionConfig;

#[derive(Debug, StructOpt)]
#[structopt(name = "tpctools", about = "TPC Benchmark Tools - Robust High-Performance Data Converter")]
enum Opt {
    Convert {
        #[structopt(long, short = "b", help = "Benchmark type (tpch or tpcds)")]
        benchmark: String,
        #[structopt(long, short = "i", help = "Input directory path")]
        input: String,
        #[structopt(long, short = "o", help = "Output directory path")]
        output: String,
        #[structopt(long, help = "Enable default partitioning schemes")]
        enable_partitioning: bool,
        #[structopt(long, help = "Custom partitions (format: table1:col1,col2;table2:col3)")]
        custom_partitions: Option<String>,
        #[structopt(long, help = "Resume conversion, skip already converted tables")]
        resume: bool,
        #[structopt(long, help = "Process tables sequentially (safer for resource-constrained environments)")]
        sequential: bool,
    },
    Generate {
        #[structopt(long, short = "b", help = "Benchmark type (tpch or tpcds)")]
        benchmark: String,
        #[structopt(long, short = "s", help = "Scale factor")]
        scale: usize,
        #[structopt(long, short = "p", help = "Number of partitions")]
        partitions: usize,
        #[structopt(long, short = "o", help = "Output directory path")]
        output: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize comprehensive logging
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .format_timestamp_secs()
        .init();

    log::info!("🚀 TPC Tools - High-Performance Data Converter Starting");

    let opt = Opt::from_args();

    match opt {
        Opt::Convert {
            benchmark,
            input,
            output,
            enable_partitioning,
            custom_partitions,
            resume,
            sequential,
        } => {
            let benchmark_impl: Box<dyn tpctools::Tpc> = match benchmark.as_str() {
                "tpch" => Box::new(tpctools::tpch::TpcH {}),
                "tpcds" => Box::new(tpctools::tpcds::TpcDs {}),
                _ => {
                    eprintln!("❌ Unknown benchmark: {}. Supported: tpch, tpcds", benchmark);
                    std::process::exit(1);
                }
            };

            // Check system limits if processing large datasets
            check_system_limits();

            let partition_config = if enable_partitioning || custom_partitions.is_some() {
                let mut config = if enable_partitioning {
                    get_default_partition_config(&benchmark)
                } else {
                    PartitionConfig::new()
                };

                if let Some(custom_config_str) = custom_partitions {
                    let custom_config = parse_custom_partition_config(&custom_config_str);
                    // Merge custom config with default (custom takes precedence)
                    for table in benchmark_impl.get_table_names() {
                        if let Some(custom_columns) = custom_config.get_partition_columns(table) {
                            config = config.with_table_partition(table, custom_columns.clone());
                        }
                    }
                }

                config.print_configuration();
                Some(config)
            } else {
                None
            };

            // Print conversion strategy
            if sequential {
                println!("🔄 Processing tables sequentially to avoid resource limits...");
                println!("💡 This is safer for large datasets and resource-constrained environments");
                tpctools::convert_to_parquet_with_partitions_sequential(
                    benchmark_impl.as_ref(),
                    &input,
                    &output,
                    partition_config,
                    resume,
                ).await?;
            } else {
                println!("⚡ Processing tables in parallel for maximum performance...");
                println!("💡 Use --sequential if you encounter resource limits");
                tpctools::convert_to_parquet_with_partitions(
                    benchmark_impl.as_ref(),
                    &input,
                    &output,
                    partition_config,
                    resume,
                ).await?;
            }

            println!("🎉 Conversion completed successfully!");
        }
        Opt::Generate {
            benchmark,
            scale,
            partitions,
            output,
        } => {
            let benchmark_impl: Box<dyn tpctools::Tpc> = match benchmark.as_str() {
                "tpch" => Box::new(tpctools::tpch::TpcH {}),
                "tpcds" => Box::new(tpctools::tpcds::TpcDs {}),
                _ => {
                    eprintln!("❌ Unknown benchmark: {}. Supported: tpch, tpcds", benchmark);
                    std::process::exit(1);
                }
            };

            println!("📊 Generating {} data at scale {} with {} partitions", benchmark, scale, partitions);
            benchmark_impl.generate(scale, partitions, "", &output)?;
            println!("✅ Data generation completed!");
        }
    }

    log::info!("🏁 TPC Tools completed successfully");
    Ok(())
}

fn check_system_limits() {
    println!("🔍 Checking system limits...");

    // Try to get current ulimit -n value
    if let Ok(output) = std::process::Command::new("sh")
        .arg("-c")
        .arg("ulimit -n")
        .output()
    {
        if let Ok(limit_str) = String::from_utf8(output.stdout) {
            if let Ok(limit) = limit_str.trim().parse::<u32>() {
                println!("📊 Current file descriptor limit: {}", limit);
                if limit < 8192 {
                    println!("⚠️  WARNING: File descriptor limit is low ({}).", limit);
                    println!("   For large datasets, consider increasing it:");
                    println!("   - Temporary: ulimit -n 65536");
                    println!("   - Or use --sequential flag for safer processing");
                    println!();
                } else {
                    println!("✅ File descriptor limit looks good for parallel processing");
                }
            }
        }
    }

    // Check available memory
    if let Ok(output) = std::process::Command::new("free")
        .arg("-h")
        .output()
    {
        if let Ok(memory_info) = String::from_utf8(output.stdout) {
            println!("💾 Memory information:");
            for line in memory_info.lines().take(2) {
                println!("   {}", line);
            }
        }
    }
    println!();
}

fn get_default_partition_config(benchmark: &str) -> PartitionConfig {
    match benchmark {
        "tpcds" => {
            PartitionConfig::new()
                .with_table_partition("store_sales", vec!["ss_sold_date_sk".to_string()])
                .with_table_partition("store_returns", vec!["sr_returned_date_sk".to_string()])
                .with_table_partition("catalog_sales", vec!["cs_sold_date_sk".to_string()])
                .with_table_partition("catalog_returns", vec!["cr_returned_date_sk".to_string()])
                .with_table_partition("web_sales", vec!["ws_sold_date_sk".to_string()])
                .with_table_partition("web_returns", vec!["wr_returned_date_sk".to_string()])
                .with_table_partition("inventory", vec!["inv_date_sk".to_string()])
        },
        "tpch" => {
            PartitionConfig::new()
                .with_table_partition("lineitem", vec!["l_shipdate".to_string()])
                .with_table_partition("orders", vec!["o_orderdate".to_string()])
        },
        _ => PartitionConfig::new(),
    }
}

fn parse_custom_partition_config(config_str: &str) -> PartitionConfig {
    let mut partition_config = PartitionConfig::new();

    // Format: table1:col1,col2;table2:col3,col4
    for table_config in config_str.split(';') {
        let parts: Vec<&str> = table_config.split(':').collect();
        if parts.len() == 2 {
            let table_name = parts[0].trim();
            let columns: Vec<String> = parts[1]
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();

            if !columns.is_empty() {
                partition_config = partition_config.with_table_partition(table_name, columns.clone());
                println!("📋 Custom partition: {} by {:?}", table_name, columns);
            }
        } else {
            eprintln!("⚠️  Invalid partition config format: {}", table_config);
            eprintln!("   Expected format: table1:col1,col2;table2:col3");
        }
    }

    partition_config
}

