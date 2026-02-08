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

use std::fs;
use std::future::Future;
use std::io::Result;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::time::Instant;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaBuilder};
use log::{debug, info};
use datafusion::common::config::TableParquetOptions;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::DataFusionError;
use datafusion::prelude::*;
use futures::stream::{self, StreamExt};

pub mod tpcds;
pub mod tpch;

#[async_trait]
pub trait Tpc {
    fn generate(
        &self,
        scale: usize,
        partitions: usize,
        input_path: &str,
        output_path: &str,
    ) -> Result<()>;

    fn get_table_names(&self) -> Vec<&str>;

    fn get_table_ext(&self) -> &str;

    fn get_schema(&self, table: &str) -> Schema;

    fn get_partition_col(&self, table: &str) -> Option<&str> {
        let _ = table;
        None
    }
}

/// Map a user-facing compression name to the string DataFusion expects.
fn normalize_compression(name: &str) -> datafusion::error::Result<String> {
    match name {
        "none" => Ok("uncompressed".to_string()),
        "snappy" => Ok("snappy".to_string()),
        "zstd" => Ok("zstd(4)".to_string()),
        "lz4" => Ok("lz4".to_string()),
        "lz0" => Ok("lzo".to_string()),
        other => Err(DataFusionError::NotImplemented(format!(
            "Invalid compression format: {}",
            other,
        ))),
    }
}

fn build_parquet_options(compression: &str, batch_size: usize, dictionary: bool) -> datafusion::error::Result<TableParquetOptions> {
    let mut opts = TableParquetOptions::default();
    opts.global.compression = Some(normalize_compression(compression)?);
    opts.global.dictionary_enabled = Some(dictionary);
    opts.global.write_batch_size = batch_size;
    Ok(opts)
}

#[allow(clippy::too_many_arguments)]
pub async fn convert_to_parquet(
    benchmark: &dyn Tpc,
    input_path: &str,
    output_path: &str,
    hive_partition: bool,
    concurrency: usize,
    batch_size: usize,
    compression: &str,
    dictionary: bool,
) -> datafusion::error::Result<()> {
    let start = Instant::now();
    let concurrency = if concurrency == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    } else {
        concurrency
    };
    debug!("effective concurrency: {}", concurrency);

    let table_ext = benchmark.get_table_ext().to_string();
    let tables: Vec<(String, Schema, Option<String>)> = benchmark
        .get_table_names()
        .into_iter()
        .map(|t| {
            let schema = benchmark.get_schema(t);
            let partition_col = benchmark.get_partition_col(t).map(|s| s.to_string());
            (t.to_string(), schema, partition_col)
        })
        .collect();

    let tables_count = tables.len();
    info!(
        "converting {} tables from '{}' to '{}'",
        tables_count,
        input_path,
        output_path
    );

    // Shared context for non-hive single-file conversions (target_partitions=1)
    let shared_config = SessionConfig::new()
        .with_batch_size(batch_size)
        .with_target_partitions(1);
    let shared_ctx = SessionContext::new_with_config(shared_config);

    // Prepare all work items upfront (validation + directory creation),
    // then process them lazily with bounded concurrency via buffer_unordered.
    type WorkFuture = Pin<Box<dyn Future<Output = datafusion::error::Result<()>> + Send>>;
    let mut work: Vec<WorkFuture> = Vec::new();

    for (table, schema, partition_col) in tables {
        let has_trailing_ignore = schema
            .fields
            .last()
            .is_some_and(|f| f.name() == "ignore");
        let csv_schema = if has_trailing_ignore {
            schema.clone()
        } else {
            let mut builder = SchemaBuilder::from(schema.fields);
            builder.push(Field::new("__trailing_delimiter", DataType::Utf8, true));
            builder.finish()
        };

        let file_ext = format!(".{}", table_ext);
        let path = format!("{}/{}.{}", input_path, table, table_ext);
        if !Path::new(&path).exists() {
            return Err(DataFusionError::Execution(format!(
                "path does not exist: {:?}",
                path
            )));
        }

        let output_dir_name = format!("{}/{}.parquet", output_path, table);
        if Path::new(&output_dir_name).exists() {
            return Err(DataFusionError::Execution(format!(
                "output dir already exists: {}",
                output_dir_name
            )));
        }

        if hive_partition {
            if let Some(partition_col) = partition_col {
                let comp = compression.to_string();
                let target_parts = std::cmp::min(4, concurrency);

                work.push(Box::pin(async move {
                    let start = Instant::now();
                    info!(
                        "writing hive-partitioned parquet for '{}' (partition by {})",
                        table, partition_col
                    );
                    let config = SessionConfig::new()
                        .with_batch_size(batch_size)
                        .with_target_partitions(target_parts);
                    let ctx = SessionContext::new_with_config(config);
                    let options = CsvReadOptions::new()
                        .schema(&csv_schema)
                        .delimiter(b'|')
                        .has_header(false)
                        .file_extension(&file_ext);
                    let df = ctx.read_csv(&path, options).await?;

                    let trailing_col = if has_trailing_ignore {
                        "ignore"
                    } else {
                        "__trailing_delimiter"
                    };
                    let df = df.drop_columns(&[trailing_col])?;
                    let df = df.sort(vec![col(&partition_col).sort(true, true)])?;

                    let table_parquet_options =
                        build_parquet_options(&comp, batch_size, dictionary)?;
                    let write_options =
                        DataFrameWriteOptions::new().with_partition_by(vec![partition_col]);

                    df.write_parquet(&output_dir_name, write_options, Some(table_parquet_options))
                        .await?;

                    info!(
                        "conversion of '{}' (hive-partitioned) completed in {} ms",
                        table,
                        start.elapsed().as_millis()
                    );
                    Ok(())
                }));
                continue;
            }
        }

        // Non-hive: create output dir, enumerate partition files
        debug!("creating directory: {}", output_dir_name);
        fs::create_dir(Path::new(&output_dir_name))?;

        let dir_path = PathBuf::from(&path);
        let mut file_vec = vec![];
        if dir_path.is_dir() {
            let files = fs::read_dir(&dir_path)?;
            for file in files {
                let file = file?;
                file_vec.push(file);
            }
        }

        debug!(
            "found {} partition files for table '{}'",
            file_vec.len(),
            table
        );

        for (part, file) in file_vec.iter().enumerate() {
            let dest_file = format!("{}/part-{}.parquet", output_dir_name, part);
            let file_path = file.path();
            let comp = compression.to_string();
            let csv_schema = csv_schema.clone();
            let file_ext = file_ext.clone();
            let ctx = shared_ctx.clone();

            work.push(Box::pin(async move {
                debug!("writing {}", dest_file);
                let options = CsvReadOptions::new()
                    .schema(&csv_schema)
                    .delimiter(b'|')
                    .has_header(false)
                    .file_extension(&file_ext);
                let trailing_col = if csv_schema
                    .fields
                    .last()
                    .is_some_and(|f| f.name() == "ignore")
                {
                    "ignore"
                } else {
                    "__trailing_delimiter"
                };
                convert_tbl(
                    &ctx,
                    &file_path,
                    &dest_file,
                    &options,
                    "parquet",
                    &comp,
                    batch_size,
                    dictionary,
                    &[trailing_col],
                )
                .await
            }));
        }
    }

    info!(
        "prepared {} work items, processing with concurrency={}",
        work.len(),
        concurrency
    );

    // Process all work items with bounded concurrency (lazy — no semaphore needed)
    let results: Vec<datafusion::error::Result<()>> = stream::iter(work)
        .buffer_unordered(concurrency)
        .collect()
        .await;

    for result in results {
        result?;
    }

    info!(
        "conversion of all {} tables completed in {} ms",
        tables_count,
        start.elapsed().as_millis()
    );
    Ok(())
}

pub(crate) fn move_or_copy(
    source_path: &Path,
    dest_path: &Path,
) -> std::result::Result<(), std::io::Error> {
    if is_same_device(source_path, dest_path)? {
        debug!(
            "moving {} to {}",
            source_path.display(),
            dest_path.display()
        );
        fs::rename(source_path, dest_path)
    } else {
        debug!(
            "copying {} to {}",
            source_path.display(),
            dest_path.display()
        );
        fs::copy(source_path, dest_path)?;
        fs::remove_file(source_path)
    }
}

#[cfg(unix)]
fn is_same_device(path1: &Path, path2: &Path) -> std::result::Result<bool, std::io::Error> {
    use std::os::unix::fs::MetadataExt;
    let meta1 = fs::metadata(path1)?;
    let meta2 = fs::metadata(path2.parent().unwrap())?;
    Ok(meta1.dev() == meta2.dev())
}

#[cfg(windows)]
fn is_same_device(path1: &Path, path2: &Path) -> std::result::Result<bool, std::io::Error> {
    use std::os::windows::fs::MetadataExt;
    let meta1 = fs::metadata(path1)?;
    let meta2 = fs::metadata(path2.parent().unwrap())?;
    Ok(meta1.volume_serial_number() == meta2.volume_serial_number())
}

pub async fn convert_tbl(
    ctx: &SessionContext,
    input_path: &Path,
    output_filename: &str,
    options: &CsvReadOptions<'_>,
    file_format: &str,
    compression: &str,
    batch_size: usize,
    dictionary: bool,
    drop_cols: &[&str],
) -> datafusion::error::Result<()> {
    debug!(
        "converting '{}' to {}",
        input_path.display(),
        output_filename
    );

    let start = Instant::now();

    // build plan to read the TBL file
    let csv_filename = format!("{}", input_path.display());
    let df = ctx.read_csv(&csv_filename, options.clone()).await?;
    let df = if drop_cols.is_empty() { df } else { df.drop_columns(drop_cols)? };

    match file_format {
        "csv" => {
            df.write_csv(output_filename, DataFrameWriteOptions::new(), None)
                .await?;
        }
        "parquet" => {
            let table_parquet_options = build_parquet_options(compression, batch_size, dictionary)?;
            df.write_parquet(
                output_filename,
                DataFrameWriteOptions::new(),
                Some(table_parquet_options),
            )
            .await?;
        }
        other => {
            return Err(DataFusionError::NotImplemented(format!(
                "Invalid output format: {}",
                other
            )))
        }
    }
    info!(
        "conversion of '{}' completed in {} ms",
        input_path.display(),
        start.elapsed().as_millis()
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn move_or_copy_same_device() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("source.txt");
        let dest = dir.path().join("dest.txt");
        fs::write(&src, b"hello").unwrap();

        move_or_copy(&src, &dest).unwrap();

        assert!(!src.exists());
        assert!(dest.exists());
        assert_eq!(fs::read_to_string(&dest).unwrap(), "hello");
    }

    #[test]
    fn move_or_copy_into_subdirectory() {
        let dir = TempDir::new().unwrap();
        let sub = dir.path().join("sub");
        fs::create_dir(&sub).unwrap();

        let src = dir.path().join("file.txt");
        let dest = sub.join("file.txt");
        fs::write(&src, b"data").unwrap();

        move_or_copy(&src, &dest).unwrap();

        assert!(!src.exists());
        assert!(dest.exists());
        assert_eq!(fs::read_to_string(&dest).unwrap(), "data");
    }

    #[test]
    fn move_or_copy_large_file_preserves_data() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("large.bin");
        let dest = dir.path().join("large_dest.bin");

        let data: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();
        {
            let mut f = fs::File::create(&src).unwrap();
            f.write_all(&data).unwrap();
        }

        move_or_copy(&src, &dest).unwrap();

        assert!(!src.exists());
        let read_back = fs::read(&dest).unwrap();
        assert_eq!(read_back.len(), 1_000_000);
        assert_eq!(read_back, data);
    }

    #[test]
    fn move_or_copy_nonexistent_source_returns_err() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("does_not_exist.txt");
        let dest = dir.path().join("dest.txt");

        let result = move_or_copy(&src, &dest);
        assert!(result.is_err());
    }

    #[test]
    fn is_same_device_same_directory() {
        let dir = TempDir::new().unwrap();
        let file1 = dir.path().join("a.txt");
        let file2 = dir.path().join("b.txt");
        fs::write(&file1, b"a").unwrap();

        let result = is_same_device(&file1, &file2).unwrap();
        assert!(result);
    }
}
