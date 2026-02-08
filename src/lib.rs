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
use std::io::Result;
use std::path::{Path, PathBuf};
use std::time::Instant;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaBuilder};
use datafusion::common::config::TableParquetOptions;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::DataFusionError;
use datafusion::prelude::*;

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

pub async fn convert_to_parquet(
    benchmark: &dyn Tpc,
    input_path: &str,
    output_path: &str,
    hive_partition: bool,
) -> datafusion::error::Result<()> {
    for table in benchmark.get_table_names() {
        println!("Converting table {}", table);
        let schema = benchmark.get_schema(table);

        // Append a placeholder field to absorb the trailing delimiter
        // that TPC data generators add at the end of every line.
        // TPC-H schemas already include a trailing "ignore" field, so skip those.
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

        let file_ext = format!(".{}", benchmark.get_table_ext());
        let options = CsvReadOptions::new()
            .schema(&csv_schema)
            .delimiter(b'|')
            .has_header(false)
            .file_extension(&file_ext);

        let path = format!("{}/{}.{}", input_path, table, benchmark.get_table_ext());
        let path = Path::new(&path);
        if !path.exists() {
            panic!("path does not exist: {:?}", path);
        }

        // create output dir
        let output_dir_name = format!("{}/{}.parquet", output_path, table);
        let output_dir = Path::new(&output_dir_name);
        if output_dir.exists() {
            panic!("output dir already exists: {}", output_dir.display());
        }

        if hive_partition {
            if let Some(partition_col) = benchmark.get_partition_col(table) {
                println!(
                    "Writing hive-partitioned parquet for {} (partition by {})",
                    table, partition_col
                );
                let path_str = format!("{}", path.display());
                let config = SessionConfig::new().with_batch_size(8192);
                let ctx = SessionContext::new_with_config(config);
                let df = ctx.read_csv(&path_str, options.clone()).await?;

                let trailing_col = if has_trailing_ignore {
                    "ignore"
                } else {
                    "__trailing_delimiter"
                };
                let df = df.drop_columns(&[trailing_col])?;

                let mut table_parquet_options = TableParquetOptions::default();
                table_parquet_options.global.compression =
                    Some("snappy".to_string());

                let write_options = DataFrameWriteOptions::new()
                    .with_partition_by(vec![partition_col.to_string()]);

                df.write_parquet(
                    &output_dir_name,
                    write_options,
                    Some(table_parquet_options),
                )
                .await?;

                continue;
            }
        }

        println!("Creating directory: {}", output_dir.display());
        fs::create_dir(output_dir)?;

        let x = PathBuf::from(path);
        let mut file_vec = vec![];
        if x.is_dir() {
            let files = fs::read_dir(path)?;
            for file in files {
                let file = file?;
                file_vec.push(file);
            }
        }

        for (part, file) in file_vec.iter().enumerate() {
            let dest_file = format!("{}/part-{}.parquet", output_dir.display(), part);
            println!("Writing {}", dest_file);
            let options = options.clone();
            convert_tbl(&file.path(), &dest_file, &options, "parquet", "snappy", 8192).await?;
        }
    }

    Ok(())
}

pub(crate) fn move_or_copy(
    source_path: &Path,
    dest_path: &Path,
) -> std::result::Result<(), std::io::Error> {
    if is_same_device(source_path, dest_path)? {
        println!(
            "Moving {} to {}",
            source_path.display(),
            dest_path.display()
        );
        fs::rename(source_path, dest_path)
    } else {
        println!(
            "Copying {} to {}",
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
    input_path: &Path,
    output_filename: &str,
    options: &CsvReadOptions<'_>,
    file_format: &str,
    compression: &str,
    batch_size: usize,
) -> datafusion::error::Result<()> {
    println!(
        "Converting '{}' to {}",
        input_path.display(),
        output_filename
    );

    let start = Instant::now();

    let config = SessionConfig::new().with_batch_size(batch_size);
    let ctx = SessionContext::new_with_config(config);

    // build plan to read the TBL file
    let csv_filename = format!("{}", input_path.display());
    let df = ctx.read_csv(&csv_filename, options.clone()).await?;

    match file_format {
        "csv" => {
            df.write_csv(output_filename, DataFrameWriteOptions::new(), None)
                .await?;
        }
        "parquet" => {
            let compression_str = match compression {
                "none" => "uncompressed",
                "snappy" => "snappy",
                "lz4" => "lz4",
                "lz0" => "lzo",
                other => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Invalid compression format: {}",
                        other
                    )))
                }
            };
            let mut table_parquet_options = TableParquetOptions::default();
            table_parquet_options.global.compression = Some(compression_str.to_string());
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
    println!("Conversion completed in {} ms", start.elapsed().as_millis());

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
