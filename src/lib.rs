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

use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaBuilder};
use datafusion::error::DataFusionError;
use datafusion::prelude::*;
use tokio::task::JoinSet;
use log::{info, warn, error, debug};
use tempfile::NamedTempFile;

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
    ) -> std::io::Result<()>;

    fn get_table_names(&self) -> Vec<&str>;

    fn get_table_ext(&self) -> &str;

    fn get_schema(&self, table: &str) -> Schema;
}

#[derive(Debug, Clone)]
pub struct PartitionConfig {
    table_partitions: HashMap<String, Vec<String>>,
}

impl PartitionConfig {
    pub fn new() -> Self {
        Self {
            table_partitions: HashMap::new(),
        }
    }

    pub fn with_table_partition(mut self, table: &str, columns: Vec<String>) -> Self {
        self.table_partitions.insert(table.to_string(), columns);
        self
    }

    pub fn get_partition_columns(&self, table: &str) -> Option<&Vec<String>> {
        self.table_partitions.get(table)
    }

    pub fn print_configuration(&self) {
        println!("📊 Partition Configuration:");
        for (table, columns) in &self.table_partitions {
            println!("   🗂️  {} partitioned by: {:?}", table, columns);
        }
        println!();
    }
}

#[derive(Debug, Clone)]
pub struct ConversionStats {
    pub total_rows: Arc<AtomicU64>,
    pub skipped_rows: Arc<AtomicU64>,
    pub processed_files: Arc<AtomicU64>,
    pub failed_files: Arc<AtomicU64>,
    pub table_name: String,
}

impl ConversionStats {
    pub fn new(table_name: String) -> Self {
        Self {
            total_rows: Arc::new(AtomicU64::new(0)),
            skipped_rows: Arc::new(AtomicU64::new(0)),
            processed_files: Arc::new(AtomicU64::new(0)),
            failed_files: Arc::new(AtomicU64::new(0)),
            table_name,
        }
    }

    pub fn add_processed_rows(&self, count: u64) {
        self.total_rows.fetch_add(count, Ordering::Relaxed);
    }

    pub fn add_skipped_rows(&self, count: u64) {
        self.skipped_rows.fetch_add(count, Ordering::Relaxed);
    }

    pub fn add_processed_file(&self) {
        self.processed_files.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_failed_file(&self) {
        self.failed_files.fetch_add(1, Ordering::Relaxed);
    }

    pub fn print_summary(&self) {
        let total = self.total_rows.load(Ordering::Relaxed);
        let skipped = self.skipped_rows.load(Ordering::Relaxed);
        let processed_files = self.processed_files.load(Ordering::Relaxed);
        let failed_files = self.failed_files.load(Ordering::Relaxed);
        
        println!("\n📊 CONVERSION SUMMARY for {}:", self.table_name);
        println!("   ✅ Total rows processed: {}", total);
        println!("   ⚠️  Rows skipped (errors): {}", skipped);
        println!("   📁 Files processed: {}", processed_files);
        println!("   ❌ Files failed: {}", failed_files);
        
        if total > 0 {
            let skip_rate = (skipped as f64 / total as f64) * 100.0;
            println!("   📈 Skip rate: {:.4}%", skip_rate);
        }
        
        if skipped > 0 {
            warn!("⚠️  {} rows were skipped due to parsing errors in table {}. Check logs for details.", skipped, self.table_name);
        }
    }
}

pub async fn convert_to_parquet_with_partitions_sequential(
    benchmark: &dyn Tpc,
    input_path: &str,
    output_path: &str,
    partition_config: Option<PartitionConfig>,
    resume: bool,
) -> datafusion::error::Result<()> {
    let table_names = benchmark.get_table_names();

    // Pre-collect all the data we need from the benchmark trait object
    let table_data: Vec<(String, Schema, String)> = table_names
        .iter()
        .map(|&table| {
            let mut schema_builder = SchemaBuilder::from(benchmark.get_schema(table).fields);
            schema_builder.push(Field::new("__placeholder", DataType::Utf8, true));
            let schema = schema_builder.finish();
            let table_ext = benchmark.get_table_ext().to_string();
            (table.to_string(), schema, table_ext)
        })
        .collect();

    // Process tables sequentially
    for (table_name, schema, table_ext) in table_data {
        // Check if already exists and we're resuming
        if resume && table_already_converted(&table_name, output_path) {
            println!("⏭️  Skipping {} (already exists)", table_name);
            continue;
        }

        convert_table(
            &table_name,
            schema,
            &table_ext,
            input_path,
            output_path,
            partition_config.as_ref(),
        ).await?;
    }

    println!("✅ All tables converted successfully!");
    Ok(())
}

pub async fn convert_to_parquet_with_partitions(
    benchmark: &dyn Tpc,
    input_path: &str,
    output_path: &str,
    partition_config: Option<PartitionConfig>,
    resume: bool,
) -> datafusion::error::Result<()> {
    let table_names = benchmark.get_table_names();
    let input_path = input_path.to_string();
    let output_path = output_path.to_string();

    // Pre-collect all the data we need from the benchmark trait object
    let mut table_data: Vec<(String, Schema, String)> = table_names
        .iter()
        .map(|&table| {
            let mut schema_builder = SchemaBuilder::from(benchmark.get_schema(table).fields);
            schema_builder.push(Field::new("__placeholder", DataType::Utf8, true));
            let schema = schema_builder.finish();
            let table_ext = benchmark.get_table_ext().to_string();
            (table.to_string(), schema, table_ext)
        })
        .collect();

    // Filter out already converted tables if resuming
    if resume {
        table_data.retain(|(table_name, _, _)| {
            let already_exists = table_already_converted(table_name, &output_path);
            if already_exists {
                println!("⏭️  Skipping {} (already exists)", table_name);
            }
            !already_exists
        });
    }

    let mut join_set = JoinSet::new();

    // Spawn a task for each remaining table
    for (table_name, schema, table_ext) in table_data {
        let input_path_clone = input_path.clone();
        let output_path_clone = output_path.clone();
        let partition_config_clone = partition_config.clone();

        join_set.spawn(async move {
            convert_table(
                &table_name,
                schema,
                &table_ext,
                &input_path_clone,
                &output_path_clone,
                partition_config_clone.as_ref(),
            ).await
        });
    }

    // Wait for all tasks to complete
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(table_result) => {
                if let Err(e) = table_result {
                    return Err(e);
                }
            }
            Err(join_error) => {
                return Err(DataFusionError::External(Box::new(join_error)));
            }
        }
    }

    println!("✅ All tables converted successfully!");
    Ok(())
}

fn table_already_converted(table_name: &str, output_path: &str) -> bool {
    let output_dir = format!("{}/{}.snappy.parquet", output_path, table_name);
    let path = Path::new(&output_dir);

    if !path.exists() {
        return false;
    }

    // Check if directory has parquet files
    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries {
            if let Ok(entry) = entry {
                if entry.path().extension().and_then(|s| s.to_str()) == Some("parquet") {
                    return true;
                }
                // Also check subdirectories for partitioned data
                if entry.path().is_dir() {
                    if let Ok(sub_entries) = fs::read_dir(entry.path()) {
                        for sub_entry in sub_entries {
                            if let Ok(sub_entry) = sub_entry {
                                if sub_entry.path().extension().and_then(|s| s.to_str()) == Some("parquet") {
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    false
}

// Keep the original function for backward compatibility
pub async fn convert_to_parquet(
    benchmark: &dyn Tpc,
    input_path: &str,
    output_path: &str,
) -> datafusion::error::Result<()> {
    convert_to_parquet_with_partitions(benchmark, input_path, output_path, None, false).await
}

async fn convert_table(
    table: &str,
    schema: Schema,
    table_ext: &str,
    input_path: &str,
    output_path: &str,
    partition_config: Option<&PartitionConfig>,
) -> datafusion::error::Result<()> {
    let partition_cols = partition_config
        .and_then(|config| config.get_partition_columns(table))
        .cloned();

    let is_partitioned = partition_cols.is_some();

    if is_partitioned {
        println!("🗂️  Converting table {} with HIVE partitioning", table);
    } else {
        println!("📄 Converting table {}", table);
    }

    let stats = Arc::new(ConversionStats::new(table.to_string()));

    let file_ext = format!(".{}", table_ext);
    let options = CsvReadOptions::new()
        .schema(&schema)
        .has_header(false)
        .delimiter(b'|')
        .file_extension(&file_ext);

    let path = format!("{}/{}.{}", input_path, table, table_ext);
    let path = Path::new(&path);
    if !path.exists() {
        return Err(DataFusionError::External(Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("path does not exist: {:?}", path)
        ))));
    }

    // create output dir
    let output_dir_name = format!("{}/{}.snappy.parquet", output_path, table);
    let output_dir = Path::new(&output_dir_name);

    // Create directory if it doesn't exist (instead of panicking)
    if !output_dir.exists() {
        println!("📁 Creating directory: {}", output_dir.display());
        fs::create_dir(&output_dir)?;
    } else {
        println!("📁 Using existing directory: {}", output_dir.display());
    }

    let x = PathBuf::from(path);
    let mut file_vec = vec![];

    if x.is_dir() {
        let files = fs::read_dir(path)?;
        for file in files {
            let file = file?;
            file_vec.push(file.path());
        }
    } else {
        // Single file case
        file_vec.push(x);
    }

    let mut part = 0;
    for file_path in &file_vec {
        let file_name = file_path.file_name().unwrap().to_str().unwrap();
        let stub = if file_name.ends_with(".dat") || file_name.ends_with(".tbl") {
            &file_name[0..file_name.len() - 4] // remove .dat or .tbl
        } else {
            file_name
        };

        let output_parts_dir = format!("{}/{}-temp.parquet", output_dir.display(), stub);
        println!("⚡ Writing {}", output_parts_dir);
        let options = options.clone();

        convert_tbl_robust(
            file_path,
            &output_parts_dir,
            &options,
            "parquet",
            "snappy",
            8192,
            partition_cols.clone(),
            stats.clone(),
        )
        .await?;

        // Handle the output based on what was actually created
        let output_path_obj = Path::new(&output_parts_dir);

        if output_path_obj.is_dir() {
            // Directory was created (partitioned case)
            if partition_cols.is_some() {
                // For partitioned data, copy the entire directory structure
                copy_partitioned_data(&output_parts_dir, &output_dir.display().to_string(), &mut part)?;
            } else {
                // Read files from the directory
                let paths = fs::read_dir(&output_parts_dir)?;
                for path in paths {
                    let path = path?;
                    if path.path().is_file() && path.path().extension().and_then(|s| s.to_str()) == Some("parquet") {
                        let dest_file = format!("{}/part-{}.parquet", output_dir.display(), part);
                        part += 1;
                        let dest_path = Path::new(&dest_file);
                        move_or_copy(&path.path(), &dest_path)?;
                    }
                }
            }

            println!("🗑️  Removing {}", output_parts_dir);
            fs::remove_dir_all(Path::new(&output_parts_dir))?;
        } else if output_path_obj.is_file() {
            // Single file was created (non-partitioned case)
            let dest_file = format!("{}/part-{}.parquet", output_dir.display(), part);
            part += 1;
            let dest_path = Path::new(&dest_file);
            move_or_copy(&output_path_obj, &dest_path)?;

            println!("🗑️  Removing {}", output_parts_dir);
            // File will be moved, so no need to remove
        } else {
            return Err(DataFusionError::Execution(format!(
                "Expected output path {} to be either a file or directory, but it doesn't exist",
                output_parts_dir
            )));
        }
    }

    // Print conversion statistics
    stats.print_summary();

    if is_partitioned {
        println!("✅ Completed conversion of table {} with partitioning", table);
        if let Some(partition_columns) = &partition_cols {
            println!("🗂️  Partitioning by: {:?}", partition_columns);
        }
    } else {
        println!("✅ Completed conversion of table {}", table);
    }
    Ok(())
}

// Helper function to copy partitioned data while maintaining directory structure
fn copy_partitioned_data(
    source_dir: &str,
    dest_dir: &str,
    part_counter: &mut usize,
) -> std::io::Result<()> {
    fn copy_recursive(src: &Path, dst: &Path, part_counter: &mut usize) -> std::io::Result<()> {
        if src.is_dir() {
            fs::create_dir_all(dst)?;
            for entry in fs::read_dir(src)? {
                let entry = entry?;
                let src_path = entry.path();
                let dst_path = dst.join(entry.file_name());
                copy_recursive(&src_path, &dst_path, part_counter)?;
            }
        } else if src.extension().and_then(|s| s.to_str()) == Some("parquet") {
            // For partitioned data, maintain the original filename structure
            let parent = dst.parent().unwrap();
            fs::create_dir_all(parent)?;
            move_or_copy(src, dst)?;
            *part_counter += 1;
        }
        Ok(())
    }

    copy_recursive(Path::new(source_dir), Path::new(dest_dir), part_counter)
}

pub(crate) fn move_or_copy(
    source_path: &Path,
    dest_path: &Path,
) -> std::io::Result<()> {
    if is_same_device(&source_path, &dest_path)? {
        fs::rename(&source_path, &dest_path)
    } else {
        fs::copy(&source_path, &dest_path)?;
        fs::remove_file(&source_path)
    }
}

#[cfg(unix)]
fn is_same_device(path1: &Path, path2: &Path) -> std::io::Result<bool> {
    use std::os::unix::fs::MetadataExt;
    let meta1 = fs::metadata(path1)?;
    let meta2 = fs::metadata(path2.parent().unwrap())?;
    Ok(meta1.dev() == meta2.dev())
}

#[cfg(windows)]
fn is_same_device(path1: &Path, path2: &Path) -> std::io::Result<bool> {
    use std::os::windows::fs::MetadataExt;
    let meta1 = fs::metadata(path1)?;
    let meta2 = fs::metadata(path2.parent().unwrap())?;
    Ok(meta1.volume_serial_number() == meta2.volume_serial_number())
}

pub async fn convert_tbl_robust(
    input_path: &Path,
    output_filename: &str,
    options: &CsvReadOptions<'_>,
    file_format: &str,
    _compression: &str,
    batch_size: usize,
    partition_cols: Option<Vec<String>>,
    stats: Arc<ConversionStats>,
) -> datafusion::error::Result<()> {
    let start = Instant::now();
    
    info!("🔄 Starting robust conversion of: {}", input_path.display());

    let config = SessionConfig::new().with_batch_size(batch_size);
    let ctx = SessionContext::new_with_config(config);

    // First attempt: try normal DataFusion reading
    match try_normal_conversion(&ctx, input_path, output_filename, options, file_format, partition_cols.clone()).await {
        Ok(_) => {
            stats.add_processed_file();
            info!("✅ Successfully converted {} without errors", input_path.display());
            println!("⏱️  Conversion completed in {} ms", start.elapsed().as_millis());
            return Ok(());
        }
        Err(e) => {
            warn!("⚠️  Normal conversion failed for {}: {}. Switching to robust mode.", input_path.display(), e);
        }
    }

    // Fallback: robust line-by-line processing
    match try_robust_conversion(input_path, output_filename, options, file_format, partition_cols, stats.clone()).await {
        Ok(_) => {
            stats.add_processed_file();
            info!("✅ Robust conversion completed for {}", input_path.display());
        }
        Err(e) => {
            stats.add_failed_file();
            error!("❌ Failed to convert {} even with robust mode: {}", input_path.display(), e);
            return Err(e);
        }
    }

    println!("⏱️  Conversion completed in {} ms", start.elapsed().as_millis());
    Ok(())
}

async fn try_normal_conversion(
    ctx: &SessionContext,
    input_path: &Path,
    output_filename: &str,
    options: &CsvReadOptions<'_>,
    file_format: &str,
    partition_cols: Option<Vec<String>>,
) -> datafusion::error::Result<()> {
    let csv_filename = format!("{}", input_path.display());
    let mut df = ctx.read_csv(&csv_filename, options.clone()).await?;
    let schema = df.schema();

    let selection = df
        .schema()
        .fields()
        .iter()
        .take(schema.fields().len() - 1)
        .map(|d| Expr::Column(Column::new(None::<String>, d.name())))
        .collect();

    df = df.select(selection)?;

    write_dataframe(df, output_filename, file_format, partition_cols).await
}

async fn try_robust_conversion(
    input_path: &Path,
    output_filename: &str,
    options: &CsvReadOptions<'_>,
    file_format: &str,
    partition_cols: Option<Vec<String>>,
    stats: Arc<ConversionStats>,
) -> datafusion::error::Result<()> {
    info!("🛠️  Starting robust line-by-line processing for {}", input_path.display());

    // Create a temporary cleaned file
    let mut temp_file = NamedTempFile::new()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let file = std::fs::File::open(input_path)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let reader = BufReader::new(file);

    let mut line_number = 0u64;
    let mut skipped_lines = 0u64;
    let mut processed_lines = 0u64;

    // Process line by line with error handling
    for line_result in reader.lines() {
        line_number += 1;
        
        match line_result {
            Ok(line) => {
                if line.trim().is_empty() {
                    continue;
                }

                // Validate and potentially fix the line
                match validate_and_fix_line(&line, options.schema.as_ref().unwrap(), line_number) {
                    Ok(fixed_line) => {
                        writeln!(temp_file, "{}", fixed_line)
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;
                        processed_lines += 1;
                    }
                    Err(e) => {
                        debug!("⚠️  Skipping line {}: {}", line_number, e);
                        skipped_lines += 1;
                    }
                }
            }
            Err(e) => {
                warn!("⚠️  Error reading line {}: {}", line_number, e);
                skipped_lines += 1;
            }
        }

        // Report progress for large files
        if line_number % 1_000_000 == 0 {
            info!("📊 Processed {} million lines, skipped {}", line_number / 1_000_000, skipped_lines);
        }
    }

    stats.add_processed_rows(processed_lines);
    stats.add_skipped_rows(skipped_lines);

    info!("📋 Line processing complete: {} processed, {} skipped", processed_lines, skipped_lines);

    // Now process the cleaned temporary file with DataFusion
    let temp_path = temp_file.path();
    let config = SessionConfig::new().with_batch_size(8192);
    let ctx = SessionContext::new_with_config(config);
    
    let csv_filename = format!("{}", temp_path.display());
    let mut df = ctx.read_csv(&csv_filename, options.clone()).await?;
    let schema = df.schema();

    let selection = df
        .schema()
        .fields()
        .iter()
        .take(schema.fields().len() - 1)
        .map(|d| Expr::Column(Column::new(None::<String>, d.name())))
        .collect();

    df = df.select(selection)?;

    write_dataframe(df, output_filename, file_format, partition_cols).await?;

    Ok(())
}

fn validate_and_fix_line(line: &str, schema: &Schema, line_number: u64) -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let fields: Vec<&str> = line.split('|').collect();
    let expected_fields = schema.fields().len() - 1; // Minus placeholder column

    if fields.len() != expected_fields {
        return Err(format!("Expected {} fields, got {}", expected_fields, fields.len()).into());
    }

    let mut fixed_fields = Vec::new();

    for (i, field_value) in fields.iter().enumerate() {
        if i >= schema.fields().len() - 1 {
            break; // Skip placeholder column
        }

        let field = &schema.fields()[i];
        let cleaned_value = match field.data_type() {
            DataType::Int64 => {
                if field_value.trim().is_empty() {
                    "".to_string()
                } else {
                    match field_value.trim().parse::<i64>() {
                        Ok(_) => field_value.trim().to_string(),
                        Err(_) => {
                            // Try to handle overflow by capping at max value
                            if let Ok(val) = field_value.trim().parse::<u64>() {
                                if val > i64::MAX as u64 {
                                    warn!("⚠️  Integer overflow at line {}, field {}: {} -> {}", 
                                          line_number, i, field_value, i64::MAX);
                                    i64::MAX.to_string()
                                } else {
                                    field_value.trim().to_string()
                                }
                            } else {
                                return Err(format!("Invalid integer in field {}: {}", i, field_value).into());
                            }
                        }
                    }
                }
            }
            DataType::Decimal128(_, _) => {
                if field_value.trim().is_empty() {
                    "".to_string()
                } else {
                    match field_value.trim().parse::<f64>() {
                        Ok(_) => field_value.trim().to_string(),
                        Err(_) => return Err(format!("Invalid decimal in field {}: {}", i, field_value).into()),
                    }
                }
            }
            DataType::Utf8 => {
                // Remove any problematic characters
                field_value.replace('\0', "").replace('\r', "").replace('\n', " ")
            }
            DataType::Date32 => {
                if field_value.trim().is_empty() {
                    "".to_string()
                } else {
                    field_value.trim().to_string()
                }
            }
            _ => field_value.to_string(),
        };

        fixed_fields.push(cleaned_value);
    }

    Ok(fixed_fields.join("|"))
}

async fn write_dataframe(
    df: DataFrame,
    output_filename: &str,
    file_format: &str,
    partition_cols: Option<Vec<String>>,
) -> datafusion::error::Result<()> {
    match file_format {
        "parquet" => {
            use datafusion::dataframe::DataFrameWriteOptions;
            let mut write_options = DataFrameWriteOptions::new();

            if let Some(partition_columns) = partition_cols {
                let schema_fields: Vec<_> = df.schema().fields().iter().map(|f| f.name()).collect();
                for col in &partition_columns {
                    if !schema_fields.contains(&col) {
                        return Err(DataFusionError::Plan(format!(
                            "Partition column '{}' not found in table schema. Available columns: {:?}",
                            col, schema_fields
                        )));
                    }
                }

                info!("🗂️  Partitioning by: {:?}", partition_columns);
                write_options = write_options.with_partition_by(partition_columns);
            }

            df.write_parquet(output_filename, write_options, None).await?;
        }
        _ => {
            return Err(DataFusionError::NotImplemented(format!(
                "Invalid output format: {}",
                file_format
            )));
        }
    }
    Ok(())
}

// Legacy function for backward compatibility
pub async fn convert_tbl(
    input_path: &Path,
    output_filename: &str,
    options: &CsvReadOptions<'_>,
    file_format: &str,
    compression: &str,
    batch_size: usize,
    partition_cols: Option<Vec<String>>,
) -> datafusion::error::Result<()> {
    let stats = Arc::new(ConversionStats::new("legacy".to_string()));
    convert_tbl_robust(input_path, output_filename, options, file_format, compression, batch_size, partition_cols, stats).await
}



