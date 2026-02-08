use std::fs;
use std::path::Path;

use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::prelude::*;
use datafusion::prelude::SessionConfig;
use tempfile::TempDir;

use tpctools::tpch::TpcH;
use tpctools::{convert_tbl, convert_to_parquet, Tpc};

/// Create a small pipe-delimited nation fixture file (5 rows, TPC-H format).
/// Format: n_nationkey|n_name|n_regionkey|n_comment|
fn create_nation_tbl(path: &Path) {
    let data = "\
0|ALGERIA|0|haggle. carefully final deposits detect slyly agai|
1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold|
2|BRAZIL|1|y alongside of the pending deposits. carefully special|
3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are|
4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are|
";
    fs::write(path, data).unwrap();
}

/// Create a small pipe-delimited region fixture file (3 rows, TPC-H format).
fn create_region_tbl(path: &Path) {
    let data = "\
0|AFRICA|lar deposits. blithely final packages cajole.|
1|AMERICA|hs use ironic, even requests. s|
2|ASIA|ges. thinly even pinto beans ca|
";
    fs::write(path, data).unwrap();
}

fn nation_schema() -> Schema {
    TpcH::new().get_schema("nation")
}

fn region_schema() -> Schema {
    TpcH::new().get_schema("region")
}

fn csv_options(schema: &Schema) -> CsvReadOptions<'_> {
    CsvReadOptions::new()
        .schema(schema)
        .delimiter(b'|')
        .has_header(false)
        .file_extension(".tbl")
}

fn test_ctx() -> SessionContext {
    let config = SessionConfig::new()
        .with_batch_size(8192)
        .with_target_partitions(1);
    SessionContext::new_with_config(config)
}

// --- convert_tbl tests ---

#[tokio::test]
async fn convert_tbl_to_parquet_snappy() {
    let dir = TempDir::new().unwrap();
    let input = dir.path().join("nation.tbl");
    create_nation_tbl(&input);

    let output = dir.path().join("nation_output.parquet");
    let output_str = output.to_str().unwrap();

    let schema = nation_schema();
    let options = csv_options(&schema);

    let ctx = test_ctx();
    convert_tbl(&ctx, &input, output_str, &options, "parquet", "snappy", 8192, false, &[])
        .await
        .unwrap();

    assert!(output.exists());

    // Read back and verify row count
    let ctx = SessionContext::new();
    let df = ctx
        .read_parquet(output_str, ParquetReadOptions::default())
        .await
        .unwrap();
    let count = df.count().await.unwrap();
    assert_eq!(count, 5);
}

#[tokio::test]
async fn convert_tbl_to_csv() {
    let dir = TempDir::new().unwrap();
    let input = dir.path().join("nation.tbl");
    create_nation_tbl(&input);

    let output = dir.path().join("nation_output.csv");
    let output_str = output.to_str().unwrap();

    let schema = nation_schema();
    let options = csv_options(&schema);

    let ctx = test_ctx();
    convert_tbl(&ctx, &input, output_str, &options, "csv", "none", 8192, false, &[])
        .await
        .unwrap();

    assert!(output.exists());
}

#[tokio::test]
async fn convert_tbl_invalid_format() {
    let dir = TempDir::new().unwrap();
    let input = dir.path().join("nation.tbl");
    create_nation_tbl(&input);

    let output = dir.path().join("nation_output.json");
    let output_str = output.to_str().unwrap();

    let schema = nation_schema();
    let options = csv_options(&schema);

    let ctx = test_ctx();
    let result = convert_tbl(&ctx, &input, output_str, &options, "json", "none", 8192, false, &[]).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn convert_tbl_invalid_compression() {
    let dir = TempDir::new().unwrap();
    let input = dir.path().join("nation.tbl");
    create_nation_tbl(&input);

    let output = dir.path().join("nation_output.parquet");
    let output_str = output.to_str().unwrap();

    let schema = nation_schema();
    let options = csv_options(&schema);

    let ctx = test_ctx();
    let result = convert_tbl(&ctx, &input, output_str, &options, "parquet", "brotli", 8192, false, &[]).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn convert_tbl_compression_none() {
    let dir = TempDir::new().unwrap();
    let input = dir.path().join("nation.tbl");
    create_nation_tbl(&input);

    let output = dir.path().join("output_none.parquet");
    let output_str = output.to_str().unwrap();

    let schema = nation_schema();
    let options = csv_options(&schema);

    let ctx = test_ctx();
    convert_tbl(&ctx, &input, output_str, &options, "parquet", "none", 8192, false, &[])
        .await
        .unwrap();

    assert!(output.exists());
}

#[tokio::test]
async fn convert_tbl_compression_lz4() {
    let dir = TempDir::new().unwrap();
    let input = dir.path().join("nation.tbl");
    create_nation_tbl(&input);

    let output = dir.path().join("output_lz4.parquet");
    let output_str = output.to_str().unwrap();

    let schema = nation_schema();
    let options = csv_options(&schema);

    let ctx = test_ctx();
    convert_tbl(&ctx, &input, output_str, &options, "parquet", "lz4", 8192, false, &[])
        .await
        .unwrap();

    assert!(output.exists());
}

// --- convert_to_parquet tests ---

struct TestTpc;

#[async_trait]
impl Tpc for TestTpc {
    fn generate(
        &self,
        _scale: usize,
        _partitions: usize,
        _input_path: &str,
        _output_path: &str,
    ) -> std::io::Result<()> {
        Ok(())
    }

    fn get_table_names(&self) -> Vec<&str> {
        vec!["nation", "region"]
    }

    fn get_table_ext(&self) -> &str {
        "tbl"
    }

    fn get_schema(&self, table: &str) -> Schema {
        match table {
            "nation" => nation_schema(),
            "region" => region_schema(),
            _ => panic!("unknown table: {}", table),
        }
    }
}

#[tokio::test]
async fn convert_to_parquet_end_to_end() {
    let dir = TempDir::new().unwrap();
    let input_dir = dir.path().join("input");
    let output_dir = dir.path().join("output");
    fs::create_dir(&input_dir).unwrap();
    fs::create_dir(&output_dir).unwrap();

    // Create directory structure: input/nation.tbl/part-0.tbl
    let nation_dir = input_dir.join("nation.tbl");
    fs::create_dir(&nation_dir).unwrap();
    create_nation_tbl(&nation_dir.join("part-0.tbl"));

    let region_dir = input_dir.join("region.tbl");
    fs::create_dir(&region_dir).unwrap();
    create_region_tbl(&region_dir.join("part-0.tbl"));

    let tpc = TestTpc;
    convert_to_parquet(
        &tpc,
        input_dir.to_str().unwrap(),
        output_dir.to_str().unwrap(),
        false,
        1,
        8192,
        "snappy",
        false,
    )
    .await
    .unwrap();

    // Verify output directories exist
    let nation_parquet = output_dir.join("nation.parquet");
    let region_parquet = output_dir.join("region.parquet");
    assert!(nation_parquet.exists(), "nation.parquet dir should exist");
    assert!(region_parquet.exists(), "region.parquet dir should exist");

    // Verify parquet part files exist inside
    let nation_part0 = nation_parquet.join("part-0.parquet");
    assert!(nation_part0.is_file(), "nation.parquet/part-0.parquet should be a file");
    let region_part0 = region_parquet.join("part-0.parquet");
    assert!(region_part0.is_file(), "region.parquet/part-0.parquet should be a file");

    // Read back nation parquet and verify row count
    let ctx = SessionContext::new();
    let df = ctx
        .read_parquet(
            nation_parquet.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();
    let count = df.count().await.unwrap();
    assert_eq!(count, 5);
}

#[tokio::test]
async fn convert_to_parquet_concurrent() {
    let dir = TempDir::new().unwrap();
    let input_dir = dir.path().join("input");
    let output_dir = dir.path().join("output");
    fs::create_dir(&input_dir).unwrap();
    fs::create_dir(&output_dir).unwrap();

    // Create directory structure: input/nation.tbl/part-0.tbl
    let nation_dir = input_dir.join("nation.tbl");
    fs::create_dir(&nation_dir).unwrap();
    create_nation_tbl(&nation_dir.join("part-0.tbl"));

    let region_dir = input_dir.join("region.tbl");
    fs::create_dir(&region_dir).unwrap();
    create_region_tbl(&region_dir.join("part-0.tbl"));

    let tpc = TestTpc;
    convert_to_parquet(
        &tpc,
        input_dir.to_str().unwrap(),
        output_dir.to_str().unwrap(),
        false,
        4,
        8192,
        "snappy",
        false,
    )
    .await
    .unwrap();

    // Verify output directories exist
    let nation_parquet = output_dir.join("nation.parquet");
    let region_parquet = output_dir.join("region.parquet");
    assert!(nation_parquet.exists(), "nation.parquet dir should exist");
    assert!(region_parquet.exists(), "region.parquet dir should exist");

    // Verify parquet part files exist inside
    let nation_part0 = nation_parquet.join("part-0.parquet");
    assert!(
        nation_part0.is_file(),
        "nation.parquet/part-0.parquet should be a file"
    );
    let region_part0 = region_parquet.join("part-0.parquet");
    assert!(
        region_part0.is_file(),
        "region.parquet/part-0.parquet should be a file"
    );

    // Read back nation parquet and verify row count
    let ctx = SessionContext::new();
    let df = ctx
        .read_parquet(
            nation_parquet.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();
    let count = df.count().await.unwrap();
    assert_eq!(count, 5);

    // Read back region parquet and verify row count
    let df = ctx
        .read_parquet(
            region_parquet.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();
    let count = df.count().await.unwrap();
    assert_eq!(count, 3);
}

#[tokio::test]
async fn convert_tbl_compression_zstd() {
    let dir = TempDir::new().unwrap();
    let input = dir.path().join("nation.tbl");
    create_nation_tbl(&input);

    let output = dir.path().join("output_zstd.parquet");
    let output_str = output.to_str().unwrap();

    let schema = nation_schema();
    let options = csv_options(&schema);

    let ctx = test_ctx();
    convert_tbl(&ctx, &input, output_str, &options, "parquet", "zstd", 8192, false, &[])
        .await
        .unwrap();

    assert!(output.exists());

    // Read back and verify row count
    let ctx = SessionContext::new();
    let df = ctx
        .read_parquet(output_str, ParquetReadOptions::default())
        .await
        .unwrap();
    let count = df.count().await.unwrap();
    assert_eq!(count, 5);
}

#[tokio::test]
async fn convert_to_parquet_zstd_compression() {
    let dir = TempDir::new().unwrap();
    let input_dir = dir.path().join("input");
    let output_dir = dir.path().join("output");
    fs::create_dir(&input_dir).unwrap();
    fs::create_dir(&output_dir).unwrap();

    let nation_dir = input_dir.join("nation.tbl");
    fs::create_dir(&nation_dir).unwrap();
    create_nation_tbl(&nation_dir.join("part-0.tbl"));

    let region_dir = input_dir.join("region.tbl");
    fs::create_dir(&region_dir).unwrap();
    create_region_tbl(&region_dir.join("part-0.tbl"));

    let tpc = TestTpc;
    convert_to_parquet(
        &tpc,
        input_dir.to_str().unwrap(),
        output_dir.to_str().unwrap(),
        false,
        2,
        8192,
        "zstd",
        false,
    )
    .await
    .unwrap();

    // Verify output directories exist
    let nation_parquet = output_dir.join("nation.parquet");
    assert!(nation_parquet.exists(), "nation.parquet dir should exist");

    // Read back and verify row count
    let ctx = SessionContext::new();
    let df = ctx
        .read_parquet(
            nation_parquet.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();
    let count = df.count().await.unwrap();
    assert_eq!(count, 5);
}

// --- Hive partitioned convert_to_parquet tests ---

/// Create a small pipe-delimited orders fixture (4 rows, 2 distinct dates).
fn create_orders_tbl(path: &Path) {
    let data = "\
1|370|O|172799.49|1996-01-02|5-LOW|Clerk#000000951|0|nstructions sleep|
2|781|F|38426.09|1993-06-01|1-URGENT|Clerk#000000880|0|foxes pending|
3|1234|F|205654.30|1993-06-01|5-LOW|Clerk#000000955|0|sly final accounts|
4|1369|O|56000.00|1996-01-02|5-LOW|Clerk#000000124|0|deposits blithely|
";
    fs::write(path, data).unwrap();
}

fn orders_schema() -> Schema {
    TpcH::new().get_schema("orders")
}

struct HiveTestTpc;

#[async_trait]
impl Tpc for HiveTestTpc {
    fn generate(
        &self,
        _scale: usize,
        _partitions: usize,
        _input_path: &str,
        _output_path: &str,
    ) -> std::io::Result<()> {
        Ok(())
    }

    fn get_table_names(&self) -> Vec<&str> {
        vec!["orders"]
    }

    fn get_table_ext(&self) -> &str {
        "tbl"
    }

    fn get_schema(&self, table: &str) -> Schema {
        match table {
            "orders" => orders_schema(),
            _ => panic!("unknown table: {}", table),
        }
    }

    fn get_partition_col(&self, table: &str) -> Option<&str> {
        match table {
            "orders" => Some("o_orderdate"),
            _ => None,
        }
    }
}

#[tokio::test]
async fn convert_to_parquet_hive_partitioned() {
    let dir = TempDir::new().unwrap();
    let input_dir = dir.path().join("input");
    let output_dir = dir.path().join("output");
    fs::create_dir(&input_dir).unwrap();
    fs::create_dir(&output_dir).unwrap();

    // Create directory structure: input/orders.tbl/part-0.tbl
    let orders_dir = input_dir.join("orders.tbl");
    fs::create_dir(&orders_dir).unwrap();
    create_orders_tbl(&orders_dir.join("part-0.tbl"));

    let tpc = HiveTestTpc;
    convert_to_parquet(
        &tpc,
        input_dir.to_str().unwrap(),
        output_dir.to_str().unwrap(),
        true,
        1,
        8192,
        "snappy",
        false,
    )
    .await
    .unwrap();

    // Verify output directory exists
    let orders_parquet = output_dir.join("orders.parquet");
    assert!(orders_parquet.exists(), "orders.parquet dir should exist");

    // Verify hive partition directories exist
    let entries: Vec<_> = fs::read_dir(&orders_parquet)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .collect();
    assert!(
        entries.len() >= 2,
        "expected at least 2 partition directories, found {}",
        entries.len()
    );

    // Check that directory names look like hive partitions
    let dir_names: Vec<String> = entries
        .iter()
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    for name in &dir_names {
        assert!(
            name.starts_with("o_orderdate="),
            "expected hive partition dir, got: {}",
            name
        );
    }

    // Read back and verify total row count
    let ctx = SessionContext::new();
    let df = ctx
        .read_parquet(
            orders_parquet.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();
    let count = df.count().await.unwrap();
    assert_eq!(count, 4);
}
