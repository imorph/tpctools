use std::fs;
use std::path::{Path, PathBuf};

use tempfile::TempDir;

use tpctools::tpcds::TpcDs;
use tpctools::tpch::TpcH;
use tpctools::{convert_to_parquet, Tpc};

/// Look for a generator binary directory.
///
/// 1. Check the environment variable (e.g. `TPCH_DBGEN_DIR`).
/// 2. Fall back to `which <binary>` and verify the support file lives next to it.
///
/// Returns `None` when the binary cannot be located.
fn find_generator_dir(binary: &str, support_file: &str, env_var: &str) -> Option<PathBuf> {
    // Check env var first
    if let Ok(dir) = std::env::var(env_var) {
        let dir = PathBuf::from(&dir);
        if dir.join(binary).exists() && dir.join(support_file).exists() {
            return Some(dir);
        }
    }

    // Fall back to `which`
    let output = std::process::Command::new("which")
        .arg(binary)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let bin_path = PathBuf::from(String::from_utf8_lossy(&output.stdout).trim());
    let dir = bin_path.parent()?;
    if dir.join(support_file).exists() {
        Some(dir.to_path_buf())
    } else {
        None
    }
}

/// Holds two temp directories: one for the generator working copy, one for output.
/// Symlinks the binary + support file into the generator temp dir so tests are
/// isolated and the generator has a writable cwd.
struct GeneratorSetup {
    _generator_tmp: TempDir,
    _output_tmp: TempDir,
    generator_path: String,
    output_path: String,
}

impl GeneratorSetup {
    fn new(source_dir: &Path, binary: &str, support_file: &str) -> Self {
        let generator_tmp = TempDir::new().expect("failed to create generator temp dir");
        let output_tmp = TempDir::new().expect("failed to create output temp dir");

        // Symlink binary and support file into the temp generator dir.
        // Fall back to copy if symlinks fail (e.g. on some Windows setups).
        for name in &[binary, support_file] {
            let src = source_dir.join(name);
            let dst = generator_tmp.path().join(name);
            #[cfg(unix)]
            {
                if std::os::unix::fs::symlink(&src, &dst).is_err() {
                    fs::copy(&src, &dst).expect("failed to copy generator file");
                }
            }
            #[cfg(not(unix))]
            {
                fs::copy(&src, &dst).expect("failed to copy generator file");
            }
        }

        // Make sure the binary is executable (symlink preserves this, copy may not)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let bin_dst = generator_tmp.path().join(binary);
            let meta = fs::metadata(&bin_dst).expect("binary metadata");
            let mut perms = meta.permissions();
            perms.set_mode(perms.mode() | 0o111);
            fs::set_permissions(&bin_dst, perms).ok();
        }

        let generator_path = generator_tmp.path().to_str().unwrap().to_string();
        let output_path = output_tmp.path().to_str().unwrap().to_string();

        GeneratorSetup {
            _generator_tmp: generator_tmp,
            _output_tmp: output_tmp,
            generator_path,
            output_path,
        }
    }
}

macro_rules! require_tpch {
    () => {
        match find_generator_dir("dbgen", "dists.dss", "TPCH_DBGEN_DIR") {
            Some(dir) => GeneratorSetup::new(&dir, "dbgen", "dists.dss"),
            None => {
                println!("SKIPPED: dbgen not found (set TPCH_DBGEN_DIR or add dbgen to PATH)");
                return;
            }
        }
    };
}

macro_rules! require_tpcds {
    () => {
        match find_generator_dir("dsdgen", "tpcds.idx", "TPCDS_DSDGEN_DIR") {
            Some(dir) => GeneratorSetup::new(&dir, "dsdgen", "tpcds.idx"),
            None => {
                println!(
                    "SKIPPED: dsdgen not found (set TPCDS_DSDGEN_DIR or add dsdgen to PATH)"
                );
                return;
            }
        }
    };
}

// ---------------------------------------------------------------------------
// TPC-H generate tests
// ---------------------------------------------------------------------------

#[test]
fn tpch_generate_single_partition() {
    let setup = require_tpch!();
    let tpch = TpcH::new();
    tpch.generate(1, 1, &setup.generator_path, &setup.output_path)
        .expect("generate failed");

    let tables = tpch.get_table_names();
    assert_eq!(tables.len(), 8);

    for table in &tables {
        let table_dir = PathBuf::from(&setup.output_path).join(format!("{}.tbl", table));
        assert!(
            table_dir.is_dir(),
            "expected directory for table {}: {}",
            table,
            table_dir.display()
        );
        let part_file = table_dir.join("part-0.tbl");
        assert!(
            part_file.is_file(),
            "expected part-0.tbl for table {}: {}",
            table,
            part_file.display()
        );
        let meta = fs::metadata(&part_file).unwrap();
        assert!(meta.len() > 0, "part-0.tbl for {} should be non-empty", table);
    }
}

#[test]
fn tpch_generate_multi_partition() {
    let setup = require_tpch!();
    let tpch = TpcH::new();
    tpch.generate(1, 2, &setup.generator_path, &setup.output_path)
        .expect("generate failed");

    let tables = tpch.get_table_names();

    for table in &tables {
        let table_dir = PathBuf::from(&setup.output_path).join(format!("{}.tbl", table));
        assert!(
            table_dir.is_dir(),
            "expected directory for table {}",
            table
        );

        for i in 1..=2 {
            let part_file = table_dir.join(format!("part-{}.tbl", i));
            assert!(
                part_file.is_file(),
                "expected part-{}.tbl for table {}: {}",
                i,
                table,
                part_file.display()
            );
            let meta = fs::metadata(&part_file).unwrap();
            assert!(
                meta.len() > 0,
                "part-{}.tbl for {} should be non-empty",
                i,
                table
            );
        }
    }
}

// ---------------------------------------------------------------------------
// TPC-DS generate tests
// ---------------------------------------------------------------------------

#[test]
fn tpcds_generate_single_partition() {
    let setup = require_tpcds!();
    let tpcds = TpcDs::new();
    tpcds
        .generate(1, 1, &setup.generator_path, &setup.output_path)
        .expect("generate failed");

    // Not all 24 tables produce output at scale 1; check a known subset.
    let must_exist = [
        "customer",
        "customer_address",
        "customer_demographics",
        "date_dim",
        "item",
        "store_sales",
    ];

    for table in &must_exist {
        let table_dir = PathBuf::from(&setup.output_path).join(format!("{}.dat", table));
        assert!(
            table_dir.is_dir(),
            "expected directory for table {}: {}",
            table,
            table_dir.display()
        );
        let part_file = table_dir.join("part-1.dat");
        assert!(
            part_file.is_file(),
            "expected part-1.dat for table {}: {}",
            table,
            part_file.display()
        );
        let meta = fs::metadata(&part_file).unwrap();
        assert!(meta.len() > 0, "part-1.dat for {} should be non-empty", table);
    }
}

#[test]
fn tpcds_generate_multi_partition() {
    let setup = require_tpcds!();
    let tpcds = TpcDs::new();
    tpcds
        .generate(1, 2, &setup.generator_path, &setup.output_path)
        .expect("generate failed");

    let must_exist = [
        "customer",
        "customer_address",
        "customer_demographics",
        "date_dim",
        "item",
        "store_sales",
    ];

    for table in &must_exist {
        let table_dir = PathBuf::from(&setup.output_path).join(format!("{}.dat", table));
        assert!(
            table_dir.is_dir(),
            "expected directory for table {}",
            table
        );

        for i in 1..=2 {
            let part_file = table_dir.join(format!("part-{}.dat", i));
            assert!(
                part_file.is_file(),
                "expected part-{}.dat for table {}: {}",
                i,
                table,
                part_file.display()
            );
        }
    }
}

// ---------------------------------------------------------------------------
// End-to-end generate + convert to parquet tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tpch_generate_and_convert_to_parquet() {
    let setup = require_tpch!();
    let tpch = TpcH::new();

    tpch.generate(1, 2, &setup.generator_path, &setup.output_path)
        .expect("generate failed");

    let parquet_dir = TempDir::new().expect("failed to create parquet output dir");
    let parquet_path = parquet_dir.path().to_str().unwrap();

    convert_to_parquet(&tpch, &setup.output_path, parquet_path)
        .await
        .expect("convert_to_parquet failed");

    let tables = tpch.get_table_names();
    let ctx = datafusion::prelude::SessionContext::new();

    for table in &tables {
        let pq_table_dir = parquet_dir.path().join(format!("{}.parquet", table));
        assert!(
            pq_table_dir.is_dir(),
            "{}.parquet directory should exist",
            table
        );

        let df = ctx
            .read_parquet(
                pq_table_dir.to_str().unwrap(),
                datafusion::prelude::ParquetReadOptions::default(),
            )
            .await
            .expect(&format!("failed to read parquet for {}", table));
        let count = df.count().await.expect("count failed");
        assert!(count > 0, "{} parquet should have rows, got 0", table);
    }
}

#[tokio::test]
async fn tpcds_generate_and_convert_to_parquet() {
    let setup = require_tpcds!();
    let tpcds = TpcDs::new();

    tpcds
        .generate(1, 2, &setup.generator_path, &setup.output_path)
        .expect("generate failed");

    let parquet_dir = TempDir::new().expect("failed to create parquet output dir");
    let parquet_path = parquet_dir.path().to_str().unwrap();

    convert_to_parquet(&tpcds, &setup.output_path, parquet_path)
        .await
        .expect("convert_to_parquet failed");

    // Check a subset of tables that are guaranteed to produce output at scale 1
    let must_exist = [
        "customer",
        "customer_address",
        "date_dim",
        "item",
        "store_sales",
    ];

    let ctx = datafusion::prelude::SessionContext::new();

    for table in &must_exist {
        let pq_table_dir = parquet_dir.path().join(format!("{}.parquet", table));
        assert!(
            pq_table_dir.is_dir(),
            "{}.parquet directory should exist",
            table
        );

        let df = ctx
            .read_parquet(
                pq_table_dir.to_str().unwrap(),
                datafusion::prelude::ParquetReadOptions::default(),
            )
            .await
            .expect(&format!("failed to read parquet for {}", table));
        let count = df.count().await.expect("count failed");
        assert!(count > 0, "{} parquet should have rows, got 0", table);
    }
}
