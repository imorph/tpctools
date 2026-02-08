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
use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::Instant;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema};

use crate::{move_or_copy, Tpc};

pub struct TpcH {}

impl Default for TpcH {
    fn default() -> Self {
        Self::new()
    }
}

impl TpcH {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl Tpc for TpcH {
    fn generate(
        &self,
        scale: usize,
        partitions: usize,
        generator_path: &str,
        output_path: &str,
    ) -> Result<()> {
        let mut handles = vec![];

        let start = Instant::now();

        if partitions == 1 {
            let generator_path = generator_path.to_owned();
            handles.push(thread::spawn(move || {
                println!("Generating partition 1 of 1 ...");
                let output = Command::new("./dbgen")
                    .current_dir(generator_path)
                    .arg("-f")
                    .arg("-s")
                    .arg(format!("{}", scale))
                    .output()
                    .expect("failed to generate data");
                println!("{:?}", output);
            }));
        } else {
            for i in 1..=partitions {
                let generator_path = generator_path.to_owned();
                handles.push(thread::spawn(move || {
                    println!("Generating partition {} of {} ...", i, partitions);
                    let output = Command::new("./dbgen")
                        .current_dir(generator_path)
                        .arg("-f")
                        .arg("-s")
                        .arg(format!("{}", scale))
                        .arg("-C")
                        .arg(format!("{}", partitions))
                        .arg("-S")
                        .arg(format!("{}", i))
                        .output()
                        .expect("failed to generate data");
                    println!("{:?}", output);
                }));
            }
        }

        // wait for all threads to finish
        for h in handles {
            h.join().unwrap();
        }

        let duration = start.elapsed();

        println!(
            "Generated TPC-H data at scale factor {} with {} partitions in: {:?}",
            scale, partitions, duration
        );

        let tables = [
            "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
        ];

        if !Path::new(&output_path).exists() {
            println!("Creating directory {}", output_path);
            fs::create_dir(output_path)?;
        }

        for table in &tables {
            let output_dir = format!("{}/{}.tbl", output_path, table);
            if !Path::new(&output_dir).exists() {
                println!("Creating directory {}", output_dir);
                fs::create_dir(&output_dir)?;
            }

            if partitions == 1 {
                let filename = format!("{}/{}.tbl", generator_path, table);
                let filename2 = format!("{}/part-0.tbl", output_dir);
                if Path::new(&filename).exists() {
                    move_or_copy(Path::new(&filename), Path::new(&filename2))?;
                }
            } else {
                for i in 1..=partitions {
                    let filename = format!("{}/{}.tbl.{}", generator_path, table, i);
                    let filename2 = format!("{}/part-{}.tbl", output_dir, i);
                    if Path::new(&filename).exists() {
                        move_or_copy(Path::new(&filename), Path::new(&filename2))?;
                    }
                }
            }
        }

        Ok(())
    }

    fn get_table_names(&self) -> Vec<&str> {
        vec![
            "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
        ]
    }

    fn get_schema(&self, table: &str) -> Schema {
        // note that the schema intentionally uses signed integers so that any generated Parquet
        // files can also be used to benchmark tools that only support signed integers, such as
        // Apache Spark

        match table {
            "part" => Schema::new(vec![
                Field::new("p_partkey", DataType::Int64, false),
                Field::new("p_name", DataType::Utf8, false),
                Field::new("p_mfgr", DataType::Utf8, false),
                Field::new("p_brand", DataType::Utf8, false),
                Field::new("p_type", DataType::Utf8, false),
                Field::new("p_size", DataType::Int32, false),
                Field::new("p_container", DataType::Utf8, false),
                Field::new("p_retailprice", DataType::Decimal128(11, 2), false),
                Field::new("p_comment", DataType::Utf8, false),
                Field::new("ignore", DataType::Utf8, true),
            ]),

            "supplier" => Schema::new(vec![
                Field::new("s_suppkey", DataType::Int64, false),
                Field::new("s_name", DataType::Utf8, false),
                Field::new("s_address", DataType::Utf8, false),
                Field::new("s_nationkey", DataType::Int64, false),
                Field::new("s_phone", DataType::Utf8, false),
                Field::new("s_acctbal", DataType::Decimal128(11, 2), false),
                Field::new("s_comment", DataType::Utf8, false),
                Field::new("ignore", DataType::Utf8, true),
            ]),

            "partsupp" => Schema::new(vec![
                Field::new("ps_partkey", DataType::Int64, false),
                Field::new("ps_suppkey", DataType::Int64, false),
                Field::new("ps_availqty", DataType::Int32, false),
                Field::new("ps_supplycost", DataType::Decimal128(11, 2), false),
                Field::new("ps_comment", DataType::Utf8, false),
                Field::new("ignore", DataType::Utf8, true),
            ]),

            "customer" => Schema::new(vec![
                Field::new("c_custkey", DataType::Int64, false),
                Field::new("c_name", DataType::Utf8, false),
                Field::new("c_address", DataType::Utf8, false),
                Field::new("c_nationkey", DataType::Int64, false),
                Field::new("c_phone", DataType::Utf8, false),
                Field::new("c_acctbal", DataType::Decimal128(11, 2), false),
                Field::new("c_mktsegment", DataType::Utf8, false),
                Field::new("c_comment", DataType::Utf8, false),
                Field::new("ignore", DataType::Utf8, true),
            ]),

            "orders" => Schema::new(vec![
                Field::new("o_orderkey", DataType::Int64, false),
                Field::new("o_custkey", DataType::Int64, false),
                Field::new("o_orderstatus", DataType::Utf8, false),
                Field::new("o_totalprice", DataType::Decimal128(11, 2), false),
                Field::new("o_orderdate", DataType::Date32, false),
                Field::new("o_orderpriority", DataType::Utf8, false),
                Field::new("o_clerk", DataType::Utf8, false),
                Field::new("o_shippriority", DataType::Int32, false),
                Field::new("o_comment", DataType::Utf8, false),
                Field::new("ignore", DataType::Utf8, true),
            ]),

            "lineitem" => Schema::new(vec![
                Field::new("l_orderkey", DataType::Int64, false),
                Field::new("l_partkey", DataType::Int64, false),
                Field::new("l_suppkey", DataType::Int64, false),
                Field::new("l_linenumber", DataType::Int32, false),
                Field::new("l_quantity", DataType::Decimal128(11, 2), false),
                Field::new("l_extendedprice", DataType::Decimal128(11, 2), false),
                Field::new("l_discount", DataType::Decimal128(11, 2), false),
                Field::new("l_tax", DataType::Decimal128(11, 2), false),
                Field::new("l_returnflag", DataType::Utf8, false),
                Field::new("l_linestatus", DataType::Utf8, false),
                Field::new("l_shipdate", DataType::Date32, false),
                Field::new("l_commitdate", DataType::Date32, false),
                Field::new("l_receiptdate", DataType::Date32, false),
                Field::new("l_shipinstruct", DataType::Utf8, false),
                Field::new("l_shipmode", DataType::Utf8, false),
                Field::new("l_comment", DataType::Utf8, false),
                Field::new("ignore", DataType::Utf8, true),
            ]),

            "nation" => Schema::new(vec![
                Field::new("n_nationkey", DataType::Int64, false),
                Field::new("n_name", DataType::Utf8, false),
                Field::new("n_regionkey", DataType::Int64, false),
                Field::new("n_comment", DataType::Utf8, false),
                Field::new("ignore", DataType::Utf8, true),
            ]),

            "region" => Schema::new(vec![
                Field::new("r_regionkey", DataType::Int64, false),
                Field::new("r_name", DataType::Utf8, false),
                Field::new("r_comment", DataType::Utf8, false),
                Field::new("ignore", DataType::Utf8, true),
            ]),

            _ => unimplemented!(),
        }
    }

    fn get_table_ext(&self) -> &str {
        "tbl"
    }

    fn get_partition_col(&self, table: &str) -> Option<&str> {
        match table {
            "lineitem" => Some("l_shipdate"),
            "orders" => Some("o_orderdate"),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Tpc;

    fn tpch() -> TpcH {
        TpcH::new()
    }

    #[test]
    fn table_names_returns_8_tables() {
        let t = tpch();
        let names = t.get_table_names();
        assert_eq!(names.len(), 8);
        assert!(names.contains(&"nation"));
        assert!(names.contains(&"region"));
        assert!(names.contains(&"part"));
        assert!(names.contains(&"supplier"));
        assert!(names.contains(&"partsupp"));
        assert!(names.contains(&"customer"));
        assert!(names.contains(&"orders"));
        assert!(names.contains(&"lineitem"));
    }

    #[test]
    fn table_ext_is_tbl() {
        assert_eq!(tpch().get_table_ext(), "tbl");
    }

    #[test]
    fn field_counts() {
        let t = tpch();
        let expected = vec![
            ("nation", 5),
            ("region", 4),
            ("part", 10),
            ("supplier", 8),
            ("partsupp", 6),
            ("customer", 9),
            ("orders", 10),
            ("lineitem", 17),
        ];
        for (table, count) in expected {
            assert_eq!(
                t.get_schema(table).fields().len(),
                count,
                "field count mismatch for {}",
                table
            );
        }
    }

    #[test]
    fn all_schemas_have_trailing_ignore_field() {
        let t = tpch();
        for table in t.get_table_names() {
            let schema = t.get_schema(table);
            let last = schema.fields().last().unwrap();
            assert_eq!(last.name(), "ignore", "table {} missing trailing ignore", table);
            assert_eq!(*last.data_type(), DataType::Utf8);
            assert!(last.is_nullable());
        }
    }

    #[test]
    fn primary_key_fields_are_non_nullable_int64() {
        let t = tpch();
        let cases = vec![
            ("nation", "n_nationkey"),
            ("region", "r_regionkey"),
            ("part", "p_partkey"),
            ("supplier", "s_suppkey"),
            ("customer", "c_custkey"),
            ("orders", "o_orderkey"),
            ("lineitem", "l_orderkey"),
        ];
        for (table, pk) in cases {
            let schema = t.get_schema(table);
            let field = schema.field_with_name(pk).unwrap();
            assert!(!field.is_nullable(), "{}.{} should be non-nullable", table, pk);
            assert_eq!(*field.data_type(), DataType::Int64, "{}.{} should be Int64", table, pk);
        }
    }

    #[test]
    fn decimal_fields_use_decimal128_11_2() {
        let t = tpch();
        let schema = t.get_schema("lineitem");
        let field = schema.field_with_name("l_extendedprice").unwrap();
        assert_eq!(*field.data_type(), DataType::Decimal128(11, 2));
    }

    #[test]
    fn date_fields_use_date32() {
        let t = tpch();
        let schema = t.get_schema("orders");
        let field = schema.field_with_name("o_orderdate").unwrap();
        assert_eq!(*field.data_type(), DataType::Date32);

        let schema = t.get_schema("lineitem");
        for name in &["l_shipdate", "l_commitdate", "l_receiptdate"] {
            let field = schema.field_with_name(name).unwrap();
            assert_eq!(*field.data_type(), DataType::Date32, "{} should be Date32", name);
        }
    }

    #[test]
    fn first_field_name_prefix() {
        let t = tpch();
        let expected = vec![
            ("nation", "n_"),
            ("region", "r_"),
            ("part", "p_"),
            ("supplier", "s_"),
            ("partsupp", "ps_"),
            ("customer", "c_"),
            ("orders", "o_"),
            ("lineitem", "l_"),
        ];
        for (table, prefix) in expected {
            let schema = t.get_schema(table);
            let first = schema.fields().first().unwrap();
            assert!(
                first.name().starts_with(prefix),
                "table {} first field {} doesn't start with {}",
                table,
                first.name(),
                prefix
            );
        }
    }

    #[test]
    fn partition_col_fact_tables() {
        let t = tpch();
        assert_eq!(t.get_partition_col("lineitem"), Some("l_shipdate"));
        assert_eq!(t.get_partition_col("orders"), Some("o_orderdate"));
    }

    #[test]
    fn partition_col_dimension_tables_return_none() {
        let t = tpch();
        for table in &["nation", "region", "part", "supplier", "partsupp", "customer"] {
            assert_eq!(t.get_partition_col(table), None, "{} should not be partitioned", table);
        }
    }

    #[test]
    #[should_panic]
    fn invalid_table_name_panics() {
        tpch().get_schema("nonexistent");
    }
}
