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

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use log::{debug, error, info, warn};
use std::fs;
use std::io::Result;
use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::Instant;

use crate::{move_or_copy, Tpc};

pub struct TpcDs {}

impl Default for TpcDs {
    fn default() -> Self {
        Self::new()
    }
}

impl TpcDs {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl Tpc for TpcDs {
    fn generate(
        &self,
        scale: usize,
        partitions: usize,
        generator_path: &str,
        output_path: &str,
    ) -> Result<()> {
        let mut handles = vec![];

        let start = Instant::now();

        for i in 1..=partitions {
            let generator_path = generator_path.to_owned();
            let output_path = output_path.to_owned();
            handles.push(thread::spawn(move || {
                info!("generating TPC-DS partition {} of {} ...", i, partitions);
                debug!(
                    "running ./dsdgen -FORCE -DIR {} -SCALE {} -CHILD {} -PARALLEL {} in {}",
                    output_path, scale, i, partitions, generator_path
                );
                let output = Command::new("./dsdgen")
                    .current_dir(&generator_path)
                    .arg("-FORCE")
                    .arg("-DIR")
                    .arg(&output_path)
                    .arg("-SCALE")
                    .arg(format!("{}", scale))
                    .arg("-CHILD")
                    .arg(format!("{}", i))
                    .arg("-PARALLEL")
                    .arg(format!("{}", partitions))
                    .output();

                match output {
                    Ok(o) => {
                        if o.status.success() {
                            debug!("dsdgen partition {}: exit code 0", i);
                        } else {
                            warn!("dsdgen partition {} exited with status {}", i, o.status);
                            let stderr = String::from_utf8_lossy(&o.stderr);
                            if !stderr.is_empty() {
                                warn!("dsdgen stderr: {}", stderr);
                            }
                        }
                    }
                    Err(e) => error!(
                        "failed to spawn dsdgen with path {}: {:?}",
                        generator_path, e
                    ),
                }
            }));
        }

        // wait for all threads to finish
        for h in handles {
            h.join().unwrap();
        }

        let duration = start.elapsed();

        info!(
            "generated TPC-DS data at scale factor {} with {} partitions in {:?}",
            scale, partitions, duration
        );

        let tables = self.get_table_names();

        // Create per-table directories sequentially
        for table in &tables {
            let output_dir = format!("{}/{}.dat", output_path, table);
            if !Path::new(&output_dir).exists() {
                debug!("creating directory {}", output_dir);
                fs::create_dir(&output_dir)?;
            }
        }

        // Collect all (source, destination) pairs for file moves
        let mut file_pairs = Vec::new();
        for table in &tables {
            let output_dir = format!("{}/{}.dat", output_path, table);
            for i in 1..=partitions {
                let src = format!("{}/{}_{}_{}.dat", output_path, table, i, partitions);
                let dst = format!("{}/part-{}.dat", output_dir, i);
                if Path::new(&src).exists() {
                    file_pairs.push((src, dst));
                }
            }
        }

        // Move/copy files in parallel
        thread::scope(|s| {
            let handles: Vec<_> = file_pairs
                .iter()
                .map(|(src, dst)| {
                    s.spawn(|| move_or_copy(Path::new(src), Path::new(dst)))
                })
                .collect();
            for handle in handles {
                handle.join().unwrap()?;
            }
            Ok::<(), std::io::Error>(())
        })?;

        Ok(())
    }

    fn get_table_names(&self) -> Vec<&str> {
        vec![
            "call_center",
            "catalog_page",
            "catalog_sales",
            "catalog_returns",
            "customer",
            "customer_address",
            "customer_demographics",
            "date_dim",
            "income_band",
            "household_demographics",
            "inventory",
            "store",
            "ship_mode",
            "reason",
            "promotion",
            "item",
            "store_sales",
            "store_returns",
            "web_page",
            "warehouse",
            "time_dim",
            "web_site",
            "web_sales",
            "web_returns",
        ]
    }

    fn get_schema(&self, table: &str) -> Schema {
        match table {
            "customer_address" => Schema::new(vec![
                Field::new("ca_address_sk", DataType::Int64, false),
                Field::new("ca_address_id", DataType::Utf8, false),
                Field::new("ca_street_number", DataType::Utf8, true),
                Field::new("ca_street_name", DataType::Utf8, true),
                Field::new("ca_street_type", DataType::Utf8, true),
                Field::new("ca_suite_number", DataType::Utf8, true),
                Field::new("ca_city", DataType::Utf8, true),
                Field::new("ca_county", DataType::Utf8, true),
                Field::new("ca_state", DataType::Utf8, true),
                Field::new("ca_zip", DataType::Utf8, true),
                Field::new("ca_country", DataType::Utf8, true),
                Field::new("ca_gmt_offset", make_decimal_type(5, 2), true),
                Field::new("ca_location_type", DataType::Utf8, true),
            ]),

            "customer_demographics" => Schema::new(vec![
                Field::new("cd_demo_sk", DataType::Int64, false),
                Field::new("cd_gender", DataType::Utf8, true),
                Field::new("cd_marital_status", DataType::Utf8, true),
                Field::new("cd_education_status", DataType::Utf8, true),
                Field::new("cd_purchase_estimate", DataType::Int64, true),
                Field::new("cd_credit_rating", DataType::Utf8, true),
                Field::new("cd_dep_count", DataType::Int64, true),
                Field::new("cd_dep_employed_count", DataType::Int64, true),
                Field::new("cd_dep_college_count", DataType::Int64, true),
            ]),

            "date_dim" => Schema::new(vec![
                Field::new("d_date_sk", DataType::Int64, false),
                Field::new("d_date_id", DataType::Utf8, false),
                Field::new("d_date", DataType::Date32, true),
                Field::new("d_month_seq", DataType::Int64, true),
                Field::new("d_week_seq", DataType::Int64, true),
                Field::new("d_quarter_seq", DataType::Int64, true),
                Field::new("d_year", DataType::Int64, true),
                Field::new("d_dow", DataType::Int64, true),
                Field::new("d_moy", DataType::Int64, true),
                Field::new("d_dom", DataType::Int64, true),
                Field::new("d_qoy", DataType::Int64, true),
                Field::new("d_fy_year", DataType::Int64, true),
                Field::new("d_fy_quarter_seq", DataType::Int64, true),
                Field::new("d_fy_week_seq", DataType::Int64, true),
                Field::new("d_day_name", DataType::Utf8, true),
                Field::new("d_quarter_name", DataType::Utf8, true),
                Field::new("d_holiday", DataType::Utf8, true),
                Field::new("d_weekend", DataType::Utf8, true),
                Field::new("d_following_holiday", DataType::Utf8, true),
                Field::new("d_first_dom", DataType::Int64, true),
                Field::new("d_last_dom", DataType::Int64, true),
                Field::new("d_same_day_ly", DataType::Int64, true),
                Field::new("d_same_day_lq", DataType::Int64, true),
                Field::new("d_current_day", DataType::Utf8, true),
                Field::new("d_current_week", DataType::Utf8, true),
                Field::new("d_current_month", DataType::Utf8, true),
                Field::new("d_current_quarter", DataType::Utf8, true),
                Field::new("d_current_year", DataType::Utf8, true),
            ]),

            "warehouse" => Schema::new(vec![
                Field::new("w_warehouse_sk", DataType::Int64, false),
                Field::new("w_warehouse_id", DataType::Utf8, false),
                Field::new("w_warehouse_name", DataType::Utf8, true),
                Field::new("w_warehouse_sq_ft", DataType::Int64, true),
                Field::new("w_street_number", DataType::Utf8, true),
                Field::new("w_street_name", DataType::Utf8, true),
                Field::new("w_street_type", DataType::Utf8, true),
                Field::new("w_suite_number", DataType::Utf8, true),
                Field::new("w_city", DataType::Utf8, true),
                Field::new("w_county", DataType::Utf8, true),
                Field::new("w_state", DataType::Utf8, true),
                Field::new("w_zip", DataType::Utf8, true),
                Field::new("w_country", DataType::Utf8, true),
                Field::new("w_gmt_offset", make_decimal_type(5, 2), true),
            ]),

            "ship_mode" => Schema::new(vec![
                Field::new("sm_ship_mode_sk", DataType::Int64, false),
                Field::new("sm_ship_mode_id", DataType::Utf8, false),
                Field::new("sm_type", DataType::Utf8, true),
                Field::new("sm_code", DataType::Utf8, true),
                Field::new("sm_carrier", DataType::Utf8, true),
                Field::new("sm_contract", DataType::Utf8, true),
            ]),

            "time_dim" => Schema::new(vec![
                Field::new("t_time_sk", DataType::Int64, false),
                Field::new("t_time_id", DataType::Utf8, false),
                Field::new("t_time", DataType::Int64, true),
                Field::new("t_hour", DataType::Int64, true),
                Field::new("t_minute", DataType::Int64, true),
                Field::new("t_second", DataType::Int64, true),
                Field::new("t_am_pm", DataType::Utf8, true),
                Field::new("t_shift", DataType::Utf8, true),
                Field::new("t_sub_shift", DataType::Utf8, true),
                Field::new("t_meal_time", DataType::Utf8, true),
            ]),

            "reason" => Schema::new(vec![
                Field::new("r_reason_sk", DataType::Int64, false),
                Field::new("r_reason_id", DataType::Utf8, false),
                Field::new("r_reason_desc", DataType::Utf8, true),
            ]),

            "income_band" => Schema::new(vec![
                Field::new("ib_income_band_sk", DataType::Int64, false),
                Field::new("ib_lower_bound", DataType::Int64, true),
                Field::new("ib_upper_bound", DataType::Int64, true),
            ]),

            "item" => Schema::new(vec![
                Field::new("i_item_sk", DataType::Int64, false),
                Field::new("i_item_id", DataType::Utf8, false),
                Field::new("i_rec_start_date", DataType::Date32, true),
                Field::new("i_rec_end_date", DataType::Date32, true),
                Field::new("i_item_desc", DataType::Utf8, true),
                Field::new("i_current_price", make_decimal_type(7, 2), true),
                Field::new("i_wholesale_cost", make_decimal_type(7, 2), true),
                Field::new("i_brand_id", DataType::Int64, true),
                Field::new("i_brand", DataType::Utf8, true),
                Field::new("i_class_id", DataType::Int64, true),
                Field::new("i_class", DataType::Utf8, true),
                Field::new("i_category_id", DataType::Int64, true),
                Field::new("i_category", DataType::Utf8, true),
                Field::new("i_manufact_id", DataType::Int64, true),
                Field::new("i_manufact", DataType::Utf8, true),
                Field::new("i_size", DataType::Utf8, true),
                Field::new("i_formulation", DataType::Utf8, true),
                Field::new("i_color", DataType::Utf8, true),
                Field::new("i_units", DataType::Utf8, true),
                Field::new("i_container", DataType::Utf8, true),
                Field::new("i_manager_id", DataType::Int64, true),
                Field::new("i_product_name", DataType::Utf8, true),
            ]),

            "store" => Schema::new(vec![
                Field::new("s_store_sk", DataType::Int64, false),
                Field::new("s_store_id", DataType::Utf8, false),
                Field::new("s_rec_start_date", DataType::Date32, true),
                Field::new("s_rec_end_date", DataType::Date32, true),
                Field::new("s_closed_date_sk", DataType::Int64, true),
                Field::new("s_store_name", DataType::Utf8, true),
                Field::new("s_number_employees", DataType::Int64, true),
                Field::new("s_floor_space", DataType::Int64, true),
                Field::new("s_hours", DataType::Utf8, true),
                Field::new("s_manager", DataType::Utf8, true),
                Field::new("s_market_id", DataType::Int64, true),
                Field::new("s_geography_class", DataType::Utf8, true),
                Field::new("s_market_desc", DataType::Utf8, true),
                Field::new("s_market_manager", DataType::Utf8, true),
                Field::new("s_division_id", DataType::Int64, true),
                Field::new("s_division_name", DataType::Utf8, true),
                Field::new("s_company_id", DataType::Int64, true),
                Field::new("s_company_name", DataType::Utf8, true),
                Field::new("s_street_number", DataType::Utf8, true),
                Field::new("s_street_name", DataType::Utf8, true),
                Field::new("s_street_type", DataType::Utf8, true),
                Field::new("s_suite_number", DataType::Utf8, true),
                Field::new("s_city", DataType::Utf8, true),
                Field::new("s_county", DataType::Utf8, true),
                Field::new("s_state", DataType::Utf8, true),
                Field::new("s_zip", DataType::Utf8, true),
                Field::new("s_country", DataType::Utf8, true),
                Field::new("s_gmt_offset", make_decimal_type(5, 2), true),
                Field::new("s_tax_precentage", make_decimal_type(5, 2), true),
            ]),

            "call_center" => Schema::new(vec![
                Field::new("cc_call_center_sk", DataType::Int64, false),
                Field::new("cc_call_center_id", DataType::Utf8, false),
                Field::new("cc_rec_start_date", DataType::Date32, true),
                Field::new("cc_rec_end_date", DataType::Date32, true),
                Field::new("cc_closed_date_sk", DataType::Int64, true),
                Field::new("cc_open_date_sk", DataType::Int64, true),
                Field::new("cc_name", DataType::Utf8, true),
                Field::new("cc_class", DataType::Utf8, true),
                Field::new("cc_employees", DataType::Int64, true),
                Field::new("cc_sq_ft", DataType::Int64, true),
                Field::new("cc_hours", DataType::Utf8, true),
                Field::new("cc_manager", DataType::Utf8, true),
                Field::new("cc_mkt_id", DataType::Int64, true),
                Field::new("cc_mkt_class", DataType::Utf8, true),
                Field::new("cc_mkt_desc", DataType::Utf8, true),
                Field::new("cc_market_manager", DataType::Utf8, true),
                Field::new("cc_division", DataType::Int64, true),
                Field::new("cc_division_name", DataType::Utf8, true),
                Field::new("cc_company", DataType::Int64, true),
                Field::new("cc_company_name", DataType::Utf8, true),
                Field::new("cc_street_number", DataType::Utf8, true),
                Field::new("cc_street_name", DataType::Utf8, true),
                Field::new("cc_street_type", DataType::Utf8, true),
                Field::new("cc_suite_number", DataType::Utf8, true),
                Field::new("cc_city", DataType::Utf8, true),
                Field::new("cc_county", DataType::Utf8, true),
                Field::new("cc_state", DataType::Utf8, true),
                Field::new("cc_zip", DataType::Utf8, true),
                Field::new("cc_country", DataType::Utf8, true),
                Field::new("cc_gmt_offset", make_decimal_type(5, 2), true),
                Field::new("cc_tax_percentage", make_decimal_type(5, 2), true),
            ]),

            "customer" => Schema::new(vec![
                Field::new("c_customer_sk", DataType::Int64, false),
                Field::new("c_customer_id", DataType::Utf8, false),
                Field::new("c_current_cdemo_sk", DataType::Int64, true),
                Field::new("c_current_hdemo_sk", DataType::Int64, true),
                Field::new("c_current_addr_sk", DataType::Int64, true),
                Field::new("c_first_shipto_date_sk", DataType::Int64, true),
                Field::new("c_first_sales_date_sk", DataType::Int64, true),
                Field::new("c_salutation", DataType::Utf8, true),
                Field::new("c_first_name", DataType::Utf8, true),
                Field::new("c_last_name", DataType::Utf8, true),
                Field::new("c_preferred_cust_flag", DataType::Utf8, true),
                Field::new("c_birth_day", DataType::Int64, true),
                Field::new("c_birth_month", DataType::Int64, true),
                Field::new("c_birth_year", DataType::Int64, true),
                Field::new("c_birth_country", DataType::Utf8, true),
                Field::new("c_login", DataType::Utf8, true),
                Field::new("c_email_address", DataType::Utf8, true),
                Field::new("c_last_review_date_sk", DataType::Utf8, true),
            ]),

            "web_site" => Schema::new(vec![
                Field::new("web_site_sk", DataType::Int64, false),
                Field::new("web_site_id", DataType::Utf8, false),
                Field::new("web_rec_start_date", DataType::Date32, true),
                Field::new("web_rec_end_date", DataType::Date32, true),
                Field::new("web_name", DataType::Utf8, true),
                Field::new("web_open_date_sk", DataType::Int64, true),
                Field::new("web_close_date_sk", DataType::Int64, true),
                Field::new("web_class", DataType::Utf8, true),
                Field::new("web_manager", DataType::Utf8, true),
                Field::new("web_mkt_id", DataType::Int64, true),
                Field::new("web_mkt_class", DataType::Utf8, true),
                Field::new("web_mkt_desc", DataType::Utf8, true),
                Field::new("web_market_manager", DataType::Utf8, true),
                Field::new("web_company_id", DataType::Int64, true),
                Field::new("web_company_name", DataType::Utf8, true),
                Field::new("web_street_number", DataType::Utf8, true),
                Field::new("web_street_name", DataType::Utf8, true),
                Field::new("web_street_type", DataType::Utf8, true),
                Field::new("web_suite_number", DataType::Utf8, true),
                Field::new("web_city", DataType::Utf8, true),
                Field::new("web_county", DataType::Utf8, true),
                Field::new("web_state", DataType::Utf8, true),
                Field::new("web_zip", DataType::Utf8, true),
                Field::new("web_country", DataType::Utf8, true),
                Field::new("web_gmt_offset", make_decimal_type(5, 2), true),
                Field::new("web_tax_percentage", make_decimal_type(5, 2), true),
            ]),

            "store_returns" => Schema::new(vec![
                Field::new("sr_returned_date_sk", DataType::Int64, true),
                Field::new("sr_return_time_sk", DataType::Int64, true),
                Field::new("sr_item_sk", DataType::Int64, false),
                Field::new("sr_customer_sk", DataType::Int64, true),
                Field::new("sr_cdemo_sk", DataType::Int64, true),
                Field::new("sr_hdemo_sk", DataType::Int64, true),
                Field::new("sr_addr_sk", DataType::Int64, true),
                Field::new("sr_store_sk", DataType::Int64, true),
                Field::new("sr_reason_sk", DataType::Int64, true),
                Field::new("sr_ticket_number", DataType::Int64, false),
                Field::new("sr_return_quantity", DataType::Int64, true),
                Field::new("sr_return_amt", make_decimal_type(7, 2), true),
                Field::new("sr_return_tax", make_decimal_type(7, 2), true),
                Field::new("sr_return_amt_inc_tax", make_decimal_type(7, 2), true),
                Field::new("sr_fee", make_decimal_type(7, 2), true),
                Field::new("sr_return_ship_cost", make_decimal_type(7, 2), true),
                Field::new("sr_refunded_cash", make_decimal_type(7, 2), true),
                Field::new("sr_reversed_charge", make_decimal_type(7, 2), true),
                Field::new("sr_store_credit", make_decimal_type(7, 2), true),
                Field::new("sr_net_loss", make_decimal_type(7, 2), true),
            ]),

            "household_demographics" => Schema::new(vec![
                Field::new("hd_demo_sk", DataType::Int64, false),
                Field::new("hd_income_band_sk", DataType::Int64, true),
                Field::new("hd_buy_potential", DataType::Utf8, true),
                Field::new("hd_dep_count", DataType::Int64, true),
                Field::new("hd_vehicle_count", DataType::Int64, true),
            ]),

            "web_page" => Schema::new(vec![
                Field::new("wp_web_page_sk", DataType::Int64, false),
                Field::new("wp_web_page_id", DataType::Utf8, false),
                Field::new("wp_rec_start_date", DataType::Date32, true),
                Field::new("wp_rec_end_date", DataType::Date32, true),
                Field::new("wp_creation_date_sk", DataType::Int64, true),
                Field::new("wp_access_date_sk", DataType::Int64, true),
                Field::new("wp_autogen_flag", DataType::Utf8, true),
                Field::new("wp_customer_sk", DataType::Int64, true),
                Field::new("wp_url", DataType::Utf8, true),
                Field::new("wp_type", DataType::Utf8, true),
                Field::new("wp_char_count", DataType::Int64, true),
                Field::new("wp_link_count", DataType::Int64, true),
                Field::new("wp_image_count", DataType::Int64, true),
                Field::new("wp_max_ad_count", DataType::Int64, true),
            ]),

            "promotion" => Schema::new(vec![
                Field::new("p_promo_sk", DataType::Int64, false),
                Field::new("p_promo_id", DataType::Utf8, false),
                Field::new("p_start_date_sk", DataType::Int64, true),
                Field::new("p_end_date_sk", DataType::Int64, true),
                Field::new("p_item_sk", DataType::Int64, true),
                Field::new("p_cost", make_decimal_type(15, 2), true),
                Field::new("p_response_target", DataType::Int64, true),
                Field::new("p_promo_name", DataType::Utf8, true),
                Field::new("p_channel_dmail", DataType::Utf8, true),
                Field::new("p_channel_email", DataType::Utf8, true),
                Field::new("p_channel_catalog", DataType::Utf8, true),
                Field::new("p_channel_tv", DataType::Utf8, true),
                Field::new("p_channel_radio", DataType::Utf8, true),
                Field::new("p_channel_press", DataType::Utf8, true),
                Field::new("p_channel_event", DataType::Utf8, true),
                Field::new("p_channel_demo", DataType::Utf8, true),
                Field::new("p_channel_details", DataType::Utf8, true),
                Field::new("p_purpose", DataType::Utf8, true),
                Field::new("p_discount_active", DataType::Utf8, true),
            ]),

            "catalog_page" => Schema::new(vec![
                Field::new("cp_catalog_page_sk", DataType::Int64, false),
                Field::new("cp_catalog_page_id", DataType::Utf8, false),
                Field::new("cp_start_date_sk", DataType::Int64, true),
                Field::new("cp_end_date_sk", DataType::Int64, true),
                Field::new("cp_department", DataType::Utf8, true),
                Field::new("cp_catalog_number", DataType::Int64, true),
                Field::new("cp_catalog_page_number", DataType::Int64, true),
                Field::new("cp_description", DataType::Utf8, true),
                Field::new("cp_type", DataType::Utf8, true),
            ]),

            "inventory" => Schema::new(vec![
                Field::new("inv_date_sk", DataType::Int64, false),
                Field::new("inv_item_sk", DataType::Int64, false),
                Field::new("inv_warehouse_sk", DataType::Int64, false),
                Field::new("inv_quantity_on_hand", DataType::Int64, true),
            ]),

            "catalog_returns" => Schema::new(vec![
                Field::new("cr_returned_date_sk", DataType::Int64, true),
                Field::new("cr_returned_time_sk", DataType::Int64, true),
                Field::new("cr_item_sk", DataType::Int64, false),
                Field::new("cr_refunded_customer_sk", DataType::Int64, true),
                Field::new("cr_refunded_cdemo_sk", DataType::Int64, true),
                Field::new("cr_refunded_hdemo_sk", DataType::Int64, true),
                Field::new("cr_refunded_addr_sk", DataType::Int64, true),
                Field::new("cr_returning_customer_sk", DataType::Int64, true),
                Field::new("cr_returning_cdemo_sk", DataType::Int64, true),
                Field::new("cr_returning_hdemo_sk", DataType::Int64, true),
                Field::new("cr_returning_addr_sk", DataType::Int64, true),
                Field::new("cr_call_center_sk", DataType::Int64, true),
                Field::new("cr_catalog_page_sk", DataType::Int64, true),
                Field::new("cr_ship_mode_sk", DataType::Int64, true),
                Field::new("cr_warehouse_sk", DataType::Int64, true),
                Field::new("cr_reason_sk", DataType::Int64, true),
                Field::new("cr_order_number", DataType::Int64, false),
                Field::new("cr_return_quantity", DataType::Int64, true),
                Field::new("cr_return_amount", make_decimal_type(7, 2), true),
                Field::new("cr_return_tax", make_decimal_type(7, 2), true),
                Field::new("cr_return_amt_inc_tax", make_decimal_type(7, 2), true),
                Field::new("cr_fee", make_decimal_type(7, 2), true),
                Field::new("cr_return_ship_cost", make_decimal_type(7, 2), true),
                Field::new("cr_refunded_cash", make_decimal_type(7, 2), true),
                Field::new("cr_reversed_charge", make_decimal_type(7, 2), true),
                Field::new("cr_store_credit", make_decimal_type(7, 2), true),
                Field::new("cr_net_loss", make_decimal_type(7, 2), true),
            ]),

            "web_returns" => Schema::new(vec![
                Field::new("wr_returned_date_sk", DataType::Int64, true),
                Field::new("wr_returned_time_sk", DataType::Int64, true),
                Field::new("wr_item_sk", DataType::Int64, false),
                Field::new("wr_refunded_customer_sk", DataType::Int64, true),
                Field::new("wr_refunded_cdemo_sk", DataType::Int64, true),
                Field::new("wr_refunded_hdemo_sk", DataType::Int64, true),
                Field::new("wr_refunded_addr_sk", DataType::Int64, true),
                Field::new("wr_returning_customer_sk", DataType::Int64, true),
                Field::new("wr_returning_cdemo_sk", DataType::Int64, true),
                Field::new("wr_returning_hdemo_sk", DataType::Int64, true),
                Field::new("wr_returning_addr_sk", DataType::Int64, true),
                Field::new("wr_web_page_sk", DataType::Int64, true),
                Field::new("wr_reason_sk", DataType::Int64, true),
                Field::new("wr_order_number", DataType::Int64, false),
                Field::new("wr_return_quantity", DataType::Int64, true),
                Field::new("wr_return_amt", make_decimal_type(7, 2), true),
                Field::new("wr_return_tax", make_decimal_type(7, 2), true),
                Field::new("wr_return_amt_inc_tax", make_decimal_type(7, 2), true),
                Field::new("wr_fee", make_decimal_type(7, 2), true),
                Field::new("wr_return_ship_cost", make_decimal_type(7, 2), true),
                Field::new("wr_refunded_cash", make_decimal_type(7, 2), true),
                Field::new("wr_reversed_charge", make_decimal_type(7, 2), true),
                Field::new("wr_account_credit", make_decimal_type(7, 2), true),
                Field::new("wr_net_loss", make_decimal_type(7, 2), true),
            ]),

            "web_sales" => Schema::new(vec![
                Field::new("ws_sold_date_sk", DataType::Int64, true),
                Field::new("ws_sold_time_sk", DataType::Int64, true),
                Field::new("ws_ship_date_sk", DataType::Int64, true),
                Field::new("ws_item_sk", DataType::Int64, false),
                Field::new("ws_bill_customer_sk", DataType::Int64, true),
                Field::new("ws_bill_cdemo_sk", DataType::Int64, true),
                Field::new("ws_bill_hdemo_sk", DataType::Int64, true),
                Field::new("ws_bill_addr_sk", DataType::Int64, true),
                Field::new("ws_ship_customer_sk", DataType::Int64, true),
                Field::new("ws_ship_cdemo_sk", DataType::Int64, true),
                Field::new("ws_ship_hdemo_sk", DataType::Int64, true),
                Field::new("ws_ship_addr_sk", DataType::Int64, true),
                Field::new("ws_web_page_sk", DataType::Int64, true),
                Field::new("ws_web_site_sk", DataType::Int64, true),
                Field::new("ws_ship_mode_sk", DataType::Int64, true),
                Field::new("ws_warehouse_sk", DataType::Int64, true),
                Field::new("ws_promo_sk", DataType::Int64, true),
                Field::new("ws_order_number", DataType::Int64, false),
                Field::new("ws_quantity", DataType::Int64, true),
                Field::new("ws_wholesale_cost", make_decimal_type(7, 2), true),
                Field::new("ws_list_price", make_decimal_type(7, 2), true),
                Field::new("ws_sales_price", make_decimal_type(7, 2), true),
                Field::new("ws_ext_discount_amt", make_decimal_type(7, 2), true),
                Field::new("ws_ext_sales_price", make_decimal_type(7, 2), true),
                Field::new("ws_ext_wholesale_cost", make_decimal_type(7, 2), true),
                Field::new("ws_ext_list_price", make_decimal_type(7, 2), true),
                Field::new("ws_ext_tax", make_decimal_type(7, 2), true),
                Field::new("ws_coupon_amt", make_decimal_type(7, 2), true),
                Field::new("ws_ext_ship_cost", make_decimal_type(7, 2), true),
                Field::new("ws_net_paid", make_decimal_type(7, 2), true),
                Field::new("ws_net_paid_inc_tax", make_decimal_type(7, 2), true),
                Field::new("ws_net_paid_inc_ship", make_decimal_type(7, 2), true),
                Field::new("ws_net_paid_inc_ship_tax", make_decimal_type(7, 2), true),
                Field::new("ws_net_profit", make_decimal_type(7, 2), true),
            ]),

            "catalog_sales" => Schema::new(vec![
                Field::new("cs_sold_date_sk", DataType::Int64, true),
                Field::new("cs_sold_time_sk", DataType::Int64, true),
                Field::new("cs_ship_date_sk", DataType::Int64, true),
                Field::new("cs_bill_customer_sk", DataType::Int64, true),
                Field::new("cs_bill_cdemo_sk", DataType::Int64, true),
                Field::new("cs_bill_hdemo_sk", DataType::Int64, true),
                Field::new("cs_bill_addr_sk", DataType::Int64, true),
                Field::new("cs_ship_customer_sk", DataType::Int64, true),
                Field::new("cs_ship_cdemo_sk", DataType::Int64, true),
                Field::new("cs_ship_hdemo_sk", DataType::Int64, true),
                Field::new("cs_ship_addr_sk", DataType::Int64, true),
                Field::new("cs_call_center_sk", DataType::Int64, true),
                Field::new("cs_catalog_page_sk", DataType::Int64, true),
                Field::new("cs_ship_mode_sk", DataType::Int64, true),
                Field::new("cs_warehouse_sk", DataType::Int64, true),
                Field::new("cs_item_sk", DataType::Int64, false),
                Field::new("cs_promo_sk", DataType::Int64, true),
                Field::new("cs_order_number", DataType::Int64, false),
                Field::new("cs_quantity", DataType::Int64, true),
                Field::new("cs_wholesale_cost", make_decimal_type(7, 2), true),
                Field::new("cs_list_price", make_decimal_type(7, 2), true),
                Field::new("cs_sales_price", make_decimal_type(7, 2), true),
                Field::new("cs_ext_discount_amt", make_decimal_type(7, 2), true),
                Field::new("cs_ext_sales_price", make_decimal_type(7, 2), true),
                Field::new("cs_ext_wholesale_cost", make_decimal_type(7, 2), true),
                Field::new("cs_ext_list_price", make_decimal_type(7, 2), true),
                Field::new("cs_ext_tax", make_decimal_type(7, 2), true),
                Field::new("cs_coupon_amt", make_decimal_type(7, 2), true),
                Field::new("cs_ext_ship_cost", make_decimal_type(7, 2), true),
                Field::new("cs_net_paid", make_decimal_type(7, 2), true),
                Field::new("cs_net_paid_inc_tax", make_decimal_type(7, 2), true),
                Field::new("cs_net_paid_inc_ship", make_decimal_type(7, 2), true),
                Field::new("cs_net_paid_inc_ship_tax", make_decimal_type(7, 2), true),
                Field::new("cs_net_profit", make_decimal_type(7, 2), true),
            ]),

            "store_sales" => Schema::new(vec![
                Field::new("ss_sold_date_sk", DataType::Int64, true),
                Field::new("ss_sold_time_sk", DataType::Int64, true),
                Field::new("ss_item_sk", DataType::Int64, false),
                Field::new("ss_customer_sk", DataType::Int64, true),
                Field::new("ss_cdemo_sk", DataType::Int64, true),
                Field::new("ss_hdemo_sk", DataType::Int64, true),
                Field::new("ss_addr_sk", DataType::Int64, true),
                Field::new("ss_store_sk", DataType::Int64, true),
                Field::new("ss_promo_sk", DataType::Int64, true),
                Field::new("ss_ticket_number", DataType::Int64, false),
                Field::new("ss_quantity", DataType::Int64, true),
                Field::new("ss_wholesale_cost", make_decimal_type(7, 2), true),
                Field::new("ss_list_price", make_decimal_type(7, 2), true),
                Field::new("ss_sales_price", make_decimal_type(7, 2), true),
                Field::new("ss_ext_discount_amt", make_decimal_type(7, 2), true),
                Field::new("ss_ext_sales_price", make_decimal_type(7, 2), true),
                Field::new("ss_ext_wholesale_cost", make_decimal_type(7, 2), true),
                Field::new("ss_ext_list_price", make_decimal_type(7, 2), true),
                Field::new("ss_ext_tax", make_decimal_type(7, 2), true),
                Field::new("ss_coupon_amt", make_decimal_type(7, 2), true),
                Field::new("ss_net_paid", make_decimal_type(7, 2), true),
                Field::new("ss_net_paid_inc_tax", make_decimal_type(7, 2), true),
                Field::new("ss_net_profit", make_decimal_type(7, 2), true),
            ]),

            _ => panic!(),
        }
    }

    fn get_table_ext(&self) -> &str {
        "dat"
    }

    fn get_partition_col(&self, table: &str) -> Option<&str> {
        match table {
            "store_sales" => Some("ss_sold_date_sk"),
            "catalog_sales" => Some("cs_sold_date_sk"),
            "web_sales" => Some("ws_sold_date_sk"),
            "store_returns" => Some("sr_returned_date_sk"),
            "catalog_returns" => Some("cr_returned_date_sk"),
            "web_returns" => Some("wr_returned_date_sk"),
            "inventory" => Some("inv_date_sk"),
            _ => None,
        }
    }
}

fn make_decimal_type(p: u8, s: i8) -> DataType {
    DataType::Decimal128(p, s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Tpc;

    fn tpcds() -> TpcDs {
        TpcDs::new()
    }

    #[test]
    fn table_names_returns_24_tables() {
        let t = tpcds();
        let names = t.get_table_names();
        assert_eq!(names.len(), 24);
        assert!(names.contains(&"store_sales"));
        assert!(names.contains(&"catalog_sales"));
        assert!(names.contains(&"web_sales"));
        assert!(names.contains(&"customer"));
        assert!(names.contains(&"date_dim"));
        assert!(names.contains(&"item"));
    }

    #[test]
    fn table_ext_is_dat() {
        assert_eq!(tpcds().get_table_ext(), "dat");
    }

    #[test]
    fn field_counts() {
        let t = tpcds();
        let expected = vec![
            ("customer_address", 13),
            ("customer_demographics", 9),
            ("date_dim", 28),
            ("warehouse", 14),
            ("ship_mode", 6),
            ("time_dim", 10),
            ("reason", 3),
            ("income_band", 3),
            ("item", 22),
            ("store", 29),
            ("call_center", 31),
            ("customer", 18),
            ("web_site", 26),
            ("store_returns", 20),
            ("household_demographics", 5),
            ("web_page", 14),
            ("promotion", 19),
            ("catalog_page", 9),
            ("inventory", 4),
            ("catalog_returns", 27),
            ("web_returns", 24),
            ("web_sales", 34),
            ("catalog_sales", 34),
            ("store_sales", 23),
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
    fn schemas_do_not_have_trailing_ignore_field() {
        let t = tpcds();
        for table in t.get_table_names() {
            let schema = t.get_schema(table);
            let last = schema.fields().last().unwrap();
            assert_ne!(
                last.name(),
                "ignore",
                "table {} should not have trailing ignore field",
                table
            );
        }
    }

    #[test]
    fn primary_key_fields_are_non_nullable_int64() {
        let t = tpcds();
        let cases = vec![
            ("customer_address", "ca_address_sk"),
            ("customer_demographics", "cd_demo_sk"),
            ("date_dim", "d_date_sk"),
            ("warehouse", "w_warehouse_sk"),
            ("ship_mode", "sm_ship_mode_sk"),
            ("time_dim", "t_time_sk"),
            ("reason", "r_reason_sk"),
            ("income_band", "ib_income_band_sk"),
            ("item", "i_item_sk"),
            ("store", "s_store_sk"),
            ("call_center", "cc_call_center_sk"),
            ("customer", "c_customer_sk"),
            ("web_site", "web_site_sk"),
            ("household_demographics", "hd_demo_sk"),
            ("web_page", "wp_web_page_sk"),
            ("promotion", "p_promo_sk"),
            ("catalog_page", "cp_catalog_page_sk"),
        ];
        for (table, pk) in cases {
            let schema = t.get_schema(table);
            let field = schema.field_with_name(pk).unwrap();
            assert!(!field.is_nullable(), "{}.{} should be non-nullable", table, pk);
            assert_eq!(*field.data_type(), DataType::Int64, "{}.{} should be Int64", table, pk);
        }
    }

    #[test]
    fn decimal_precision_5_2_for_offsets() {
        let t = tpcds();
        let schema = t.get_schema("customer_address");
        let field = schema.field_with_name("ca_gmt_offset").unwrap();
        assert_eq!(*field.data_type(), DataType::Decimal128(5, 2));
    }

    #[test]
    fn decimal_precision_7_2_for_monetary() {
        let t = tpcds();
        let schema = t.get_schema("store_sales");
        let field = schema.field_with_name("ss_wholesale_cost").unwrap();
        assert_eq!(*field.data_type(), DataType::Decimal128(7, 2));
    }

    #[test]
    fn decimal_precision_15_2_for_promotion_cost() {
        let t = tpcds();
        let schema = t.get_schema("promotion");
        let field = schema.field_with_name("p_cost").unwrap();
        assert_eq!(*field.data_type(), DataType::Decimal128(15, 2));
    }

    #[test]
    fn date32_fields() {
        let t = tpcds();
        let schema = t.get_schema("date_dim");
        let field = schema.field_with_name("d_date").unwrap();
        assert_eq!(*field.data_type(), DataType::Date32);

        let schema = t.get_schema("item");
        let field = schema.field_with_name("i_rec_start_date").unwrap();
        assert_eq!(*field.data_type(), DataType::Date32);
    }

    #[test]
    fn make_decimal_type_helper() {
        assert_eq!(make_decimal_type(5, 2), DataType::Decimal128(5, 2));
        assert_eq!(make_decimal_type(7, 2), DataType::Decimal128(7, 2));
        assert_eq!(make_decimal_type(15, 2), DataType::Decimal128(15, 2));
    }

    #[test]
    fn all_tables_have_non_empty_schemas() {
        let t = tpcds();
        for table in t.get_table_names() {
            let schema = t.get_schema(table);
            assert!(
                !schema.fields().is_empty(),
                "table {} has empty schema",
                table
            );
        }
    }

    #[test]
    fn first_field_name_prefix() {
        let t = tpcds();
        let cases = vec![
            ("customer_address", "ca_"),
            ("customer_demographics", "cd_"),
            ("date_dim", "d_"),
            ("warehouse", "w_"),
            ("ship_mode", "sm_"),
            ("time_dim", "t_"),
            ("reason", "r_"),
            ("income_band", "ib_"),
            ("item", "i_"),
            ("store", "s_"),
            ("call_center", "cc_"),
            ("customer", "c_"),
            ("store_sales", "ss_"),
            ("store_returns", "sr_"),
            ("catalog_sales", "cs_"),
            ("catalog_returns", "cr_"),
            ("web_sales", "ws_"),
            ("web_returns", "wr_"),
            ("web_page", "wp_"),
            ("web_site", "web_"),
            ("catalog_page", "cp_"),
            ("household_demographics", "hd_"),
            ("promotion", "p_"),
            ("inventory", "inv_"),
        ];
        for (table, prefix) in cases {
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
        let t = tpcds();
        assert_eq!(t.get_partition_col("store_sales"), Some("ss_sold_date_sk"));
        assert_eq!(t.get_partition_col("catalog_sales"), Some("cs_sold_date_sk"));
        assert_eq!(t.get_partition_col("web_sales"), Some("ws_sold_date_sk"));
        assert_eq!(t.get_partition_col("store_returns"), Some("sr_returned_date_sk"));
        assert_eq!(t.get_partition_col("catalog_returns"), Some("cr_returned_date_sk"));
        assert_eq!(t.get_partition_col("web_returns"), Some("wr_returned_date_sk"));
        assert_eq!(t.get_partition_col("inventory"), Some("inv_date_sk"));
    }

    #[test]
    fn partition_col_dimension_tables_return_none() {
        let t = tpcds();
        let dimension_tables = [
            "call_center", "catalog_page", "customer", "customer_address",
            "customer_demographics", "date_dim", "income_band",
            "household_demographics", "store", "ship_mode", "reason",
            "promotion", "item", "web_page", "warehouse", "time_dim", "web_site",
        ];
        for table in &dimension_tables {
            assert_eq!(t.get_partition_col(table), None, "{} should not be partitioned", table);
        }
    }

    #[test]
    #[should_panic]
    fn invalid_table_name_panics() {
        tpcds().get_schema("nonexistent");
    }
}
