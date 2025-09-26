// Copyright 2025 OPPO.
//
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

use crate::util::{bytes_to_string, handle_rpc_result, parse_size_string};
use clap::{Parser, Subcommand};
use curvine_client::file::FsClient;
use num_bigint::BigInt;
use std::sync::Arc;

#[derive(Parser, Debug)]
pub struct QuotaCommand {
    #[command(subcommand)]
    pub action: QuotaAction,
}

#[derive(Subcommand, Debug)]
pub enum QuotaAction {
    #[command(name = "add")]
    Add {
        #[arg(long)]
        path: String,
        #[arg(long = "quota-size")]
        quota_size: String,
    },

    #[command(name = "list")]
    List,

    #[command(name = "remove")]
    Remove {
        #[arg(long)]
        path: String,
    },

    #[command(name = "update")]
    Update {
        #[arg(long)]
        path: String,
        #[arg(long = "quota-size")]
        quota_size: String,
    },
}

impl QuotaCommand {
    pub async fn execute(&self, client: Arc<FsClient>) {
        match &self.action {
            QuotaAction::Add { path, quota_size } => {
                self.add_quota(&client, path, quota_size).await;
            }
            QuotaAction::List => {
                self.list_quotas(&client).await;
            }
            QuotaAction::Remove { path } => {
                self.remove_quota(&client, path).await;
            }
            QuotaAction::Update { path, quota_size } => {
                self.update_quota(&client, path, quota_size).await;
            }
        }
    }

    async fn add_quota(&self, client: &Arc<FsClient>, path: &str, quota_size_str: &str) {
        let quota_size_bytes = match parse_size_string(quota_size_str) {
            Ok(size) => size,
            Err(e) => {
                eprintln!("❌ Error parsing quota size '{}': {}", quota_size_str, e);
                std::process::exit(1);
            }
        };

        let quota_size_i64 = match quota_size_bytes.to_string().parse::<i64>() {
            Ok(size) => size,
            Err(_) => {
                eprintln!("❌ Error: quota size too large");
                std::process::exit(1);
            }
        };

        handle_rpc_result(client.add_quota(path, quota_size_i64)).await;

        println!(
            "✅ Successfully added quota definition for path {} with size {}.",
            path, quota_size_str
        );
    }

    async fn list_quotas(&self, client: &Arc<FsClient>) {
        let quotas = handle_rpc_result(client.get_quota_table()).await;

        if quotas.is_empty() {
            println!("No quota definitions found.");
            return;
        }

        println!(
            "{:<40} {:>15} {:>15} {:>15}",
            "Curvine path", "Capacity", "Used", "State"
        );
        println!("{}", "-".repeat(85));

        for quota in quotas {
            let capacity = bytes_to_string(&BigInt::from(quota.quota_size));
            let used = match quota.state {
                curvine_common::state::QuotaState::Calculating => "Calculating".to_string(),
                _ => {
                    if quota.used_size == 0 {
                        "0".to_string()
                    } else {
                        bytes_to_string(&BigInt::from(quota.used_size))
                    }
                }
            };

            println!(
                "{:<40} {:>15} {:>15} {:>15}",
                quota.path, capacity, used, quota.state
            );
        }
    }

    async fn remove_quota(&self, client: &Arc<FsClient>, path: &str) {
        handle_rpc_result(client.remove_quota(path)).await;

        println!(
            "✅ Successfully removed quota definition for path {}.",
            path
        );
    }

    async fn update_quota(&self, client: &Arc<FsClient>, path: &str, quota_size_str: &str) {
        let quota_size_bytes = match parse_size_string(quota_size_str) {
            Ok(size) => size,
            Err(e) => {
                eprintln!("❌ Error parsing quota size '{}': {}", quota_size_str, e);
                std::process::exit(1);
            }
        };

        let quota_size_i64 = match quota_size_bytes.to_string().parse::<i64>() {
            Ok(size) => size,
            Err(_) => {
                eprintln!("❌ Error: quota size too large");
                std::process::exit(1);
            }
        };

        handle_rpc_result(client.update_quota(path, quota_size_i64)).await;

        println!(
            "✅ Successfully updated quota for path {} to size {}.",
            path, quota_size_str
        );
    }
}
