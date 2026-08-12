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

use crate::file::FsContext;
use curvine_core_error::CommonResult;
use curvine_error::FsResult;
use curvine_io::DataSlice;
use curvine_metrics::{
    Counter, CounterVec, Gauge, HistogramVec, MetricFamilyType, Metrics, Metrics as m,
};
use curvine_model::{MetricType, MetricValue};
use curvine_rpc::handler::RpcReceiveStats;
use curvine_runtime::common::TimeSpent;
use curvine_runtime::sync::FastDashMap;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::future::Future;

pub struct ClientMetrics {
    pub mount_cache_hits: CounterVec,
    pub mount_cache_misses: CounterVec,
    pub last_value_map: FastDashMap<String, f64>,

    pub metadata_operation_duration: HistogramVec,
    pub write_bytes: Counter,
    pub write_time_us: Counter,
    pub read_bytes: Counter,
    pub read_time_us: Counter,
    pub read_block_receive_bytes: Counter,
    pub read_block_receive_duration_us: HistogramVec,
    pub fuse_read_pattern_total: CounterVec,
    pub block_idle_conn: Gauge,
}

impl ClientMetrics {
    pub const PREFIX: &'static str = "client";

    pub fn new(buckets: &[f64]) -> CommonResult<Self> {
        let cm = Self {
            mount_cache_hits: m::new_counter_vec(
                "client_mount_cache_hits",
                "mount cache miss count",
                &["id"],
            )?,
            mount_cache_misses: m::new_counter_vec(
                "client_mount_cache_misses",
                "mount cache miss count",
                &["id"],
            )?,

            last_value_map: FastDashMap::default(),

            metadata_operation_duration: m::new_histogram_vec_with_buckets(
                "client_metadata_operation_duration",
                "metadata operation duration",
                &["operation"],
                buckets,
            )?,
            write_bytes: m::new_counter("client_write_bytes", "write bytes total")?,
            write_time_us: m::new_counter("client_write_time_us", "write time us total")?,
            read_bytes: m::new_counter("client_read_bytes", "read bytes total")?,
            read_time_us: m::new_counter("client_read_time_us", "read time us total")?,
            read_block_receive_bytes: m::new_counter(
                "client_read_block_receive_bytes",
                "ReadBlock response payload bytes received by raw RPC clients",
            )?,
            read_block_receive_duration_us: m::new_histogram_vec(
                "client_read_block_receive_duration_us",
                "ReadBlock raw RPC receive duration by frame segment in microseconds",
                &["stage"],
            )?,
            fuse_read_pattern_total: m::new_counter_vec(
                "client_fuse_read_pattern_total",
                "Curvine reader mode selected for FUSE reads after applying the offset; \
                 pattern=detector_disabled means smart-prefetch pattern detection is disabled",
                &["pattern"],
            )?,
            block_idle_conn: m::new_gauge("client_block_idle_conn", "block idle conn total")?,
        };

        Ok(cm)
    }

    /// Run a write future and record elapsed time; bytes are counted only on success.
    pub async fn track_write<F>(&self, len: i64, fut: F) -> FsResult<i64>
    where
        F: Future<Output = FsResult<()>>,
    {
        let spent = TimeSpent::new();
        let res = fut.await;
        self.write_time_us.inc_by(spent.used_us() as i64);

        res?;
        self.write_bytes.inc_by(len);
        Ok(len)
    }

    /// Run a read future and record elapsed time; returned bytes are counted on success.
    pub async fn track_read<F>(&self, fut: F) -> FsResult<DataSlice>
    where
        F: Future<Output = FsResult<DataSlice>>,
    {
        let spent = TimeSpent::new();
        let res = fut.await;
        self.read_time_us.inc_by(spent.used_us() as i64);

        let slice = res?;
        self.read_bytes.inc_by(slice.len() as i64);
        Ok(slice)
    }

    pub fn record_fuse_read_pattern(&self, pattern: &'static str) {
        self.fuse_read_pattern_total
            .with_label_values(&[pattern])
            .inc();
    }

    pub fn record_read_block_receive(&self, stats: RpcReceiveStats) {
        self.read_block_receive_bytes
            .inc_by(stats.payload_len as i64);
        for (stage, elapsed_us) in [
            ("protocol", stats.protocol_read_us),
            ("header", stats.header_read_us),
            ("payload", stats.payload_read_us),
        ] {
            self.read_block_receive_duration_us
                .with_label_values(&[stage])
                .observe(elapsed_us as f64);
        }
    }

    pub fn text_output(&self) -> CommonResult<String> {
        Metrics::text_output()
    }

    pub fn encode() -> CommonResult<Vec<MetricValue>> {
        let cm = FsContext::get_metrics();
        let mut metric_values = Vec::new();
        let metric_families = Metrics::registry().gather();
        for mf in metric_families {
            let name = mf.get_name().to_string();
            if !name.starts_with(Self::PREFIX) {
                continue;
            }

            let metric_type = match mf.get_field_type() {
                MetricFamilyType::COUNTER => MetricType::Counter,
                MetricFamilyType::GAUGE => MetricType::Gauge,
                _ => MetricType::Gauge,
            };

            for metric in mf.get_metric() {
                let mut tags = HashMap::new();
                for label_pair in metric.get_label() {
                    tags.insert(
                        label_pair.get_name().to_string(),
                        label_pair.get_value().to_string(),
                    );
                }

                let value = match metric_type {
                    MetricType::Counter => {
                        if metric.has_counter() {
                            metric.get_counter().get_value()
                        } else {
                            0.0
                        }
                    }
                    MetricType::Gauge => {
                        if metric.has_gauge() {
                            metric.get_gauge().get_value()
                        } else {
                            0.0
                        }
                    }
                    MetricType::Histogram => {
                        if metric.has_histogram() {
                            metric.get_histogram().get_sample_count() as f64
                        } else {
                            0.0
                        }
                    }
                };

                let incr_value = {
                    let key = format!("{}:{:?}", name, tags);
                    let mut last_value = cm.last_value_map.entry(key).or_insert(0.0);
                    let incr_value = value - *last_value;
                    *last_value = value;
                    incr_value
                };

                if incr_value > 0f64 {
                    metric_values.push(MetricValue {
                        metric_type,
                        name: name.clone(),
                        value: incr_value,
                        tags,
                    });
                }
            }
        }

        Ok(metric_values)
    }
}

impl Debug for ClientMetrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ClientMetrics")
    }
}
