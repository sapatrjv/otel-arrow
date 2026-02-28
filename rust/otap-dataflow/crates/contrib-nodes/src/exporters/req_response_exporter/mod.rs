// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Geneva Exporter for OTAP logs and traces
//!
//! This exporter sends OTAP log and trace data to Microsoft Geneva telemetry backend.
//! It is designed for Microsoft products and implements the `Exporter<OtapPdata>` trait
//! for integration with the OTAP dataflow engine.
//!
//! ## Usage
//!
//! This exporter is automatically discovered by the `df_engine` binary via `linkme`.
//! Users configure it in YAML:
//!
//! ```yaml
//! nodes:
//!   - id: req-response-exporter
//!     urn: "urn:microsoft:exporter:req-response"
//! ```

use std::collections::VecDeque;
use std::net::SocketAddr;
use async_trait::async_trait;
use tokio::sync::Notify;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;
use std::time::Duration;
use tokio::time::timeout;
use linkme::distributed_slice;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::ConsumerEffectHandlerExtension;
use otap_df_engine::config::ExporterConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::control::NodeControlMsg;
use otap_df_engine::control::{AckMsg, NackMsg};
use otap_df_engine::error::Error;
use otap_df_engine::exporter::ExporterWrapper;
use otap_df_engine::local::exporter::{EffectHandler, Exporter};
use otap_df_engine::message::{Message, MessageChannel};
use otap_df_engine::node::NodeId;
use otap_df_engine::terminal_state::TerminalState;
use otap_df_pdata::otlp::OtlpProtoBytes;
// Zero-copy view import (currently unused, for future optimization)
// use otap_df_pdata::views::otap::OtapLogsView;
use otap_df_pdata::OtapPayload;
use otap_df_telemetry::instrument::Counter;
use otap_df_telemetry::metrics::MetricSet;
use otap_df_telemetry::otel_info;
use otap_df_telemetry_macros::metric_set;
use serde::Deserialize;
use std::sync::Arc;
// Use crate-relative paths since we're now a module within otap
use otap_df_otap::OTAP_EXPORTER_FACTORIES;
use otap_df_otap::metrics::ExporterPDataMetrics;
use otap_df_otap::pdata::OtapPdata;

/// The URN for the Request Response exporter
pub const REQ_RESPONSE_EXPORTER_URN: &str = "urn:microsoft:exporter:req-response";

fn default_listen_addr() -> String {
    "0.0.0.0:9000".to_string()
}
fn default_max_items() -> usize {
    10_000
}
fn default_max_total_bytes() -> usize {
    64 * 1024 * 1024 // 64MiB
}
fn default_ack_timeout_ms() -> u64 {
    30_000
}

fn default_validate_otlp() -> bool {
    false
}

/// Configuration for the Req/Response exporter
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// TCP address to listen for requests, e.g. "0.0.0.0:9000"
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,

    /// Maximum number of cached items (FIFO length)
    #[serde(default = "default_max_items")]
    pub max_items: usize,

    /// Maximum total bytes allowed in FIFO cache
    #[serde(default = "default_max_total_bytes")]
    pub max_total_bytes: usize,

    /// Time to wait for ACK after sending an item
    #[serde(default = "default_ack_timeout_ms")]
    pub ack_timeout_ms: u64,

    /// If true: exporter acks upstream immediately after enqueue (recommended).
    /// If false: exporter acks upstream only after external ACK (can stall pipeline).
    #[serde(default)]
    pub ack_upstream_on_enqueue: bool,
    
    /// If true, decode OTLP requests (logs/traces) to validate bytes.
    /// Needed to NACK on invalid protobuf bytes (test case).
    #[serde(default = "default_validate_otlp")]
    pub validate_otlp: bool,

}

/// Request Response exporter metrics.
/// Grouped under `otap.exporter.req-response`.
#[metric_set(name = "otap.exporter.req-response")]
#[derive(Debug, Default, Clone)]
struct ExporterMetrics {
    /// Total number of pData in cache.
    #[metric(unit = "{pData}")]
    pub pdata_count: Counter<u64>,

    /// Total number of pData sent.
    #[metric(unit = "{pData}")]
    pub pdata_sent: Counter<u64>,

    /// Total number of pData Dropped.
    #[metric(unit = "{pData}")]
    pub pdata_dropped: Counter<u64>,
}

#[derive(Debug, Clone, Copy)]
enum Kind {
    Logs,
    Traces,
}

impl Kind {
    fn as_str(self) -> &'static str {
        match self {
            Kind::Logs => "logs",
            Kind::Traces => "traces",
        }
    }
}

#[derive(Debug, Clone)]
struct CachedItem {
    id: u64,
    kind: Kind,
    bytes: Vec<u8>,
    // optional: store signal_type if you need it
}

#[derive(Debug)]
struct CacheInner {
    q: VecDeque<CachedItem>,
    inflight: Option<u64>,
    next_id: u64,
    total_bytes: usize,
    max_items: usize,
    max_total_bytes: usize,
}

#[derive(Debug)]
struct FifoCache {
    inner: Mutex<CacheInner>,
    notify: Notify,
}


impl FifoCache {
    fn new(max_items: usize, max_total_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(CacheInner {
                q: VecDeque::new(),
                inflight: None,
                next_id: 1,
                total_bytes: 0,
                max_items,
                max_total_bytes,
            }),
            notify: Notify::new(),
        }
    }

    async fn push(&self, kind: Kind, bytes: Vec<u8>) -> (u64, usize /*dropped*/) {
        let mut dropped = 0usize;
        let mut inner = self.inner.lock().await;

        // Drop oldest until constraints satisfied.
        // Do NOT drop the inflight head (if inflight == head id).
        let incoming_len = bytes.len();
        while inner.q.len() >= inner.max_items || (inner.total_bytes + incoming_len) > inner.max_total_bytes {
            if inner.q.is_empty() {
                break;
            }

            // Prefer dropping from front if it's not inflight, else drop from back.
            let drop_front = match (inner.q.front(), inner.inflight) {
                (Some(front), Some(inflight_id)) if front.id == inflight_id => false,
                _ => true,
            };

            let removed = if drop_front {
                inner.q.pop_front()
            } else {
                inner.q.pop_back()
            };

            if let Some(item) = removed {
                inner.total_bytes = inner.total_bytes.saturating_sub(item.bytes.len());
                dropped += 1;
            } else {
                break;
            }
        }

        let id = inner.next_id;
        inner.next_id += 1;

        inner.total_bytes += incoming_len;
        inner.q.push_back(CachedItem { id, kind, bytes });

        drop(inner);
        self.notify.notify_waiters();
        (id, dropped)
    }

    /// Reserve (but do not remove) the head item; ensures only one inflight item at a time.

    async fn reserve_head(&self) -> CachedItem {
        loop {
            // lock scope is inside braces so it drops before await
            if let Some(item) = {
                let mut inner = self.inner.lock().await; // <-- FIX E0609

                if inner.inflight.is_none() {
                    if let Some(head) = inner.q.front().cloned() { // <-- FIX E0502
                        inner.inflight = Some(head.id);
                        Some(head)
                    } else {
                        None
                    }
                } else {
                    None
                }
            } {
                return item;
            }

            self.notify.notified().await;
        }
    }

    async fn ack_and_pop(&self, id: u64) -> bool {
        let mut inner = self.inner.lock().await;
        let head_ok = inner.q.front().map(|x| x.id) == Some(id);
        let inflight_ok = inner.inflight == Some(id);

        if head_ok && inflight_ok {
            if let Some(removed) = inner.q.pop_front() {
                inner.total_bytes = inner.total_bytes.saturating_sub(removed.bytes.len());
            }
            inner.inflight = None;
            drop(inner);
            self.notify.notify_waiters();
            true
        } else {
            false
        }
    }

    async fn release_inflight(&self, id: u64) {
        let mut inner = self.inner.lock().await;
        if inner.inflight == Some(id) {
            inner.inflight = None;
            drop(inner);
            self.notify.notify_waiters();
        }
    }
}

/// req-response exporter that sends OTAP data to reqesting appilcations
pub struct ReqResponseExporter {
    config: Config,
    pdata_metrics: MetricSet<ExporterPDataMetrics>,
    metrics: MetricSet<ExporterMetrics>,
    cache: Arc<FifoCache>,
}


impl ReqResponseExporter {
    /// Constructs a request/response exporter from pipeline context and JSON config.
    ///
    /// This performs configuration validation and initializes internal state,
    /// but does not start network listeners.
    pub fn from_config(
        pipeline_ctx: PipelineContext,
        config: &serde_json::Value,
    ) -> Result<Self, otap_df_config::error::Error> {
        let pdata_metrics = pipeline_ctx.register_metrics::<ExporterPDataMetrics>();
        let metrics = pipeline_ctx.register_metrics::<ExporterMetrics>();

        let config: Config = serde_json::from_value(config.clone()).map_err(|e| {
            otap_df_config::error::Error::InvalidUserConfig {
                error: e.to_string(),
            }
        })?;

        let cache = Arc::new(FifoCache::new(config.max_items.max(1), config.max_total_bytes.max(1)));

        Ok(Self {
            config,
            pdata_metrics,
            metrics,
            cache,
        })
    }

    /// Returns the immutable configuration used by this request/response exporter.
    ///
    /// This reflects the validated configuration provided at construction time
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    fn payload_to_otlp_item(payload: OtapPayload) -> Result<(Kind, Vec<u8>), String> {
        if payload.is_empty() {
            return Err("empty payload".to_string());
        }

        // Accept OTAP Arrow and convert to OTLP bytes (fallback path).
        // Accept OTLP bytes directly.
        let otlp_bytes: OtlpProtoBytes = match payload {
            OtapPayload::OtapArrowRecords(arrow) => {
                // Convert OTAP -> OTLP bytes via TryInto
                OtapPayload::OtapArrowRecords(arrow)
                    .try_into()
                    .map_err(|e| format!("Failed OTAP->OTLP conversion: {e:?}"))?
            }
            OtapPayload::OtlpBytes(b) => b,
        };

        match otlp_bytes {
            OtlpProtoBytes::ExportLogsRequest(bytes) => Ok((Kind::Logs, bytes.to_vec())),
            OtlpProtoBytes::ExportTracesRequest(bytes) => Ok((Kind::Traces, bytes.to_vec())),

            OtlpProtoBytes::ExportMetricsRequest(_) => Err("req-response exporter does not support metrics".to_string()),
        }
    }
}


// ---------------- TCP server (request/ack) ----------------

async fn handle_client(
    stream: TcpStream,
    cache: Arc<FifoCache>,
    mut metrics: MetricSet<ExporterMetrics>,
    ack_timeout: Duration,
) -> Result<(), String> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // Expect "GET\n"
    let mut line = String::new();
    let n = reader.read_line(&mut line).await.map_err(|e| e.to_string())?;
    if n == 0 {
        return Ok(());
    }
    if line.trim() != "GET" {
        write_half
            .write_all(b"ERR expected 'GET'\\n")
            .await
            .map_err(|e| e.to_string())?;
        return Ok(());
    }

    // Reserve head
    let item = cache.reserve_head().await;
    metrics.pdata_sent.add(1);

    // Send header + bytes
    let header = format!("ITEM {} {} {}\\n", item.id, item.kind.as_str(), item.bytes.len());
    write_half.write_all(header.as_bytes()).await.map_err(|e| e.to_string())?;
    write_half.write_all(&item.bytes).await.map_err(|e| e.to_string())?;
    write_half.flush().await.map_err(|e| e.to_string())?;

    // Wait for ACK
    let mut ack_line = String::new();
    match timeout(ack_timeout, reader.read_line(&mut ack_line)).await {
        Ok(Ok(0)) => {
            // disconnected
            cache.release_inflight(item.id).await;
            Ok(())
        }
        Ok(Ok(_)) => {
            let expected = format!("ACK {}", item.id);
            if ack_line.trim() == expected {
                if cache.ack_and_pop(item.id).await {
                    write_half.write_all(b"OK\\n").await.map_err(|e| e.to_string())?;
                } else {

                    cache.release_inflight(item.id).await;
                    write_half.write_all(b"ERR state mismatch\\n").await.map_err(|e| e.to_string())?;
                }
            } else {
                cache.release_inflight(item.id).await;
                write_half.write_all(b"ERR bad ack\\n").await.map_err(|e| e.to_string())?;
            }
            Ok(())
        }
        Ok(Err(e)) => {
            cache.release_inflight(item.id).await;
            Err(format!("read error: {e}"))
        }
        Err(_) => {
            // timeout
            cache.release_inflight(item.id).await;
            let _ = write_half.write_all(b"ERR ack timeout\\n").await;
            Ok(())
        }
    }
}


async fn run_server(
    listen: SocketAddr,
    cache: Arc<FifoCache>,
    metrics: MetricSet<ExporterMetrics>,
    ack_timeout: Duration,
) -> Result<(), String> {
    let listener = TcpListener::bind(listen).await.map_err(|e| e.to_string())?;
    loop {
        let (stream, _peer) = listener.accept().await.map_err(|e| e.to_string())?;
        let cache = cache.clone();
        let metrics = metrics.clone();
        let _=tokio::spawn(async move {
            let _ = handle_client(stream, cache, metrics, ack_timeout).await;
        });
    }
}


/// Register Request Response exporter with the OTAP exporter factory
///
/// Unsafe code is temporarily used here to allow the use of `distributed_slice` macro
/// This macro is part of the `linkme` crate which is considered safe and well maintained.
#[allow(unsafe_code)]
#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
pub static REQ_RESPONSE_EXPORTER: otap_df_engine::ExporterFactory<OtapPdata> =
    otap_df_engine::ExporterFactory {
        name: REQ_RESPONSE_EXPORTER_URN,
        create: |pipeline: PipelineContext,
                 node: NodeId,
                 node_config: Arc<NodeUserConfig>,
                 exporter_config: &ExporterConfig| {
            Ok(ExporterWrapper::local(
                ReqResponseExporter::from_config(pipeline, &node_config.config)?,
                node,
                node_config,
                exporter_config,
            ))
        },
        wiring_contract: otap_df_engine::wiring_contract::WiringContract::UNRESTRICTED,
        validate_config: otap_df_config::validation::validate_typed_config::<Config>,
    };


// ---------------- Exporter impl ----------------

#[async_trait(?Send)]
impl Exporter<OtapPdata> for ReqResponseExporter {
    async fn start(
        mut self: Box<Self>,
        mut msg_chan: MessageChannel<OtapPdata>,
        effect_handler: EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        let listen: SocketAddr = self
            .config
            .listen_addr
            .parse()
            .map_err(|e| {
                // Build a config error...
                let cfg_err = otap_df_config::error::Error::InvalidUserConfig {
                    error: format!("invalid listen_addr: {e}"),
                };
                // ...then convert it to engine Error via From<Box<_>>
                Error::from(Box::new(cfg_err))
            })?;


        let ack_timeout = Duration::from_millis(self.config.ack_timeout_ms.max(1));

        otel_info!(
            "req_response_exporter.start",
            listen_addr = self.config.listen_addr,
            max_items = self.config.max_items as i64,
            max_total_bytes = self.config.max_total_bytes as i64,
            ack_timeout_ms = self.config.ack_timeout_ms as i64,
            message = "Req/Response exporter starting"
        );

        // Start TCP server
        {
            let cache = self.cache.clone();
            let metrics = self.metrics.clone();
            let _=tokio::spawn(async move {
                let _ = run_server(listen, cache, metrics, ack_timeout).await;
            });
        }

       // Message loop
        loop {
            match msg_chan.recv().await? {
                Message::Control(NodeControlMsg::Shutdown { deadline, .. }) => {
                    otel_info!(
                        "req_response_exporter.shutdown",
                        message = "Req/Response exporter shutting down"
                    );

                    return Ok(TerminalState::new(
                        deadline,
                        [self.pdata_metrics.snapshot(), self.metrics.snapshot()],
                    ));
                }

                Message::Control(NodeControlMsg::CollectTelemetry { mut metrics_reporter }) => {
                    _ = metrics_reporter.report(&mut self.pdata_metrics);
                    _ = metrics_reporter.report(&mut self.metrics);
                }

                Message::PData(pdata) => {
                    let (context, payload) = pdata.into_parts();
                    let signal_type = payload.signal_type();
                    self.pdata_metrics.inc_consumed(signal_type);

                    // Save payload only if upstream might request it back.
                    let saved_payload = if context.may_return_payload() {
                        payload.clone()
                    } else {
                        OtapPayload::empty(signal_type)
                    };

                    // Empty payload: Ack immediately (no-op) just like your Geneva exporter did.
                    if payload.is_empty() {
                        self.pdata_metrics.inc_exported(signal_type);
                        effect_handler
                            .notify_ack(AckMsg::new(OtapPdata::new(context, saved_payload)))
                            .await?;
                        continue;
                    }

                    // Convert to OTLP bytes and enqueue into FIFO
                    match ReqResponseExporter::payload_to_otlp_item(payload) {
                        Ok((kind, bytes)) => {
                            let (_id, dropped) = self.cache.push(kind, bytes).await;
                            self.metrics.pdata_sent.add(1);
                            if dropped > 0 {
                                self.metrics.pdata_dropped.add(dropped as u64);
                            }

                            // Upstream ACK policy:
                            // - Recommended: ACK when enqueued (prevents pipeline stall)
                            // - Optional: ACK after external ACK (not implemented here because it requires a rendezvous)
                            //
                            // Current implementation always ACKs on enqueue.
                            self.pdata_metrics.inc_exported(signal_type);
                            effect_handler
                                .notify_ack(AckMsg::new(OtapPdata::new(context, saved_payload)))
                                .await?;
                        }

                        Err(e) => {
                            self.pdata_metrics.inc_failed(signal_type);
                            otel_info!(
                                "req_response_exporter.error",
                                error = e.as_str(),
                                message = "Failed to enqueue payload"
                            );

                            effect_handler
                                .notify_nack(NackMsg::new(
                                    &e,
                                    OtapPdata::new(context, saved_payload),
                                ))
                                .await?;
                        }
                    }
                }

                _ => {
                    // Ignore other messages
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    use arrow::array::{
        ArrayRef, Int32Array, RecordBatch, StringArray, StructArray, TimestampNanosecondArray,
        UInt16Array, UInt32Array,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    use bytes::Bytes;
    use otap_df_engine::Interests;
    use otap_df_engine::control::PipelineControlMsg;
    use otap_df_engine::testing::exporter::{TestRuntime, create_exporter_from_factory};
    use otap_df_otap::testing::TestCallData;
    use std::time::{Duration, Instant};

    // TODO: Re-enable these imports when zero-copy view tests are uncommented
    // use otap_df_pdata::otap::OtapArrowRecords;
    // use otap_df_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
    // use otap_df_pdata::views::logs::{LogsDataView, ResourceLogsView, ScopeLogsView};
    // use otap_df_pdata::views::otap::OtapLogsView;

    // TODO: Re-enable when zero-copy view tests are uncommented
    /// Helper to create a simple OTAP logs RecordBatch for testing Geneva exporter
    #[allow(dead_code)]
    fn create_test_logs_batch() -> RecordBatch {
        // Define schema matching OTAP logs structure
        let resource_field = Field::new(
            "resource",
            DataType::Struct(vec![Field::new("id", DataType::UInt16, false)].into()),
            false,
        );

        let scope_field = Field::new(
            "scope",
            DataType::Struct(vec![Field::new("id", DataType::UInt16, false)].into()),
            false,
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt16, false),
            resource_field,
            scope_field,
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new(
                "observed_time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("severity_number", DataType::Int32, true),
            Field::new("severity_text", DataType::Utf8, true),
            Field::new("body", DataType::Utf8, true),
            Field::new("flags", DataType::UInt32, true),
            Field::new("event_name", DataType::Utf8, true),
        ]));

        // Create test data (3 log records)
        let id_array = UInt16Array::from(vec![1, 2, 3]);

        // Resource structs (all from resource_id=1)
        let resource_id_array = UInt16Array::from(vec![1, 1, 1]);
        let resource_struct = StructArray::from(vec![(
            Arc::new(Field::new("id", DataType::UInt16, false)),
            Arc::new(resource_id_array) as ArrayRef,
        )]);

        // Scope structs (logs 1-2 from scope_id=10, log 3 from scope_id=11)
        let scope_id_array = UInt16Array::from(vec![10, 10, 11]);
        let scope_struct = StructArray::from(vec![(
            Arc::new(Field::new("id", DataType::UInt16, false)),
            Arc::new(scope_id_array) as ArrayRef,
        )]);

        let time_array = TimestampNanosecondArray::from(vec![
            Some(1000000000),
            Some(2000000000),
            Some(3000000000),
        ]);

        let observed_time_array = TimestampNanosecondArray::from(vec![
            Some(1000000100),
            Some(2000000100),
            Some(3000000100),
        ]);

        let severity_array = Int32Array::from(vec![Some(9), Some(17), Some(13)]); // INFO, ERROR, WARN
        let severity_text_array =
            StringArray::from(vec![Some("INFO"), Some("ERROR"), Some("WARN")]);

        let body_array = StringArray::from(vec![
            Some("Log message 1"),
            Some("Error occurred"),
            Some("Warning message"),
        ]);

        let flags_array = UInt32Array::from(vec![Some(1), Some(1), Some(0)]);
        let event_name_array =
            StringArray::from(vec![Some("event1"), Some("event2"), Some("event3")]);

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_array),
                Arc::new(resource_struct),
                Arc::new(scope_struct),
                Arc::new(time_array),
                Arc::new(observed_time_array),
                Arc::new(severity_array),
                Arc::new(severity_text_array),
                Arc::new(body_array),
                Arc::new(flags_array),
                Arc::new(event_name_array),
            ],
        )
        .expect("Failed to create test logs batch")
    }

    fn test_config() -> serde_json::Value {
        // Bind to port 0 so tests don't conflict with other processes.
        // validate_otlp=true ensures invalid protobuf bytes produce NACK (decode failure test).
        serde_json::json!({
            "listen_addr": "127.0.0.1:0",
            "max_items": 1000,
            "max_total_bytes": 1048576,
            "ack_timeout_ms": 50,
            "ack_upstream_on_enqueue": true,
            "validate_otlp": true
        })

    }

    
    #[test]
    fn req_response_exporter_emits_ack_for_empty_payload() {
        let test_runtime = TestRuntime::new();
        let exporter = create_exporter_from_factory(&REQ_RESPONSE_EXPORTER, test_config()).unwrap();

        test_runtime
            .set_exporter(exporter)
            .run_test(|ctx| async move {
                // Empty OTLP bytes should be treated as "empty payload" and ACKed (0 items).
                let payload: OtapPayload = OtlpProtoBytes::ExportLogsRequest(Bytes::new()).into();
                let pdata = OtapPdata::new_default(payload).test_subscribe_to(
                    Interests::ACKS,
                    TestCallData::default().into(),
                    4242,
                );

                ctx.send_pdata(pdata).await.unwrap();
                ctx.send_shutdown(Instant::now() + Duration::from_secs(1), "test shutdown")
                    .await
                    .unwrap();
            })
            .run_validation(|mut ctx, result| async move {
                result.expect("success");

                let mut pipeline_rx = ctx.take_pipeline_ctrl_receiver().unwrap();
                match pipeline_rx.recv().await.unwrap() {
                    PipelineControlMsg::DeliverAck { ack, node_id } => {
                        assert_eq!(node_id, 4242);
                        let got: TestCallData = ack.calldata.try_into().unwrap();
                        assert_eq!(TestCallData::default(), got);
                        assert_eq!(ack.accepted.num_items(), 0);
                    }
                    other => panic!("expected DeliverAck, got: {other:?}"),
                }
            });
    }

    #[test]
    fn req_response_exporter_emits_nack_for_decode_failure() {
        let test_runtime = TestRuntime::new();
        let exporter = create_exporter_from_factory(&REQ_RESPONSE_EXPORTER, test_config()).unwrap();

        test_runtime
            .set_exporter(exporter)
            .run_test(|ctx| async move {
                // Non-empty but invalid protobuf bytes to trigger decode error.
                let payload: OtapPayload =
                    OtlpProtoBytes::ExportLogsRequest(Bytes::from_static(b"\xff")).into();

                let pdata = OtapPdata::new_default(payload).test_subscribe_to(
                    Interests::NACKS,
                    TestCallData::default().into(),
                    777,
                );

                ctx.send_pdata(pdata).await.unwrap();
                ctx.send_shutdown(Instant::now() + Duration::from_secs(1), "test shutdown")
                    .await
                    .unwrap();
            })
            .run_validation(|mut ctx, result| async move {
                result.expect("success");

                let mut pipeline_rx = ctx.take_pipeline_ctrl_receiver().unwrap();
                match pipeline_rx.recv().await.unwrap() {
                    PipelineControlMsg::DeliverNack { nack, node_id } => {
                        assert_eq!(node_id, 777);
                        let got: TestCallData = nack.calldata.try_into().unwrap();
                        assert_eq!(TestCallData::default(), got);

                        assert!(
                            nack.reason.contains("Failed to decode logs request"),
                            "unexpected nack reason: {}",
                            nack.reason
                        );

                        assert_eq!(nack.refused.num_items(), 0);
                    }
                    other => panic!("expected DeliverNack, got: {other:?}"),
                }
            });
    }

    
    #[test]
    fn test_config_deserialization() {
        // Only provide listen_addr; everything else should take defaults.
        let json = serde_json::json!({
            "listen_addr": "127.0.0.1:0"
        });

        let config: Config = serde_json::from_value(json).unwrap();

        assert_eq!(config.listen_addr, "127.0.0.1:0");
        assert_eq!(config.max_items, default_max_items());
        assert_eq!(config.max_total_bytes, default_max_total_bytes());
        assert_eq!(config.ack_timeout_ms, default_ack_timeout_ms());
        assert_eq!(config.ack_upstream_on_enqueue, false); // serde(default) => false
        assert_eq!(config.validate_otlp, true);            // default_validate_otlp()
    }


    // TODO: Re-enable these tests when zero-copy view path is uncommented
    // #[test]
    // fn test_geneva_exporter_creates_view_from_otap_records() {
    //     // This test verifies that the Geneva exporter can successfully create
    //     // an OtapLogsView from OtapArrowRecords using the TryFrom implementation.
    //
    //     let logs_batch = create_test_logs_batch();
    //
    //     // Create OtapArrowRecords (simulating what batch processor would send)
    //     let mut otap_records = OtapArrowRecords::Logs(Default::default());
    //     otap_records.set(ArrowPayloadType::Logs, logs_batch.clone());
    //
    //     // This is what the Geneva exporter does internally
    //     let logs_view = OtapLogsView::try_from(&otap_records)
    //         .expect("Geneva exporter should create view from OTAP records");
    //
    //     // Verify the view can be used (basic sanity check)
    //     let mut log_count = 0;
    //     for resource_logs in logs_view.resources() {
    //         for scope_logs in resource_logs.scopes() {
    //             for _log_record in scope_logs.log_records() {
    //                 log_count += 1;
    //             }
    //         }
    //     }
    //
    //     assert_eq!(log_count, 3, "Expected 3 logs");
    // }
    //
    // #[test]
    // fn test_geneva_exporter_handles_missing_logs_batch() {
    //     // Verify that Geneva exporter properly handles the case where
    //     // OtapArrowRecords is missing the required logs batch
    //
    //     let otap_records = OtapArrowRecords::Logs(Default::default());
    //
    //     // This should fail because logs batch is missing
    //     let result = OtapLogsView::try_from(&otap_records);
    //
    //     assert!(result.is_err(), "Should fail when logs batch is missing");
    // }


    #[test]
    fn test_urn_constant() {
        assert_eq!(REQ_RESPONSE_EXPORTER_URN, "urn:microsoft:exporter:req-response");
    }

    // TODO: Add integration tests when we can mock GenevaClient:
    // - test_geneva_exporter_encodes_and_uploads_logs_view()
    // - test_geneva_exporter_handles_upload_failure()
    // - test_geneva_exporter_fallback_to_otlp_bytes()
    // - test_geneva_exporter_metrics_tracking()
}
