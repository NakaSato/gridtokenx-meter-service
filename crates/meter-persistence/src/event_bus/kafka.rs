//! Kafka-backed [`MeterEventPublisher`].
//!
//! Mirrors the producer setup in the sibling
//! `iam-persistence/src/event_bus/kafka.rs`: an rdkafka [`FutureProducer`] with
//! durable `acks=all` + retries. Publishing is **best-effort and non-blocking**
//! — [`MeterEventPublisher::publish`] serializes the event, spawns the send on
//! the Tokio runtime, and returns immediately, so a slow/unreachable broker can
//! never delay or fail a meter registration. Delivery outcome is logged only.

use std::time::Duration;

use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tracing::{error, info, warn};

use meter_core::event::{MeterEvent, MeterEventPublisher};

/// Send timeout for a single publish attempt.
const SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// Publishes meter domain events to a single Kafka topic.
#[derive(Clone)]
pub struct KafkaMeterEventPublisher {
    producer: FutureProducer,
    topic: String,
}

impl KafkaMeterEventPublisher {
    /// Builds a producer against `bootstrap_servers`, publishing to `topic`.
    ///
    /// # Errors
    /// Returns an error if the rdkafka client cannot be created (bad config).
    /// Note: this does **not** connect — broker reachability is only exercised
    /// on the first send.
    pub fn new(bootstrap_servers: &str, topic: &str) -> Result<Self> {
        info!(
            brokers = %bootstrap_servers,
            topic = %topic,
            "Initializing meter-service Kafka producer"
        );

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .set("acks", "all")
            .set("retries", "3")
            // Keepalive avoids the idle stale-socket wedge seen on other
            // long-lived producers in this platform.
            .set("socket.keepalive.enable", "true")
            .create()
            .map_err(|e| anyhow::anyhow!("Failed to create Kafka producer: {e}"))?;

        Ok(Self {
            producer,
            topic: topic.to_string(),
        })
    }
}

impl MeterEventPublisher for KafkaMeterEventPublisher {
    fn publish(&self, event: MeterEvent) {
        let payload = match serde_json::to_vec(&event) {
            Ok(bytes) => bytes,
            Err(e) => {
                error!(error = %e, event_type = %event.event_type, "Failed to serialize meter event; dropping");
                return;
            }
        };
        // Partition by meter serial for stable ordering per meter.
        let key = event.data.serial_number.clone();
        let event_type = event.event_type.clone();
        let producer = self.producer.clone();
        let topic = self.topic.clone();

        // Fire-and-forget: never block or fail the caller (registration).
        tokio::spawn(async move {
            let record = FutureRecord::to(&topic).key(&key).payload(&payload);
            match producer.send(record, SEND_TIMEOUT).await {
                Ok((partition, offset)) => {
                    info!(
                        %event_type, %topic, partition, offset,
                        "Meter event published to Kafka"
                    );
                }
                Err((e, _)) => {
                    warn!(error = %e, %event_type, %topic, "Failed to publish meter event to Kafka (best-effort, dropped)");
                }
            }
        });
    }
}
