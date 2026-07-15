//! Outbound event publishing for the meter service.
//!
//! Best-effort Kafka fan-out of meter domain events to downstream read-models
//! (trading service, aggregator bridge). Gated OFF by default at the service
//! level (`METER_EVENTS_ENABLED`); when disabled no producer is constructed.

pub mod kafka;

pub use kafka::KafkaMeterEventPublisher;
