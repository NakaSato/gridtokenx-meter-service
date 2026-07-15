//! Persistence layer for the meter service.
//!
//! Concrete SQLx/PostgreSQL implementations of the `meter-core` traits.

/// Best-effort outbound event publishing (Kafka).
pub mod event_bus;
/// SQLx-based `PostgreSQL` repositories.
pub mod repository;

pub use event_bus::KafkaMeterEventPublisher;
pub use repository::MeterRepository;
