//! Persistence layer for the meter service.
//!
//! Concrete SQLx/PostgreSQL implementations of the `meter-core` traits.

/// Non-database infrastructure adapters (mint gateway).
pub mod infra;
/// SQLx-based PostgreSQL repositories.
pub mod repository;

pub use infra::DisabledMintGateway;
pub use repository::MeterRepository;
