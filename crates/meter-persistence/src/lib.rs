//! Persistence layer for the meter service.
//!
//! Concrete SQLx/PostgreSQL implementations of the `meter-core` traits.

/// SQLx-based PostgreSQL repositories.
pub mod repository;

pub use repository::MeterRepository;
