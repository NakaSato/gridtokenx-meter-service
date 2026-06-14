//! Business logic and domain services for the GridTokenX meter service.
//!
//! Implements read workflows over the meter repository, independent of any
//! specific I/O implementation (the repository is injected as a trait object).

/// Meter read service.
pub mod meter_service;

pub use meter_service::MeterService;
