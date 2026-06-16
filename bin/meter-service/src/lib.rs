//! Library for the GridTokenX meter service.
//!
//! Holds the startup wiring and telemetry init for the meter-service binary.

/// Service startup and dependency wiring.
pub mod startup;
/// Observability / logging init.
pub mod telemetry;
