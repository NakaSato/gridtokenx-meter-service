//! Library for the `GridTokenX` meter service.
//!
//! Holds the startup wiring and telemetry init for the meter-service binary.

/// Background poller that pushes mint-status transitions to SSE subscribers.
pub mod mint_poller;
/// Service startup and dependency wiring.
pub mod startup;
/// Observability / logging init.
pub mod telemetry;
