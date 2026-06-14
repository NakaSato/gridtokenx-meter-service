//! API layer for the meter service.
//!
//! REST handlers, shared application state, and custom Axum middleware
//! (JWT auth extraction).

/// HTTP request handlers.
pub mod handlers;
/// Custom Axum middleware (JWT auth).
pub mod middleware;
/// Shared application state (DI container).
pub mod state;

pub use state::AppState;
