//! Core primitives, domain models, and traits for the GridTokenX meter service.
//!
//! Shared logic and definitions used by all other crates in the meter workspace,
//! following the "Sync Core" design pattern (see superproject CLAUDE.md).

pub mod config;
pub mod domain;
pub mod error;
pub mod traits;

pub use error::{ApiError, Result};
pub use traits::MeterRepositoryTrait;
