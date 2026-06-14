//! Non-database infrastructure adapters (blockchain mint gateway).

/// Mint gateway implementations.
pub mod mint;

pub use mint::{DisabledMintGateway, NatsMintGateway};
