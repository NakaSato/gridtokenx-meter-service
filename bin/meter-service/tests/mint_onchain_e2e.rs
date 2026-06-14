//! Stage 2C — REAL on-chain mint E2E against the live Chain Bridge + Solana.
//!
//! Unlike `mint_e2e.rs` (which stands in a fake bridge), this runs the full path
//! end to end with NO fake: meter-service ingests a device reading, the real
//! `NatsMintGateway` publishes `chain.tx.mint`, the live Chain Bridge BUILDS,
//! signs (Vault / insecure dev signer), and SUBMITS the generation mint to the
//! Solana validator, and the reply carries a real on-chain signature + slot which
//! the repo writes to the `blockchain_*` tracking columns.
//!
//! Requires the full chain stack up (`./scripts/app.sh init` → validator +
//! deployed energy_token + bootstrapped mint, and the chain-bridge container with
//! NATS). Ignored by default. Run:
//!   DATABASE_URL=postgresql://gridtokenx_user:gridtokenx_password@127.0.0.1:7001/gridtokenx \
//!   NATS_URL=nats://127.0.0.1:9020 TEST_METER_SERIAL=bf780e3f-f6b7-41f7-9b74-92459b1bc895 \
//!   cargo test -p gridtokenx-meter-service --test mint_onchain_e2e -- --ignored --nocapture

use std::sync::Arc;
use std::time::Duration;

use meter_core::traits::{MeterRepositoryTrait, MintGateway};
use meter_logic::MeterService;
use meter_persistence::{MeterRepository, NatsMintGateway};
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires live NATS + Postgres + chain-bridge + Solana validator"]
async fn real_onchain_mint_writes_tracking_columns() {
    let db = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://gridtokenx_user:gridtokenx_password@127.0.0.1:7001/gridtokenx".to_string()
    });
    let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:9020".to_string());
    let serial = std::env::var("TEST_METER_SERIAL")
        .unwrap_or_else(|_| "bf780e3f-f6b7-41f7-9b74-92459b1bc895".to_string());

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db)
        .await
        .expect("pg connect");
    let gw_client = async_nats::connect(&nats_url).await.expect("nats connect");
    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool.clone()));
    // Generous timeout: a real build+sign+submit+confirm round-trip is slower than
    // the fake-bridge reply.
    let mint: Arc<dyn MintGateway> = Arc::new(NatsMintGateway::new(
        gw_client,
        "spiffe://gridtokenx.th/prod/meter-service".to_string(),
        Duration::from_secs(45),
    ));
    let service = MeterService::new(repo, mint);

    // Fresh reading id; current time so the (meter, 15-min window) PDA is unlikely
    // to collide with a prior run's on-chain mint-record.
    let reading_id = Uuid::new_v4();
    let now_ms = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_millis(),
    )
    .expect("millis fits i64");

    let minted = service
        .ingest_device_reading(reading_id, &serial, 3.25, now_ms)
        .await
        .expect("ingest");
    assert!(minted, "reading should be minted on-chain via real bridge");

    let row = sqlx::query(
        "SELECT minted, mint_tx_signature, blockchain_tx_signature, blockchain_status,
                on_chain_confirmed, on_chain_slot
         FROM meter_readings WHERE id = $1",
    )
    .bind(reading_id)
    .fetch_one(&pool)
    .await
    .expect("fetch row");

    let minted_col: bool = row.get("minted");
    let mint_sig: Option<String> = row.get("mint_tx_signature");
    let bc_sig: Option<String> = row.get("blockchain_tx_signature");
    let bc_status: Option<String> = row.get("blockchain_status");
    let confirmed: Option<bool> = row.get("on_chain_confirmed");
    let slot: Option<i64> = row.get("on_chain_slot");

    let sig = mint_sig.clone().expect("a real on-chain signature");
    assert!(minted_col, "minted");
    assert_eq!(bc_sig, mint_sig, "blockchain_tx_signature mirrors mint sig");
    // A real base58 Solana signature is ~87-88 chars — definitely not the fake sentinel.
    assert!(sig.len() >= 80, "signature looks real (len {}): {sig}", sig.len());
    assert_ne!(sig, "FAKESIG_E2E_2B3", "must be a real signature, not the fake");
    assert_eq!(bc_status.as_deref(), Some("submitted"), "status submitted");
    assert_eq!(confirmed, Some(false), "confirmation is the confirmer's job");
    assert!(slot.unwrap_or(0) > 0, "real on-chain slot > 0");

    // Leave the row in place so the signature can be confirmed on-chain out-of-band
    // (`solana confirm <sig>`); print everything needed.
    println!("✅ REAL on-chain mint");
    println!("   reading_id = {reading_id}");
    println!("   signature  = {sig}");
    println!("   slot       = {}", slot.unwrap_or(0));

    // Best-effort cleanup of the DB row (the on-chain mint-record PDA persists and
    // is the real idempotency anchor; the row is just bookkeeping for this test).
    sqlx::query("DELETE FROM meter_readings WHERE id = $1")
        .bind(reading_id)
        .execute(&pool)
        .await
        .expect("cleanup");
}
