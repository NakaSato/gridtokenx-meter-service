//! Live mint-path integration test (Stage 2B + 3) against real NATS + Postgres.
//!
//! It stands in a FAKE Chain Bridge (a NATS responder on `chain.tx.mint`) so the
//! full meter-service mint contract is exercised end to end — gateway publish →
//! reply decode → repo writes the `blockchain_*` tracking columns — WITHOUT
//! needing Solana/Vault. The real bridge handler is covered by chain-bridge's own
//! suite; live on-chain mint is the separate Stage 2C milestone.
//!
//! Ignored by default (needs live infra). Run:
//!   DATABASE_URL=postgresql://gridtokenx_user:gridtokenx_password@127.0.0.1:7001/gridtokenx \
//!   NATS_URL=nats://127.0.0.1:9020 TEST_METER_SERIAL=<registered-serial> \
//!   cargo test -p gridtokenx-meter-service --test mint_e2e -- --ignored --nocapture

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use meter_core::traits::{MeterRepositoryTrait, MintGateway};
use meter_logic::MeterService;
use meter_persistence::{MeterRepository, NatsMintGateway};
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires live NATS + Postgres"]
async fn mint_path_writes_onchain_tracking_columns() {
    let db = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://gridtokenx_user:gridtokenx_password@127.0.0.1:7001/gridtokenx".to_string());
    let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:9020".to_string());
    let serial = std::env::var("TEST_METER_SERIAL").expect("set TEST_METER_SERIAL to a registered meter");

    // Fake Chain Bridge: reply success with a known signature + slot.
    let bridge = async_nats::connect(&nats_url).await.expect("nats connect (bridge)");
    let mut sub = bridge.subscribe("chain.tx.mint".to_string()).await.expect("subscribe mint");
    let bridge_pub = bridge.clone();
    let fake_sig = "FAKESIG_E2E_2B3";
    let fake_slot = 4242u64;
    tokio::spawn(async move {
        if let Some(msg) = sub.next().await {
            let v: serde_json::Value = serde_json::from_slice(&msg.payload).expect("decode intent");
            let reply = v["reply_subject"].as_str().expect("reply_subject").to_string();
            let corr = v["correlation_id"].as_str().unwrap_or("").to_string();
            let body = serde_json::json!({
                "correlation_id": corr,
                "success": true,
                "signature": fake_sig,
                "error": null,
                "slot": fake_slot,
                "deduplicated": false,
            });
            bridge_pub.publish(reply, serde_json::to_vec(&body).expect("encode reply").into()).await.expect("publish reply");
            let _ = bridge_pub.flush().await;
        }
    });

    // meter-service side: real gateway + repo + service.
    let pool = PgPoolOptions::new().max_connections(2).connect(&db).await.expect("pg connect");
    let gw_client = async_nats::connect(&nats_url).await.expect("nats connect (gateway)");
    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool.clone()));
    let mint: Arc<dyn MintGateway> = Arc::new(NatsMintGateway::new(
        gw_client,
        "spiffe://gridtokenx.th/prod/meter-service".to_string(),
        Duration::from_secs(10),
    ));
    let service = MeterService::new(repo, mint);

    let reading_id = Uuid::new_v4();
    let minted = service
        .ingest_device_reading(reading_id, &serial, 7.5, 1_700_000_000_000)
        .await
        .expect("ingest");
    assert!(minted, "reading should be minted via fake bridge");

    // Verify the on-chain tracking columns were written.
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

    assert!(minted_col, "minted");
    assert_eq!(mint_sig.as_deref(), Some(fake_sig), "mint_tx_signature");
    assert_eq!(bc_sig.as_deref(), Some(fake_sig), "blockchain_tx_signature");
    // Trigger advances status to 'submitted' when the tx signature is first set;
    // 'confirmed' is the confirmer's job, not the submit path.
    assert_eq!(bc_status.as_deref(), Some("submitted"), "blockchain_status");
    assert_eq!(confirmed, Some(false), "on_chain_confirmed stays false until finality");
    assert_eq!(slot, i64::try_from(fake_slot).ok(), "on_chain_slot");

    // Cleanup.
    sqlx::query("DELETE FROM meter_readings WHERE id = $1")
        .bind(reading_id)
        .execute(&pool)
        .await
        .expect("cleanup");

    println!("✅ mint path wrote on-chain tracking columns (sig={fake_sig}, slot={fake_slot})");
}
