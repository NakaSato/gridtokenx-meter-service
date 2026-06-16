//! Live HTTP end-to-end test against the real router + shared Postgres.
//!
//! Drives the exact production route table in-process via
//! `tower::ServiceExt::oneshot` (no socket bind) and asserts the full meter
//! flow: register → submit → list → stats → realtime SSE. It also proves the
//! read-only `mint_status` projection is wired through every read path.
//!
//! Self-contained: it picks an existing wallet-bearing user from the DB, uses a
//! unique throwaway serial, and deletes the meter + readings it created. It does
//! NOT mutate the user it borrows.
//!
//! Ignored by default (needs live Postgres). Run:
//!
//! ```text
//! DATABASE_URL=postgresql://gridtokenx_user:gridtokenx_password@127.0.0.1:7001/gridtokenx \
//! JWT_SECRET=dev-jwt-secret-key-minimum-32-characters-long-for-development-2025 \
//! cargo test -p gridtokenx-meter-service --test e2e_http -- --ignored --nocapture
//! ```

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::{header, Request, Response, StatusCode};
use futures::StreamExt;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tokio::sync::broadcast;
use tower::ServiceExt;
use uuid::Uuid;

use gridtokenx_meter_service::startup::build_app;
use meter_api::{AppState, ReadingEvent};
use meter_core::traits::MeterRepositoryTrait;
use meter_logic::MeterService;
use meter_persistence::MeterRepository;

const DEFAULT_DB: &str =
    "postgresql://gridtokenx_user:gridtokenx_password@127.0.0.1:7001/gridtokenx";
const DEFAULT_SECRET: &str =
    "dev-jwt-secret-key-minimum-32-characters-long-for-development-2025";

/// HS256 JWT with `sub` + `exp`, matching what the auth extractor verifies.
fn mint_jwt(user_id: Uuid, secret: &str) -> String {
    let exp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_secs()
        + 3600;
    let claims = serde_json::json!({ "sub": user_id, "exp": exp });
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .expect("mint jwt")
}

/// HS256 JWT with an explicit `exp` (seconds since epoch), for expiry tests.
fn mint_jwt_with_exp(user_id: Uuid, secret: &str, exp: u64) -> String {
    let claims = serde_json::json!({ "sub": user_id, "exp": exp });
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .expect("mint jwt")
}

fn authed(method: &str, uri: &str, token: &str, body: Option<serde_json::Value>) -> Request<Body> {
    let mut b = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {token}"));
    let body = match body {
        Some(json) => {
            b = b.header(header::CONTENT_TYPE, "application/json");
            Body::from(serde_json::to_vec(&json).expect("encode body"))
        }
        None => Body::empty(),
    };
    b.body(body).expect("build request")
}

async fn json_body(resp: Response<Body>) -> serde_json::Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("read body");
    serde_json::from_slice(&bytes).expect("parse json")
}

#[tokio::test]
#[ignore = "requires live Postgres"]
async fn http_e2e_register_submit_stream_stats() {
    let db = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB.to_string());
    let secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| DEFAULT_SECRET.to_string());

    let pool: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&db)
        .await
        .expect("connect Postgres");

    // Borrow an existing wallet-bearing user; registration joins users for the
    // wallet and the FK requires a real row.
    let user_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM users WHERE wallet_address IS NOT NULL AND wallet_address <> '' LIMIT 1",
    )
    .fetch_optional(&pool)
    .await
    .expect("query user");
    let Some(user_id) = user_id else {
        eprintln!("SKIP: no wallet-bearing user in DB");
        return;
    };

    let token = mint_jwt(user_id, &secret);
    let serial = format!("E2E-AUTO-{}", Uuid::new_v4());

    // Wire the same DI graph as production.
    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool.clone()));
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(256);
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret.as_str()),
        readings_tx,
    };
    let app = build_app(state);

    // Run the flow, then always clean up — even on assertion panic.
    let result = run_flow(&app, &token, &serial).await;
    cleanup(&pool, &serial).await;
    result.expect("e2e flow");
}

/// The assertions, isolated so the caller can clean up regardless of outcome.
#[allow(clippy::too_many_lines)]
async fn run_flow(
    app: &axum::Router,
    token: &str,
    serial: &str,
) -> Result<(), String> {
    macro_rules! check {
        ($cond:expr, $($arg:tt)*) => {
            if !$cond { return Err(format!($($arg)*)); }
        };
    }

    // 1. health
    let resp = app
        .clone()
        .oneshot(Request::builder().uri("/health").body(Body::empty()).expect("req"))
        .await
        .expect("health");
    check!(resp.status() == StatusCode::OK, "health status {}", resp.status());

    // 2. register
    let resp = app
        .clone()
        .oneshot(authed(
            "POST",
            "/api/v1/meters",
            token,
            Some(serde_json::json!({
                "serial_number": serial,
                "meter_type": "smart_meter",
                "location": "e2e-auto",
            })),
        ))
        .await
        .expect("register");
    check!(resp.status() == StatusCode::OK, "register status {}", resp.status());
    let v = json_body(resp).await;
    check!(v["success"] == serde_json::json!(true), "register not success: {v}");
    check!(v["meter"]["serial_number"] == serde_json::json!(serial), "serial mismatch: {v}");

    // 3. subscribe SSE BEFORE submitting so the broadcast subscription is live.
    let stream_resp = app
        .clone()
        .oneshot(authed("GET", "/api/v1/meters/readings/stream", token, None))
        .await
        .expect("stream");
    check!(stream_resp.status() == StatusCode::OK, "stream status {}", stream_resp.status());
    let mut events = stream_resp.into_body().into_data_stream();

    // 4. submit a reading
    let resp = app
        .clone()
        .oneshot(authed(
            "POST",
            &format!("/api/v1/meters/{serial}/readings"),
            token,
            Some(serde_json::json!({ "kwh": 4.2 })),
        ))
        .await
        .expect("submit");
    check!(resp.status() == StatusCode::OK, "submit status {}", resp.status());
    let reading = json_body(resp).await;
    let reading_id = reading["id"].as_str().unwrap_or_default().to_string();
    check!(!reading_id.is_empty(), "submit returned no id: {reading}");
    check!(
        reading["mint_status"] == serde_json::json!("pending"),
        "fresh reading not pending: {reading}"
    );

    // 5. SSE delivered the same reading, carrying mint_status.
    let mut sse_text = String::new();
    let got = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(chunk) = events.next().await {
            let bytes = chunk.expect("sse chunk");
            sse_text.push_str(&String::from_utf8_lossy(&bytes));
            if sse_text.contains(&reading_id) {
                return true;
            }
        }
        false
    })
    .await
    .unwrap_or(false);
    check!(got, "SSE did not deliver reading {reading_id}; got: {sse_text}");
    check!(
        sse_text.contains("\"mint_status\":\"pending\""),
        "SSE event missing mint_status: {sse_text}"
    );

    // 6. readings list contains it with mint_status.
    let resp = app
        .clone()
        .oneshot(authed("GET", "/api/v1/meters/readings?limit=10", token, None))
        .await
        .expect("readings");
    check!(resp.status() == StatusCode::OK, "readings status {}", resp.status());
    let list = json_body(resp).await;
    let arr = list.as_array().cloned().unwrap_or_default();
    let found = arr
        .iter()
        .find(|r| r["id"] == serde_json::json!(reading_id));
    check!(found.is_some(), "submitted reading not in list");
    check!(
        found.expect("found")["mint_status"] == serde_json::json!("pending"),
        "listed reading not pending"
    );

    // 7. stats expose the three mint counters as integers.
    let resp = app
        .clone()
        .oneshot(authed("GET", "/api/v1/meters/stats", token, None))
        .await
        .expect("stats");
    check!(resp.status() == StatusCode::OK, "stats status {}", resp.status());
    let stats = json_body(resp).await;
    for key in ["minted_count", "pending_count", "denied_count"] {
        check!(stats[key].is_i64(), "stats.{key} not an integer: {stats}");
    }
    check!(
        stats["pending_count"].as_i64().unwrap_or(0) >= 1,
        "pending_count should count our reading: {stats}"
    );

    Ok(())
}

/// `GET /api/v1/me/meters` — the last route with no e2e coverage. Register a
/// meter, assert it appears in the caller's meter list with the right serial.
#[tokio::test]
#[ignore = "requires live Postgres"]
async fn http_e2e_my_meters() {
    let db = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB.to_string());
    let secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| DEFAULT_SECRET.to_string());

    let pool: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&db)
        .await
        .expect("connect Postgres");

    let user_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM users WHERE wallet_address IS NOT NULL AND wallet_address <> '' LIMIT 1",
    )
    .fetch_optional(&pool)
    .await
    .expect("query user");
    let Some(user_id) = user_id else {
        eprintln!("SKIP: no wallet-bearing user in DB");
        return;
    };

    let token = mint_jwt(user_id, &secret);
    let serial = format!("E2E-MINE-{}", Uuid::new_v4());

    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool.clone()));
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(16);
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret.as_str()),
        readings_tx,
    };
    let app = build_app(state);

    let result = run_my_meters(&app, &token, &serial).await;
    cleanup(&pool, &serial).await;
    result.expect("my-meters flow");
}

async fn run_my_meters(app: &axum::Router, token: &str, serial: &str) -> Result<(), String> {
    macro_rules! check {
        ($cond:expr, $($arg:tt)*) => {
            if !$cond { return Err(format!($($arg)*)); }
        };
    }

    register(app, token, serial).await?;

    let resp = app
        .clone()
        .oneshot(authed("GET", "/api/v1/me/meters", token, None))
        .await
        .expect("my meters");
    check!(resp.status() == StatusCode::OK, "my-meters status {}", resp.status());
    let meters = json_body(resp).await;
    let arr = meters.as_array().cloned().unwrap_or_default();
    let mine = arr
        .iter()
        .find(|m| m["serial_number"] == serde_json::json!(serial));
    check!(mine.is_some(), "registered meter {serial} absent from /me/meters: {meters}");

    Ok(())
}

/// Error-contract paths through the real router + DB: invalid kWh → 400,
/// unknown serial → 404, duplicate serial → 409. The duplicate path is the DB
/// unique-constraint behavior, which has no unit coverage.
#[tokio::test]
#[ignore = "requires live Postgres"]
async fn http_e2e_error_paths() {
    let db = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB.to_string());
    let secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| DEFAULT_SECRET.to_string());

    let pool: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&db)
        .await
        .expect("connect Postgres");

    let user_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM users WHERE wallet_address IS NOT NULL AND wallet_address <> '' LIMIT 1",
    )
    .fetch_optional(&pool)
    .await
    .expect("query user");
    let Some(user_id) = user_id else {
        eprintln!("SKIP: no wallet-bearing user in DB");
        return;
    };

    let token = mint_jwt(user_id, &secret);
    let serial = format!("E2E-ERR-{}", Uuid::new_v4());

    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool.clone()));
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(16);
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret.as_str()),
        readings_tx,
    };
    let app = build_app(state);

    let result = run_error_paths(&app, &token, &serial).await;
    cleanup(&pool, &serial).await;
    result.expect("error-path flow");
}

async fn run_error_paths(app: &axum::Router, token: &str, serial: &str) -> Result<(), String> {
    macro_rules! check {
        ($cond:expr, $($arg:tt)*) => {
            if !$cond { return Err(format!($($arg)*)); }
        };
    }

    register(app, token, serial).await?;

    // Negative kWh → 400 BadRequest.
    let resp = app
        .clone()
        .oneshot(authed(
            "POST",
            &format!("/api/v1/meters/{serial}/readings"),
            token,
            Some(serde_json::json!({ "kwh": -1.0 })),
        ))
        .await
        .expect("submit neg");
    check!(
        resp.status() == StatusCode::BAD_REQUEST,
        "negative kwh: expected 400, got {}",
        resp.status()
    );

    // Submit to a serial that does not exist → 404 NotFound.
    let unknown = format!("{serial}-NOPE");
    let resp = app
        .clone()
        .oneshot(authed(
            "POST",
            &format!("/api/v1/meters/{unknown}/readings"),
            token,
            Some(serde_json::json!({ "kwh": 1.0 })),
        ))
        .await
        .expect("submit unknown");
    check!(
        resp.status() == StatusCode::NOT_FOUND,
        "unknown serial: expected 404, got {}",
        resp.status()
    );

    // Re-register the same serial → 409 Conflict (DB unique constraint).
    let resp = app
        .clone()
        .oneshot(authed(
            "POST",
            "/api/v1/meters",
            token,
            Some(serde_json::json!({
                "serial_number": serial,
                "meter_type": "smart_meter",
                "location": "e2e-auto",
            })),
        ))
        .await
        .expect("dup register");
    check!(
        resp.status() == StatusCode::CONFLICT,
        "duplicate serial: expected 409, got {}",
        resp.status()
    );

    Ok(())
}

/// Register a meter for `token`'s user. Returns `Err` with context on failure.
async fn register(app: &axum::Router, token: &str, serial: &str) -> Result<(), String> {
    let resp = app
        .clone()
        .oneshot(authed(
            "POST",
            "/api/v1/meters",
            token,
            Some(serde_json::json!({
                "serial_number": serial,
                "meter_type": "smart_meter",
                "location": "e2e-auto",
            })),
        ))
        .await
        .expect("register");
    if resp.status() != StatusCode::OK {
        return Err(format!("register {serial} status {}", resp.status()));
    }
    Ok(())
}

/// Submit a reading and return the new reading's id.
async fn submit(app: &axum::Router, token: &str, serial: &str, kwh: f64) -> Result<String, String> {
    let resp = app
        .clone()
        .oneshot(authed(
            "POST",
            &format!("/api/v1/meters/{serial}/readings"),
            token,
            Some(serde_json::json!({ "kwh": kwh })),
        ))
        .await
        .expect("submit");
    if resp.status() != StatusCode::OK {
        return Err(format!("submit {serial} status {}", resp.status()));
    }
    let reading = json_body(resp).await;
    let id = reading["id"].as_str().unwrap_or_default().to_string();
    if id.is_empty() {
        return Err(format!("submit {serial} returned no id: {reading}"));
    }
    Ok(id)
}

/// GET the user's readings list, returning the array of ids.
async fn reading_ids(app: &axum::Router, token: &str) -> Result<Vec<String>, String> {
    let resp = app
        .clone()
        .oneshot(authed("GET", "/api/v1/meters/readings?limit=200", token, None))
        .await
        .expect("readings");
    if resp.status() != StatusCode::OK {
        return Err(format!("readings status {}", resp.status()));
    }
    let list = json_body(resp).await;
    Ok(list
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|r| r["id"].as_str().map(str::to_string))
        .collect())
}

/// Two users must not see each other's readings (list scoping) and one user's
/// submit must NOT reach another user's open SSE stream (per-user filter).
#[tokio::test]
#[ignore = "requires live Postgres"]
async fn http_e2e_multi_user_isolation() {
    let db = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB.to_string());
    let secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| DEFAULT_SECRET.to_string());

    let pool: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&db)
        .await
        .expect("connect Postgres");

    // Need two DISTINCT wallet-bearing users.
    let users: Vec<Uuid> = sqlx::query_scalar(
        "SELECT id FROM users WHERE wallet_address IS NOT NULL AND wallet_address <> '' LIMIT 2",
    )
    .fetch_all(&pool)
    .await
    .expect("query users");
    if users.len() < 2 {
        eprintln!("SKIP: need 2 wallet-bearing users in DB, found {}", users.len());
        return;
    }
    let (user_a, user_b) = (users[0], users[1]);

    let token_a = mint_jwt(user_a, &secret);
    let token_b = mint_jwt(user_b, &secret);
    let serial_a = format!("E2E-ISO-A-{}", Uuid::new_v4());
    let serial_b = format!("E2E-ISO-B-{}", Uuid::new_v4());

    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool.clone()));
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(256);
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret.as_str()),
        readings_tx,
    };
    let app = build_app(state);

    let result = run_isolation(&app, &token_a, &token_b, &serial_a, &serial_b).await;
    cleanup(&pool, &serial_a).await;
    cleanup(&pool, &serial_b).await;
    result.expect("isolation flow");
}

#[allow(clippy::too_many_lines)]
async fn run_isolation(
    app: &axum::Router,
    token_a: &str,
    token_b: &str,
    serial_a: &str,
    serial_b: &str,
) -> Result<(), String> {
    macro_rules! check {
        ($cond:expr, $($arg:tt)*) => {
            if !$cond { return Err(format!($($arg)*)); }
        };
    }

    register(app, token_a, serial_a).await?;
    register(app, token_b, serial_b).await?;

    // A opens its SSE stream BEFORE any submit so both readings are in the window.
    let stream_resp = app
        .clone()
        .oneshot(authed("GET", "/api/v1/meters/readings/stream", token_a, None))
        .await
        .expect("stream");
    check!(stream_resp.status() == StatusCode::OK, "stream status {}", stream_resp.status());
    let mut events = stream_resp.into_body().into_data_stream();

    // B submits FIRST, then A. Broadcast preserves order, so by the time A's own
    // reading arrives on the stream, B's would already have arrived if it leaked.
    let id_b = submit(app, token_b, serial_b, 7.7).await?;
    let id_a = submit(app, token_a, serial_a, 4.2).await?;

    // Drive A's stream until A's own reading shows up (proves the stream is live,
    // not merely silent), collecting everything seen along the way.
    let mut sse_text = String::new();
    let got_a = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(chunk) = events.next().await {
            let bytes = chunk.expect("sse chunk");
            sse_text.push_str(&String::from_utf8_lossy(&bytes));
            if sse_text.contains(&id_a) {
                return true;
            }
        }
        false
    })
    .await
    .unwrap_or(false);
    check!(got_a, "A's own reading {id_a} never arrived on A's stream; got: {sse_text}");
    check!(
        !sse_text.contains(&id_b),
        "LEAK: B's reading {id_b} appeared on A's SSE stream: {sse_text}"
    );

    // List scoping: A sees its own reading, never B's; and vice versa.
    let a_ids = reading_ids(app, token_a).await?;
    check!(a_ids.contains(&id_a), "A's list missing its own reading {id_a}");
    check!(!a_ids.contains(&id_b), "LEAK: A's list contains B's reading {id_b}");

    let b_ids = reading_ids(app, token_b).await?;
    check!(b_ids.contains(&id_b), "B's list missing its own reading {id_b}");
    check!(!b_ids.contains(&id_a), "LEAK: B's list contains A's reading {id_a}");

    Ok(())
}

/// Every protected route must reject a request whose JWT is missing, malformed,
/// wrongly signed, or expired — all 401, before any DB access. Uses a lazy pool
/// so it needs NO live Postgres (auth fails in the extractor first).
#[tokio::test]
async fn auth_rejects_bad_tokens() {
    let secret = "test-secret-minimum-32-characters-long-aaaaaaaaaa";
    // Lazy pool: never connects unless a query runs — and auth rejection runs
    // before any handler touches the DB.
    let pool = PgPool::connect_lazy(DEFAULT_DB).expect("lazy pool");
    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool));
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(16);
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret),
        readings_tx,
    };
    let app = build_app(state);

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_secs();
    let user = Uuid::new_v4();

    // Each case is a request whose auth must be rejected on a protected route.
    let no_header = Request::builder()
        .method("GET")
        .uri("/api/v1/meters/stats")
        .body(Body::empty())
        .expect("req");

    let no_bearer = Request::builder()
        .method("GET")
        .uri("/api/v1/meters/stats")
        .header(header::AUTHORIZATION, "Token abc")
        .body(Body::empty())
        .expect("req");

    let garbage = authed("GET", "/api/v1/meters/stats", "not-a-jwt", None);

    // Structurally valid JWT, but signed with the WRONG secret.
    let wrong_secret = mint_jwt(user, "some-other-secret-that-is-also-32-chars-long!!");
    let wrong_sig = authed("GET", "/api/v1/meters/stats", &wrong_secret, None);

    // Right secret, but exp is well in the past (beyond jsonwebtoken's default
    // 60s leeway).
    let expired_tok = mint_jwt_with_exp(user, secret, now - 3600);
    let expired = authed("GET", "/api/v1/meters/stats", &expired_tok, None);

    let cases = [
        ("missing header", no_header),
        ("non-Bearer scheme", no_bearer),
        ("garbage token", garbage),
        ("wrong signature", wrong_sig),
        ("expired token", expired),
    ];

    for (name, req) in cases {
        let resp = app.clone().oneshot(req).await.expect("oneshot");
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "{name}: expected 401, got {}",
            resp.status()
        );
    }
    // Valid-token acceptance is covered by the live e2e tests (which exercise
    // the full DB-backed handler). Adding it here would force a DB connection
    // (and a long acquire timeout when no Postgres is up), so it's omitted.
}

/// Remove the meter + readings this test created. Never touches the user.
async fn cleanup(pool: &PgPool, serial: &str) {
    let _ = sqlx::query("DELETE FROM meter_readings WHERE meter_serial = $1")
        .bind(serial)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM meters WHERE serial_number = $1")
        .bind(serial)
        .execute(pool)
        .await;
}
