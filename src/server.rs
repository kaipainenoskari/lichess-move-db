// Optional HTTP server for query API
use anyhow::Result;
use axum::extract::Query;
use axum::routing::get;
use axum::Json;
use serde::Deserialize;
use std::path::PathBuf;
use tower_http::cors::CorsLayer;

#[derive(Deserialize)]
pub struct QueryParams {
    pub fen: String,
    pub bucket: String,
}

#[derive(Clone)]
enum DbBackend {
    Sqlite(PathBuf),
    Postgres(sqlx::PgPool),
}

#[derive(Clone)]
struct AppState {
    db: DbBackend,
    band_width: u32,
}

pub async fn serve(db: &str, bind: &str, band_width: u32) -> Result<()> {
    let state = if db.contains("://") {
        let pool = sqlx::postgres::PgPoolOptions::new().connect(db).await?;
        AppState {
            db: DbBackend::Postgres(pool),
            band_width,
        }
    } else {
        AppState {
            db: DbBackend::Sqlite(PathBuf::from(db)),
            band_width,
        }
    };
    let cors = CorsLayer::permissive();
    let app = axum::Router::new()
        .route("/", get(handle_query))
        .route("/query", get(handle_query))
        .layer(cors)
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn handle_query(
    Query(params): Query<QueryParams>,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Result<Json<crate::store::QueryResult>, (axum::http::StatusCode, String)> {
    let result = match &state.db {
        DbBackend::Sqlite(path) => crate::store::query_moves(
            path,
            &params.fen,
            &params.bucket,
            state.band_width,
        )
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
        DbBackend::Postgres(pool) => {
            crate::store_postgres::query_moves(pool, &params.fen, &params.bucket, state.band_width)
                .await
                .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
        }
    };
    let r = result?;
    Ok(Json(r))
}
