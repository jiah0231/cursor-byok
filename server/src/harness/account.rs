use std::path::{Path, PathBuf};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde_json::{json, Value};
use sqlx::{Connection, Row, SqliteConnection};

use crate::{store::TabMode, Error, Result};

const EMAIL: &str = "cursor@ai.com";
const SIGN_UP_TYPE: &str = "Google";
const SUBJECT: &str = "cursor-local-user";
const MEMBERSHIP_TYPE: &str = "ultra";
const SUBSCRIPTION_STATUS: &str = "active";
const AUTH_KEYS: &[&str] = &[
    "cursorAuth/accessToken",
    "cursorAuth/refreshToken",
    "cursorAuth/cachedEmail",
    "cursorAuth/cachedSignUpType",
    "cursorAuth/stripeMembershipType",
    "cursorAuth/stripeSubscriptionStatus",
];

pub async fn prepare_for_tab_mode(mode: TabMode) -> Result<()> {
    prepare_for_tab_mode_at(mode, &state_db_path()?).await
}

async fn prepare_for_tab_mode_at(mode: TabMode, path: &Path) -> Result<()> {
    match mode {
        TabMode::Direct => remove_legacy_synthetic_account_at(path).await,
        TabMode::Public | TabMode::Custom => inject_if_missing_at(path).await,
    }
}

fn state_db_path() -> Result<PathBuf> {
    let home = dirs::home_dir()
        .ok_or_else(|| Error::Config("cannot resolve user home directory".into()))?;
    match std::env::consts::OS {
        "macos" => {
            Ok(home.join("Library/Application Support/Cursor/User/globalStorage/state.vscdb"))
        }
        "windows" => Ok(std::env::var_os("APPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join("AppData/Roaming"))
            .join("Cursor/User/globalStorage/state.vscdb")),
        "linux" => Ok(std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".config"))
            .join("Cursor/User/globalStorage/state.vscdb")),
        platform => Err(Error::Config(format!(
            "Cursor account injection is unsupported on {platform}"
        ))),
    }
}

async fn inject_if_missing_at(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true);
    let mut connection = SqliteConnection::connect_with(&options).await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS ItemTable (key TEXT UNIQUE ON CONFLICT REPLACE, value BLOB)",
    )
    .execute(&mut connection)
    .await?;

    let account = sqlx::query("SELECT CAST(value AS TEXT) AS value FROM ItemTable WHERE key = ?")
        .bind("cursorAuth/accessToken")
        .fetch_optional(&mut connection)
        .await?;
    if account.is_some_and(|row| {
        row.try_get::<String, _>("value")
            .is_ok_and(|value| !value.trim().is_empty())
    }) {
        return Ok(());
    }

    let token = local_token()?;
    let values = [
        ("cursorAuth/accessToken", token.as_str()),
        ("cursorAuth/refreshToken", token.as_str()),
        ("cursorAuth/cachedEmail", EMAIL),
        ("cursorAuth/cachedSignUpType", SIGN_UP_TYPE),
        ("cursorAuth/stripeMembershipType", MEMBERSHIP_TYPE),
        ("cursorAuth/stripeSubscriptionStatus", SUBSCRIPTION_STATUS),
    ];
    let mut transaction = connection.begin().await?;
    for (key, value) in values {
        sqlx::query("INSERT OR REPLACE INTO ItemTable(key, value) VALUES(?, ?)")
            .bind(key)
            .bind(value)
            .execute(&mut *transaction)
            .await?;
    }
    transaction.commit().await?;
    tracing::info!(
        email = EMAIL,
        subject = SUBJECT,
        "injected local Cursor account"
    );
    Ok(())
}

async fn remove_legacy_synthetic_account_at(path: &Path) -> Result<()> {
    if !tokio::fs::try_exists(path).await? {
        return Ok(());
    }

    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(false);
    let mut connection = SqliteConnection::connect_with(&options).await?;
    let has_item_table: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'ItemTable'",
    )
    .fetch_one(&mut connection)
    .await?;
    if has_item_table == 0 {
        return Ok(());
    }

    let access_token = item_value(&mut connection, "cursorAuth/accessToken").await?;
    let cached_email = item_value(&mut connection, "cursorAuth/cachedEmail").await?;
    let Some(access_token) = access_token.filter(|value| !value.trim().is_empty()) else {
        return Ok(());
    };

    if cached_email.as_deref() != Some(EMAIL) || !is_known_synthetic_token(&access_token) {
        return Ok(());
    }

    let mut transaction = connection.begin().await?;
    for key in AUTH_KEYS {
        sqlx::query("DELETE FROM ItemTable WHERE key = ?")
            .bind(key)
            .execute(&mut *transaction)
            .await?;
    }
    transaction.commit().await?;
    tracing::warn!(
        email = EMAIL,
        "removed legacy synthetic Cursor account before using direct TAB mode; sign in to Cursor with a real account for official TAB"
    );
    Ok(())
}

async fn item_value(connection: &mut SqliteConnection, key: &str) -> Result<Option<String>> {
    let row = sqlx::query("SELECT CAST(value AS TEXT) AS value FROM ItemTable WHERE key = ?")
        .bind(key)
        .fetch_optional(connection)
        .await?;
    Ok(row.and_then(|row| row.try_get::<String, _>("value").ok()))
}

fn is_known_synthetic_token(token: &str) -> bool {
    let mut parts = token.split('.');
    let Some(_header) = parts.next() else {
        return false;
    };
    let Some(payload) = parts.next() else {
        return false;
    };
    if parts.next().is_none() || parts.next().is_some() {
        return false;
    }
    let Ok(payload) = URL_SAFE_NO_PAD.decode(payload) else {
        return false;
    };
    let Ok(payload) = serde_json::from_slice::<Value>(&payload) else {
        return false;
    };
    payload.get("email").and_then(Value::as_str) == Some(EMAIL)
        && payload.get("iss").and_then(Value::as_str) == Some("cursor-client")
        && payload.get("type").and_then(Value::as_str) == Some("session")
}

fn local_token() -> Result<String> {
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
    let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&json!({
        "sub": SUBJECT,
        "email": EMAIL,
        "type": "session",
        "iss": "cursor-client",
        "scope": "openid profile email",
        "exp": 4070908800_u64
    }))?);
    Ok(format!("{header}.{payload}.{SUBJECT}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn values(path: &Path) -> std::collections::HashMap<String, String> {
        let options = sqlx::sqlite::SqliteConnectOptions::new().filename(path);
        let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
        sqlx::query("SELECT key, CAST(value AS TEXT) AS value FROM ItemTable")
            .fetch_all(&mut connection)
            .await
            .unwrap()
            .into_iter()
            .map(|row| (row.get::<String, _>("key"), row.get::<String, _>("value")))
            .collect()
    }

    #[tokio::test]
    async fn public_mode_injects_the_local_account_only_when_missing() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.vscdb");

        prepare_for_tab_mode_at(TabMode::Public, &path).await.unwrap();

        let stored = values(&path).await;
        let token = &stored["cursorAuth/accessToken"];
        assert_eq!(stored["cursorAuth/refreshToken"], *token);
        assert_eq!(stored["cursorAuth/cachedEmail"], EMAIL);
        assert_eq!(stored["cursorAuth/cachedSignUpType"], SIGN_UP_TYPE);
        assert_eq!(stored["cursorAuth/stripeMembershipType"], MEMBERSHIP_TYPE);
        assert_eq!(
            stored["cursorAuth/stripeSubscriptionStatus"],
            SUBSCRIPTION_STATUS
        );
        let payload = token.split('.').nth(1).unwrap();
        let payload: serde_json::Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).unwrap()).unwrap();
        assert_eq!(payload["sub"], SUBJECT);
        assert_eq!(payload["email"], EMAIL);
        assert_eq!(payload["exp"], 4070908800_u64);

        let options = sqlx::sqlite::SqliteConnectOptions::new().filename(&path);
        let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
        sqlx::query("UPDATE ItemTable SET value = 'existing-token' WHERE key = ?")
            .bind("cursorAuth/accessToken")
            .execute(&mut connection)
            .await
            .unwrap();
        drop(connection);
        prepare_for_tab_mode_at(TabMode::Public, &path).await.unwrap();

        assert_eq!(values(&path).await["cursorAuth/accessToken"], "existing-token");
    }

    #[tokio::test]
    async fn direct_mode_removes_a_known_legacy_synthetic_account() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.vscdb");
        prepare_for_tab_mode_at(TabMode::Public, &path).await.unwrap();

        prepare_for_tab_mode_at(TabMode::Direct, &path).await.unwrap();

        let stored = values(&path).await;
        for key in AUTH_KEYS {
            assert!(!stored.contains_key(*key), "legacy key {key} was not removed");
        }
    }

    #[tokio::test]
    async fn direct_mode_preserves_an_ambiguous_or_real_account() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.vscdb");
        prepare_for_tab_mode_at(TabMode::Public, &path).await.unwrap();

        let options = sqlx::sqlite::SqliteConnectOptions::new().filename(&path);
        let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
        sqlx::query("UPDATE ItemTable SET value = 'real-or-opaque-token' WHERE key = ?")
            .bind("cursorAuth/accessToken")
            .execute(&mut connection)
            .await
            .unwrap();
        sqlx::query("UPDATE ItemTable SET value = 'person@example.com' WHERE key = ?")
            .bind("cursorAuth/cachedEmail")
            .execute(&mut connection)
            .await
            .unwrap();
        drop(connection);

        prepare_for_tab_mode_at(TabMode::Direct, &path).await.unwrap();

        let stored = values(&path).await;
        assert_eq!(stored["cursorAuth/accessToken"], "real-or-opaque-token");
        assert_eq!(stored["cursorAuth/cachedEmail"], "person@example.com");
    }

    #[tokio::test]
    async fn direct_mode_does_not_delete_an_unparseable_token_even_with_the_legacy_email() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.vscdb");
        prepare_for_tab_mode_at(TabMode::Public, &path).await.unwrap();

        let options = sqlx::sqlite::SqliteConnectOptions::new().filename(&path);
        let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
        sqlx::query("UPDATE ItemTable SET value = 'opaque-token' WHERE key = ?")
            .bind("cursorAuth/accessToken")
            .execute(&mut connection)
            .await
            .unwrap();
        drop(connection);

        prepare_for_tab_mode_at(TabMode::Direct, &path).await.unwrap();

        let stored = values(&path).await;
        assert_eq!(stored["cursorAuth/accessToken"], "opaque-token");
        assert_eq!(stored["cursorAuth/cachedEmail"], EMAIL);
    }

    #[tokio::test]
    async fn direct_mode_does_not_create_a_cursor_state_database() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("missing/state.vscdb");

        prepare_for_tab_mode_at(TabMode::Direct, &path).await.unwrap();

        assert!(!path.exists());
    }
}
