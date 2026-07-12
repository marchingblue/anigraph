use anyhow::{Context, Result};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

pub const TOKEN_URL: &str = "https://anilist.co/api/v2/oauth/token";

/// PKCE code verifier (RFC 7636 §4.1). 32 bytes random, base64url no-pad.
#[derive(Debug, Clone)]
pub struct PkceVerifier(String);

impl Default for PkceVerifier {
    fn default() -> Self {
        Self::new()
    }
}

impl PkceVerifier {
    pub fn new() -> Self {
        use rand::Rng;
        let mut bytes = [0u8; 32];
        rand::thread_rng().fill(&mut bytes);
        Self(URL_SAFE_NO_PAD.encode(bytes))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// code_challenge = BASE64URL(SHA-256(code_verifier))
    pub fn challenge(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.0.as_bytes());
        URL_SAFE_NO_PAD.encode(hasher.finalize())
    }
}

/// AniList OAuth2 token set.
///
/// Notes:
/// - AniList does NOT support refresh tokens — tokens last 1 year then expire.
/// - Scopes are not supported — tokens provide (almost) full access.
/// - See: https://docs.anilist.co/guide/auth/
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenSet {
    pub access_token: String,
    pub expires_in: u64,
    /// Unix timestamp when we received this token (set locally, not from API)
    #[serde(skip)]
    pub created_at: u64,
}

impl TokenSet {
    pub fn is_expired(&self) -> bool {
        let now = unix_now();
        now >= self.created_at + self.expires_in.saturating_sub(60)
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub fn token_path() -> Result<PathBuf> {
    let base = dirs::config_dir().context("could not determine config dir")?;
    Ok(base.join("rouge").join("token.json"))
}

pub fn save_tokens(tokens: &TokenSet) -> Result<PathBuf> {
    let path = token_path()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let json = serde_json::to_string_pretty(tokens)?;
    std::fs::write(&path, json)?;
    Ok(path)
}

pub fn load_tokens() -> Result<Option<TokenSet>> {
    let path = token_path()?;
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(&path)?;
    let mut ts: TokenSet = serde_json::from_str(&raw)?;
    // If created_at is 0 (old format), set it to now
    if ts.created_at == 0 {
        ts.created_at = unix_now();
    }
    Ok(Some(ts))
}

/// Start the OAuth2 PKCE flow. Opens browser and returns token after callback.
pub async fn authenticate(client_id: &str, client_secret: &str) -> Result<TokenSet> {
    let verifier = PkceVerifier::new();
    let redirect_uri = "http://127.0.0.1:8765/callback";

    // Build authorize URL with PKCE challenge
    let auth_url = format!(
        "https://anilist.co/api/v2/oauth/authorize?client_id={}&redirect_uri={}&response_type=code&code_challenge={}&code_challenge_method=S256",
        client_id,
        urlencoding::encode(redirect_uri),
        verifier.challenge(),
    );

    // Start local callback server
    let code_fut = start_callback_server();

    // Open browser
    eprintln!("opening browser to:\n  {auth_url}\n");
    eprintln!("(if it doesn't open, paste the URL above into your browser)");
    open_browser(&auth_url);

    let code = code_fut.await?;

    // Exchange code for token
    let client = Client::builder()
        .user_agent("rouge/0.1.0")
        .build()
        .context("building HTTP client")?;

    let params = [
        ("grant_type", "authorization_code"),
        ("client_id", client_id),
        ("client_secret", client_secret),
        ("redirect_uri", redirect_uri),
        ("code", &code),
        ("code_verifier", verifier.as_str()),
    ];

    let resp = client
        .post(TOKEN_URL)
        .form(&params)
        .send()
        .await
        .context("sending token request")?;

    let status = resp.status();
    let body = resp.text().await.context("reading token response")?;

    if !status.is_success() {
        anyhow::bail!("token exchange failed ({status}): {body}");
    }

    let mut token: TokenSet =
        serde_json::from_str(&body).context("parsing AniList token response")?;
    token.created_at = unix_now();

    Ok(token)
}

/// Open a URL in the user's default browser.
fn open_browser(url: &str) {
    #[cfg(target_os = "linux")]
    let cmd = "xdg-open";
    #[cfg(target_os = "macos")]
    let cmd = "open";
    #[cfg(target_os = "windows")]
    let cmd = "rundll32";

    #[cfg(target_os = "windows")]
    let args = vec!["url.dll,FileProtocolHandler", url];
    #[cfg(not(target_os = "windows"))]
    let args = &[url];

    match std::process::Command::new(cmd).args(args).status() {
        Ok(s) if s.success() => {}
        Ok(s) => eprintln!("warning: {cmd} exited with {s}"),
        Err(e) => eprintln!("warning: could not open browser: {e}"),
    }
}

async fn start_callback_server() -> Result<String> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:8765").await?;
    tracing::info!("callback server listening on 127.0.0.1:8765");

    let (stream, _) = listener.accept().await?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await?;

    // Extract code from GET /callback?code=... HTTP/1.1
    let code = line
        .split_whitespace()
        .nth(1)
        .and_then(|path| path.split('?').nth(1))
        .and_then(|qs| {
            qs.split('&')
                .find(|p| p.starts_with("code="))
                .map(|p| p[5..].to_string())
        })
        .ok_or_else(|| anyhow::anyhow!("No code in callback"))?;

    // Send success response to browser
    let response = "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n\
        <html><body><h1>Authenticated!</h1>\
        <p>You can close this tab and return to the terminal.</p>\
        </body></html>";
    reader.into_inner().write_all(response.as_bytes()).await?;

    Ok(code)
}
