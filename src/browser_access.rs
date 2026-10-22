//! Explicit trust boundary for an authenticated operator reverse proxy.
//! Every user admitted by that proxy has full Fugue operator access. The proxy
//! verifies end-user identity and replaces Authorization with its private token;
//! Fugue never treats client-provided identity headers as authentication.
use anyhow::{Context, Result, bail};
use axum::http::{HeaderMap, Method, header};
use sha2::{Digest, Sha256};
use std::{path::Path, sync::OnceLock};
use subtle::ConstantTimeEq;

static PROXY: OnceLock<Option<ProxyAccess>> = OnceLock::new();

struct ProxyAccess {
    origin: String,
    host: String,
    token_hash: [u8; 32],
}

impl ProxyAccess {
    fn new(origin: &str, token: &str) -> Result<Self> {
        let url = reqwest::Url::parse(origin).context("Invalid FUGUE_PUBLIC_ORIGIN")?;
        if url.scheme() != "https"
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
        {
            bail!("FUGUE_PUBLIC_ORIGIN must be an HTTPS origin without a path or credentials");
        }
        if !(32..=256).contains(&token.len()) || !token.bytes().all(|byte| byte.is_ascii_graphic())
        {
            bail!("The trusted proxy token must be 32-256 printable ASCII bytes");
        }
        let origin = url.origin().ascii_serialization();
        let host = origin
            .strip_prefix("https://")
            .context("Invalid HTTPS origin")?
            .to_owned();
        Ok(Self {
            origin,
            host,
            token_hash: Sha256::digest(token.as_bytes()).into(),
        })
    }

    fn allows(&self, method: &Method, headers: &HeaderMap) -> bool {
        if headers.get_all(header::HOST).iter().count() != 1
            || headers
                .get(header::HOST)
                .and_then(|value| value.to_str().ok())
                != Some(&self.host)
            || headers.get_all(header::AUTHORIZATION).iter().count() != 1
        {
            return false;
        }
        let Some(token) = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
        else {
            return false;
        };
        let hash: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        if !bool::from(hash.ct_eq(&self.token_hash)) {
            return false;
        }
        let origins: Vec<_> = headers.get_all(header::ORIGIN).iter().collect();
        if origins.is_empty() {
            return matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS);
        }
        origins.len() == 1 && origins[0].to_str().ok() == Some(&self.origin)
    }
}

pub fn configure_from_environment() -> Result<()> {
    let origin = std::env::var("FUGUE_PUBLIC_ORIGIN").ok();
    let file = std::env::var_os("FUGUE_PROXY_TOKEN_FILE");
    let proxy = match (origin, file) {
        (None, None) => None,
        (Some(origin), Some(file)) => {
            let path = Path::new(&file);
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if std::fs::metadata(path)?.permissions().mode() & 0o077 != 0 {
                    bail!("FUGUE_PROXY_TOKEN_FILE must be readable only by its owner (0600)");
                }
            }
            let token = std::fs::read_to_string(path).context("Read trusted proxy token file")?;
            Some(ProxyAccess::new(&origin, token.trim())?)
        }
        _ => bail!(
            "Set both FUGUE_PUBLIC_ORIGIN and FUGUE_PROXY_TOKEN_FILE for authenticated proxy access"
        ),
    };
    PROXY
        .set(proxy)
        .map_err(|_| anyhow::anyhow!("Browser access was already configured"))
}

pub(crate) fn allows_proxy(method: &Method, headers: &HeaderMap) -> bool {
    PROXY
        .get()
        .and_then(Option::as_ref)
        .is_some_and(|proxy| proxy.allows(method, headers))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn public_operator_access_requires_proxy_token_and_exact_browser_origin() {
        let token = "test-proxy-token-that-is-long-enough";
        let proxy = ProxyAccess::new("https://fugue.example.com", token).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, "fugue.example.com".parse().unwrap());
        headers.insert("x-forwarded-email", "operator@example.com".parse().unwrap());
        assert!(!proxy.allows(&Method::GET, &headers));
        headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        );
        assert!(proxy.allows(&Method::GET, &headers));
        assert!(!proxy.allows(&Method::POST, &headers));
        headers.insert(header::ORIGIN, "https://fugue.example.com".parse().unwrap());
        assert!(proxy.allows(&Method::POST, &headers));
        headers.insert(
            header::ORIGIN,
            "https://untrusted.example.com".parse().unwrap(),
        );
        assert!(!proxy.allows(&Method::POST, &headers));
        assert!(!proxy.allows(&Method::GET, &headers));
        headers.insert(header::ORIGIN, "https://fugue.example.com".parse().unwrap());
        headers.insert(header::AUTHORIZATION, "Bearer untrusted".parse().unwrap());
        assert!(!proxy.allows(&Method::POST, &headers));
    }

    #[test]
    fn invalid_proxy_configuration_fails_closed() {
        for origin in [
            "http://example.com",
            "https://example.com/path",
            "https://user@example.com",
            "https://example.com/?x=1",
        ] {
            assert!(ProxyAccess::new(origin, "test-proxy-token-that-is-long-enough").is_err());
        }
        assert!(ProxyAccess::new("https://example.com", "short").is_err());
    }
}
