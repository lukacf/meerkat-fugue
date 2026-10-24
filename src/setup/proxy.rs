//! Browser routing to a registered local repository. The destination is never
//! selected by an incoming URL or header, and response bodies remain streamed.
use anyhow::{Context, Result, bail};
use axum::{
    body::Body,
    extract::Request,
    http::{HeaderMap, header},
    response::Response,
};

pub(super) fn client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(std::time::Duration::from_secs(5))
        .build()?)
}

fn strip_hop_headers(headers: &mut HeaderMap) {
    let names: Vec<_> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| name.trim().parse::<header::HeaderName>().ok())
        .collect();
    for name in names {
        headers.remove(name);
    }
    for name in [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ] {
        headers.remove(name);
    }
}

fn location(value: &str, port: u16, hub: &str, prefix: &str) -> Result<String> {
    let local = format!("http://127.0.0.1:{port}");
    let local_url = reqwest::Url::parse(&local)?;
    let url = local_url.join(value)?;
    let mut path = url.path().to_owned();
    if let Some(query) = url.query() {
        path.push('?');
        path.push_str(query);
    }
    if let Some(fragment) = url.fragment() {
        path.push('#');
        path.push_str(fragment);
    }
    if url.origin() == local_url.origin() {
        return Ok(format!("{prefix}{path}"));
    }
    if url.origin() == reqwest::Url::parse(hub)?.origin() {
        return Ok(path);
    }
    // External authentication redirects are allowed, but never credentials in URLs.
    if url.scheme() != "https" || !url.username().is_empty() || url.password().is_some() {
        bail!("Repository returned an invalid redirect");
    }
    Ok(url.into())
}

pub(super) async fn forward(
    client: &reqwest::Client,
    port: u16,
    hub: &str,
    prefix: &str,
    request: Request,
) -> Result<Response> {
    let (parts, body) = request.into_parts();
    let path = parts
        .uri
        .path()
        .strip_prefix(prefix)
        .context("Invalid repository path")?;
    if !path.starts_with('/') || path.starts_with("//") {
        bail!("Invalid repository path");
    }
    let query = parts
        .uri
        .query()
        .map(|query| format!("?{query}"))
        .unwrap_or_default();
    let local = format!("http://127.0.0.1:{port}");
    let url = format!("{local}{path}{query}");
    let mut headers = parts.headers;
    strip_hop_headers(&mut headers);
    headers.remove(header::COOKIE);
    // Preserve browser Host/Origin and the authenticated proxy assertion.
    // Both outer and inner services independently enforce the same boundary.
    headers.remove("forwarded");
    for name in ["x-forwarded-host", "x-forwarded-proto", "x-forwarded-for"] {
        headers.remove(name);
    }
    let response = client
        .request(parts.method, url)
        .headers(headers)
        .body(reqwest::Body::wrap_stream(body.into_data_stream()))
        .send()
        .await
        .context("Repository service is not available yet; retry shortly")?;
    let status = response.status();
    let mut headers = response.headers().clone();
    strip_hop_headers(&mut headers);
    headers.remove(header::SET_COOKIE);
    if let Some(value) = headers.get(header::LOCATION) {
        let mapped = location(value.to_str()?, port, hub, prefix)?;
        headers.insert(header::LOCATION, mapped.parse()?);
    }
    let mut result = Response::new(Body::from_stream(response.bytes_stream()));
    *result.status_mut() = status;
    *result.headers_mut() = headers;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn redirects_keep_repository_scope_and_shared_console_origin() {
        let prefix = "/repositories/repository-test";
        assert_eq!(
            location("/issues?x=1", 9001, "http://127.0.0.1:8788", prefix).unwrap(),
            format!("{prefix}/issues?x=1")
        );
        assert_eq!(
            location(
                "http://127.0.0.1:8788/console",
                9001,
                "http://127.0.0.1:8788",
                prefix
            )
            .unwrap(),
            "/console"
        );
        assert!(
            location(
                "http://127.0.0.1:9002/issues",
                9001,
                "http://127.0.0.1:8788",
                prefix
            )
            .is_err()
        );
    }
}
