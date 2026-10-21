use meerkat_fugue::config::TrackerConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = TrackerConfig::from_mcp_env("linear")?;
    meerkat_fugue::mcp_server::run_linear_mcp_server(config).await
}
