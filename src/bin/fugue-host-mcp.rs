#[tokio::main]
async fn main() -> anyhow::Result<()> {
    meerkat_fugue::mcp_server::run_host_mcp_server().await
}
