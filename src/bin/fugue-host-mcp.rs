#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if meerkat_fugue::build_identity::print_if_requested()? {
        return Ok(());
    }
    meerkat_fugue::mcp_server::run_host_mcp_server().await
}
