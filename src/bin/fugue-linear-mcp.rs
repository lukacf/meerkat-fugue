use meerkat_fugue::config::TrackerConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = TrackerConfig {
        kind: std::env::var("FUGUE_TRACKER_KIND").unwrap_or_else(|_| "linear".to_string()),
        endpoint: std::env::var("FUGUE_IMS_ENDPOINT")
            .or_else(|_| std::env::var("LINEAR_ENDPOINT"))
            .unwrap_or_else(|_| "https://api.linear.app/graphql".to_string()),
        api_key: std::env::var("LINEAR_API_KEY").ok(),
        project_slug: std::env::var("FUGUE_IMS_PROJECT")
            .or_else(|_| std::env::var("LINEAR_PROJECT_SLUG"))
            .ok(),
        intake_states: Vec::new(),
        in_flight_states: Vec::new(),
        active_states: Vec::new(),
        max_attachment_files: 20,
        max_attachment_bytes: 25 * 1024 * 1024,
        max_attachment_total_bytes: 100 * 1024 * 1024,
        terminal_states: Vec::new(),
    };
    meerkat_fugue::mcp_server::run_linear_mcp_server(config).await
}
