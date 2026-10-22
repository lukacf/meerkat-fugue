//! Operator commands for the local Ensemble daemon.

use std::fs::OpenOptions;
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};
use serde::Serialize;

use super::config::{EnsembleConfig, RepoId};

const DEFAULT_URL: &str = "http://127.0.0.1:8788";

#[derive(Debug, Args)]
pub struct EnsembleArgs {
    #[command(subcommand)]
    command: EnsembleCommand,
}

#[derive(Debug, Subcommand)]
enum EnsembleCommand {
    /// Configure repositories for model-discovered, persistent specialist teams.
    Init {
        /// Existing Git repository roots to add to the Ensemble.
        #[arg(required = true, num_args = 1..)]
        repositories: Vec<PathBuf>,
        #[arg(short, long, default_value = "ensemble.yaml")]
        output: PathBuf,
        #[arg(long, default_value = "My Ensemble")]
        name: String,
        /// Model for all team members; defaults to Fugue's standard agent model.
        #[arg(long)]
        model: Option<String>,
        /// Leave repositories disconnected instead of peering all representatives.
        #[arg(long)]
        no_peers: bool,
    },
    /// Start repository teams and the local cockpit; restore existing conversations.
    Serve {
        #[arg(short, long, default_value = "ensemble.yaml")]
        config: PathBuf,
        /// Loopback address only; Ensemble is a local operator service.
        #[arg(long, default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
        host: IpAddr,
        #[arg(short, long, default_value_t = 8788)]
        port: u16,
    },
    /// Print the running Ensemble's native team state as JSON.
    Status {
        #[arg(long, default_value = DEFAULT_URL)]
        url: String,
    },
    /// Give a repository representative an objective to coordinate with its team.
    Ask {
        /// Repository ID from ensemble.yaml.
        repository: String,
        /// Objective, question, or explicit repository-study request.
        message: String,
        #[arg(long, default_value = DEFAULT_URL)]
        url: String,
    },
}

#[derive(Serialize)]
struct MessageRequest<'a> {
    message: &'a str,
}

pub async fn run(args: EnsembleArgs) -> Result<()> {
    match args.command {
        EnsembleCommand::Init {
            repositories,
            output,
            name,
            model,
            no_peers,
        } => {
            let mut config = EnsembleConfig::for_repositories(&repositories, name)?;
            if let Some(model) = model {
                if model.trim().is_empty() {
                    bail!("model must be nonempty");
                }
                config.model = model;
            }
            if no_peers {
                config.peers.clear();
            }
            write_new_config(&output, &config)?;
            println!(
                "Created {} with {} repositories and {} representative connections.",
                output.display(),
                config.repositories.len(),
                config.peers.len()
            );
            for (id, repository) in &config.repositories {
                println!("  {id}: {}", repository.path.display());
            }
            println!(
                "Start: meerkat-fugue ensemble serve --config {}",
                output.display()
            );
            println!(
                "First serve studies each repository to discover and prepare its specialists (uses your configured model)."
            );
            println!("Cockpit: {DEFAULT_URL}");
            Ok(())
        }
        EnsembleCommand::Serve { config, host, port } => {
            super::serve(config, SocketAddr::new(host, port)).await
        }
        EnsembleCommand::Status { url } => {
            let response = client()?
                .get(endpoint(&url, "api/ensemble")?)
                .send()
                .await
                .context("contact Ensemble; start `meerkat-fugue ensemble serve` first")?;
            print_response(response).await
        }
        EnsembleCommand::Ask {
            repository,
            message,
            url,
        } => {
            let repository = RepoId::try_from(repository).map_err(anyhow::Error::msg)?;
            if message.trim().is_empty() {
                bail!("message must be nonempty");
            }
            let response = client()?
                .post(endpoint(&url, &format!("api/repos/{repository}/messages"))?)
                .json(&MessageRequest { message: &message })
                .send()
                .await
                .context("submit objective to Ensemble")?;
            print_response(response).await
        }
    }
}

fn write_new_config(path: &std::path::Path, config: &EnsembleConfig) -> Result<()> {
    let body = serde_yaml::to_string(config).context("encode Ensemble configuration")?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| {
            format!(
                "create {}; existing files are never overwritten",
                path.display()
            )
        })?;
    file.write_all(
        b"# Long-lived repository teams. Each edge connects representatives in both directions.\n\
          # First serve discovers repository-specific specialists. Code-capable members can edit their checkout.\n"
    ).context("write configuration header")?;
    file.write_all(body.as_bytes())
        .context("write Ensemble configuration")?;
    Ok(())
}

fn client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(30))
        .build()
        .context("build Ensemble HTTP client")
}

fn endpoint(base: &str, path: &str) -> Result<reqwest::Url> {
    let mut url = reqwest::Url::parse(base).context("invalid Ensemble URL")?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        bail!("Ensemble URL must be an HTTP or HTTPS server address");
    }
    url.set_path(&format!("/{path}"));
    url.set_query(None);
    url.set_fragment(None);
    Ok(url)
}

async fn print_response(response: reqwest::Response) -> Result<()> {
    let status = response.status();
    let body = response.text().await.context("read Ensemble response")?;
    if !status.is_success() {
        bail!("Ensemble returned {status}: {body}");
    }
    // The server owns this typed response. Validate JSON without parsing its fields again.
    let json: Box<serde_json::value::RawValue> =
        serde_json::from_str(&body).context("Ensemble returned an invalid JSON response")?;
    println!("{}", json.get());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct TestCli {
        #[command(flatten)]
        args: EnsembleArgs,
    }

    #[test]
    fn parses_operator_commands_and_requires_an_objective() {
        assert!(
            TestCli::try_parse_from(["ensemble", "init", "/tmp/api", "/tmp/web", "--no-peers"])
                .is_ok()
        );
        assert!(
            TestCli::try_parse_from(["ensemble", "ask", "api", "Study the API contract"]).is_ok()
        );
        assert!(TestCli::try_parse_from(["ensemble", "ask", "api"]).is_err());
    }

    #[test]
    fn endpoint_discards_old_path_query_and_fragment() {
        assert_eq!(
            endpoint("http://localhost:8788/stale?x=y#z", "api/ensemble")
                .unwrap()
                .as_str(),
            "http://localhost:8788/api/ensemble"
        );
        assert!(endpoint("file:///tmp/x", "api/ensemble").is_err());
    }

    #[test]
    fn refuses_to_overwrite_existing_configuration() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("ensemble.yaml");
        std::fs::write(&file, "keep me").unwrap();
        let config: EnsembleConfig = serde_yaml::from_str("repositories: {}").unwrap();
        assert!(write_new_config(&file, &config).is_err());
        assert_eq!(std::fs::read_to_string(file).unwrap(), "keep me");
    }
}
