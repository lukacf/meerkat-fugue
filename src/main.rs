use std::fs;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use meerkat_fugue::backend::{
    ConfigureGcpSystemdInput, RepoOnboardInput, configure_gcp_systemd_backend, onboard_repo,
};
use meerkat_fugue::config::WorkflowConfigManager;
use meerkat_fugue::http;
use meerkat_fugue::onboarding::{
    GeneratedOnboardingFiles, NewRepoOnboardingOptions, OnboardingMode, render_new_repo_onboarding,
};
use meerkat_fugue::orchestrator::Orchestrator;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "meerkat-fugue")]
#[command(
    about = "Run Fugue issue queues and persistent repository teams.",
    long_about = "Run Fugue issue queues and persistent repository teams.\n\nUse `ensemble` to give local repositories long-lived specialist teams connected through representatives, with a shared cockpit and native Meta consoles.\n\nPass a fugue.yaml file to observe a configured issue queue, launch native Meta issue mobs, and record durable Workpad evidence. Pass a repository directory to generate an onboarding plan for that repo.",
    after_help = "Examples:\n  meerkat-fugue ensemble init /path/to/api /path/to/web\n      Configure persistent repository teams and representative connections.\n\n  meerkat-fugue ensemble serve\n      Start the local Ensemble cockpit and restore repository teams.\n\n  meerkat-fugue fugue.yaml\n      Start polling the queue described by fugue.yaml.\n\n  meerkat-fugue /path/to/repo --backend default --linear-team LUC --dry-run\n      Preview repository onboarding without mutating the remote backend.\n\n  meerkat-fugue stage /path/to/repo --linear-project-slug my-project --linear-team LUC\n      Write starter Fugue config files for an existing repository.",
    disable_help_subcommand = true
)]
struct Cli {
    #[command(subcommand)]
    command: Option<CliCommand>,

    #[arg(long, hide = true)]
    managed_child: bool,
    #[arg(long, hide = true)]
    managed_workspace: Option<PathBuf>,

    /// Path to fugue.yaml. When a directory is
    /// provided, Fugue onboards that repository instead of starting a queue.
    #[arg(value_name = "WORKFLOW")]
    workflow: Option<PathBuf>,

    /// Start the optional HTTP status API/dashboard on PORT, overriding server.port from config.
    #[arg(long)]
    port: Option<u16>,

    /// Bind address for the optional HTTP status API/dashboard when it is enabled.
    #[arg(long, default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    host: IpAddr,

    /// Load repository teams into the main Fugue UI from an Ensemble configuration.
    #[arg(long, value_name = "PATH", conflicts_with = "ota_update")]
    ensemble_config: Option<PathBuf>,

    /// Run the configured OTA update once after loading WORKFLOW, then exit without polling.
    #[arg(long)]
    ota_update: bool,

    /// Validate configuration and tracker/repository access without starting agents.
    #[arg(long, conflicts_with = "ota_update")]
    check: bool,

    /// Backend profile to use when the positional argument is a repository directory.
    #[arg(long)]
    backend: Option<String>,

    /// Mob flow to use when onboarding a repository directory.
    #[arg(long)]
    flow: Option<String>,

    /// Linear project name to use when onboarding a repository directory.
    #[arg(long)]
    project_name: Option<String>,

    /// Linear team key/name/id to use when onboarding a repository directory.
    #[arg(long)]
    linear_team: Option<String>,

    /// Replace an existing remote queue service during repository onboarding.
    #[arg(long)]
    replace_service: bool,

    /// Print the repository onboarding plan without mutating GCP.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Subcommand)]
enum CliCommand {
    /// Print the immutable build identity as JSON and exit.
    BuildInfo,
    /// Inspect or migrate one stopped repository runtime to native storage.
    MigrateStorage(MigrateStorageArgs),
    /// Start Fugue with one browser address for all repositories and their setup.
    Serve(ServeArgs),
    /// Open local first-run setup for credentials, repositories, and tickets.
    Setup(SetupArgs),
    /// Run long-lived repository teams connected through their representatives.
    Ensemble(meerkat_fugue::ensemble::cli::EnsembleArgs),
    /// Configure Fugue operator backends.
    Configure(ConfigureCommand),
    /// Stage an existing repository for Fugue by generating a validated config.
    Stage(StageArgs),
    /// Alias for stage.
    Init(StageArgs),
}

#[derive(Debug, Args)]
struct SetupArgs {
    /// Private application state directory. Defaults to the user's state directory.
    #[arg(long)]
    state_dir: Option<PathBuf>,
    #[arg(long, default_value_t = 8788)]
    port: u16,
    /// Reopen setup instead of resuming this directory's configured workspace.
    #[arg(long)]
    reconfigure: bool,
}

#[derive(Debug, Args)]
struct ServeArgs {
    /// Private persistent state, kept outside repository checkouts.
    #[arg(long)]
    state_dir: Option<PathBuf>,
    #[arg(long, default_value_t = 8788)]
    port: u16,
}

#[derive(Debug, Args)]
struct MigrateStorageArgs {
    /// Exact repository runtime directory containing runtime.sqlite and mob.sqlite3.
    #[arg(long)]
    runtime_dir: PathBuf,
    /// Apply the native migration after taking a complete stopped-state backup.
    /// Without this flag, inspect the directory without changing its databases.
    #[arg(long)]
    apply: bool,
    /// Directory for temporary census copies. Defaults to the runtime's parent.
    #[arg(long)]
    scratch_dir: Option<PathBuf>,
}

async fn migrate_storage(args: MigrateStorageArgs) -> anyhow::Result<()> {
    use meerkat_meta::storage_migrate_native::{NativeMigrateRequest, migrate_state_dir_native};

    let state = fs::canonicalize(&args.runtime_dir)
        .context("Open the existing repository runtime directory; stop Fugue before migrating")?;
    for file in ["runtime.sqlite", "mob.sqlite3"] {
        if !state.join(file).is_file() {
            anyhow::bail!(
                "{} does not contain {file}. Select the repository runtime directory, not the application state root. Restore a missing store from a complete backup before migrating.",
                state.display()
            );
        }
    }
    let mode = if args.apply {
        meerkat_meta::MigrateMode::Apply
    } else {
        meerkat_meta::MigrateMode::DryRun
    };
    let mut request =
        NativeMigrateRequest::new(&state, mode).with_mob_store(state.join("mob.sqlite3"));
    if let Some(scratch) = args.scratch_dir {
        request = request.with_scratch_dir(scratch);
    }
    // Meta owns the offline fence, census, migration journal and refusal policy.
    // Keep its typed result intact for operators and installation tooling.
    let report = migrate_state_dir_native(&request).await;
    println!("{}", serde_json::to_string_pretty(&report)?);
    if report.has_refusals() {
        anyhow::bail!("Native storage migration refused; inspect the JSON report above");
    }
    Ok(())
}

fn setup_state_dir(path: Option<PathBuf>) -> anyhow::Result<PathBuf> {
    if let Some(path) = path {
        return Ok(std::path::absolute(path)?);
    }
    meerkat_fugue::setup::default_state_dir()
}

#[derive(Debug, Args)]
struct ConfigureCommand {
    #[command(subcommand)]
    command: ConfigureSubcommand,
}

#[derive(Debug, Subcommand)]
enum ConfigureSubcommand {
    /// Configure a deploy backend.
    Backend(ConfigureBackendCommand),
}

#[derive(Debug, Args)]
struct ConfigureBackendCommand {
    #[command(subcommand)]
    command: ConfigureBackendSubcommand,
}

#[derive(Debug, Subcommand)]
enum ConfigureBackendSubcommand {
    /// Store a GCP VM plus systemd backend profile.
    GcpSystemd(ConfigureGcpSystemdArgs),
}

#[derive(Debug, Args)]
struct ConfigureGcpSystemdArgs {
    /// Backend profile name.
    #[arg(long, default_value = "default")]
    name: String,

    #[arg(long)]
    gcp_project: String,

    #[arg(long)]
    zone: String,

    #[arg(long)]
    instance: String,

    #[arg(long)]
    remote_user: String,

    #[arg(long)]
    fugue_root: String,

    #[arg(long)]
    bin_dir: Option<String>,

    #[arg(long, default_value_t = 4001)]
    port_start: u16,

    #[arg(long, default_value_t = 4099)]
    port_end: u16,

    /// Default Linear team for flows without a specific mapping.
    #[arg(long)]
    default_team: Option<String>,

    /// Flow-to-team mapping, for example implementation_mob=LUC.
    #[arg(long = "flow-team", value_parser = parse_key_value)]
    flow_teams: Vec<(String, String)>,

    /// Make this profile the default backend.
    #[arg(long)]
    default: bool,
}

#[derive(Debug, Args)]
struct StageArgs {
    /// Existing repository to stage. Defaults to the current directory.
    #[arg(default_value = ".")]
    repo: PathBuf,

    /// Directory where generated Fugue files are written. Defaults to the repo root.
    #[arg(long)]
    output_dir: Option<PathBuf>,

    /// GitHub clone URL. Defaults to git remote.origin.url from the repo.
    #[arg(long)]
    github_repo_url: Option<String>,

    /// Linear project slug to poll.
    #[arg(long)]
    linear_project_slug: Option<String>,

    /// Linear team key or name.
    #[arg(long)]
    linear_team: Option<String>,

    /// Linear API key literal or env reference written into config.
    #[arg(long, default_value = "$LINEAR_API_KEY")]
    linear_api_key: String,

    /// Workspace root written into config.
    #[arg(long, default_value = "./fugue_workspaces")]
    workspace_root: String,

    /// Optional HTTP status API port written into config.
    #[arg(long)]
    server_port: Option<u16>,

    /// Overwrite generated files that already exist.
    #[arg(long)]
    force: bool,

    /// Print generated files without writing them.
    #[arg(long)]
    dry_run: bool,
}

fn main() -> anyhow::Result<()> {
    // Native machine transitions need the same deep stacks as Meta's gateways.
    // Size the root thread too, so direct binary execution is independent of
    // Cargo's test environment and the OS main-thread stack default.
    let result = std::thread::Builder::new()
        .name("fugue-runtime".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| meerkat_meta::gateway_composition::gateway_tokio_runtime()?.block_on(run()))?
        .join()
        .map_err(|_| anyhow::anyhow!("Fugue runtime thread panicked"))?;
    if result
        .as_ref()
        .is_err_and(|error| error.is::<meerkat_fugue::setup::managed::RestartRequested>())
    {
        std::process::exit(meerkat_fugue::setup::managed::RESTART_EXIT_CODE);
    }
    if result
        .as_ref()
        .is_err_and(|error| error.is::<meerkat_fugue::setup::managed::ReconfigureRequested>())
    {
        std::process::exit(meerkat_fugue::setup::managed::RECONFIGURE_EXIT_CODE);
    }
    result
}

async fn run() -> anyhow::Result<()> {
    init_logging();
    meerkat_fugue::browser_access::configure_from_environment()?;

    let cli = Cli::parse();
    match cli.command {
        Some(CliCommand::MigrateStorage(args)) => return migrate_storage(args).await,
        Some(CliCommand::Serve(args)) => {
            return meerkat_fugue::setup::host::serve(setup_state_dir(args.state_dir)?, args.port)
                .await;
        }
        Some(CliCommand::Setup(args)) => {
            if !cli.host.is_loopback() {
                anyhow::bail!("First-run setup requires a loopback listener");
            }
            return meerkat_fugue::setup::managed::supervise(
                &setup_state_dir(args.state_dir)?,
                args.port,
                args.reconfigure,
            )
            .await;
        }
        Some(CliCommand::BuildInfo) => {
            println!(
                "{}",
                serde_json::to_string(&meerkat_fugue::build_identity::current())?
            );
            return Ok(());
        }
        Some(CliCommand::Ensemble(args)) => {
            return meerkat_fugue::ensemble::cli::run(args).await;
        }
        Some(CliCommand::Configure(command)) => {
            configure(command)?;
            return Ok(());
        }
        Some(CliCommand::Stage(args)) | Some(CliCommand::Init(args)) => {
            stage_repository(args)?;
            return Ok(());
        }
        None => {}
    }

    if cli.managed_child && cli.workflow.is_none() {
        let root = cli
            .managed_workspace
            .context("Managed workspace directory is required")?;
        return meerkat_fugue::setup::web::serve(
            &root,
            SocketAddr::new(cli.host, cli.port.unwrap_or(8788)),
        )
        .await;
    }

    if cli.workflow.is_none() && !Path::new("fugue.yaml").exists() && !cli.check && !cli.ota_update
    {
        if !cli.host.is_loopback() {
            anyhow::bail!("First-run setup requires a loopback listener");
        }
        return meerkat_fugue::setup::managed::supervise(
            &setup_state_dir(None)?,
            cli.port.unwrap_or(8788),
            false,
        )
        .await;
    }

    let workflow_path = cli.workflow.unwrap_or_else(|| PathBuf::from("fugue.yaml"));
    if workflow_path.is_dir() {
        if cli.check {
            anyhow::bail!("--check requires a workflow file");
        }
        if cli.ensemble_config.is_some() {
            anyhow::bail!("--ensemble-config requires a workflow file, not repository onboarding");
        }
        let report = onboard_repo(RepoOnboardInput {
            repo: workflow_path,
            backend: cli.backend,
            flow: cli.flow,
            project_name: cli.project_name,
            linear_team: cli.linear_team,
            port: cli.port,
            replace_service: cli.replace_service,
            dry_run: cli.dry_run,
        })
        .await?;
        println!("Onboarded Fugue queue {}", report.queue_slug);
        println!("  backend: {}", report.backend_name);
        println!("  service: {}.service", report.service_name);
        println!(
            "  Linear project: {} ({})",
            report.project_slug, report.project_url
        );
        println!("  status port: {}", report.port);
        println!("  tunnel: {}", report.tunnel_command);
        return Ok(());
    }
    if !workflow_path.exists() {
        anyhow::bail!("workflow file does not exist: {}", workflow_path.display());
    }

    let manager = WorkflowConfigManager::load(workflow_path.clone())
        .with_context(|| format!("failed to load {}", workflow_path.display()))?;

    if cli.ota_update {
        let config = manager
            .effective()
            .ota_update
            .as_ref()
            .context("ota_update is not configured")?;
        let report = meerkat_fugue::ota_update::run_ota_update(config).await?;
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    manager.validate_dispatch()?;

    if cli.check {
        let config = manager.effective();
        let adapter = meerkat_fugue::linear_adapter::IssueAdapter::from_config(&config.tracker)?;
        let mut runtime = config.linear.clone();
        runtime.provisioning.mode = meerkat_fugue::config::NativeLinearProvisioningMode::Validate;
        adapter
            .ensure_projection_lanes(&config.tracker, &runtime)
            .await?;
        let issues = adapter.fetch_candidate_issues().await?;
        let github = meerkat_fugue::github_tools::GitHubToolHost::new();
        let mut repositories = Vec::new();
        for repo in &config
            .kapellmeister
            .shadow_mode
            .authority
            .safe_selectors
            .repositories
        {
            match github.inspect_repo(meerkat_fugue::tools::InspectRepoInput { repo: repo.clone() })
            {
                meerkat_fugue::tools::FugueToolResult::Ok {
                    output: meerkat_fugue::tools::FugueToolOutput::GitHubInspectRepo(output),
                    ..
                } => repositories.push(output.repo),
                meerkat_fugue::tools::FugueToolResult::Err { error, .. } => {
                    anyhow::bail!("repository preflight failed: {}", error.message)
                }
                _ => anyhow::bail!("repository preflight returned an unexpected result"),
            }
        }
        #[derive(serde::Serialize)]
        struct CheckReport<'a> {
            tracker: &'a str,
            candidate_issues: usize,
            repositories: Vec<meerkat_fugue::tools::GitHubRepoSnapshot>,
        }
        println!(
            "{}",
            serde_json::to_string_pretty(&CheckReport {
                tracker: &config.tracker.kind,
                candidate_issues: issues.len(),
                repositories
            })?
        );
        return Ok(());
    }

    let (refresh_tx, refresh_rx) = mpsc::channel(8);
    let mut orchestrator = Orchestrator::new(manager, refresh_rx);
    let state = orchestrator.shared_state();
    let internal_tx = orchestrator.internal_sender();
    let meta_runtime_registry = orchestrator.meta_runtime_registry();

    let config_port = orchestrator
        .config()
        .effective()
        .server
        .as_ref()
        .and_then(|server| server.port);
    let port = cli
        .port
        .or(config_port)
        .or_else(|| cli.ensemble_config.as_ref().map(|_| 8788));
    if cli.ensemble_config.is_some() && !cli.host.is_loopback() {
        anyhow::bail!("--ensemble-config requires a loopback --host");
    }
    // Reserve the UI port before formation can spend model tokens or acquire
    // persistent team ownership, and retain this exact listener through startup.
    let listener = match port {
        Some(port) => {
            let addr = SocketAddr::new(cli.host, port);
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .with_context(|| format!("bind main Fugue HTTP server at {addr}"))?;
            info!(address = %listener.local_addr()?, "http_status reserved");
            Some(listener)
        }
        None => None,
    };
    let directory = if let Some(listener) = &listener {
        if cli.host.is_loopback() && std::env::var_os("FUGUE_HUB_URL").is_none() {
            let port = listener.local_addr()?.port();
            let root = setup_state_dir(None)?
                .join("directories")
                .join(format!("ui-{port}"));
            let directory = meerkat_fugue::setup::directory::Directory::open(root, port)?;
            directory.restore().await?;
            Some(directory)
        } else {
            None
        }
    } else {
        None
    };
    if let Some(directory) = &directory {
        meta_runtime_registry
            .use_console(directory.console())
            .map_err(anyhow::Error::msg)?;
    }
    let ensemble = if listener.is_some() && cli.host.is_loopback() {
        let effective = orchestrator.config().effective();
        let path = cli.ensemble_config.unwrap_or_else(|| {
            effective
                .meerkat
                .state_root
                .clone()
                .unwrap_or_else(|| PathBuf::from(".fugue"))
                .join("repositories.yaml")
        });
        let workspace = meerkat_fugue::ensemble::workspace::Workspace::open(
            path,
            true,
            effective.meerkat.model.clone(),
            effective.meerkat.auth_binding.clone(),
            effective
                .meerkat
                .repository_runtime
                .as_ref()
                .map(|_| meta_runtime_registry.clone()),
        )?;
        if let Some(directory) = &directory {
            workspace.use_console(directory.console())?;
        }
        workspace.start().await?;
        if effective.meerkat.repository_runtime.is_some() {
            workspace.await_ready().await?;
        }
        Some(workspace)
    } else {
        None
    };
    let reconfigure = CancellationToken::new();
    let mut ensemble_routes = ensemble
        .as_ref()
        .map(|workspace| {
            meerkat_fugue::ensemble::http::workspace_router(Arc::clone(workspace), true)
        })
        .transpose()?;
    if let Some(directory) = &directory {
        ensemble_routes = Some(ensemble_routes.unwrap_or_default().merge(
            meerkat_fugue::setup::directory::api_router(Arc::clone(directory)),
        ));
    } else if let Ok(hub) = std::env::var("FUGUE_HUB_URL") {
        ensemble_routes = Some(
            ensemble_routes
                .unwrap_or_default()
                .merge(meerkat_fugue::setup::directory::hub_router(hub)?),
        );
    }
    if cli.managed_child {
        let root = cli
            .managed_workspace
            .as_ref()
            .context("Managed workspace directory is required")?;
        let settings = meerkat_fugue::setup::settings::router(root, reconfigure.clone())?;
        ensemble_routes = Some(ensemble_routes.unwrap_or_default().merge(settings));
    }
    let http_handle = listener.map(|listener| {
        tokio::spawn(http::serve_on_listener(
            listener,
            state.clone(),
            orchestrator.config().effective().clone(),
            refresh_tx.clone(),
            internal_tx.clone(),
            meta_runtime_registry.clone(),
            ensemble_routes,
        ))
    });

    let shutdown = CancellationToken::new();
    let orchestrator_shutdown = shutdown.clone();
    let mut orchestrator_handle =
        tokio::spawn(async move { orchestrator.run_until_shutdown(orchestrator_shutdown).await });

    let result: anyhow::Result<()> = async {
        if let Some(mut http_handle) = http_handle {
            tokio::select! {
                _ = reconfigure.cancelled() => {
                    shutdown.cancel();
                    let settled = orchestrator_handle.await;
                    http_handle.abort();
                    settled.context("orchestrator task join failed")??;
                    return Err(meerkat_fugue::setup::managed::ReconfigureRequested.into());
                }
                result = &mut orchestrator_handle => {
                    http_handle.abort();
                    result.context("orchestrator task join failed")??;
                }
                result = &mut http_handle => {
                    shutdown.cancel();
                    orchestrator_handle.await.context("orchestrator task join failed")??;
                    result.context("http task join failed")??;
                }
                signal = shutdown_signal() => {
                    info!("shutdown requested");
                    shutdown.cancel();
                    let orchestrator_result = orchestrator_handle.await;
                    http_handle.abort();
                    signal.context("failed to listen for shutdown signal")?;
                    orchestrator_result.context("orchestrator task join failed")??;
                }
            }
        } else {
            tokio::select! {
                result = &mut orchestrator_handle => {
                    result.context("orchestrator task join failed")??;
                }
                signal = shutdown_signal() => {
                    info!("shutdown requested");
                    shutdown.cancel();
                    let orchestrator_result = orchestrator_handle.await;
                    signal.context("failed to listen for shutdown signal")?;
                    orchestrator_result.context("orchestrator task join failed")??;
                }
            }
        }
        Ok(())
    }
    .await;

    if let Some(directory) = directory {
        directory.shutdown().await?;
    }

    // Every server/orchestrator/signal exit settles the one shared team owner,
    // including errors. An HTTP bind failure must not leave its agents running.
    if let Some(ensemble) = ensemble
        && let Err(cleanup) = ensemble.shutdown().await
    {
        return match result {
            Ok(()) => Err(cleanup.context("shut down integrated Ensemble")),
            Err(error) => Err(error.context(format!("Ensemble cleanup failed: {cleanup:#}"))),
        };
    }
    result
}

/// Systemd sends SIGTERM; interactive runs normally receive SIGINT.
async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}

fn configure(command: ConfigureCommand) -> anyhow::Result<()> {
    match command.command {
        ConfigureSubcommand::Backend(backend) => match backend.command {
            ConfigureBackendSubcommand::GcpSystemd(args) => {
                let path = configure_gcp_systemd_backend(ConfigureGcpSystemdInput {
                    name: args.name.clone(),
                    gcp_project: args.gcp_project,
                    zone: args.zone,
                    instance: args.instance,
                    remote_user: args.remote_user,
                    fugue_root: args.fugue_root,
                    bin_dir: args.bin_dir,
                    port_start: args.port_start,
                    port_end: args.port_end,
                    default_team: args.default_team,
                    flow_teams: args.flow_teams.into_iter().collect(),
                    make_default: args.default,
                })?;
                println!("Configured backend {} in {}", args.name, path.display());
            }
        },
    }
    Ok(())
}

fn init_logging() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_target(false)
        .json()
        .init();
}

fn parse_key_value(value: &str) -> Result<(String, String), String> {
    let (key, mapped) = value
        .split_once('=')
        .ok_or_else(|| "expected KEY=VALUE".to_string())?;
    if key.trim().is_empty() || mapped.trim().is_empty() {
        return Err("expected non-empty KEY=VALUE".to_string());
    }
    Ok((key.trim().to_string(), mapped.trim().to_string()))
}

fn stage_repository(args: StageArgs) -> anyhow::Result<()> {
    let repo = canonical_dir(&args.repo, "repository")?;
    let output_dir = match &args.output_dir {
        Some(path) => canonical_or_create_dir(path, "output directory")?,
        None => repo.clone(),
    };
    let github_repo_url = match args.github_repo_url {
        Some(url) if !url.trim().is_empty() => url,
        Some(_) => anyhow::bail!("--github-repo-url cannot be empty"),
        None => infer_git_origin_url(&repo)?,
    };
    let linear_project_slug = required_value(
        args.linear_project_slug,
        "LINEAR_PROJECT_SLUG",
        "--linear-project-slug",
    )?;
    let linear_team = required_value(args.linear_team, "LINEAR_TEAM", "--linear-team")?;
    let mode = OnboardingMode::NativeDefault;

    let options = NewRepoOnboardingOptions {
        mode,
        linear_api_key: args.linear_api_key.as_str(),
        linear_project_slug: linear_project_slug.as_str(),
        linear_team: linear_team.as_str(),
        github_repo_url: github_repo_url.as_str(),
        workspace_root: args.workspace_root.as_str(),
        linear_mcp_command: "fugue-linear-mcp",
        github_mcp_command: "fugue-github-mcp",
        server_port: args.server_port,
    };
    let generated = render_new_repo_onboarding(&options);
    validate_generated_files(&generated)?;

    if args.dry_run {
        print_generated_files(&generated);
        return Ok(());
    }

    write_generated_files(&output_dir, &generated, args.force)?;
    println!("Staged Fugue config for {}", repo.display());
    println!(
        "  wrote {}",
        output_dir.join(generated.config_path).display()
    );
    if let Some(prompt_path) = generated.prompt_path {
        println!("  wrote {}", output_dir.join(prompt_path).display());
    }
    if let Some(context_path) = generated.native_launch_context_path {
        println!("  wrote {}", output_dir.join(context_path).display());
    }
    println!(
        "Run with: meerkat-fugue {}",
        output_dir.join(generated.config_path).display()
    );
    Ok(())
}

fn validate_generated_files(generated: &GeneratedOnboardingFiles) -> anyhow::Result<()> {
    let staging_dir = unique_temp_dir("meerkat-fugue-stage")?;
    let validation_result = (|| {
        fs::create_dir_all(&staging_dir)
            .with_context(|| format!("failed to create {}", staging_dir.display()))?;
        write_generated_files(&staging_dir, generated, true)?;
        generated
            .validate_in_dir(&staging_dir)
            .context("generated Fugue configuration did not validate")?;
        Ok::<(), anyhow::Error>(())
    })();
    let cleanup_result = fs::remove_dir_all(&staging_dir);
    validation_result?;
    cleanup_result.with_context(|| format!("failed to remove {}", staging_dir.display()))?;
    Ok(())
}

fn unique_temp_dir(prefix: &str) -> anyhow::Result<PathBuf> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before UNIX_EPOCH")?
        .as_nanos();
    Ok(std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id())))
}

fn canonical_dir(path: &Path, label: &str) -> anyhow::Result<PathBuf> {
    let canonical = path
        .canonicalize()
        .with_context(|| format!("{label} does not exist: {}", path.display()))?;
    if !canonical.is_dir() {
        anyhow::bail!("{label} is not a directory: {}", canonical.display());
    }
    Ok(canonical)
}

fn canonical_or_create_dir(path: &Path, label: &str) -> anyhow::Result<PathBuf> {
    fs::create_dir_all(path)
        .with_context(|| format!("failed to create {label}: {}", path.display()))?;
    canonical_dir(path, label)
}

fn infer_git_origin_url(repo: &Path) -> anyhow::Result<String> {
    let output = Command::new("git")
        .args(["config", "--get", "remote.origin.url"])
        .current_dir(repo)
        .output()
        .with_context(|| format!("failed to inspect git origin in {}", repo.display()))?;
    if !output.status.success() {
        anyhow::bail!(
            "could not infer GitHub URL from remote.origin.url in {}; pass --github-repo-url",
            repo.display()
        );
    }
    let url = String::from_utf8(output.stdout)
        .context("git remote.origin.url was not valid UTF-8")?
        .trim()
        .to_string();
    if url.is_empty() {
        anyhow::bail!(
            "remote.origin.url is empty in {}; pass --github-repo-url",
            repo.display()
        );
    }
    Ok(url)
}

fn required_value(option: Option<String>, env_var: &str, flag: &str) -> anyhow::Result<String> {
    match option {
        Some(value) if !value.trim().is_empty() => Ok(value),
        Some(_) => anyhow::bail!("{flag} cannot be empty"),
        None => std::env::var(env_var).with_context(|| format!("pass {flag} or set {env_var}")),
    }
}

fn write_generated_files(
    output_dir: &Path,
    generated: &GeneratedOnboardingFiles,
    force: bool,
) -> anyhow::Result<()> {
    write_generated_file(
        output_dir,
        generated.config_path,
        &generated.config_contents,
        force,
    )?;
    if let (Some(path), Some(contents)) = (generated.prompt_path, &generated.prompt_contents) {
        write_generated_file(output_dir, path, contents, force)?;
    }
    if let (Some(path), Some(contents)) = (
        generated.native_launch_context_path,
        &generated.native_launch_context_contents,
    ) {
        write_generated_file(output_dir, path, contents, force)?;
    }
    Ok(())
}

fn write_generated_file(
    output_dir: &Path,
    relative_path: &str,
    contents: &str,
    force: bool,
) -> anyhow::Result<()> {
    let path = output_dir.join(relative_path);
    if path.exists() && !force {
        anyhow::bail!(
            "{} already exists; pass --force to overwrite",
            path.display()
        );
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(&path, contents).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

fn print_generated_files(generated: &GeneratedOnboardingFiles) {
    println!("--- {}", generated.config_path);
    print!("{}", generated.config_contents);
    if let (Some(path), Some(contents)) = (generated.prompt_path, &generated.prompt_contents) {
        println!("\n--- {path}");
        print!("{contents}");
    }
    if let (Some(path), Some(contents)) = (
        generated.native_launch_context_path,
        &generated.native_launch_context_contents,
    ) {
        println!("\n--- {path}");
        print!("{contents}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn storage_migration_is_read_only_unless_apply_is_explicit() {
        let cli = Cli::try_parse_from([
            "meerkat-fugue",
            "migrate-storage",
            "--runtime-dir",
            "/state/team",
        ])
        .expect("migration command");
        let Some(CliCommand::MigrateStorage(args)) = cli.command else {
            panic!("expected migration command");
        };
        assert!(!args.apply);
        assert_eq!(args.runtime_dir, PathBuf::from("/state/team"));
        assert!(Cli::try_parse_from(["meerkat-fugue", "migrate-storage", "--apply"]).is_err());
    }

    #[tokio::test]
    async fn storage_migration_rejects_application_root_without_creating_stores() {
        let directory = tempfile::tempdir().expect("temporary state root");
        let error = migrate_storage(MigrateStorageArgs {
            runtime_dir: directory.path().to_owned(),
            apply: true,
            scratch_dir: None,
        })
        .await
        .expect_err("wrong directory must be refused");
        assert!(error.to_string().contains("not the application state root"));
        assert_eq!(
            fs::read_dir(directory.path())
                .expect("read untouched root")
                .count(),
            0
        );
    }

    fn top_level_help() -> String {
        Cli::command().render_long_help().to_string()
    }

    #[test]
    fn top_level_help_describes_issue_queues_and_repository_teams() {
        let help = top_level_help();

        assert!(help.contains("Run Fugue issue queues and persistent repository teams"));
        assert!(help.contains("long-lived specialist teams connected through representatives"));
        assert!(help.contains("native Meta issue mobs"));
        assert!(help.contains("durable Workpad evidence"));
        assert!(help.contains("Examples:"));
        assert!(help.contains("meerkat-fugue fugue.yaml"));
        assert!(help.contains("meerkat-fugue stage /path/to/repo"));
        assert!(help.contains("meerkat-fugue ensemble init /path/to/api /path/to/web"));
        assert!(help.contains("meerkat-fugue ensemble serve"));
    }

    #[test]
    fn top_level_help_keeps_expected_arguments_and_options() {
        let help = top_level_help();

        assert!(help.contains("[WORKFLOW]"));
        assert!(help.contains("Path to fugue.yaml"));
        assert!(help.contains("--port <PORT>"));
        assert!(help.contains("Start the optional HTTP status API/dashboard on PORT"));
        assert!(help.contains("--host <HOST>"));
        assert!(help.contains("--ensemble-config <PATH>"));
        assert!(help.contains("Bind address for the optional HTTP status API/dashboard"));
        assert!(help.contains("--ota-update"));
        assert!(help.contains("Run the configured OTA update once after loading WORKFLOW"));
        assert!(help.contains("configure"));
        assert!(help.contains("stage"));
        assert!(help.contains("init"));
    }

    #[test]
    fn top_level_parser_keeps_existing_flags_and_defaults() {
        let defaults = Cli::parse_from(["meerkat-fugue"]);
        assert!(defaults.command.is_none());
        assert_eq!(defaults.workflow, None);
        assert_eq!(defaults.port, None);
        assert_eq!(defaults.host, IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(defaults.ensemble_config, None);
        assert!(!defaults.ota_update);
        assert_eq!(defaults.backend, None);
        assert_eq!(defaults.flow, None);
        assert_eq!(defaults.project_name, None);
        assert_eq!(defaults.linear_team, None);
        assert!(!defaults.replace_service);
        assert!(!defaults.dry_run);

        let parsed = Cli::parse_from([
            "meerkat-fugue",
            "--port",
            "0",
            "--host",
            "0.0.0.0",
            "--ota-update",
            "--backend",
            "default",
            "--flow",
            "planner-coder-reviewer",
            "--project-name",
            "Fugue",
            "--linear-team",
            "LUC",
            "--replace-service",
            "--dry-run",
            "fugue.yaml",
        ]);

        assert!(parsed.command.is_none());
        assert_eq!(parsed.workflow, Some(PathBuf::from("fugue.yaml")));
        assert_eq!(parsed.port, Some(0));
        assert_eq!(parsed.host, IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        assert!(parsed.ota_update);
        assert_eq!(parsed.backend.as_deref(), Some("default"));
        assert_eq!(parsed.flow.as_deref(), Some("planner-coder-reviewer"));
        assert_eq!(parsed.project_name.as_deref(), Some("Fugue"));
        assert_eq!(parsed.linear_team.as_deref(), Some("LUC"));
        assert!(parsed.replace_service);
        assert!(parsed.dry_run);

        let help_path = Cli::parse_from(["meerkat-fugue", "help"]);
        assert!(help_path.command.is_none());
        assert_eq!(help_path.workflow, Some(PathBuf::from("help")));
    }

    #[test]
    fn integrated_ensemble_flag_preserves_normal_workflow_and_rejects_ota_only_mode() {
        let cli = Cli::try_parse_from([
            "meerkat-fugue",
            "fugue.yaml",
            "--ensemble-config",
            "teams/ensemble.yaml",
            "--port",
            "8798",
        ])
        .expect("integrated queue and repository teams");
        assert!(cli.command.is_none());
        assert_eq!(cli.workflow, Some(PathBuf::from("fugue.yaml")));
        assert_eq!(
            cli.ensemble_config,
            Some(PathBuf::from("teams/ensemble.yaml"))
        );
        assert_eq!(cli.port, Some(8798));
        assert!(
            Cli::try_parse_from([
                "meerkat-fugue",
                "fugue.yaml",
                "--ensemble-config",
                "ensemble.yaml",
                "--ota-update",
            ])
            .is_err()
        );
    }
}
