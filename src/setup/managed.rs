//! Process lifecycle for a configured local workspace. Each child receives its
//! own credential environment; the supervisor retains the original environment.
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use super::{
    connections::{ConnectionStore, Connections},
    workflow::WorkflowSetup,
};

pub const RESTART_EXIT_CODE: i32 = 42;
pub const RECONFIGURE_EXIT_CODE: i32 = 43;

#[derive(Debug, thiserror::Error)]
#[error("workspace restart requested")]
pub struct RestartRequested;

#[derive(Debug, thiserror::Error)]
#[error("workspace reconfiguration requested")]
pub struct ReconfigureRequested;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActiveWorkspace {
    pub generation: String,
    pub setup: WorkflowSetup,
}

impl ActiveWorkspace {
    pub fn load(root: &Path) -> Result<Option<Self>> {
        let bytes = match std::fs::read(root.join("workspace.json")) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let manifest: Self = serde_json::from_slice(&bytes).context("read workspace manifest")?;
        if !manifest.generation.starts_with("generation-")
            || !manifest
                .generation
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            bail!("Invalid workspace generation");
        }
        Ok(Some(manifest))
    }

    pub fn workflow(&self, root: &Path) -> PathBuf {
        root.join(&self.generation).join("fugue.yaml")
    }
    pub fn ensemble(&self, root: &Path) -> PathBuf {
        root.join(&self.generation).join("ensemble.yaml")
    }

    pub fn activate(&self, root: &Path) -> Result<()> {
        super::save_json(&root.join("workspace.json"), self)
    }
}

/// Prepare immutable config files and a checkout. Does not start agents or
/// change the active manifest until the operator presses Start.
pub async fn prepare(
    root: &Path,
    setup: &WorkflowSetup,
    connections: &Connections,
    port: u16,
) -> Result<ActiveWorkspace> {
    let binary = std::env::current_exe()?;
    let bin_dir = binary
        .parent()
        .context("Fugue executable has no directory")?;
    let mut generated = setup.render(connections, root, port, bin_dir)?;
    let workflow_path = root.join("fugue.yaml");
    let definition = generated
        .workflow
        .clone()
        .into_workflow_definition(&workflow_path)?;
    let config = crate::config::ServiceConfig::from_definition(workflow_path, &definition)?;
    crate::tracker_binding::validate(
        &crate::meta_runtime::meta_runtime_state_path(&config.meerkat),
        &config.tracker_context_id(),
    )?;
    if matches!(
        connections.tracker,
        Some(super::connections::TrackerConnection::Jira { .. })
    ) {
        let states = connections
            .jira_client(setup.tracker_project.as_deref().unwrap_or_default())?
            .list_project_states()
            .await?
            .states;
        let lanes = &setup.lanes;
        for (role, name) in [
            ("draft", &lanes.parked),
            ("ready", &lanes.intake),
            ("in progress", &lanes.active),
            ("awaiting review", &lanes.review_action),
            ("merge", &lanes.deploy_action),
            ("blocked", &lanes.blocked),
            ("completed", &lanes.success),
            ("canceled", &lanes.canceled),
        ] {
            if !states
                .iter()
                .any(|state| state.name.eq_ignore_ascii_case(name))
            {
                bail!(
                    "Jira has no {name:?} status for {role}. Load the project's statuses and update the workflow mapping"
                );
            }
        }
    }
    let github = connections
        .github
        .as_ref()
        .context("Connect GitHub first")?;
    github.check().await?;
    let mut access = tokio::process::Command::new("gh");
    for name in super::connections::SERVICE_CREDENTIAL_ENV {
        access.env_remove(name);
    }
    access
        .args([
            "repo",
            "view",
            &format!("{}/{}/{}", github.host, setup.owner, setup.repository),
            "--json",
            "viewerPermission,defaultBranchRef",
        ])
        .envs(connections.environment())
        .env("GH_PROMPT_DISABLED", "1")
        .stdin(Stdio::null())
        .kill_on_drop(true);
    let access = tokio::time::timeout(Duration::from_secs(30), access.output())
        .await
        .context("Repository access check timed out")??;
    if !access.status.success() {
        bail!("Cannot read this repository with the saved GitHub account");
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct RepositoryAccess {
        viewer_permission: String,
        default_branch_ref: Option<Branch>,
    }
    #[derive(Deserialize)]
    struct Branch {
        name: String,
    }
    let access: RepositoryAccess =
        serde_json::from_slice(&access.stdout).context("Invalid repository access response")?;
    if !matches!(
        access.viewer_permission.as_str(),
        "ADMIN" | "MAINTAIN" | "WRITE"
    ) {
        bail!("This GitHub account needs write access to push branches and open pull requests");
    }
    let branch = access
        .default_branch_ref
        .filter(|branch| !branch.name.is_empty())
        .context("The repository needs an initial commit and default branch before setup")?;
    if let Some(deploy) = &mut generated.workflow.deploy {
        deploy.base_branch = branch.name;
    }

    for helper in [
        "fugue-github-mcp",
        "fugue-host-mcp",
        "fugue-jira-mcp",
        "fugue-linear-mcp",
    ] {
        if !bin_dir.join(helper).is_file() {
            bail!("Missing {helper}. Install all Fugue binaries together before starting");
        }
    }
    if generated.checkout.exists() {
        let output = tokio::process::Command::new("git")
            .arg("-C")
            .arg(&generated.checkout)
            .args(["rev-parse", "--show-toplevel"])
            .output()
            .await?;
        if !output.status.success() {
            bail!("The checkout path is not a Git repository");
        }
        let actual = PathBuf::from(String::from_utf8(output.stdout)?.trim());
        if actual.canonicalize()? != generated.checkout.canonicalize()? {
            bail!("Choose the checkout root, not a subdirectory");
        }
        let remote = tokio::process::Command::new("git")
            .arg("-C")
            .arg(&generated.checkout)
            .args(["remote", "get-url", "origin"])
            .output()
            .await?;
        let remote = String::from_utf8(remote.stdout)?;
        let remote = remote.trim().trim_end_matches(".git");
        let https = format!(
            "https://{}/{}/{}",
            github.host, setup.owner, setup.repository
        );
        let ssh = format!("git@{}:{}/{}", github.host, setup.owner, setup.repository);
        if remote != https && remote != ssh {
            bail!("The checkout's origin does not match the selected GitHub repository");
        }
    } else {
        if setup.checkout.is_some() {
            bail!("The selected local checkout does not exist");
        }
        let parent = generated
            .checkout
            .parent()
            .context("Checkout has no parent directory")?;
        std::fs::create_dir_all(parent)?;
        // An owned temporary directory makes failure and cancellation retryable
        // without deleting an existing user checkout.
        let pending = tempfile::Builder::new()
            .prefix(".fugue-clone-")
            .tempdir_in(parent)?;
        let pending_checkout = pending.path().join("repository");
        let mut clone = tokio::process::Command::new("git");
        for name in super::connections::SERVICE_CREDENTIAL_ENV {
            clone.env_remove(name);
        }
        clone
            .arg("-c")
            .arg("credential.helper=")
            .arg("-c")
            .arg(format!(
                "credential.https://{}.helper=!gh auth git-credential",
                github.host
            ))
            .arg("clone")
            .arg(format!(
                "https://{}/{}/{}.git",
                github.host, setup.owner, setup.repository
            ))
            .arg(&pending_checkout)
            .envs(connections.environment())
            .env("GIT_TERMINAL_PROMPT", "0")
            .stdin(Stdio::null())
            .kill_on_drop(true);
        let output = tokio::time::timeout(Duration::from_secs(180), clone.output())
            .await
            .context("Repository clone timed out; check the connection and retry")??;
        if !output.status.success() {
            bail!("Could not clone this repository. Check GitHub access and Git installation");
        }
        std::fs::rename(&pending_checkout, &generated.checkout)?;
    }
    let generation = format!(
        "generation-{}-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
        std::process::id()
    );
    let directory = root.join(&generation);
    std::fs::create_dir(&directory)?;
    if let Some(jira) = &mut generated.workflow.linear.jira {
        write_jira_fields(&directory, setup, jira)?;
    }
    std::fs::write(
        directory.join("fugue.yaml"),
        serde_yaml::to_string(&generated.workflow)?,
    )?;
    std::fs::write(
        directory.join("ensemble.yaml"),
        serde_yaml::to_string(&generated.ensemble)?,
    )?;
    let manifest = ActiveWorkspace {
        generation,
        setup: setup.clone(),
    };
    let mut check = tokio::process::Command::new(&binary);
    for name in super::connections::SERVICE_CREDENTIAL_ENV {
        check.env_remove(name);
    }
    check
        .arg(manifest.workflow(root))
        .arg("--check")
        .envs(connections.environment())
        .stdin(Stdio::null())
        .kill_on_drop(true);
    let output = tokio::time::timeout(Duration::from_secs(60), check.output())
        .await
        .context("Tracker and repository checks timed out")??;
    if !output.status.success() {
        // Runtime error details may contain upstream response bodies. Return an
        // actionable boundary error without reflecting credentials or responses.
        bail!(
            "Workflow access check failed. Check tracker credentials, project, selected statuses, and repository access"
        );
    }
    Ok(manifest)
}

/// Keep project-specific issue defaults in this immutable private generation.
fn write_jira_fields(
    directory: &Path,
    setup: &WorkflowSetup,
    jira: &mut crate::jira::JiraConfig,
) -> Result<()> {
    #[derive(Serialize)]
    struct CreateFields<'a> {
        labels: &'a [String],
    }
    jira.create_fields_file = if setup.jira_issue_labels.is_empty() {
        None
    } else {
        Some(write_private_json(
            directory.join("jira-create-fields.json"),
            &CreateFields {
                labels: &setup.jira_issue_labels,
            },
        )?)
    };
    jira.transition_fields_file = if setup.jira_transition_fields.is_empty() {
        None
    } else {
        Some(write_private_json(
            directory.join("jira-transition-fields.json"),
            &setup.jira_transition_fields,
        )?)
    };
    Ok(())
}

fn write_private_json(path: PathBuf, value: &impl Serialize) -> Result<String> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&path)?;
    file.write_all(&serde_json::to_vec_pretty(value)?)?;
    file.sync_all()?;
    Ok(path.display().to_string())
}

pub async fn supervise(root: &Path, port: u16, mut open_setup: bool) -> Result<()> {
    if port == 0 {
        bail!("Choose a fixed port so setup and the running workspace share one address");
    }
    let store = ConnectionStore::open(root)?;
    drop(store);
    let root = root.canonicalize()?;
    let executable = std::env::current_exe()?;
    loop {
        let store = ConnectionStore::open(&root)?;
        let connections = store.load()?;
        drop(store);
        let manifest = if open_setup {
            None
        } else {
            ActiveWorkspace::load(&root)?
        };
        let was_runtime = manifest.is_some();
        let mut child = tokio::process::Command::new(&executable);
        for name in super::connections::SERVICE_CREDENTIAL_ENV {
            child.env_remove(name);
        }
        child
            .arg("--managed-child")
            .arg("--managed-workspace")
            .arg(&root)
            .arg("--port")
            .arg(port.to_string())
            .envs(connections.environment())
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GH_PROMPT_DISABLED", "1")
            .kill_on_drop(true)
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        if let Some(manifest) = manifest {
            child
                .arg(manifest.workflow(&root))
                .arg("--ensemble-config")
                .arg(manifest.ensemble(&root));
        }
        let mut child = child.spawn().context("start managed Fugue workspace")?;
        let status = tokio::select! {
            status=child.wait()=>status?,
            signal=shutdown_signal()=>{
                signal?;
                if let Some(id)=child.id() {
                    #[cfg(unix)] {
                        let sent=tokio::process::Command::new("kill").args(["-TERM",&id.to_string()]).status().await?;
                        if !sent.success() { bail!("Could not request graceful workspace shutdown"); }
                    }
                    #[cfg(not(unix))] child.start_kill()?;
                }
                child.wait().await?;
                return Ok(());
            }
        };
        if status.code() == Some(RESTART_EXIT_CODE) {
            open_setup = false;
            continue;
        }
        if status.code() == Some(RECONFIGURE_EXIT_CODE) {
            open_setup = true;
            continue;
        }
        if !status.success() {
            if was_runtime {
                std::fs::write(
                    root.join("startup-error.txt"),
                    "Fugue could not start the saved workspace. Check its connections and configuration, then prepare and start again.",
                )?;
                open_setup = true;
                continue;
            }
            bail!("Fugue workspace stopped ({status}); reopen setup to check its connections");
        }
        return Ok(());
    }
}

pub async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! { result=tokio::signal::ctrl_c()=>result,_=terminate.recv()=>Ok(()) }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn tracker_binding_setup_refuses_before_contacting_replacement_tracker() {
        use super::super::connections::{
            GitHubConnection, GitHubCredential, ModelConnection, ModelCredential, ModelProvider,
            TrackerConnection,
        };
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let mut setup = jira_setup();
        setup.tracker_project = Some("APP".into());
        let mut connections = Connections {
            model: Some(ModelConnection {
                provider: ModelProvider::Openai,
                model: crate::config::default_agent_model().into(),
                credential: ModelCredential::Environment,
            }),
            github: Some(GitHubConnection {
                host: "github.com".into(),
                credential: GitHubCredential::GhCli,
            }),
            tracker: Some(serde_json::from_str(
                r#"{"kind":"jira","site":"https://first.example.com","email":"fixture@example.com","api_token":"fixture-token"}"#,
            ).unwrap()),
        };
        let rendered = setup
            .render(&connections, root, 8788, Path::new("/opt/fugue/bin"))
            .unwrap();
        let path = root.join("fugue.yaml");
        let definition = rendered.workflow.into_workflow_definition(&path).unwrap();
        let config = crate::config::ServiceConfig::from_definition(path, &definition).unwrap();
        let state = crate::meta_runtime::meta_runtime_state_path(&config.meerkat);
        crate::tracker_binding::bind(&state, &config.tracker_context_id()).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        if let Some(TrackerConnection::Jira { site, .. }) = &mut connections.tracker {
            *site = format!("https://{}", listener.local_addr().unwrap());
        }
        let error = tokio::time::timeout(
            Duration::from_secs(2),
            prepare(root, &setup, &connections, 8788),
        )
        .await
        .expect("setup must fail before waiting for provider access")
        .err()
        .expect("changed tracker must be refused");
        assert!(matches!(
            error.downcast_ref(),
            Some(crate::tracker_binding::BindingError::DifferentTracker)
        ));
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        assert!(!root.join("workspace.json").exists());
        assert!(!root.join("generations").exists());
        assert!(!rendered.checkout.exists());
    }

    #[test]
    fn interrupted_activation_keeps_committed_generation_and_allows_retry() {
        let root = tempfile::tempdir().unwrap();
        let mut workspace = ActiveWorkspace {
            generation: "generation-old".into(),
            setup: jira_setup(),
        };
        workspace.activate(root.path()).unwrap();
        // A process died before replacing the manifest with a complete write.
        std::fs::write(root.path().join("workspace.pending"), b"{\"generation\":").unwrap();
        assert_eq!(
            ActiveWorkspace::load(root.path())
                .unwrap()
                .unwrap()
                .generation,
            "generation-old"
        );
        workspace.generation = "generation-new".into();
        workspace.activate(root.path()).unwrap();
        let restored = ActiveWorkspace::load(root.path()).unwrap().unwrap();
        assert_eq!(restored.generation, "generation-new");
        assert_eq!(
            restored.setup.jira_issue_labels,
            workspace.setup.jira_issue_labels
        );
    }

    fn jira_setup() -> WorkflowSetup {
        serde_json::from_str(
            r#"{
            "owner":"example", "repository":"app", "lanes":{},
            "jira_jql":"labels = fugue-app",
            "jira_issue_labels":["fugue-app","regression"],
            "jira_transition_fields":{"Closed":{"resolution":{"name":"Done"}}}
        }"#,
        )
        .unwrap()
    }

    #[test]
    fn jira_defaults_are_private_and_clearing_them_keeps_old_generation_intact() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let mut setup = jira_setup();
        setup.validate().unwrap();
        let mut jira = crate::jira::JiraConfig::default();
        write_jira_fields(first.path(), &setup, &mut jira).unwrap();
        let create_path = jira.create_fields_file.clone().unwrap();
        let transition_path = jira.transition_fields_file.clone().unwrap();
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct CreateFields {
            labels: Vec<String>,
        }
        let created: CreateFields =
            serde_json::from_slice(&std::fs::read(&create_path).unwrap()).unwrap();
        assert_eq!(created.labels, setup.jira_issue_labels);
        assert_eq!(
            std::fs::read_to_string(&transition_path).unwrap(),
            serde_json::to_string_pretty(&setup.jira_transition_fields).unwrap()
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for path in [&create_path, &transition_path] {
                assert_eq!(
                    std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
                    0o600
                );
            }
        }
        setup.jira_issue_labels.clear();
        setup.jira_transition_fields.clear();
        write_jira_fields(second.path(), &setup, &mut jira).unwrap();
        assert!(jira.create_fields_file.is_none());
        assert!(jira.transition_fields_file.is_none());
        assert_eq!(std::fs::read_dir(second.path()).unwrap().count(), 0);
        assert!(Path::new(&create_path).is_file());
        assert!(Path::new(&transition_path).is_file());
    }

    #[test]
    fn jira_defaults_never_overwrite_existing_generation_files() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jira-create-fields.json");
        std::fs::write(&path, "preserved").unwrap();
        assert!(
            write_jira_fields(
                directory.path(),
                &jira_setup(),
                &mut crate::jira::JiraConfig::default()
            )
            .is_err()
        );
        assert_eq!(std::fs::read_to_string(path).unwrap(), "preserved");
    }

    #[test]
    fn jira_labels_validate_before_generation_and_old_drafts_default_to_empty() {
        let mut setup = jira_setup();
        for invalid in [
            "",
            "two words",
            "comma,label",
            "line\nbreak",
            "control\0",
            "tab\tlabel",
        ] {
            setup.jira_issue_labels = vec![invalid.into()];
            assert!(setup.validate().is_err(), "accepted {invalid:?}");
        }
        let old: WorkflowSetup =
            serde_json::from_str(r#"{"owner":"example","repository":"app","lanes":{}}"#).unwrap();
        assert!(old.jira_issue_labels.is_empty());
        old.validate().unwrap();
    }
}
