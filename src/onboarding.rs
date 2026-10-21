use std::fmt;

use crate::config::{ServiceConfig, parse_native_workflow};
use crate::error::{FugueError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnboardingMode {
    NativeDefault,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NewRepoOnboardingOptions<'a> {
    pub mode: OnboardingMode,
    pub linear_api_key: &'a str,
    pub linear_project_slug: &'a str,
    pub linear_team: &'a str,
    pub github_repo_url: &'a str,
    pub workspace_root: &'a str,
    pub linear_mcp_command: &'a str,
    pub github_mcp_command: &'a str,
    pub server_port: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedOnboardingFiles {
    pub config_path: &'static str,
    pub prompt_path: Option<&'static str>,
    pub native_launch_context_path: Option<&'static str>,
    pub config_contents: String,
    pub prompt_contents: Option<String>,
    pub native_launch_context_contents: Option<String>,
}

impl GeneratedOnboardingFiles {
    pub fn validate_in_dir(&self, base_dir: &std::path::Path) -> Result<ServiceConfig> {
        let config_path = base_dir.join(self.config_path);
        match self.config_path {
            "fugue.yaml" => {
                let definition = parse_native_workflow(&config_path, &self.config_contents)?;
                ServiceConfig::from_definition(config_path, &definition)
            }
            other => Err(FugueError::InvalidConfig(format!(
                "unsupported generated onboarding config path {other}"
            ))),
        }
    }
}

pub fn render_new_repo_onboarding(
    options: &NewRepoOnboardingOptions<'_>,
) -> GeneratedOnboardingFiles {
    match options.mode {
        OnboardingMode::NativeDefault => GeneratedOnboardingFiles {
            config_path: "fugue.yaml",
            prompt_path: Some("prompts/fugue-worker.md"),
            native_launch_context_path: Some("prompts/native-launch-context.md"),
            config_contents: render_native_fugue_yaml(options),
            prompt_contents: Some(render_worker_prompt()),
            native_launch_context_contents: Some(render_native_launch_context()),
        },
    }
}

fn render_native_fugue_yaml(options: &NewRepoOnboardingOptions<'_>) -> String {
    let mut rendered = String::new();
    rendered.push_str("version: 1\n");
    rendered.push_str("# Native Fugue retake onboarding is the default for new repositories.\n");
    rendered.push_str("# Production queues should use the native Meta execution path only.\n\n");
    rendered.push_str("linear:\n");
    rendered.push_str(&format!(
        "  api_key: {}\n",
        yaml_scalar(options.linear_api_key)
    ));
    rendered.push_str(&format!(
        "  project_slug: {}\n",
        yaml_scalar(options.linear_project_slug)
    ));
    rendered.push_str(&format!("  team: {}\n", yaml_scalar(options.linear_team)));
    rendered.push_str("  provisioning:\n");
    rendered.push_str("    mode: validate\n");
    rendered.push_str("  lanes:\n");
    rendered.push_str("    parked: Backlog\n");
    rendered.push_str("    ready: Queue\n");
    rendered.push_str("    active: Active\n");
    rendered.push_str("    do_review: Review\n");
    rendered.push_str("    deploy: Deploy\n");
    rendered.push_str("    blocked: Blocked\n");
    rendered.push_str("    success: Done\n");
    rendered.push_str("    canceled: Canceled\n");
    rendered.push_str("    duplicate: Duplicate\n");
    rendered.push_str("  labels: []\n");
    rendered.push('\n');
    rendered.push_str("tracker_lanes:\n");
    rendered.push_str("  parked: Backlog\n");
    rendered.push_str("  intake: Queue\n");
    rendered.push_str("  active: Active\n");
    rendered.push_str("  review_action: Review\n");
    rendered.push_str("  deploy_action: Deploy\n");
    rendered.push_str("  blocked: Blocked\n");
    rendered.push_str("  success: Done\n");
    rendered.push_str("  canceled: Canceled\n\n");
    rendered.push_str("linear_projections:\n");
    rendered.push_str("  planner_coder_reviewer:\n");
    rendered.push_str("    node_lanes:\n");
    rendered.push_str("      planner: Active\n");
    rendered.push_str("      coder: Active\n");
    rendered.push_str("      reviewer: Active\n");
    rendered.push_str("    node_phase_labels:\n");
    rendered.push_str("      planner: fugue:phase:planning\n");
    rendered.push_str("      coder: fugue:phase:implementing\n");
    rendered.push_str("      reviewer: fugue:phase:reviewing\n");
    rendered.push_str("    aggregate:\n");
    rendered.push_str("      precedence: [Active]\n");
    rendered.push_str("      fallback: Active\n");
    rendered.push_str("    terminal_lanes:\n");
    rendered.push_str("      succeeded: Done\n");
    rendered.push_str("      failed: Active\n");
    rendered.push_str("      cancelled: Canceled\n\n");
    rendered.push_str("selection:\n");
    rendered.push_str("  default_template: implementation_mob\n\n");
    rendered.push_str("mcp_servers:\n");
    rendered.push_str("  linear:\n");
    rendered.push_str(&format!(
        "    command: [{}]\n",
        yaml_scalar(options.linear_mcp_command)
    ));
    rendered.push_str("    env:\n");
    rendered.push_str(&format!(
        "      LINEAR_API_KEY: {}\n",
        yaml_scalar(options.linear_api_key)
    ));
    rendered.push_str("  github:\n");
    rendered.push_str(&format!(
        "    command: [{}]\n",
        yaml_scalar(options.github_mcp_command)
    ));
    rendered.push_str("    env:\n");
    rendered.push_str("      GITHUB_TOKEN: $GITHUB_TOKEN\n");
    rendered.push_str("  playwright:\n");
    rendered.push_str("    command: [npx, -y, \"@playwright/mcp@latest\", --headless]\n");
    rendered.push('\n');
    rendered.push_str("agents:\n");
    rendered.push_str("  planner:\n");
    rendered.push_str("    model: gpt-5.5\n");
    rendered.push_str("    tools: [mob, workgraph, mcp:linear, mcp:github]\n");
    rendered.push_str(
        "    system_prompt: Own the detailed Workpad checkbox plan and create blocking WorkGraph items from comments/reviews before implementation.\n",
    );
    rendered.push_str("  coder:\n");
    rendered.push_str("    model: claude-sonnet-4-6\n");
    rendered.push_str("    tools: [mob, shell, git, workgraph, mcp:linear, mcp:github]\n");
    rendered
        .push_str("    system_prompt: Implement the current Workpad plan and open WorkGraph obligations; update checkoffs and evidence.\n");
    rendered.push_str("  reviewer:\n");
    rendered.push_str("    model: gpt-5.5\n");
    rendered.push_str("    tools: [mob, shell, git, workgraph, mcp:linear, mcp:github]\n");
    rendered.push_str(
        "    system_prompt: Gate local implementation, inspect diff/WorkGraph yourself, close completed WorkGraph obligations, and route red findings to Active with phase evidence.\n\n",
    );
    rendered.push_str("mob_templates:\n");
    rendered.push_str("  implementation_mob:\n");
    rendered.push_str("    linear_projection: planner_coder_reviewer\n");
    rendered.push_str("    max_review_cycles: 2\n");
    rendered.push_str("    members:\n");
    rendered.push_str("      - planner\n");
    rendered.push_str("      - coder\n");
    rendered.push_str("      - reviewer\n\n");
    rendered.push_str("worker:\n");
    rendered.push_str("  prompt_file: prompts/fugue-worker.md\n");
    rendered.push_str("  native_launch_context_file: prompts/native-launch-context.md\n");
    append_runtime_sections(&mut rendered, options);
    rendered
}

fn append_runtime_sections(rendered: &mut String, options: &NewRepoOnboardingOptions<'_>) {
    rendered.push_str("\npolling:\n  interval_ms: 30000\n\nworkspace:\n");
    rendered.push_str(&format!(
        "  root: {}\n",
        yaml_scalar(options.workspace_root)
    ));
    rendered.push_str("\nhooks:\n  timeout_ms: 60000\n  after_create: |\n");
    rendered.push_str(&format!(
        "    git clone {} .\n",
        shell_single_quote(options.github_repo_url)
    ));
    rendered.push_str("  before_run: |\n");
    rendered.push_str("    git fetch --all --prune\n\n");
    rendered.push_str("agent:\n");
    rendered.push_str("  max_concurrent_agents: 3\n");
    rendered.push_str("  max_turns: 20\n");
    rendered.push_str("  max_retry_backoff_ms: 300000\n");
    rendered.push_str("  max_concurrent_agents_by_state:\n");
    rendered.push_str("    todo: 1\n\n");
    rendered.push_str("meerkat:\n");
    rendered.push_str("  enable_shell: true\n");
    rendered.push_str("  enable_builtins: true\n");
    rendered.push_str("  enable_mob: true\n");
    rendered.push_str("  first_turn_activation_prompt: Begin.\n");
    rendered.push_str("  turn_timeout_ms: 3600000\n");
    rendered.push_str("  read_timeout_ms: 5000\n");
    rendered.push_str("  stall_timeout_ms: 300000\n\n");
    rendered.push_str("kapellmeister:\n");
    rendered.push_str("  shadow_mode:\n");
    rendered.push_str("    enabled: false\n");
    if let Some(port) = options.server_port {
        rendered.push_str(&format!("\nserver:\n  port: {port}\n"));
    }
}

fn render_worker_prompt() -> String {
    "You are an issue mob member working unattended on Linear issue `{{ issue.identifier }}` under Kapellmeister direction.\n\nFollow repository instructions, keep edits scoped to the issue, maintain exactly one persistent Linear comment headed `## Fugue Workpad`, and run relevant validation. If blocked or red, record detailed findings in the Workpad for focused planning. Do not mutate Linear lifecycle lanes directly; guarded lane projection belongs to Kapellmeister typed move decisions.\n".to_string()
}

fn render_native_launch_context() -> String {
    "Native execution mob launch context:\n- Linear issue: {{ issue.identifier }} ({{ issue.title }})\n- Worktree: {{ worktree }}\n- Workspace key: {{ workspace_key }}\n- Template: {{ template_id }}\n- Backend: {{ backend }}\n- Flow: {{ flow }}\n- Members: {{ members }}\n\nAll mob members must work only inside the explicit worktree above and preserve the Fugue Linear workpad/PR handoff contract.\n".to_string()
}

fn yaml_scalar(value: &str) -> String {
    match serde_yaml::to_string(value) {
        Ok(serialized) => serialized
            .trim_end()
            .trim_start_matches("---\n")
            .to_string(),
        Err(_) => format!("{value:?}"),
    }
}

fn shell_single_quote(value: &str) -> ShellSingleQuoted<'_> {
    ShellSingleQuoted(value)
}

struct ShellSingleQuoted<'a>(&'a str);

impl fmt::Display for ShellSingleQuoted<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("'")?;
        for (index, segment) in self.0.split(char::from(39)).enumerate() {
            if index > 0 {
                formatter.write_str("'\\''")?;
            }
            formatter.write_str(segment)?;
        }
        formatter.write_str("'")
    }
}

#[cfg(test)]
mod tests {
    use crate::config::MobExecutionBackend;

    use super::*;

    fn options(mode: OnboardingMode) -> NewRepoOnboardingOptions<'static> {
        NewRepoOnboardingOptions {
            mode,
            linear_api_key: "$LINEAR_API_KEY",
            linear_project_slug: "project-slug",
            linear_team: "LUC",
            github_repo_url: "https://github.com/example/repo.git",
            workspace_root: "./fugue_workspaces",
            linear_mcp_command: "fugue-linear-mcp",
            github_mcp_command: "fugue-github-mcp",
            server_port: Some(0),
        }
    }

    fn write_generated_files(root: &std::path::Path, generated: &GeneratedOnboardingFiles) {
        if let Some(prompt_path) = generated.prompt_path {
            let prompt_path = root.join(prompt_path);
            std::fs::create_dir_all(prompt_path.parent().unwrap()).unwrap();
            std::fs::write(prompt_path, generated.prompt_contents.as_ref().unwrap()).unwrap();
        }
        if let Some(context_path) = generated.native_launch_context_path {
            let context_path = root.join(context_path);
            std::fs::create_dir_all(context_path.parent().unwrap()).unwrap();
            std::fs::write(
                context_path,
                generated.native_launch_context_contents.as_ref().unwrap(),
            )
            .unwrap();
        }
        std::fs::write(root.join(generated.config_path), &generated.config_contents).unwrap();
    }

    #[test]
    fn new_repo_onboarding_generates_native_fugue_yaml_by_default() {
        let generated = render_new_repo_onboarding(&options(OnboardingMode::NativeDefault));

        assert_eq!(generated.config_path, "fugue.yaml");
        assert_eq!(generated.prompt_path, Some("prompts/fugue-worker.md"));
        assert_eq!(
            generated.native_launch_context_path,
            Some("prompts/native-launch-context.md")
        );
        assert!(generated.config_contents.contains("version: 1"));
        assert!(
            generated
                .config_contents
                .contains("worker:\n  prompt_file: prompts/fugue-worker.md\n  native_launch_context_file: prompts/native-launch-context.md")
        );
        assert!(
            generated
                .native_launch_context_contents
                .as_deref()
                .unwrap_or_default()
                .contains("Native execution mob launch context")
        );
        assert!(
            generated
                .config_contents
                .contains("linear_projection: planner_coder_reviewer")
        );
        assert!(!generated.config_contents.contains("flow:"));
        assert!(!generated.config_contents.contains("  states:\n"));
        assert!(generated.config_contents.contains("playwright:"));
        assert!(
            generated
                .config_contents
                .contains("command: [npx, -y, \"@playwright/mcp@latest\", --headless]")
        );
        assert!(
            generated
                .config_contents
                .contains("git clone 'https://github.com/example/repo.git' .")
        );
        assert_eq!(generated.config_path, "fugue.yaml");

        let root = tempfile::tempdir().unwrap();
        write_generated_files(root.path(), &generated);
        let config = generated.validate_in_dir(root.path()).unwrap();
        assert_eq!(config.mob.backend, MobExecutionBackend::NativeExecution);
        assert!(config.mob.native.enabled);
        assert!(!config.mob.native.shadow);
        assert_eq!(config.workspace.root, root.path().join("fugue_workspaces"));
        assert_eq!(
            config.server.as_ref().and_then(|server| server.port),
            Some(0)
        );
        assert_eq!(
            config
                .mob
                .templates
                .get("implementation_mob")
                .unwrap()
                .backend,
            MobExecutionBackend::NativeExecution
        );
    }
}
