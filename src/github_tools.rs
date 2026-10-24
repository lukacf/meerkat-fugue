mod process;

use chrono::{DateTime, Utc};
use serde::Deserialize;

#[cfg(test)]
pub(crate) use tests::scripted_host;

use crate::tools::{
    BranchName, CheckCiInput, CheckCiOutput, CiStatus, CreatePrInput, CreatePrOutput,
    ExistingPrMatch, FugueToolError, FugueToolErrorCode, FugueToolOperation, FugueToolOutput,
    FugueToolResult, GitHubCheckSnapshot, GitHubPullRequestNumber, GitHubPullRequestRef,
    GitHubPullRequestSnapshot, GitHubRepoRef, GitHubRepoSnapshot, GitSha, InspectPrInput,
    InspectPrOutput, InspectRepoInput, InspectRepoOutput, LinkIssueInput, MergeMethod,
    MergePrInput, MergePrOutput, PullRequestState, RetryAdvice, ToolExecutionMetadata,
};

#[derive(Clone)]
pub struct GitHubToolHost {
    gh: GhCli,
}

impl GitHubToolHost {
    pub fn new() -> Self {
        Self { gh: GhCli::new() }
    }

    pub fn with_connections(
        connections: Option<std::sync::Arc<crate::setup::connections::Connections>>,
    ) -> Self {
        Self {
            gh: GhCli::with_runner(Box::new(ProcessCommandRunner { connections })),
        }
    }

    #[cfg(test)]
    fn with_runner(runner: Box<dyn CommandRunner>) -> Self {
        Self {
            gh: GhCli::with_runner(runner),
        }
    }

    pub fn inspect_repo(&self, input: InspectRepoInput) -> FugueToolResult {
        let operation = FugueToolOperation::GitHubInspectRepo;
        run_tool(operation, None, || {
            self.gh
                .inspect_repo(&input.repo)
                .map(|repo| FugueToolOutput::GitHubInspectRepo(InspectRepoOutput { repo }))
        })
    }

    pub fn inspect_pr(&self, input: InspectPrInput) -> FugueToolResult {
        let operation = FugueToolOperation::GitHubInspectPr;
        run_tool(operation, None, || {
            self.gh
                .resolve_pr(&input.repo, &input.pull_request)
                .map(|pull_request| {
                    FugueToolOutput::GitHubInspectPr(InspectPrOutput { pull_request })
                })
        })
    }

    pub fn create_pr(&self, input: CreatePrInput) -> FugueToolResult {
        let operation = FugueToolOperation::GitHubCreatePr;
        let idempotency_key = input.idempotency_key.clone();
        run_tool(operation, Some(idempotency_key), || {
            if let Some(existing) = self.find_existing_pr(&input)? {
                return Ok(FugueToolOutput::GitHubCreatePr(CreatePrOutput {
                    pull_request: existing,
                    created: false,
                }));
            }

            self.gh.create_pr(&input).map(|pull_request| {
                FugueToolOutput::GitHubCreatePr(CreatePrOutput {
                    pull_request,
                    created: true,
                })
            })
        })
    }

    pub fn check_ci(&self, input: CheckCiInput) -> FugueToolResult {
        let operation = FugueToolOperation::GitHubCheckCi;
        run_tool(operation, None, || {
            self.gh.check_ci(&input).map(|checks| {
                let status = aggregate_ci_status(&checks);
                FugueToolOutput::GitHubCheckCi(CheckCiOutput { status, checks })
            })
        })
    }

    /// Read all CI pages, requiring the same PR head and commit on each page. A
    /// later branch lookup must not substitute checks for a newer candidate.
    pub fn check_pr_ci(&self, pull_request: &GitHubPullRequestSnapshot) -> FugueToolResult {
        run_tool(FugueToolOperation::GitHubCheckCi, None, || {
            let checks = self.gh.check_pr_ci(pull_request)?;
            Ok(FugueToolOutput::GitHubCheckCi(CheckCiOutput {
                status: aggregate_ci_status(&checks),
                checks,
            }))
        })
    }

    pub fn merge_pr(&self, input: MergePrInput) -> FugueToolResult {
        crate::merge_gate::missing_context(input)
    }

    pub(crate) fn merge_authorized(
        &self,
        authorization: crate::merge_gate::AuthorizedMerge,
    ) -> FugueToolResult {
        let mut input = authorization.into_input();
        let operation = FugueToolOperation::GitHubMergePr;
        let idempotency_key = input.idempotency_key.clone();
        run_tool(operation, Some(idempotency_key), || {
            let expected = input
                .expected_head_sha
                .as_ref()
                .filter(|sha| !sha.0.trim().is_empty())
                .ok_or_else(|| {
                    GitHubToolFailure::new(
                        FugueToolErrorCode::PreconditionFailed,
                        "merge requires an explicit expected_head_sha",
                        RetryAdvice::RefreshStateThenRetry,
                    )
                })?;
            let current = self.gh.resolve_pr(&input.repo, &input.pull_request)?;
            if current.head_sha != *expected {
                return Err(GitHubToolFailure::new(
                    FugueToolErrorCode::PreconditionFailed,
                    "pull request head sha did not match expected_head_sha",
                    RetryAdvice::RefreshStateThenRetry,
                ));
            }
            // Bind branch selectors to the PR number observed before mutation.
            input.pull_request = GitHubPullRequestRef::Number {
                number: current.number,
            };
            let newly_merged = current.state != PullRequestState::Merged;
            let pull_request = if newly_merged {
                if current.state != PullRequestState::Open || current.draft {
                    return Err(GitHubToolFailure::new(
                        FugueToolErrorCode::PreconditionFailed,
                        "pull request must be open and ready for review",
                        RetryAdvice::RefreshStateThenRetry,
                    ));
                }
                self.gh.merge_pr(&input)?;
                self.gh.resolve_pr(&input.repo, &input.pull_request)?
            } else {
                current
            };
            if pull_request.state != PullRequestState::Merged || pull_request.head_sha != *expected
            {
                return Err(GitHubToolFailure::new(
                    FugueToolErrorCode::PreconditionFailed,
                    "GitHub has not confirmed a merge of the expected PR head; refresh before retrying",
                    RetryAdvice::RefreshStateThenRetry,
                ));
            }
            let merge_commit_sha = self.gh.merge_commit_sha(&input.repo, pull_request.number)?;
            if merge_commit_sha
                .as_ref()
                .is_none_or(|sha| sha.0.trim().is_empty())
            {
                return Err(GitHubToolFailure::new(
                    FugueToolErrorCode::PreconditionFailed,
                    "GitHub has not returned the merge commit; refresh before retrying",
                    RetryAdvice::RefreshStateThenRetry,
                ));
            }
            Ok(FugueToolOutput::GitHubMergePr(MergePrOutput {
                pull_request,
                merged: newly_merged,
                merge_commit_sha,
            }))
        })
    }

    pub fn link_issue(&self, input: LinkIssueInput) -> FugueToolResult {
        let operation = FugueToolOperation::GitHubLinkIssue;
        let idempotency_key = input.idempotency_key.clone();
        run_tool(operation, Some(idempotency_key), || {
            let pull_request = self.gh.resolve_pr(&input.repo, &input.pull_request)?;
            Err(GitHubToolFailure::new(
                FugueToolErrorCode::UnsupportedOperation,
                format!(
                    "github.link_issue is not implemented because no current repository convention for linking {:?} to PR #{} is configured",
                    input.issue, pull_request.number.0
                ),
                RetryAdvice::DoNotRetry,
            ))
        })
    }

    fn find_existing_pr(
        &self,
        input: &CreatePrInput,
    ) -> GitHubToolResult<Option<GitHubPullRequestSnapshot>> {
        match input.existing_pr_match {
            ExistingPrMatch::None => Ok(None),
            ExistingPrMatch::HeadBranch => self
                .gh
                .find_open_pr_by_head(&input.repo, &input.head_branch),
            ExistingPrMatch::TitleAndHeadBranch => Ok(self
                .gh
                .find_open_pr_by_head(&input.repo, &input.head_branch)?
                .filter(|pull_request| pull_request.title == input.title)),
        }
    }
}

impl Default for GitHubToolHost {
    fn default() -> Self {
        Self::new()
    }
}

fn run_tool(
    operation: FugueToolOperation,
    idempotency_key: Option<crate::tools::IdempotencyKey>,
    run: impl FnOnce() -> GitHubToolResult<FugueToolOutput>,
) -> FugueToolResult {
    let started_at = Utc::now();
    let metadata = ToolExecutionMetadata {
        started_at: Some(started_at),
        completed_at: Some(Utc::now()),
        ..ToolExecutionMetadata::default()
    };
    match run() {
        Ok(output) => FugueToolResult::Ok { output, metadata },
        Err(error) => FugueToolResult::Err {
            error: error.into_error(operation, idempotency_key),
            metadata,
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GitHubToolFailure {
    code: FugueToolErrorCode,
    message: String,
    retry: RetryAdvice,
}

type GitHubToolResult<T> = Result<T, GitHubToolFailure>;

impl GitHubToolFailure {
    fn new(code: FugueToolErrorCode, message: impl Into<String>, retry: RetryAdvice) -> Self {
        Self {
            code,
            message: message.into(),
            retry,
        }
    }

    fn into_error(
        self,
        operation: FugueToolOperation,
        idempotency_key: Option<crate::tools::IdempotencyKey>,
    ) -> FugueToolError {
        FugueToolError {
            operation,
            code: self.code,
            message: self.message,
            retry: self.retry,
            idempotency_key,
        }
    }
}

fn aggregate_ci_status(checks: &[GitHubCheckSnapshot]) -> CiStatus {
    if checks.is_empty() {
        CiStatus::NoChecks
    } else if checks.iter().any(|check| check.status == CiStatus::Pending) {
        CiStatus::Pending
    } else if checks
        .iter()
        .any(|check| matches!(check.status, CiStatus::Failure | CiStatus::Error))
    {
        CiStatus::Failure
    } else if checks.iter().all(|check| check.status == CiStatus::Skipped) {
        CiStatus::Skipped
    } else if checks
        .iter()
        .any(|check| check.status == CiStatus::Cancelled)
    {
        CiStatus::Cancelled
    } else {
        CiStatus::Success
    }
}

#[derive(Clone)]
struct GhCli {
    runner: Box<dyn CommandRunner>,
}

impl GhCli {
    fn new() -> Self {
        Self::with_runner(Box::new(ProcessCommandRunner::default()))
    }

    fn with_runner(runner: Box<dyn CommandRunner>) -> Self {
        Self { runner }
    }

    fn inspect_repo(&self, repo: &GitHubRepoRef) -> GitHubToolResult<GitHubRepoSnapshot> {
        let output = self.runner.run(&[
            "repo",
            "view",
            &repo_slug(repo),
            "--json",
            "nameWithOwner,defaultBranchRef,url,sshUrl",
        ])?;
        let payload: RepoView = decode(output.stdout.as_str())?;
        let default_branch = payload
            .default_branch_ref
            .map(|branch| branch.name)
            .ok_or_else(|| {
                GitHubToolFailure::new(
                    FugueToolErrorCode::ExecutionFailed,
                    "GitHub repo payload did not include defaultBranchRef",
                    RetryAdvice::DoNotRetry,
                )
            })?;
        Ok(GitHubRepoSnapshot {
            repo: repo.clone(),
            default_branch: BranchName(default_branch),
            clone_url: payload.ssh_url,
            html_url: payload.url,
        })
    }

    fn resolve_pr(
        &self,
        repo: &GitHubRepoRef,
        pull_request: &GitHubPullRequestRef,
    ) -> GitHubToolResult<GitHubPullRequestSnapshot> {
        match pull_request {
            GitHubPullRequestRef::Number { number } => self.inspect_pr_number(repo, *number),
            GitHubPullRequestRef::Branch { head_branch } => self
                .find_open_pr_by_head(repo, head_branch)?
                .ok_or_else(|| {
                    not_found(format!(
                        "open pull request for head branch {}",
                        head_branch.0
                    ))
                }),
        }
    }

    fn inspect_pr_number(
        &self,
        repo: &GitHubRepoRef,
        number: GitHubPullRequestNumber,
    ) -> GitHubToolResult<GitHubPullRequestSnapshot> {
        let number_arg = number.0.to_string();
        let output = self.runner.run(&[
            "pr",
            "view",
            &number_arg,
            "--repo",
            &repo_slug(repo),
            "--json",
            PR_FIELDS,
        ])?;
        let payload: PrView = decode(output.stdout.as_str())?;
        Ok(pr_snapshot(repo.clone(), payload))
    }

    fn find_open_pr_by_head(
        &self,
        repo: &GitHubRepoRef,
        head_branch: &BranchName,
    ) -> GitHubToolResult<Option<GitHubPullRequestSnapshot>> {
        let head = head_branch.0.as_str();
        let output = self.runner.run(&[
            "pr",
            "list",
            "--repo",
            &repo_slug(repo),
            "--head",
            head,
            "--state",
            "all",
            "--limit",
            "10",
            "--json",
            PR_FIELDS,
        ])?;
        let payload: Vec<PrView> = decode(output.stdout.as_str())?;
        let mut candidates = payload
            .into_iter()
            .map(|pull_request| pr_snapshot(repo.clone(), pull_request))
            .collect::<Vec<_>>();
        candidates.sort_by_key(|pull_request| match pull_request.state {
            PullRequestState::Open => 0,
            PullRequestState::Merged => 1,
            PullRequestState::Closed => 2,
        });
        Ok(candidates
            .into_iter()
            .find(|pull_request| pull_request.state != PullRequestState::Closed))
    }

    fn create_pr(&self, input: &CreatePrInput) -> GitHubToolResult<GitHubPullRequestSnapshot> {
        let repo_slug = repo_slug(&input.repo);
        let mut args = vec![
            "pr",
            "create",
            "--repo",
            repo_slug.as_str(),
            "--title",
            input.title.as_str(),
            "--body",
            input.body_markdown.as_str(),
            "--head",
            input.head_branch.0.as_str(),
            "--base",
            input.base_branch.0.as_str(),
        ];
        if input.draft {
            args.push("--draft");
        }
        let output = self.runner.run(&args)?;
        let url = output.stdout.trim();
        let number = parse_pr_number_from_url(url)?;
        self.inspect_pr_number(&input.repo, number)
    }

    fn check_ci(&self, input: &CheckCiInput) -> GitHubToolResult<Vec<GitHubCheckSnapshot>> {
        let output = match self.runner.run(&[
            "pr",
            "checks",
            &input.git_ref.0,
            "--repo",
            &repo_slug(&input.repo),
            "--json",
            "name,state,link,completedAt",
        ]) {
            Ok(output) => output,
            Err(err)
                if err.message.contains("unknown flag: --json")
                    || err.message.contains("no checks reported") =>
            {
                return self.check_ci_via_pr_view(input);
            }
            Err(err) => return Err(err),
        };
        let payload: Vec<CheckView> = decode(output.stdout.as_str())?;
        Ok(payload.into_iter().map(check_snapshot).collect())
    }

    fn check_ci_via_pr_view(
        &self,
        input: &CheckCiInput,
    ) -> GitHubToolResult<Vec<GitHubCheckSnapshot>> {
        let output = self.runner.run(&[
            "pr",
            "view",
            &input.git_ref.0,
            "--repo",
            &repo_slug(&input.repo),
            "--json",
            "statusCheckRollup",
        ])?;
        let payload: PrChecksView = decode(output.stdout.as_str())?;
        Ok(payload
            .status_check_rollup
            .into_iter()
            .map(fallback_check_snapshot)
            .collect())
    }

    fn check_pr_ci(
        &self,
        pull_request: &GitHubPullRequestSnapshot,
    ) -> GitHubToolResult<Vec<GitHubCheckSnapshot>> {
        if pull_request.number.0 == 0 || pull_request.head_sha.0.trim().is_empty() {
            return Err(GitHubToolFailure::new(
                FugueToolErrorCode::InvalidInput,
                "PR checks require a pull request number and expected head SHA",
                RetryAdvice::DoNotRetry,
            ));
        }
        let mut cursor = None;
        let mut seen = std::collections::BTreeSet::new();
        let mut checks = Vec::new();
        // The CLI's pr-view export drops pagination metadata. Read the native
        // connection so a failing check after the first page cannot be omitted.
        for _ in 0..100 {
            let mut args = vec![
                "api".to_owned(),
                "graphql".to_owned(),
                "--hostname".to_owned(),
                pull_request.repo.hostname().to_owned(),
                "-f".to_owned(),
                format!("query={PR_CI_QUERY}"),
                "-f".to_owned(),
                format!("owner={}", pull_request.repo.owner.0),
                "-f".to_owned(),
                format!("name={}", pull_request.repo.name.0),
                "-F".to_owned(),
                format!("number={}", pull_request.number.0),
            ];
            if let Some(cursor) = &cursor {
                args.extend(["-f".to_owned(), format!("cursor={cursor}")]);
            }
            let output = self
                .runner
                .run(&args.iter().map(String::as_str).collect::<Vec<_>>())?;
            let response: PrCiResponse = decode(&output.stdout)?;
            if !response.errors.is_empty() {
                return Err(ci_read_refusal("GitHub returned incomplete CI data"));
            }
            let pr = response.data.repository.pull_request;
            if pr.head_ref_oid != pull_request.head_sha.0 || pr.commits.nodes.len() != 1 {
                return Err(ci_read_refusal(
                    "pull request head changed or its latest commit is unavailable; refresh the candidate before retrying",
                ));
            }
            let Some(node) = pr.commits.nodes.into_iter().next() else {
                return Err(ci_read_refusal("the latest PR commit is unavailable"));
            };
            if node.commit.oid != pull_request.head_sha.0 {
                return Err(ci_read_refusal(
                    "CI belongs to a different PR commit; refresh before retrying",
                ));
            }
            let Some(rollup) = node.commit.status_check_rollup else {
                if cursor.is_some() {
                    return Err(ci_read_refusal(
                        "CI disappeared during pagination; refresh before retrying",
                    ));
                }
                return Ok(checks);
            };
            checks.extend(
                rollup
                    .contexts
                    .nodes
                    .into_iter()
                    .map(fallback_check_snapshot),
            );
            if !rollup.contexts.page_info.has_next_page {
                return Ok(checks);
            }
            let next = rollup
                .contexts
                .page_info
                .end_cursor
                .filter(|cursor| !cursor.trim().is_empty())
                .ok_or_else(|| ci_read_refusal("GitHub omitted the next CI page cursor"))?;
            if !seen.insert(next.clone()) {
                return Err(ci_read_refusal("GitHub repeated a CI page cursor"));
            }
            cursor = Some(next);
        }
        Err(ci_read_refusal(
            "CI pagination exceeded its bounded read limit",
        ))
    }

    fn merge_pr(&self, input: &MergePrInput) -> GitHubToolResult<()> {
        let number = match &input.pull_request {
            GitHubPullRequestRef::Number { number } => *number,
            GitHubPullRequestRef::Branch { .. } => {
                self.resolve_pr(&input.repo, &input.pull_request)?.number
            }
        };
        let method = match input.method {
            MergeMethod::Merge => "--merge",
            MergeMethod::Squash => "--squash",
            MergeMethod::Rebase => "--rebase",
        };
        let mut args = vec![
            "pr".to_string(),
            "merge".to_string(),
            number.0.to_string(),
            "--repo".to_string(),
            repo_slug(&input.repo),
            method.to_string(),
        ];
        if let Some(expected_head_sha) = &input.expected_head_sha {
            args.push("--match-head-commit".to_string());
            args.push(expected_head_sha.0.clone());
        }
        let arg_refs = args.iter().map(String::as_str).collect::<Vec<_>>();
        self.runner.run(&arg_refs)?;
        Ok(())
    }

    fn merge_commit_sha(
        &self,
        repo: &GitHubRepoRef,
        number: GitHubPullRequestNumber,
    ) -> GitHubToolResult<Option<GitSha>> {
        let number_arg = number.0.to_string();
        let output = self.runner.run(&[
            "pr",
            "view",
            &number_arg,
            "--repo",
            &repo_slug(repo),
            "--json",
            "mergeCommit",
        ])?;
        let payload: PrMergeCommitView = decode(output.stdout.as_str())?;
        Ok(payload.merge_commit.map(|commit| GitSha(commit.oid)))
    }
}

trait CommandRunner: Send + Sync + CommandRunnerClone {
    fn run(&self, args: &[&str]) -> GitHubToolResult<CommandOutput>;
}

trait CommandRunnerClone {
    fn clone_box(&self) -> Box<dyn CommandRunner>;
}

impl<T> CommandRunnerClone for T
where
    T: 'static + CommandRunner + Clone,
{
    fn clone_box(&self) -> Box<dyn CommandRunner> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn CommandRunner> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutput {
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, Clone, Default)]
struct ProcessCommandRunner {
    connections: Option<std::sync::Arc<crate::setup::connections::Connections>>,
}

impl CommandRunner for ProcessCommandRunner {
    fn run(&self, args: &[&str]) -> GitHubToolResult<CommandOutput> {
        let mut command = std::process::Command::new("gh");
        if let Some(connections) = &self.connections {
            command.envs(connections.github_environment());
        }
        command.env("GH_HOST", "github.com").args(args);
        execute_gh_command(command, args, std::time::Duration::from_secs(60))
    }
}

fn execute_gh_command(
    command: std::process::Command,
    args: &[&str],
    timeout: std::time::Duration,
) -> GitHubToolResult<CommandOutput> {
    let output = process::run(command, timeout).map_err(|error| {
        let retry = if error.may_have_executed() {
            RetryAdvice::RefreshStateThenRetry
        } else {
            RetryAdvice::SafeToRetry
        };
        GitHubToolFailure::new(
            FugueToolErrorCode::BackendUnavailable,
            format!("gh command failed: {error}"),
            retry,
        )
    })?;
    command_output(
        args,
        output.status.code(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

fn command_output(
    args: &[&str],
    status: Option<i32>,
    stdout: String,
    stderr: String,
) -> GitHubToolResult<CommandOutput> {
    // `gh pr checks` reports failed and pending checks using nonzero exits while
    // still returning the requested JSON. Preserve that evidence for aggregation.
    let check_evidence = args.starts_with(&["pr", "checks"])
        && args.contains(&"--json")
        && matches!(status, Some(1 | 8))
        && serde_json::from_str::<Vec<CheckView>>(&stdout).is_ok();
    if status == Some(0) || check_evidence {
        Ok(CommandOutput { stdout, stderr })
    } else {
        Err(classify_gh_failure(status, stderr))
    }
}

fn classify_gh_failure(status: Option<i32>, stderr: String) -> GitHubToolFailure {
    let lower = stderr.to_ascii_lowercase();
    let (code, retry) = if lower.contains("authentication") || lower.contains("not logged in") {
        (
            FugueToolErrorCode::AuthenticationFailed,
            RetryAdvice::DoNotRetry,
        )
    } else if lower.contains("could not resolve") || lower.contains("not found") {
        (FugueToolErrorCode::NotFound, RetryAdvice::DoNotRetry)
    } else if lower.contains("rate limit") {
        (
            FugueToolErrorCode::RateLimited,
            RetryAdvice::RetryAfter { seconds: 60 },
        )
    } else if lower.contains("permission") || lower.contains("forbidden") {
        (
            FugueToolErrorCode::PermissionDenied,
            RetryAdvice::DoNotRetry,
        )
    } else {
        (
            FugueToolErrorCode::ExecutionFailed,
            RetryAdvice::SafeToRetry,
        )
    };
    GitHubToolFailure::new(code, format!("gh exited with {status:?}: {stderr}"), retry)
}

fn decode<T>(json: &str) -> GitHubToolResult<T>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_str(json).map_err(|err| {
        GitHubToolFailure::new(
            FugueToolErrorCode::ExecutionFailed,
            format!("failed to parse gh JSON output: {err}"),
            RetryAdvice::DoNotRetry,
        )
    })
}

fn repo_slug(repo: &GitHubRepoRef) -> String {
    repo.to_string()
}

fn not_found(item: String) -> GitHubToolFailure {
    GitHubToolFailure::new(
        FugueToolErrorCode::NotFound,
        format!("GitHub {item} not found"),
        RetryAdvice::DoNotRetry,
    )
}

const PR_FIELDS: &str = "number,title,url,state,isDraft,headRefName,baseRefName,headRefOid";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RepoView {
    default_branch_ref: Option<NamedRef>,
    url: String,
    ssh_url: String,
}

#[derive(Debug, Deserialize)]
struct NamedRef {
    name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PrView {
    number: u64,
    title: String,
    url: String,
    state: String,
    #[serde(default)]
    is_draft: bool,
    head_ref_name: String,
    base_ref_name: String,
    head_ref_oid: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PrMergeCommitView {
    merge_commit: Option<PrCommitView>,
}

#[derive(Debug, Deserialize)]
struct PrCommitView {
    oid: String,
}

fn pr_snapshot(repo: GitHubRepoRef, payload: PrView) -> GitHubPullRequestSnapshot {
    GitHubPullRequestSnapshot {
        repo,
        number: GitHubPullRequestNumber(payload.number),
        title: payload.title,
        url: payload.url,
        state: pr_state(&payload.state),
        draft: payload.is_draft,
        head_branch: BranchName(payload.head_ref_name),
        base_branch: BranchName(payload.base_ref_name),
        head_sha: GitSha(payload.head_ref_oid),
    }
}

fn pr_state(state: &str) -> PullRequestState {
    match state {
        "MERGED" | "merged" => PullRequestState::Merged,
        "CLOSED" | "closed" => PullRequestState::Closed,
        _ => PullRequestState::Open,
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CheckView {
    name: String,
    state: String,
    link: Option<String>,
    completed_at: Option<DateTime<Utc>>,
}

fn check_snapshot(payload: CheckView) -> GitHubCheckSnapshot {
    GitHubCheckSnapshot {
        name: payload.name,
        status: check_status(&payload.state),
        details_url: payload.link,
        completed_at: payload.completed_at,
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PrChecksView {
    // Explicit null is an empty rollup. An absent field or invalid element is
    // not evidence of NoChecks and must not activate the no-CI exception.
    #[serde(deserialize_with = "deserialize_check_rollup")]
    status_check_rollup: Vec<StatusCheckRollupView>,
}

// Every page repeats both the PR head and latest commit identity. These are
// compared with the reviewed candidate before any check can be accepted.
const PR_CI_QUERY: &str = r#"query($owner:String!, $name:String!, $number:Int!, $cursor:String) {
  repository(owner:$owner, name:$name) {
    pullRequest(number:$number) {
      headRefOid
      commits(last:1) { nodes { commit {
        oid
        statusCheckRollup { contexts(first:100, after:$cursor) {
          nodes {
            __typename
            ... on CheckRun { name status conclusion detailsUrl completedAt }
            ... on StatusContext { context state targetUrl }
          }
          pageInfo { hasNextPage endCursor }
        } }
      } } }
    }
  }
}"#;

#[derive(Deserialize)]
struct PrCiResponse {
    data: PrCiData,
    #[serde(default)]
    errors: Vec<PrCiError>,
}

#[derive(Deserialize)]
struct PrCiError {
    // Decode the boundary but never echo arbitrary provider diagnostics.
    #[serde(rename = "message")]
    _message: String,
}

#[derive(Deserialize)]
struct PrCiData {
    repository: PrCiRepository,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PrCiRepository {
    pull_request: PrCiPullRequest,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PrCiPullRequest {
    head_ref_oid: String,
    commits: PrCiCommits,
}

#[derive(Deserialize)]
struct PrCiCommits {
    nodes: Vec<PrCiCommitNode>,
}

#[derive(Deserialize)]
struct PrCiCommitNode {
    commit: PrCiCommit,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PrCiCommit {
    oid: String,
    #[serde(deserialize_with = "required_nullable")]
    status_check_rollup: Option<PrCiRollup>,
}

#[derive(Deserialize)]
struct PrCiRollup {
    contexts: PrCiContexts,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PrCiContexts {
    nodes: Vec<StatusCheckRollupView>,
    page_info: PrCiPageInfo,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PrCiPageInfo {
    has_next_page: bool,
    end_cursor: Option<String>,
}

fn required_nullable<'de, T: Deserialize<'de>, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<T>, D::Error> {
    Option::<T>::deserialize(deserializer)
}

fn ci_read_refusal(message: &str) -> GitHubToolFailure {
    GitHubToolFailure::new(
        FugueToolErrorCode::PreconditionFailed,
        message,
        RetryAdvice::RefreshStateThenRetry,
    )
}

fn deserialize_check_rollup<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<StatusCheckRollupView>, D::Error> {
    Option::<Vec<StatusCheckRollupView>>::deserialize(deserializer).map(Option::unwrap_or_default)
}

#[derive(Debug, Deserialize)]
#[serde(tag = "__typename")]
enum StatusCheckRollupView {
    #[serde(rename_all = "camelCase")]
    CheckRun {
        name: String,
        status: String,
        conclusion: Option<String>,
        details_url: Option<String>,
        completed_at: Option<DateTime<Utc>>,
    },
    #[serde(rename_all = "camelCase")]
    StatusContext {
        context: String,
        state: String,
        target_url: Option<String>,
    },
}

fn fallback_check_snapshot(payload: StatusCheckRollupView) -> GitHubCheckSnapshot {
    match payload {
        StatusCheckRollupView::CheckRun {
            name,
            status,
            conclusion,
            details_url,
            completed_at,
        } => {
            let state = if status == "COMPLETED" {
                conclusion.as_deref().unwrap_or("PENDING")
            } else {
                "PENDING"
            };
            GitHubCheckSnapshot {
                name,
                status: check_status(state),
                details_url,
                completed_at,
            }
        }
        StatusCheckRollupView::StatusContext {
            context,
            state,
            target_url,
        } => GitHubCheckSnapshot {
            name: context,
            status: check_status(&state),
            details_url: target_url,
            completed_at: None,
        },
    }
}

fn check_status(state: &str) -> CiStatus {
    match state.to_ascii_uppercase().as_str() {
        "SUCCESS" | "PASSED" | "NEUTRAL" => CiStatus::Success,
        "FAILURE" | "FAILED" | "TIMED_OUT" | "STARTUP_FAILURE" | "STALE" => CiStatus::Failure,
        "ERROR" | "ACTION_REQUIRED" => CiStatus::Error,
        "SKIPPED" => CiStatus::Skipped,
        "CANCELLED" | "CANCELED" => CiStatus::Cancelled,
        _ => CiStatus::Pending,
    }
}

fn parse_pr_number_from_url(url: &str) -> GitHubToolResult<GitHubPullRequestNumber> {
    let number = url
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .ok_or_else(|| {
            GitHubToolFailure::new(
                FugueToolErrorCode::ExecutionFailed,
                format!("gh pr create did not return a PR URL: {url}"),
                RetryAdvice::DoNotRetry,
            )
        })?
        .parse::<u64>()
        .map_err(|err| {
            GitHubToolFailure::new(
                FugueToolErrorCode::ExecutionFailed,
                format!("could not parse PR number from gh pr create output {url:?}: {err}"),
                RetryAdvice::DoNotRetry,
            )
        })?;
    Ok(GitHubPullRequestNumber(number))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::{GitHubOwner, GitHubRepoName, IdempotencyKey};
    use std::sync::{Arc, Mutex};

    #[cfg(unix)]
    #[test]
    fn process_command_timeout_requires_refresh_after_uncertain_effect() {
        let mut command = std::process::Command::new("sh");
        command.args(["-c", "sleep 1; printf '{}'"]);
        let started = std::time::Instant::now();
        let result = execute_gh_command(
            command,
            &["pr", "merge"],
            std::time::Duration::from_millis(150),
        );
        let error = result.expect_err("a stalled merge must not hold current review forever");
        assert_eq!(error.retry, RetryAdvice::RefreshStateThenRetry);
        assert!(error.message.contains("timed out"));
        assert!(started.elapsed() < std::time::Duration::from_millis(850));
    }

    #[derive(Clone, Default)]
    struct FakeRunner {
        calls: Arc<Mutex<Vec<Vec<String>>>>,
        outputs: Arc<Mutex<Vec<GitHubToolResult<CommandOutput>>>>,
    }

    impl FakeRunner {
        fn with_outputs(outputs: Vec<CommandOutput>) -> Self {
            Self::with_results(outputs.into_iter().map(Ok).collect())
        }

        fn with_results(outputs: Vec<GitHubToolResult<CommandOutput>>) -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
                outputs: Arc::new(Mutex::new(outputs)),
            }
        }

        fn calls(&self) -> Vec<Vec<String>> {
            self.calls
                .lock()
                .map(|calls| calls.clone())
                .unwrap_or_default()
        }
    }

    impl CommandRunner for FakeRunner {
        fn run(&self, args: &[&str]) -> GitHubToolResult<CommandOutput> {
            self.calls
                .lock()
                .map_err(|err| {
                    GitHubToolFailure::new(
                        FugueToolErrorCode::ExecutionFailed,
                        err.to_string(),
                        RetryAdvice::DoNotRetry,
                    )
                })?
                .push(args.iter().map(|arg| (*arg).to_string()).collect());

            self.outputs
                .lock()
                .map_err(|err| {
                    GitHubToolFailure::new(
                        FugueToolErrorCode::ExecutionFailed,
                        err.to_string(),
                        RetryAdvice::DoNotRetry,
                    )
                })?
                .remove(0)
        }
    }

    pub(crate) fn scripted_host(
        responses: Vec<String>,
    ) -> (GitHubToolHost, impl Fn() -> Vec<Vec<String>>) {
        let runner = FakeRunner::with_outputs(
            responses
                .into_iter()
                .map(|stdout| CommandOutput {
                    stdout,
                    stderr: String::new(),
                })
                .collect(),
        );
        (
            GitHubToolHost::with_runner(Box::new(runner.clone())),
            move || runner.calls(),
        )
    }

    fn repo() -> GitHubRepoRef {
        GitHubRepoRef {
            host: None,
            owner: GitHubOwner("lukacf".to_string()),
            name: GitHubRepoName("meerkat-fugue".to_string()),
        }
    }

    fn output(stdout: &str) -> CommandOutput {
        CommandOutput {
            stdout: stdout.to_string(),
            stderr: String::new(),
        }
    }

    #[test]
    fn inspect_repo_constructs_gh_repo_view_command() {
        let runner = FakeRunner::with_outputs(vec![output(
            r#"{"defaultBranchRef":{"name":"main"},"url":"https://github.com/lukacf/meerkat-fugue","sshUrl":"git@github.com:lukacf/meerkat-fugue.git"}"#,
        )]);
        let host = GitHubToolHost::with_runner(Box::new(runner.clone()));

        let result = host.inspect_repo(InspectRepoInput { repo: repo() });

        assert!(matches!(result, FugueToolResult::Ok { .. }));
        assert_eq!(
            runner.calls(),
            vec![vec![
                "repo".to_string(),
                "view".to_string(),
                "lukacf/meerkat-fugue".to_string(),
                "--json".to_string(),
                "nameWithOwner,defaultBranchRef,url,sshUrl".to_string(),
            ]]
        );
    }

    #[test]
    fn enterprise_repo_and_pr_commands_use_explicit_host() {
        let runner = FakeRunner::with_outputs(vec![
            output(
                r#"{"defaultBranchRef":{"name":"main"},"url":"https://git.example.com/acme/app","sshUrl":"git@git.example.com:acme/app.git"}"#,
            ),
            output(
                r#"{"number":7,"title":"Fixture","url":"https://git.example.com/acme/app/pull/7","state":"OPEN","isDraft":false,"headRefName":"test","baseRefName":"main","headRefOid":"abc123"}"#,
            ),
            output(
                r#"[{"name":"test","state":"SUCCESS","link":"https://ci.example.com/7","completedAt":null}]"#,
            ),
        ]);
        let host = GitHubToolHost::with_runner(Box::new(runner.clone()));
        let repo = GitHubRepoRef {
            host: Some("git.example.com".to_string().try_into().expect("host")),
            owner: crate::tools::GitHubOwner("acme".to_string()),
            name: crate::tools::GitHubRepoName("app".to_string()),
        };
        assert!(matches!(
            host.inspect_repo(InspectRepoInput { repo: repo.clone() }),
            FugueToolResult::Ok { .. }
        ));
        assert!(matches!(
            host.inspect_pr(InspectPrInput {
                repo: repo.clone(),
                pull_request: GitHubPullRequestRef::Number {
                    number: GitHubPullRequestNumber(7)
                }
            }),
            FugueToolResult::Ok { .. }
        ));
        assert!(matches!(
            host.check_ci(CheckCiInput {
                repo,
                git_ref: crate::tools::GitRef("test".to_string())
            }),
            FugueToolResult::Ok { .. }
        ));
        for call in runner.calls() {
            assert!(call.iter().any(|value| value == "git.example.com/acme/app"));
        }
    }

    #[test]
    fn create_pr_checks_existing_head_before_mutating() {
        let existing_pr = r#"[{"number":7,"title":"Stage 2D","url":"https://github.com/lukacf/meerkat-fugue/pull/7","state":"OPEN","isDraft":false,"headRefName":"codex/LUC-139-github-mob-tools","baseRefName":"main","headRefOid":"abc123"}]"#;
        let runner = FakeRunner::with_outputs(vec![output(existing_pr)]);
        let host = GitHubToolHost::with_runner(Box::new(runner.clone()));

        let result = host.create_pr(CreatePrInput {
            repo: repo(),
            title: "Stage 2D".to_string(),
            body_markdown: "Body".to_string(),
            head_branch: BranchName("codex/LUC-139-github-mob-tools".to_string()),
            base_branch: BranchName("main".to_string()),
            draft: false,
            existing_pr_match: ExistingPrMatch::HeadBranch,
            idempotency_key: IdempotencyKey("create-pr:LUC-139".to_string()),
        });

        match result {
            FugueToolResult::Ok {
                output: FugueToolOutput::GitHubCreatePr(output),
                ..
            } => {
                assert_eq!(output.pull_request.number, GitHubPullRequestNumber(7));
                assert!(!output.created);
            }
            other => panic!("unexpected result: {other:?}"),
        }
        assert_eq!(runner.calls().len(), 1);
        assert_eq!(
            runner.calls()[0][0..7],
            [
                "pr",
                "list",
                "--repo",
                "lukacf/meerkat-fugue",
                "--head",
                "codex/LUC-139-github-mob-tools",
                "--state"
            ]
        );
        assert!(runner.calls()[0].contains(&"all".to_string()));
    }

    #[test]
    fn create_pr_reuses_merged_same_head_pr_instead_of_opening_duplicate() {
        let existing_pr = r#"[{"number":7,"title":"Stage 2D","url":"https://github.com/lukacf/meerkat-fugue/pull/7","state":"MERGED","isDraft":false,"headRefName":"codex/LUC-139-github-mob-tools","baseRefName":"main","headRefOid":"abc123"}]"#;
        let runner = FakeRunner::with_outputs(vec![output(existing_pr)]);
        let host = GitHubToolHost::with_runner(Box::new(runner.clone()));

        let result = host.create_pr(CreatePrInput {
            repo: repo(),
            title: "Stage 2D".to_string(),
            body_markdown: "Body".to_string(),
            head_branch: BranchName("codex/LUC-139-github-mob-tools".to_string()),
            base_branch: BranchName("main".to_string()),
            draft: false,
            existing_pr_match: ExistingPrMatch::HeadBranch,
            idempotency_key: IdempotencyKey("create-pr:LUC-139".to_string()),
        });

        match result {
            FugueToolResult::Ok {
                output: FugueToolOutput::GitHubCreatePr(output),
                ..
            } => {
                assert_eq!(output.pull_request.number, GitHubPullRequestNumber(7));
                assert_eq!(output.pull_request.state, PullRequestState::Merged);
                assert!(!output.created);
            }
            other => panic!("unexpected result: {other:?}"),
        }
        assert_eq!(runner.calls().len(), 1);
    }

    #[test]
    fn create_pr_constructs_create_command_when_no_existing_match() {
        let created_url = "https://github.com/lukacf/meerkat-fugue/pull/9\n";
        let created_pr = r#"{"number":9,"title":"Stage 2D","url":"https://github.com/lukacf/meerkat-fugue/pull/9","state":"OPEN","isDraft":true,"headRefName":"codex/LUC-139-github-mob-tools","baseRefName":"main","headRefOid":"abc123"}"#;
        let runner =
            FakeRunner::with_outputs(vec![output("[]"), output(created_url), output(created_pr)]);
        let host = GitHubToolHost::with_runner(Box::new(runner.clone()));

        let result = host.create_pr(CreatePrInput {
            repo: repo(),
            title: "Stage 2D".to_string(),
            body_markdown: "Body".to_string(),
            head_branch: BranchName("codex/LUC-139-github-mob-tools".to_string()),
            base_branch: BranchName("main".to_string()),
            draft: true,
            existing_pr_match: ExistingPrMatch::HeadBranch,
            idempotency_key: IdempotencyKey("create-pr:LUC-139".to_string()),
        });

        assert!(matches!(result, FugueToolResult::Ok { .. }));
        let calls = runner.calls();
        assert_eq!(
            calls[1][0..4],
            ["pr", "create", "--repo", "lukacf/meerkat-fugue"]
        );
        assert!(calls[1].contains(&"--draft".to_string()));
    }

    fn merge_adapter_fixture(host: &GitHubToolHost, input: MergePrInput) -> FugueToolResult {
        host.merge_authorized(crate::merge_gate::AuthorizedMerge::for_adapter_fixture(
            input,
        ))
    }

    #[test]
    fn unscoped_merge_refuses_before_any_forge_operation() {
        let runner = FakeRunner::with_outputs(vec![output("{}")]);
        let host = GitHubToolHost::with_runner(Box::new(runner.clone()));
        let result = host.merge_pr(MergePrInput {
            repo: repo(),
            pull_request: GitHubPullRequestRef::Number {
                number: GitHubPullRequestNumber(11),
            },
            method: MergeMethod::Squash,
            expected_head_sha: Some(GitSha("a".repeat(40))),
            idempotency_key: IdempotencyKey("unscoped-merge".into()),
        });
        assert!(matches!(result, FugueToolResult::Err { .. }));
        assert!(
            runner.calls().is_empty(),
            "unscoped merge must use the Fugue runtime gate"
        );
    }

    #[test]
    fn merge_pr_refuses_missing_head_changed_head_and_draft_before_mutation() {
        for (expected, draft) in [
            (None, false),
            (Some("other"), false),
            (Some("abc123"), true),
        ] {
            let pr = serde_json::json!({"number":11, "title":"Change", "url":"https://github.com/example/app/pull/11",
                "state":"OPEN", "isDraft":draft, "headRefName":"feature", "baseRefName":"main", "headRefOid":"abc123"}).to_string();
            let runner = FakeRunner::with_outputs(vec![output(&pr)]);
            let host = GitHubToolHost::with_runner(Box::new(runner.clone()));
            let result = merge_adapter_fixture(
                &host,
                MergePrInput {
                    repo: repo(),
                    pull_request: GitHubPullRequestRef::Number {
                        number: GitHubPullRequestNumber(11),
                    },
                    method: MergeMethod::Squash,
                    expected_head_sha: expected.map(|sha| GitSha(sha.into())),
                    idempotency_key: IdempotencyKey("merge:11".into()),
                },
            );
            assert!(matches!(result, FugueToolResult::Err { .. }), "{result:?}");
            assert!(
                !runner
                    .calls()
                    .iter()
                    .any(|args| args.get(1).is_some_and(|arg| arg == "merge"))
            );
        }
    }

    #[test]
    fn merge_pr_zero_exit_requires_confirmed_merged_state_head_and_commit() {
        let open = serde_json::json!({"number":11, "title":"Change", "url":"https://github.com/example/app/pull/11",
            "state":"OPEN", "isDraft":false, "headRefName":"feature", "baseRefName":"main", "headRefOid":"abc123"});
        for scenario in ["queued", "changed_head", "missing_commit"] {
            let mut observed = open.clone();
            if scenario != "queued" {
                observed["state"] = "MERGED".into();
            }
            if scenario == "changed_head" {
                observed["headRefOid"] = "other".into();
            }
            let runner = FakeRunner::with_outputs(vec![
                output(&open.to_string()),
                output(""),
                output(&observed.to_string()),
                output(r#"{"mergeCommit":null}"#),
            ]);
            let host = GitHubToolHost::with_runner(Box::new(runner.clone()));
            let result = merge_adapter_fixture(
                &host,
                MergePrInput {
                    repo: repo(),
                    pull_request: GitHubPullRequestRef::Number {
                        number: GitHubPullRequestNumber(11),
                    },
                    method: MergeMethod::Squash,
                    expected_head_sha: Some(GitSha("abc123".into())),
                    idempotency_key: IdempotencyKey("merge:11".into()),
                },
            );
            assert!(
                matches!(result, FugueToolResult::Err { .. }),
                "must not report {scenario} as merged: {result:?}"
            );
            assert_eq!(
                runner
                    .calls()
                    .iter()
                    .filter(|args| args.get(1).is_some_and(|arg| arg == "merge"))
                    .count(),
                1
            );
        }
    }

    #[test]
    fn merge_pr_retry_confirms_commit_without_repeating_mutation() {
        let merged = r#"{"number":11,"title":"Change","url":"https://github.com/example/app/pull/11","state":"MERGED","isDraft":false,"headRefName":"feature","baseRefName":"main","headRefOid":"abc123"}"#;
        for commit in [
            r#"{"mergeCommit":{"oid":"merge456"}}"#,
            r#"{"mergeCommit":null}"#,
        ] {
            let runner = FakeRunner::with_outputs(vec![output(merged), output(commit)]);
            let host = GitHubToolHost::with_runner(Box::new(runner.clone()));
            let result = merge_adapter_fixture(
                &host,
                MergePrInput {
                    repo: repo(),
                    pull_request: GitHubPullRequestRef::Number {
                        number: GitHubPullRequestNumber(11),
                    },
                    method: MergeMethod::Squash,
                    expected_head_sha: Some(GitSha("abc123".into())),
                    idempotency_key: IdempotencyKey("merge:11".into()),
                },
            );
            if commit.contains("merge456") {
                assert!(matches!(
                    result,
                    FugueToolResult::Ok {
                        output: FugueToolOutput::GitHubMergePr(MergePrOutput {
                            merged: false,
                            merge_commit_sha: Some(_),
                            ..
                        }),
                        ..
                    }
                ));
            } else {
                assert!(matches!(result, FugueToolResult::Err { .. }));
            }
            assert!(
                !runner
                    .calls()
                    .iter()
                    .any(|args| args.get(1).is_some_and(|arg| arg == "merge"))
            );
        }
    }

    #[test]
    fn merge_pr_checks_head_sha_before_merge_command() {
        let open_pr = r#"{"number":11,"title":"Stage 2D","url":"https://github.com/lukacf/meerkat-fugue/pull/11","state":"OPEN","isDraft":false,"headRefName":"branch","baseRefName":"main","headRefOid":"abc123"}"#;
        let merged_pr = r#"{"number":11,"title":"Stage 2D","url":"https://github.com/lukacf/meerkat-fugue/pull/11","state":"MERGED","isDraft":false,"headRefName":"branch","baseRefName":"main","headRefOid":"abc123"}"#;
        let merge_commit = r#"{"mergeCommit":{"oid":"merge456"}}"#;
        let runner = FakeRunner::with_outputs(vec![
            output(open_pr),
            output(""),
            output(merged_pr),
            output(merge_commit),
        ]);
        let host = GitHubToolHost::with_runner(Box::new(runner.clone()));

        let result = merge_adapter_fixture(
            &host,
            MergePrInput {
                repo: repo(),
                pull_request: GitHubPullRequestRef::Number {
                    number: GitHubPullRequestNumber(11),
                },
                method: MergeMethod::Squash,
                expected_head_sha: Some(GitSha("abc123".to_string())),
                idempotency_key: IdempotencyKey("merge:11".to_string()),
            },
        );

        match result {
            FugueToolResult::Ok {
                output: FugueToolOutput::GitHubMergePr(output),
                ..
            } => {
                assert_eq!(
                    output.merge_commit_sha,
                    Some(GitSha("merge456".to_string()))
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
        assert_eq!(
            runner.calls()[1],
            vec![
                "pr".to_string(),
                "merge".to_string(),
                "11".to_string(),
                "--repo".to_string(),
                "lukacf/meerkat-fugue".to_string(),
                "--squash".to_string(),
                "--match-head-commit".to_string(),
                "abc123".to_string(),
            ]
        );
    }

    fn ci_candidate() -> GitHubPullRequestSnapshot {
        GitHubPullRequestSnapshot {
            repo: serde_json::from_value(
                serde_json::json!({"host":"github.example.test","owner":"example","name":"app"}),
            )
            .unwrap(),
            number: GitHubPullRequestNumber(11),
            title: "Change".into(),
            url: "https://github.example.test/example/app/pull/11".into(),
            state: PullRequestState::Open,
            draft: false,
            head_branch: BranchName("reused-branch".into()),
            base_branch: BranchName("main".into()),
            head_sha: GitSha("a".repeat(40)),
        }
    }

    fn ci_graph_page(
        head: &str,
        checks: serde_json::Value,
        more: bool,
        cursor: Option<&str>,
    ) -> serde_json::Value {
        serde_json::json!({"data":{"repository":{"pullRequest":{
            "headRefOid":head, "commits":{"nodes":[{"commit":{
                "oid":head,"statusCheckRollup":{"contexts":{
                    "nodes":checks,"pageInfo":{"hasNextPage":more,"endCursor":cursor}
                }}
            }}]}
        }}}})
    }

    #[test]
    fn check_pr_ci_binds_every_page_to_exact_head_and_numeric_pr() {
        let candidate = ci_candidate();
        let first = ci_graph_page(
            &candidate.head_sha.0,
            serde_json::json!([{
                "__typename":"CheckRun", "name":"unit", "status":"COMPLETED", "conclusion":"SUCCESS"
            }]),
            true,
            Some("next-page"),
        );
        for head in ["a".repeat(40), "b".repeat(40)] {
            let second = ci_graph_page(
                &head,
                serde_json::json!([{
                    "__typename":"StatusContext", "context":"external-build", "state":"FAILURE", "targetUrl":"https://ci.example.test/build/11"
                }]),
                false,
                None,
            );
            let runner = FakeRunner::with_outputs(vec![
                output(&first.to_string()),
                output(&second.to_string()),
            ]);
            let host = GitHubToolHost::with_runner(Box::new(runner.clone()));
            let result = host.check_pr_ci(&candidate);
            if head == candidate.head_sha.0 {
                match result {
                    FugueToolResult::Ok {
                        output: FugueToolOutput::GitHubCheckCi(result),
                        ..
                    } => {
                        assert_eq!(result.status, CiStatus::Failure);
                        assert_eq!(result.checks.len(), 2);
                        assert_eq!(result.checks[1].name, "external-build");
                    }
                    other => panic!("unexpected result: {other:?}"),
                }
            } else {
                assert!(matches!(
                    result,
                    FugueToolResult::Err {
                        error: FugueToolError {
                            code: FugueToolErrorCode::PreconditionFailed,
                            retry: RetryAdvice::RefreshStateThenRetry,
                            ..
                        },
                        ..
                    }
                ));
            }
            let calls = runner.calls();
            for call in &calls {
                assert_eq!(
                    call[0..4],
                    ["api", "graphql", "--hostname", "github.example.test"]
                );
                assert!(call.contains(&"number=11".to_owned()));
                assert!(call.contains(&"owner=example".to_owned()));
                assert!(call.contains(&"name=app".to_owned()));
                assert!(!call.iter().any(|arg| arg.contains("reused-branch")));
            }
            assert!(!calls[0].contains(&"cursor=next-page".to_owned()));
            assert!(calls[1].contains(&"cursor=next-page".to_owned()));
        }
    }

    #[test]
    fn check_pr_ci_refuses_incomplete_ambiguous_and_stale_responses() {
        let candidate = ci_candidate();
        for defect in [
            "missing_rollup",
            "missing_head",
            "missing_commit",
            "wrong_commit",
            "unknown_check",
            "missing_status",
            "missing_page",
            "missing_cursor",
            "partial_error",
        ] {
            let mut payload =
                ci_graph_page(&candidate.head_sha.0, serde_json::json!([]), false, None);
            match defect {
                "missing_rollup" => {
                    payload["data"]["repository"]["pullRequest"]["commits"]["nodes"][0]["commit"]
                        .as_object_mut()
                        .unwrap()
                        .remove("statusCheckRollup");
                }
                "missing_head" => {
                    payload["data"]["repository"]["pullRequest"]
                        .as_object_mut()
                        .unwrap()
                        .remove("headRefOid");
                }
                "missing_commit" => {
                    payload["data"]["repository"]["pullRequest"]["commits"]["nodes"] =
                        serde_json::json!([])
                }
                "wrong_commit" => {
                    payload["data"]["repository"]["pullRequest"]["commits"]["nodes"][0]["commit"]
                        ["oid"] = "b".repeat(40).into()
                }
                "partial_error" => {
                    payload["errors"] =
                        serde_json::json!([{"message":"private provider diagnostic"}])
                }
                _ => {
                    let contexts = &mut payload["data"]["repository"]["pullRequest"]["commits"]["nodes"]
                        [0]["commit"]["statusCheckRollup"]["contexts"];
                    match defect {
                        "unknown_check" => {
                            contexts["nodes"] = serde_json::json!([{"__typename":"UnknownCheck"}])
                        }
                        "missing_status" => {
                            contexts["nodes"] =
                                serde_json::json!([{"__typename":"CheckRun","name":"unit"}])
                        }
                        "missing_page" => {
                            contexts.as_object_mut().unwrap().remove("pageInfo");
                        }
                        "missing_cursor" => contexts["pageInfo"]["hasNextPage"] = true.into(),
                        _ => unreachable!(),
                    }
                }
            }
            let host =
                GitHubToolHost::with_runner(Box::new(FakeRunner::with_outputs(vec![output(
                    &payload.to_string(),
                )])));
            let result = host.check_pr_ci(&candidate);
            assert!(
                matches!(result, FugueToolResult::Err { .. }),
                "{defect}: {result:?}"
            );
            assert!(!format!("{result:?}").contains("private provider diagnostic"));
        }
    }

    #[test]
    fn check_pr_ci_requires_explicit_empty_rollup_and_terminating_pagination() {
        let candidate = ci_candidate();
        let empty = ci_graph_page(&candidate.head_sha.0, serde_json::json!([]), false, None);
        let mut no_rollup = empty.clone();
        no_rollup["data"]["repository"]["pullRequest"]["commits"]["nodes"][0]["commit"]["statusCheckRollup"] =
            serde_json::Value::Null;
        for payload in [empty, no_rollup.clone()] {
            let host =
                GitHubToolHost::with_runner(Box::new(FakeRunner::with_outputs(vec![output(
                    &payload.to_string(),
                )])));
            assert!(matches!(
                host.check_pr_ci(&candidate),
                FugueToolResult::Ok {
                    output: FugueToolOutput::GitHubCheckCi(CheckCiOutput {
                        status: CiStatus::NoChecks,
                        ..
                    }),
                    ..
                }
            ));
        }
        let page = ci_graph_page(
            &candidate.head_sha.0,
            serde_json::json!([]),
            true,
            Some("same-cursor"),
        );
        for second in [page.clone(), no_rollup] {
            let runner = FakeRunner::with_outputs(vec![
                output(&page.to_string()),
                output(&second.to_string()),
            ]);
            let host = GitHubToolHost::with_runner(Box::new(runner.clone()));
            assert!(matches!(
                host.check_pr_ci(&candidate),
                FugueToolResult::Err { .. }
            ));
            assert_eq!(runner.calls().len(), 2);
        }
    }

    #[test]
    fn check_pr_ci_requires_completed_check_runs_before_using_conclusion() {
        let candidate = ci_candidate();
        for (status, conclusion, expected) in [
            ("IN_PROGRESS", "SUCCESS", CiStatus::Pending),
            ("UNKNOWN", "SUCCESS", CiStatus::Pending),
            ("COMPLETED", "", CiStatus::Pending),
            ("COMPLETED", "STARTUP_FAILURE", CiStatus::Failure),
            ("COMPLETED", "STALE", CiStatus::Failure),
            ("COMPLETED", "SUCCESS", CiStatus::Success),
        ] {
            let page = ci_graph_page(
                &candidate.head_sha.0,
                serde_json::json!([{
                    "__typename":"CheckRun", "name":"unit", "status":status, "conclusion":conclusion
                }]),
                false,
                None,
            );
            let host =
                GitHubToolHost::with_runner(Box::new(FakeRunner::with_outputs(vec![output(
                    &page.to_string(),
                )])));
            assert!(
                matches!(host.check_pr_ci(&candidate), FugueToolResult::Ok { output: FugueToolOutput::GitHubCheckCi(CheckCiOutput { status, .. }), .. } if status == expected)
            );
        }
    }

    #[test]
    fn check_ci_never_hides_cancellation_among_successful_checks() {
        let checks = r#"[{"name":"unit","state":"SUCCESS","link":null,"completedAt":null},{"name":"integration","state":"CANCELLED","link":null,"completedAt":null}]"#;
        let host =
            GitHubToolHost::with_runner(Box::new(FakeRunner::with_outputs(vec![output(checks)])));
        assert!(matches!(
            host.check_ci(CheckCiInput {
                repo: repo(),
                git_ref: crate::tools::GitRef("11".into())
            }),
            FugueToolResult::Ok {
                output: FugueToolOutput::GitHubCheckCi(CheckCiOutput {
                    status: CiStatus::Cancelled,
                    ..
                }),
                ..
            }
        ));
    }

    #[test]
    fn check_ci_fallback_preserves_commit_status_evidence() {
        for (state, expected) in [
            ("SUCCESS", CiStatus::Success),
            ("FAILURE", CiStatus::Failure),
            ("PENDING", CiStatus::Pending),
            ("ERROR", CiStatus::Failure),
        ] {
            let rollup = serde_json::json!({"statusCheckRollup":[{
                "__typename":"StatusContext", "context":"external-build", "state":state,
                "targetUrl":"https://ci.example.test/build/11", "createdAt":"2026-04-29T12:00:00Z"
            }]})
            .to_string();
            let runner = FakeRunner::with_results(vec![
                Err(classify_gh_failure(Some(1), "unknown flag: --json".into())),
                Ok(output(&rollup)),
            ]);
            let host = GitHubToolHost::with_runner(Box::new(runner));
            match host.check_ci(CheckCiInput {
                repo: repo(),
                git_ref: crate::tools::GitRef("11".into()),
            }) {
                FugueToolResult::Ok {
                    output: FugueToolOutput::GitHubCheckCi(result),
                    ..
                } => {
                    assert_eq!(result.status, expected, "{state}");
                    assert_eq!(result.checks[0].name, "external-build");
                    assert_eq!(
                        result.checks[0].details_url.as_deref(),
                        Some("https://ci.example.test/build/11")
                    );
                }
                other => panic!("unexpected CI result: {other:?}"),
            }
        }
    }

    #[test]
    fn check_ci_missing_rollup_is_not_evidence_of_no_checks() {
        let runner = FakeRunner::with_results(vec![
            Err(classify_gh_failure(Some(1), "no checks reported".into())),
            Ok(output(r#"{"unexpected":"response"}"#)),
        ]);
        let host = GitHubToolHost::with_runner(Box::new(runner));
        assert!(matches!(
            host.check_ci(CheckCiInput {
                repo: repo(),
                git_ref: crate::tools::GitRef("11".into())
            }),
            FugueToolResult::Err { .. }
        ));
    }

    #[test]
    fn check_ci_maps_check_states_and_aggregates_status() {
        let checks = r#"[{"name":"test","state":"SUCCESS","link":"https://example.test","completedAt":"2026-04-29T12:00:00Z"},{"name":"lint","state":"PENDING","link":null,"completedAt":null}]"#;
        let runner = FakeRunner::with_outputs(vec![output(checks)]);
        let host = GitHubToolHost::with_runner(Box::new(runner.clone()));

        let result = host.check_ci(CheckCiInput {
            repo: repo(),
            git_ref: crate::tools::GitRef("branch".to_string()),
        });

        match result {
            FugueToolResult::Ok {
                output: FugueToolOutput::GitHubCheckCi(output),
                ..
            } => assert_eq!(output.status, CiStatus::Pending),
            other => panic!("unexpected result: {other:?}"),
        }
        assert_eq!(
            runner.calls()[0][0..5],
            ["pr", "checks", "branch", "--repo", "lukacf/meerkat-fugue"]
        );
    }

    #[test]
    fn check_exit_codes_preserve_failed_and_pending_evidence() {
        for (exit, state, expected) in [
            (1, "FAILURE", CiStatus::Failure),
            (8, "PENDING", CiStatus::Pending),
        ] {
            let output = command_output(
                &["pr", "checks", "1", "--json", "name,state,link,completedAt"],
                Some(exit),
                format!(r#"[{{"name":"test","state":"{state}","link":null,"completedAt":null}}]"#),
                String::new(),
            )
            .expect("check evidence");
            let host =
                GitHubToolHost::with_runner(Box::new(FakeRunner::with_outputs(vec![output])));
            let result = host.check_ci(CheckCiInput {
                repo: repo(),
                git_ref: crate::tools::GitRef("1".to_string()),
            });
            assert!(
                matches!(result, FugueToolResult::Ok { output: FugueToolOutput::GitHubCheckCi(CheckCiOutput { status, .. }), .. } if status == expected)
            );
        }
        assert!(
            command_output(
                &["pr", "checks", "1", "--json", "name"],
                Some(1),
                String::new(),
                "authentication failed".to_string()
            )
            .is_err()
        );
        assert!(
            command_output(
                &["pr", "merge"],
                Some(1),
                "[]".to_string(),
                "failed".to_string()
            )
            .is_err()
        );
    }

    #[test]
    fn missing_checks_are_confirmed_by_rollup_without_claiming_success() {
        let runner = FakeRunner::with_results(vec![
            Err(classify_gh_failure(
                Some(1),
                "no checks reported on the branch".to_string(),
            )),
            Ok(output(r#"{"statusCheckRollup":[]}"#)),
        ]);
        let host = GitHubToolHost::with_runner(Box::new(runner));
        let result = host.check_ci(CheckCiInput {
            repo: repo(),
            git_ref: crate::tools::GitRef("1".to_string()),
        });
        assert!(matches!(
            result,
            FugueToolResult::Ok {
                output: FugueToolOutput::GitHubCheckCi(CheckCiOutput {
                    status: CiStatus::NoChecks,
                    ..
                }),
                ..
            }
        ));
    }

    #[test]
    fn check_ci_reports_empty_check_list_without_claiming_success() {
        let runner = FakeRunner::with_outputs(vec![output("[]")]);
        let host = GitHubToolHost::with_runner(Box::new(runner));

        let result = host.check_ci(CheckCiInput {
            repo: repo(),
            git_ref: crate::tools::GitRef("docs-only-branch".to_string()),
        });

        match result {
            FugueToolResult::Ok {
                output: FugueToolOutput::GitHubCheckCi(output),
                ..
            } => {
                assert_eq!(output.status, CiStatus::NoChecks);
                assert!(output.checks.is_empty());
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn check_ci_bare_sha_pr_selector_failure_and_branch_success_are_distinct() {
        let sha_not_a_pr_ref = GitHubToolFailure::new(
            FugueToolErrorCode::NotFound,
            "gh exited with Some(1): no pull requests found for branch \"df54ff6bba8830904fb69815e95b454b94b2fbfe\"",
            RetryAdvice::DoNotRetry,
        );
        let runner = FakeRunner::with_results(vec![
            Err(sha_not_a_pr_ref),
            Ok(output(
                r#"[{"name":"test","state":"SUCCESS","link":null,"completedAt":null}]"#,
            )),
        ]);
        let host = GitHubToolHost::with_runner(Box::new(runner.clone()));

        let sha_result = host.check_ci(CheckCiInput {
            repo: repo(),
            git_ref: crate::tools::GitRef("df54ff6bba8830904fb69815e95b454b94b2fbfe".to_string()),
        });
        assert!(
            matches!(sha_result, FugueToolResult::Err { .. }),
            "gh pr checks treats a bare SHA as a PR/branch selector and can fail even when the PR branch has green checks"
        );

        let branch_result = host.check_ci(CheckCiInput {
            repo: repo(),
            git_ref: crate::tools::GitRef(
                "luka/luc-623-add-repo-level-final-pr-review-and-merge-gate".to_string(),
            ),
        });
        match branch_result {
            FugueToolResult::Ok {
                output: FugueToolOutput::GitHubCheckCi(output),
                ..
            } => assert_eq!(output.status, CiStatus::Success),
            other => panic!("unexpected branch check result: {other:?}"),
        }

        assert_eq!(
            runner.calls()[0][0..5],
            [
                "pr",
                "checks",
                "df54ff6bba8830904fb69815e95b454b94b2fbfe",
                "--repo",
                "lukacf/meerkat-fugue"
            ]
        );
        assert_eq!(
            runner.calls()[1][0..5],
            [
                "pr",
                "checks",
                "luka/luc-623-add-repo-level-final-pr-review-and-merge-gate",
                "--repo",
                "lukacf/meerkat-fugue"
            ]
        );
    }

    #[test]
    fn check_ci_falls_back_to_pr_view_when_pr_checks_json_is_unavailable() {
        let unsupported_json = GitHubToolFailure::new(
            FugueToolErrorCode::ExecutionFailed,
            "gh exited with Some(1): unknown flag: --json",
            RetryAdvice::SafeToRetry,
        );
        let rollup = r#"{"statusCheckRollup":[{"__typename":"CheckRun","name":"test","status":"COMPLETED","conclusion":"SUCCESS","detailsUrl":"https://example.test","completedAt":"2026-04-29T12:00:00Z"},{"__typename":"CheckRun","name":"lint","status":"IN_PROGRESS","conclusion":null,"detailsUrl":null,"completedAt":null}]}"#;
        let runner = FakeRunner::with_results(vec![Err(unsupported_json), Ok(output(rollup))]);
        let host = GitHubToolHost::with_runner(Box::new(runner.clone()));

        let result = host.check_ci(CheckCiInput {
            repo: repo(),
            git_ref: crate::tools::GitRef("branch".to_string()),
        });

        match result {
            FugueToolResult::Ok {
                output: FugueToolOutput::GitHubCheckCi(output),
                ..
            } => {
                assert_eq!(output.status, CiStatus::Pending);
                assert_eq!(output.checks[0].name, "test");
                assert_eq!(output.checks[0].status, CiStatus::Success);
                assert_eq!(output.checks[1].name, "lint");
                assert_eq!(output.checks[1].status, CiStatus::Pending);
            }
            other => panic!("unexpected result: {other:?}"),
        }
        assert_eq!(
            runner.calls()[0][0..5],
            ["pr", "checks", "branch", "--repo", "lukacf/meerkat-fugue"]
        );
        assert_eq!(
            runner.calls()[1],
            vec![
                "pr".to_string(),
                "view".to_string(),
                "branch".to_string(),
                "--repo".to_string(),
                "lukacf/meerkat-fugue".to_string(),
                "--json".to_string(),
                "statusCheckRollup".to_string(),
            ]
        );
    }
}
