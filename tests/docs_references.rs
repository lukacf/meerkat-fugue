#[test]
fn readme_links_to_current_operator_docs() {
    let readme = include_str!("../README.md");
    let operator_guide = include_str!("../docs/operator-guide.md");

    let operations_link = "[Operations](docs/operations.mdx)";
    let current_operator_heading = "## Rollback";
    let removed_stage6_heading = "Stage 6 native-default deployment smoke and rollback";

    assert!(readme.contains(operations_link));
    assert!(operator_guide.contains(current_operator_heading));
    assert!(!readme.contains(removed_stage6_heading));
}

#[test]
fn operator_docs_capture_post_quarantine_mcp_workpad_contract() {
    let operator_guide = include_str!("../docs/operator-guide.md");

    assert!(operator_guide.contains("Post-quarantine MCP Workpad contract"));
    assert!(operator_guide.contains("inputSchema"));
    assert!(operator_guide.contains("mcp:linear"));
    assert!(operator_guide.contains("mcp:github"));
    assert!(operator_guide.contains("planner/coder/reviewer"));
    assert!(operator_guide.contains("do not call `linear_move_issue`"));
    assert!(operator_guide.contains("observes Meta flow/member state"));
    assert!(operator_guide.contains("typed Kapellmeister `move` decision"));
    assert!(operator_guide.contains("text:\"ok\""));
    assert!(operator_guide.contains("reviewer records a gate outcome"));
    assert!(operator_guide.contains("PR/no-PR evidence"));
    assert!(operator_guide.contains("rkat-rpc ... --tcp"));
    assert!(operator_guide.contains("mob/flow_run"));
    assert!(operator_guide.contains("Transport closed"));
    assert!(operator_guide.contains("missing_tracker_project_slug"));
    assert!(operator_guide.contains("retry loops"));
    assert!(operator_guide.contains("stale active rows"));
    assert!(operator_guide.contains("Unexpected response type"));
}

#[test]
fn ota_deployment_docs_and_script_install_complete_fugue_binary_set() {
    let readme = include_str!("../README.md");
    let configuration_md = include_str!("../docs/configuration.md");
    let configuration_mdx = include_str!("../docs/configuration.mdx");
    let operations = include_str!("../docs/operations.mdx");
    let operator_guide = include_str!("../docs/operator-guide.md");
    let example = include_str!("../fugue.example.yaml");
    let ota_script = include_str!("../scripts/gcp-systemd/ota-update.sh");

    let required_binaries = [
        "meerkat-fugue",
        "fugue-linear-mcp",
        "fugue-jira-mcp",
        "fugue-github-mcp",
        "fugue-host-mcp",
    ];

    for (name, doc) in [
        ("README.md", readme),
        ("docs/configuration.md", configuration_md),
        ("docs/configuration.mdx", configuration_mdx),
        ("docs/operations.mdx", operations),
        ("docs/operator-guide.md", operator_guide),
        ("fugue.example.yaml", example),
        ("scripts/gcp-systemd/ota-update.sh", ota_script),
    ] {
        for binary in required_binaries {
            assert!(doc.contains(binary), "{name} should mention {binary}");
        }
    }

    assert!(readme.contains("atomically replacing the five binaries"));
    assert!(operations.contains("Install or restart all five binaries atomically"));
    assert!(operator_guide.contains("Install or restart all five binaries atomically"));
}

#[test]
fn operator_docs_capture_workpad_handoff_recovery_safeguards() {
    let operator_guide = include_str!("../docs/operator-guide.md");
    let native_mobs = include_str!("../docs/native-mobs.mdx");
    let operations = include_str!("../docs/operations.mdx");

    for doc in [operator_guide, native_mobs, operations] {
        assert!(doc.contains("## LUC-123 Workpad"));
        assert!(doc.contains("evidence-rich") || doc.contains("richer handoff evidence"));
        assert!(doc.contains("Meta `Cancelled`"));
        assert!(doc.contains("recovery signal"));
        assert!(doc.contains("fresh issue"));
        assert!(doc.contains("PR, check, merge"));
    }

    assert!(operator_guide.contains("Never replace durable PR or"));
    assert!(native_mobs.contains("must not overwrite PR, check, or merge evidence"));
    assert!(operations.contains("Do not move the Linear issue to a canceled lane"));
}

#[test]
fn docs_capture_pr_handoff_idempotency_smoke_guardrails() {
    let operator_guide = include_str!("../docs/operator-guide.md");
    let native_mobs = include_str!("../docs/native-mobs.mdx");
    let operations = include_str!("../docs/operations.mdx");

    assert!(operator_guide.contains("exactly one issue-specific Workpad"));
    assert!(operator_guide.contains("generic Fugue Workpad"));
    assert!(native_mobs.contains("exactly one Workpad for that issue"));
    assert!(native_mobs.contains("branch-scoped CI evidence"));
    assert!(native_mobs.contains("stale SHA-only check"));
    assert!(operations.contains("Exactly one PR exists for the issue branch"));
    assert!(operations.contains("CI/check evidence is branch-scoped"));
    assert!(operations.contains("merge SHA"));
    assert!(operations.contains("`Done` handoff"));
}

#[test]
fn native_mob_docs_capture_member_session_history_smoke_evidence() {
    let native_mobs = include_str!("../docs/native-mobs.mdx");

    assert!(native_mobs.contains("Post-merge Meta retake smokes"));
    assert!(native_mobs.contains("autonomous kickoff smoke"));
    assert!(native_mobs.contains("already open"));
    assert!(native_mobs.contains("picks it up from `Queue` without manual"));
    assert!(native_mobs.contains("real repo"));
    assert!(native_mobs.contains("`fugue/kapellmeister` member"));
    assert!(native_mobs.contains("active issue planner, coder,"));
    assert!(native_mobs.contains("peering wired to"));
    assert!(native_mobs.contains("`fugue/kapellmeister`"));
    assert!(native_mobs.contains("not a mock, stale, issue-mob container"));
    assert!(native_mobs.contains("real session ids"));
    assert!(native_mobs.contains("Playwright must observe live"));
    assert!(native_mobs.contains("transcript/event updates"));
    assert!(native_mobs.contains("without relying on a"));
    assert!(native_mobs.contains("manual browser refresh"));
    assert!(native_mobs.contains("Meta, not Fugue, replays"));
    assert!(native_mobs.contains("session ids observed before"));
    assert!(native_mobs.contains("transcript/tool history"));
    assert!(native_mobs.contains("Fugue/Kapellmeister projection"));
    assert!(native_mobs.contains("green reviewer gate"));
    assert!(native_mobs.contains("After a Fugue service restart"));
    assert!(native_mobs.contains("pre-existing Playwright browser context"));
    assert!(native_mobs.contains("durable session snapshots"));
    assert!(native_mobs.contains("runtime_restore=failed"));
    assert!(native_mobs.contains("missing durable session snapshot"));
    assert!(native_mobs.contains("stale active rows"));
    assert!(native_mobs.contains("failures to investigate"));
}

#[test]
fn docs_capture_generic_kapellmeister_private_peer_smoke_evidence() {
    let evidence =
        include_str!("../docs/smoke-evidence/LUC-504-kapellmeister-private-peer-wiring.md");

    assert!(evidence.contains("EXAMPLE-1"));
    assert!(evidence.contains("native-execution:EXAMPLE-1"));
    assert!(evidence.contains("worker_started"));
    assert!(evidence.contains("2026-01-01T00:00:00Z"));
    assert!(evidence.contains("sig-example"));
    assert!(evidence.contains("planner"));
    assert!(evidence.contains("coder"));
    assert!(evidence.contains("reviewer"));
    assert!(evidence.contains("fugue-repo-kapellmeister/kapellmeister/kapellmeister"));
    assert!(evidence.contains("uuid-example"));
    assert!(evidence.contains("inproc://fugue-repo-kapellmeister/kapellmeister/kapellmeister"));
    assert!(evidence.contains("checksum_token"));
    assert!(evidence.contains("reported reachability `reachable`"));
    assert!(evidence.contains("Current coder revalidation"));
    assert!(evidence.contains("not merely projected in the"));
}

#[test]
fn worker_prompt_documents_host_projected_compact_lane_transitions() {
    let prompt = include_str!("../prompts/fugue-worker.md");

    assert!(prompt.contains("Projected Lane Ownership"));
    assert!(prompt.contains("issue lead owns per-issue"));
    assert!(prompt.contains("Linear lane projection"));
    assert!(prompt.contains("Fugue host observes Linear and runtime state"));
    assert!(prompt.contains("configured repo handoff policy"));
    assert!(prompt.contains("active mob work projects to `Active`"));
    assert!(!prompt.contains("Typed lane-transition examples"));
    assert!(!prompt.contains("planner-to-implementing"));
}

#[test]
fn mdx_documentation_set_covers_operator_and_native_execution_topics() {
    let index = include_str!("../docs/index.mdx");
    let navigation = include_str!("../docs/_navigation.mdx");
    let overview = include_str!("../docs/overview.mdx");
    let getting_started = include_str!("../docs/getting-started.mdx");
    let configuration = include_str!("../docs/configuration.mdx");
    let native_mobs = include_str!("../docs/native-mobs.mdx");
    let operations = include_str!("../docs/operations.mdx");
    let status_api = include_str!("../docs/status-api.mdx");
    let development = include_str!("../docs/development.mdx");
    let troubleshooting = include_str!("../docs/troubleshooting.mdx");

    for doc in [
        "overview.mdx",
        "getting-started.mdx",
        "configuration.mdx",
        "native-mobs.mdx",
        "operations.mdx",
        "architecture.mdx",
        "status-api.mdx",
        "development.mdx",
        "troubleshooting.mdx",
    ] {
        assert!(index.contains(doc), "index should link to {doc}");
        assert!(navigation.contains(doc), "navigation should link to {doc}");
    }

    assert!(overview.contains("When to use Fugue"));
    assert!(getting_started.contains("cargo fmt --check"));
    assert!(getting_started.contains("--port 0"));
    assert!(configuration.contains("omitted top-level `backend` resolves to `native-execution`"));
    assert!(configuration.contains("mcp:linear"));
    assert!(configuration.contains("native-execution"));
    assert!(native_mobs.contains("Linear projection model"));
    assert!(native_mobs.contains("inputSchema"));
    assert!(native_mobs.contains("Projected Lane Ownership"));
    assert!(native_mobs.contains("report lifecycle facts to the issue lead"));
    assert!(native_mobs.contains("linear_move_issue"));
    assert!(operations.contains("Common incidents"));
    assert!(operations.contains("Real E2E warning"));
    assert!(status_api.contains("/api/v1/state"));
    assert!(status_api.contains("/console"));
    assert!(status_api.contains("Fugue does not implement its own console RPC facade"));
    assert!(status_api.contains("FUGUE_META_RPC_TCP_ADDR"));
    assert!(status_api.contains("FUGUE_META_RPC_BEARER_TOKEN"));
    assert!(development.contains("Documentation changes"));
    assert!(troubleshooting.contains("native_recovery.review_required"));
}
