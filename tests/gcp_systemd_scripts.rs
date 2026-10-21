use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

fn run(command: &mut Command) {
    let output = command.output().expect("command should run");
    assert!(
        output.status.success(),
        "command failed: status={}\nstdout={}\nstderr={}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn run_expect_failure(command: &mut Command) -> std::process::Output {
    let output = command.output().expect("command should run");
    assert!(
        !output.status.success(),
        "command unexpectedly succeeded: status={}\nstdout={}\nstderr={}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn git(path: &Path, args: &[&str]) {
    run(Command::new("git").args(args).current_dir(path));
}

fn write_executable(path: &Path, contents: &str) {
    fs::write(path, contents).unwrap();
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}

fn write_fake_cargo(fake_bin: &Path, cargo_log: &Path) {
    write_executable(
        &fake_bin.join("cargo"),
        &format!(
            r#"#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" > {}
release_dir="$CARGO_TARGET_DIR/release"
mkdir -p "$release_dir"
for binary in meerkat-fugue fugue-linear-mcp fugue-jira-mcp fugue-github-mcp fugue-host-mcp; do
  printf 'built:%s\n' "$binary" > "$release_dir/$binary"
  chmod 0755 "$release_dir/$binary"
done
"#,
            shell_quote(cargo_log)
        ),
    );
}

fn write_fake_cargo_with_sleep_meerkat(fake_bin: &Path, cargo_log: &Path) {
    write_executable(
        &fake_bin.join("cargo"),
        &format!(
            r#"#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" > {}
release_dir="$CARGO_TARGET_DIR/release"
mkdir -p "$release_dir"
cp /bin/sleep "$release_dir/meerkat-fugue"
chmod 0755 "$release_dir/meerkat-fugue"
for binary in fugue-linear-mcp fugue-jira-mcp fugue-github-mcp fugue-host-mcp; do
  printf 'built:%s\n' "$binary" > "$release_dir/$binary"
  chmod 0755 "$release_dir/$binary"
done
"#,
            shell_quote(cargo_log)
        ),
    );
}

fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', "'\\''"))
}

fn runtime_binaries() -> [&'static str; 5] {
    [
        "meerkat-fugue",
        "fugue-linear-mcp",
        "fugue-jira-mcp",
        "fugue-github-mcp",
        "fugue-host-mcp",
    ]
}

fn seed_installed_runtime_set(bin_dir: &Path, prefix: &str) {
    fs::create_dir_all(bin_dir).unwrap();
    for binary in runtime_binaries() {
        write_executable(&bin_dir.join(binary), &format!("{prefix}:{binary}\n"));
    }
}

fn assert_installed_runtime_set(bin_dir: &Path, prefix: &str) {
    for binary in runtime_binaries() {
        assert_eq!(
            fs::read_to_string(bin_dir.join(binary)).unwrap(),
            format!("{prefix}:{binary}\n")
        );
    }
}

fn line_index(haystack: &str, needle: &str) -> usize {
    haystack
        .lines()
        .position(|line| line.contains(needle))
        .unwrap_or_else(|| panic!("missing log line containing {needle:?}:\n{haystack}"))
}

fn path_with_prefix(prefix: &Path) -> String {
    let current = std::env::var_os("PATH").unwrap_or_default();
    let mut paths = vec![prefix.to_path_buf()];
    paths.extend(std::env::split_paths(&current));
    std::env::join_paths(paths)
        .unwrap()
        .to_string_lossy()
        .into_owned()
}

fn create_git_fixture(temp: &Path) -> (PathBuf, PathBuf) {
    let remote = temp.join("remote.git");
    let seed = temp.join("seed");
    let live = temp.join("live");

    fs::create_dir(&seed).unwrap();
    git(&seed, &["init", "-b", "main"]);
    git(
        &seed,
        &["config", "user.email", "fugue-script-test@example.com"],
    );
    git(&seed, &["config", "user.name", "Fugue Script Test"]);
    fs::write(seed.join("version.txt"), "one").unwrap();
    git(&seed, &["add", "."]);
    git(&seed, &["commit", "-m", "one"]);

    git(temp, &["init", "--bare", remote.to_str().unwrap()]);
    git(&remote, &["symbolic-ref", "HEAD", "refs/heads/main"]);
    git(
        &seed,
        &["remote", "add", "origin", remote.to_str().unwrap()],
    );
    git(&seed, &["push", "-u", "origin", "main"]);
    git(
        temp,
        &[
            "clone",
            "--branch",
            "main",
            remote.to_str().unwrap(),
            live.to_str().unwrap(),
        ],
    );

    fs::write(seed.join("version.txt"), "two").unwrap();
    git(&seed, &["add", "."]);
    git(&seed, &["commit", "-m", "two"]);
    git(&seed, &["push", "origin", "main"]);

    (remote, live)
}

#[test]
fn gcp_ota_update_script_builds_and_installs_complete_runtime_binary_set() {
    let temp = tempfile::tempdir().unwrap();
    let (_remote, live) = create_git_fixture(temp.path());
    let fugue_root = temp.path().join("fugue-root");
    let bin_dir = fugue_root.join("bin");
    let target_dir = fugue_root.join("target");
    fs::create_dir_all(&bin_dir).unwrap();

    let fake_bin = temp.path().join("fake-bin");
    fs::create_dir(&fake_bin).unwrap();
    let cargo_log = temp.path().join("cargo.log");
    write_fake_cargo(&fake_bin, &cargo_log);

    run(Command::new("bash")
        .arg("scripts/gcp-systemd/ota-update.sh")
        .arg("--fugue-root")
        .arg(&fugue_root)
        .arg("--repo")
        .arg(&live)
        .arg("--bin-dir")
        .arg(&bin_dir)
        .arg("--target-dir")
        .arg(&target_dir)
        .arg("--service-manager")
        .arg("none")
        .arg("--skip-health")
        .env("PATH", path_with_prefix(&fake_bin))
        .env("HOME", temp.path()));

    assert_eq!(fs::read_to_string(live.join("version.txt")).unwrap(), "two");
    let cargo_args = fs::read_to_string(cargo_log).unwrap();
    for binary in runtime_binaries() {
        assert!(
            cargo_args.contains(binary),
            "cargo args should include {binary}"
        );
        assert_eq!(
            fs::read_to_string(bin_dir.join(binary)).unwrap(),
            format!("built:{binary}\n")
        );
    }
}

#[test]
fn setup_host_installs_complete_runtime_binary_set_from_bin_source() {
    let temp = tempfile::tempdir().unwrap();
    let fugue_root = temp.path().join("fugue-root");
    let bin_source = temp.path().join("bin-source");
    fs::create_dir(&bin_source).unwrap();

    for binary in runtime_binaries() {
        write_executable(&bin_source.join(binary), &format!("source:{binary}\n"));
    }

    run(Command::new("bash")
        .arg("scripts/gcp-systemd/setup-host.sh")
        .arg("--fugue-root")
        .arg(&fugue_root)
        .arg("--bin-source")
        .arg(&bin_source));

    for binary in runtime_binaries() {
        assert_eq!(
            fs::read_to_string(fugue_root.join("bin").join(binary)).unwrap(),
            format!("source:{binary}\n")
        );
    }
}

#[test]
fn gcp_ota_update_script_rejects_non_file_installed_path_before_partial_replacement() {
    let temp = tempfile::tempdir().unwrap();
    let (_remote, live) = create_git_fixture(temp.path());
    let fugue_root = temp.path().join("fugue-root");
    let bin_dir = fugue_root.join("bin");
    let target_dir = fugue_root.join("target");
    seed_installed_runtime_set(&bin_dir, "old");
    fs::remove_file(bin_dir.join("fugue-host-mcp")).unwrap();
    fs::create_dir(bin_dir.join("fugue-host-mcp")).unwrap();

    let fake_bin = temp.path().join("fake-bin");
    fs::create_dir(&fake_bin).unwrap();
    write_fake_cargo(&fake_bin, &temp.path().join("cargo.log"));

    let output = run_expect_failure(
        Command::new("bash")
            .arg("scripts/gcp-systemd/ota-update.sh")
            .arg("--fugue-root")
            .arg(&fugue_root)
            .arg("--repo")
            .arg(&live)
            .arg("--bin-dir")
            .arg(&bin_dir)
            .arg("--target-dir")
            .arg(&target_dir)
            .arg("--service-manager")
            .arg("none")
            .arg("--skip-health")
            .env("PATH", path_with_prefix(&fake_bin))
            .env("HOME", temp.path()),
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("installed runtime path is not a regular file"));
    for binary in ["meerkat-fugue", "fugue-linear-mcp", "fugue-github-mcp"] {
        assert_eq!(
            fs::read_to_string(bin_dir.join(binary)).unwrap(),
            format!("old:{binary}\n")
        );
    }
    assert!(bin_dir.join("fugue-host-mcp").is_dir());
    assert_eq!(
        fs::read_dir(bin_dir.join("fugue-host-mcp"))
            .unwrap()
            .count(),
        0,
        "preflight should fail before creating nested temp artifacts"
    );
}

#[test]
fn gcp_ota_update_script_rolls_back_runtime_set_on_health_failure() {
    let temp = tempfile::tempdir().unwrap();
    let (_remote, live) = create_git_fixture(temp.path());
    let fugue_root = temp.path().join("fugue-root");
    let bin_dir = fugue_root.join("bin");
    let target_dir = fugue_root.join("target");
    seed_installed_runtime_set(&bin_dir, "old");

    let fake_bin = temp.path().join("fake-bin");
    fs::create_dir(&fake_bin).unwrap();
    write_fake_cargo(&fake_bin, &temp.path().join("cargo.log"));
    write_executable(
        &fake_bin.join("curl"),
        r#"#!/usr/bin/env bash
printf '500'
"#,
    );

    let output = run_expect_failure(
        Command::new("bash")
            .arg("scripts/gcp-systemd/ota-update.sh")
            .arg("--fugue-root")
            .arg(&fugue_root)
            .arg("--repo")
            .arg(&live)
            .arg("--bin-dir")
            .arg(&bin_dir)
            .arg("--target-dir")
            .arg(&target_dir)
            .arg("--service-manager")
            .arg("none")
            .arg("--health-timeout")
            .arg("0")
            .env("PATH", path_with_prefix(&fake_bin))
            .env("HOME", temp.path()),
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("health check failed"));
    assert!(stderr.contains("rolling back installed Fugue runtime binaries"));
    assert_installed_runtime_set(&bin_dir, "old");
}

#[test]
fn gcp_ota_update_script_rolls_back_runtime_set_on_restart_failure() {
    let temp = tempfile::tempdir().unwrap();
    let (_remote, live) = create_git_fixture(temp.path());
    let fugue_root = temp.path().join("fugue-root");
    let bin_dir = fugue_root.join("bin");
    let target_dir = fugue_root.join("target");
    seed_installed_runtime_set(&bin_dir, "old");

    let fake_bin = temp.path().join("fake-bin");
    fs::create_dir(&fake_bin).unwrap();
    write_fake_cargo(&fake_bin, &temp.path().join("cargo.log"));
    write_executable(
        &fake_bin.join("systemctl"),
        r#"#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *"daemon-reload"* ]]; then
  exit 0
fi
if [[ "$*" == *"restart"* ]]; then
  exit 1
fi
exit 0
"#,
    );

    let output = run_expect_failure(
        Command::new("bash")
            .arg("scripts/gcp-systemd/ota-update.sh")
            .arg("--fugue-root")
            .arg(&fugue_root)
            .arg("--repo")
            .arg(&live)
            .arg("--bin-dir")
            .arg(&bin_dir)
            .arg("--target-dir")
            .arg(&target_dir)
            .arg("--service-manager")
            .arg("user")
            .arg("--skip-health")
            .env("PATH", path_with_prefix(&fake_bin))
            .env("HOME", temp.path()),
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("systemd restart failed after runtime replacement"));
    assert!(stderr.contains("rolling back installed Fugue runtime binaries"));
    assert_installed_runtime_set(&bin_dir, "old");
}

#[test]
fn gcp_ota_update_script_stops_running_new_binary_before_health_rollback() {
    let temp = tempfile::tempdir().unwrap();
    let (_remote, live) = create_git_fixture(temp.path());
    let fugue_root = temp.path().join("fugue-root");
    let bin_dir = fugue_root.join("bin");
    let target_dir = fugue_root.join("target");
    seed_installed_runtime_set(&bin_dir, "old");

    let fake_bin = temp.path().join("fake-bin");
    fs::create_dir(&fake_bin).unwrap();
    write_fake_cargo_with_sleep_meerkat(&fake_bin, &temp.path().join("cargo.log"));
    write_executable(
        &fake_bin.join("curl"),
        r#"#!/usr/bin/env bash
printf '500'
"#,
    );

    let systemctl_log = temp.path().join("systemctl.log");
    let restart_count = temp.path().join("restart-count");
    let pid_file = temp.path().join("meerkat.pid");
    write_executable(
        &fake_bin.join("systemctl"),
        r#"#!/usr/bin/env bash
set -euo pipefail
printf 'systemctl:%s\n' "$*" >> "$FAKE_SYSTEMCTL_LOG"
if [[ "$*" == *"daemon-reload"* ]]; then
  exit 0
fi
if [[ "$*" == *"is-active"* ]]; then
  exit 0
fi
if [[ "$*" == *"stop"* ]]; then
  printf 'stop_before_restore\n' >> "$FAKE_SYSTEMCTL_LOG"
  if [[ -s "$FAKE_PID_FILE" ]]; then
    kill "$(cat "$FAKE_PID_FILE")" 2>/dev/null || true
  fi
  exit 0
fi
if [[ "$*" == *"restart"* ]]; then
  count=0
  if [[ -f "$FAKE_RESTART_COUNT" ]]; then
    count="$(cat "$FAKE_RESTART_COUNT")"
  fi
  if [[ "$count" == 0 ]]; then
    printf 'first_restart_launches_new\n' >> "$FAKE_SYSTEMCTL_LOG"
    "$FAKE_BIN_DIR/meerkat-fugue" 30 >/dev/null 2>&1 &
    printf '%s\n' "$!" > "$FAKE_PID_FILE"
    printf '1\n' > "$FAKE_RESTART_COUNT"
  else
    if grep -q 'old:meerkat-fugue' "$FAKE_BIN_DIR/meerkat-fugue"; then
      printf 'restart_after_restore_old\n' >> "$FAKE_SYSTEMCTL_LOG"
    else
      printf 'restart_after_restore_not_old\n' >> "$FAKE_SYSTEMCTL_LOG"
      exit 1
    fi
  fi
  exit 0
fi
exit 0
"#,
    );

    let output = run_expect_failure(
        Command::new("bash")
            .arg("scripts/gcp-systemd/ota-update.sh")
            .arg("--fugue-root")
            .arg(&fugue_root)
            .arg("--repo")
            .arg(&live)
            .arg("--bin-dir")
            .arg(&bin_dir)
            .arg("--target-dir")
            .arg(&target_dir)
            .arg("--service-manager")
            .arg("user")
            .arg("--health-timeout")
            .arg("0")
            .env("PATH", path_with_prefix(&fake_bin))
            .env("HOME", temp.path())
            .env("FAKE_SYSTEMCTL_LOG", &systemctl_log)
            .env("FAKE_RESTART_COUNT", &restart_count)
            .env("FAKE_PID_FILE", &pid_file)
            .env("FAKE_BIN_DIR", &bin_dir),
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("health check failed"));
    assert!(stderr.contains("stopping failed"));
    assert!(stderr.contains("rolling back installed Fugue runtime binaries"));
    assert!(!stderr.contains("Text file busy"));
    assert_installed_runtime_set(&bin_dir, "old");

    let log = fs::read_to_string(systemctl_log).unwrap();
    let first_restart = line_index(&log, "first_restart_launches_new");
    let stop = line_index(&log, "stop_before_restore");
    let restart_after_restore = line_index(&log, "restart_after_restore_old");
    assert!(
        first_restart < stop,
        "systemctl log should stop after first restart:\n{log}"
    );
    assert!(
        stop < restart_after_restore,
        "systemctl log should restart old service after stop/restore:\n{log}"
    );
}
