use std::{fs, process::Command};

use serde::Deserialize;
use serde_json::value::RawValue;

#[derive(Deserialize)]
struct MigrationReport {
    mode: String,
    refusals: Vec<Box<RawValue>>,
    applied: Option<Box<RawValue>>,
}

#[test]
fn migration_cli_preserves_native_json_refusals_and_source_databases() {
    let state = tempfile::tempdir().expect("temporary runtime");
    let invalid_database = b"not a SQLite database\n";
    for name in ["runtime.sqlite", "mob.sqlite3"] {
        fs::write(state.path().join(name), invalid_database).expect("malformed store fixture");
    }

    let output = Command::new(env!("CARGO_BIN_EXE_meerkat-fugue"))
        .arg("migrate-storage")
        .arg("--runtime-dir")
        .arg(state.path())
        .output()
        .expect("run migration CLI");

    assert!(
        !output.status.success(),
        "native refusal must fail the command"
    );
    let report: MigrationReport = serde_json::from_slice(&output.stdout)
        .expect("stdout contains only the native JSON report");
    assert_eq!(report.mode, "dry_run");
    assert!(!report.refusals.is_empty());
    assert!(report.applied.is_none());
    for name in ["runtime.sqlite", "mob.sqlite3"] {
        assert_eq!(
            fs::read(state.path().join(name)).expect("original store remains readable"),
            invalid_database
        );
    }
}
