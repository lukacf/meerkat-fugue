use super::*;

fn scope() -> HostSignalScope {
    HostSignalScope {
        repository_path: PathBuf::from("/example/repository"),
        runtime_path: PathBuf::from("/example/runtime"),
        mob_id: "example-mob".into(),
        member: "kapellmeister".into(),
        tracker: TrackerScope::Jira {
            endpoint: "https://jira.example.com".into(),
            project: "APP".into(),
            query: None,
        },
    }
}

fn message(text: &str) -> SignalEnvelope {
    SignalEnvelope {
        content: text.into(),
        mode: SignalHandlingMode::Steer,
    }
}

#[test]
fn abandoned_transaction_saves_neither_observation_nor_notification() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("signals.sqlite3");
    let mut journal = HostSignalJournal::open(&path, &scope()).unwrap();
    {
        let mut update = journal.begin().unwrap();
        update.observe_lane("APP-1", "Active");
        update
            .enqueue_once("event-1", || Ok(message("changed")))
            .unwrap();
        // Simulate cancellation or an encoding failure before the commit.
    }
    drop(journal);
    let mut journal = HostSignalJournal::open(&path, &scope()).unwrap();
    assert!(journal.pending(10).unwrap().is_empty());
    let mut update = journal.begin().unwrap();
    assert!(matches!(
        update.observe_lane("APP-1", "Active"),
        crate::linear_lane_state::LinearLaneObservation::FirstSeen { .. }
    ));
}

#[test]
fn reopening_replays_exact_content_key_and_mode_without_regenerating_them() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("signals.sqlite3");
    let mut journal = HostSignalJournal::open(&path, &scope()).unwrap();
    let mut update = journal.begin().unwrap();
    update.observe_lane("APP-1", "Active");
    let key = update
        .enqueue_once("event-1", || Ok(message("original")))
        .unwrap();
    update.commit().unwrap();
    drop(journal);

    let mut journal = HostSignalJournal::open(&path, &scope()).unwrap();
    let first = journal.pending(10).unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].key, key);
    assert_eq!(first[0].envelope, message("original"));
    let mut update = journal.begin().unwrap();
    assert!(matches!(
        update.observe_lane("APP-1", "Active"),
        crate::linear_lane_state::LinearLaneObservation::Unchanged
    ));
    assert_eq!(
        update
            .enqueue_once("event-1", || panic!(
                "do not regenerate a recorded envelope"
            ))
            .unwrap(),
        key
    );
    update.commit().unwrap();
    assert_eq!(journal.pending(10).unwrap(), first);
}

#[test]
fn acknowledging_one_delivery_preserves_newer_observations_and_refuses_conflicts() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("signals.sqlite3");
    let mut observer = HostSignalJournal::open(&path, &scope()).unwrap();
    let mut sender = HostSignalJournal::open(&path, &scope()).unwrap();
    let mut update = observer.begin().unwrap();
    let first = update
        .enqueue_once("event-1", || Ok(message("first")))
        .unwrap();
    update.commit().unwrap();
    let in_flight = sender.pending(1).unwrap();
    assert_eq!(in_flight[0].key, first);
    let mut update = observer.begin().unwrap();
    update.observe_lane("APP-1", "Review");
    let second = update
        .enqueue_once("event-2", || Ok(message("second")))
        .unwrap();
    update.commit().unwrap();
    sender.acknowledge(&first, "native-delivery-1").unwrap();
    sender.acknowledge(&first, "native-delivery-1").unwrap();
    assert!(
        sender
            .acknowledge(&first, "different-native-delivery")
            .is_err()
    );
    assert!(sender.acknowledge(&second, "").is_err());
    assert!(
        sender
            .acknowledge(&DeliveryKey("unknown".into()), "native-delivery-2")
            .is_err()
    );
    let pending = observer.pending(10).unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].key, second);
    let mut update = observer.begin().unwrap();
    assert!(matches!(
        update.observe_lane("APP-1", "Review"),
        crate::linear_lane_state::LinearLaneObservation::Unchanged
    ));
}

#[test]
fn existing_empty_corrupt_and_unknown_version_files_are_not_adopted() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("signals.sqlite3");
    for bytes in [b"".as_slice(), b"not a database".as_slice()] {
        std::fs::write(&path, bytes).unwrap();
        assert!(HostSignalJournal::open(&path, &scope()).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }
    std::fs::remove_file(&path).unwrap();
    drop(HostSignalJournal::open(&path, &scope()).unwrap());
    let connection = Connection::open(&path).unwrap();
    connection.pragma_update(None, "user_version", 99).unwrap();
    drop(connection);
    assert!(HostSignalJournal::open(&path, &scope()).is_err());
}

#[test]
fn changing_tracker_or_native_destination_cannot_retarget_a_saved_journal() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("signals.sqlite3");
    let original = scope();
    drop(HostSignalJournal::open(&path, &original).unwrap());
    let mut changed = original.clone();
    changed.mob_id = "other-mob".into();
    assert!(HostSignalJournal::open(&path, &changed).is_err());
    changed = original.clone();
    changed.tracker = TrackerScope::Internal {
        database: PathBuf::from("/example/issues.sqlite3"),
        repository: "example/repository".into(),
    };
    assert!(HostSignalJournal::open(&path, &changed).is_err());
    assert!(HostSignalJournal::open(&path, &original).is_ok());
}

#[test]
fn concurrent_creators_and_observers_do_not_overwrite_each_others_work() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("new-directory/signals.sqlite3");
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
    let workers: Vec<_> = (0..8)
        .map(|number| {
            let path = path.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let mut journal = HostSignalJournal::open(&path, &scope()).unwrap();
                let mut update = journal.begin().unwrap();
                update.observe_lane(&format!("APP-{number}"), "Active");
                update
                    .enqueue_once(&format!("event-{number}"), || Ok(message("changed")))
                    .unwrap();
                update.commit().unwrap();
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
    let mut journal = HostSignalJournal::open(&path, &scope()).unwrap();
    assert_eq!(journal.pending(100).unwrap().len(), 8);
    let mut update = journal.begin().unwrap();
    for number in 0..8 {
        assert!(matches!(
            update.observe_lane(&format!("APP-{number}"), "Active"),
            crate::linear_lane_state::LinearLaneObservation::Unchanged
        ));
    }
}

#[test]
fn endpoint_credentials_are_rejected_before_creating_storage() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("signals.sqlite3");
    for endpoint in [
        "https://user:secret@jira.example.com",
        "https://jira.example.com?token=secret",
        "https://jira.example.com#secret",
    ] {
        let mut scope = scope();
        scope.tracker = TrackerScope::Jira {
            endpoint: endpoint.into(),
            project: "APP".into(),
            query: None,
        };
        let error = HostSignalJournal::open(&path, &scope).err().unwrap();
        assert!(!error.to_string().contains("secret"));
        assert!(!path.exists());
    }
}

#[test]
fn journal_process_exit_helper() {
    let Some(path) = std::env::var_os("FUGUE_JOURNAL_PROCESS_TEST_PATH") else {
        return;
    };
    let mut journal = HostSignalJournal::open(Path::new(&path), &scope()).unwrap();
    let mut update = journal.begin().unwrap();
    update.observe_lane("APP-1", "Active");
    update
        .enqueue_once("event-1", || Ok(message("persist through process exit")))
        .unwrap();
    if std::env::var("FUGUE_JOURNAL_PROCESS_TEST_COMMIT").as_deref() == Ok("yes") {
        update.commit().unwrap();
    }
    // Both database close and transaction destructors are deliberately skipped.
    std::process::exit(0);
}

#[test]
fn process_loss_preserves_committed_intent_and_rolls_back_uncommitted_observations() {
    for commit in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("signals.sqlite3");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "host_signal_journal::tests::journal_process_exit_helper",
                "--nocapture",
            ])
            .env("FUGUE_JOURNAL_PROCESS_TEST_PATH", &path)
            .env(
                "FUGUE_JOURNAL_PROCESS_TEST_COMMIT",
                if commit { "yes" } else { "no" },
            )
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if std::time::Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("journal child exceeded its bounded local test");
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        assert!(status.success());
        let mut journal = HostSignalJournal::open(&path, &scope()).unwrap();
        assert_eq!(journal.pending(10).unwrap().len(), usize::from(commit));
        let mut update = journal.begin().unwrap();
        assert_eq!(
            matches!(
                update.observe_lane("APP-1", "Active"),
                crate::linear_lane_state::LinearLaneObservation::Unchanged
            ),
            commit
        );
    }
}
