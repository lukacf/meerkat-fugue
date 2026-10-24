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

fn binding(temp: &tempfile::TempDir) -> HostSignalBinding {
    HostSignalBinding {
        path: temp.path().join("signals.sqlite3"),
        scope: scope(),
    }
}

// Controlled sender results qualify journal mechanics only, not native delivery.
fn admitted(id: &str) -> meerkat_contracts::WireContinuationReceipt {
    meerkat_contracts::WireContinuationReceipt {
        delivery_id: id.into(),
        committed_at_ms: 1,
        status: meerkat_contracts::WireContinuationStatus::Admitted {
            session_id: "test-session".into(),
            input_id: "test-input".into(),
        },
    }
}

#[tokio::test]
async fn native_receipts_without_admission_preserve_pending_work_and_their_status() {
    use meerkat_contracts::{WireContinuationReceipt, WireContinuationStatus};
    for status in [
        WireContinuationStatus::Pending,
        WireContinuationStatus::Stranded,
        WireContinuationStatus::Refused {
            reason: "authority refused".into(),
        },
    ] {
        let temp = tempfile::tempdir().unwrap();
        let binding = binding(&temp);
        let first = enqueue(&binding, "first");
        let second = enqueue(&binding, "second");
        let third = enqueue(&binding, "third");
        let receipt = WireContinuationReceipt {
            delivery_id: "committed-but-not-admitted".into(),
            committed_at_ms: 1,
            status,
        };
        let progress = binding
            .drain_pending(std::num::NonZeroU32::new(10).unwrap(), |_, signal| {
                let response = if signal.key == first.key {
                    admitted("admitted-delivery")
                } else {
                    assert_eq!(signal.key, second.key, "do not overtake deferred delivery");
                    receipt.clone()
                };
                std::future::ready(Ok(response))
            })
            .await
            .unwrap();
        assert_eq!(progress.acknowledged, 1);
        assert_eq!(
            progress.deferred,
            Some(DeferredSignal {
                key: second.key.clone(),
                receipt
            })
        );
        assert_eq!(
            binding.open().unwrap().pending(10).unwrap(),
            vec![second, third]
        );
    }
}

fn enqueue(binding: &HostSignalBinding, event: &str) -> PendingSignal {
    let mut journal = binding.open().unwrap();
    let mut update = journal.begin().unwrap();
    let key = update.enqueue_once(event, || Ok(message(event))).unwrap();
    update.commit().unwrap();
    journal
        .pending(100)
        .unwrap()
        .into_iter()
        .find(|row| row.key == key)
        .unwrap()
}

#[tokio::test]
async fn drain_retries_the_exact_saved_request_after_admission_failure() {
    let temp = tempfile::tempdir().unwrap();
    let binding = binding(&temp);
    let first = enqueue(&binding, "event-1");
    let result = binding
        .drain_pending(std::num::NonZeroU32::new(10).unwrap(), |scope, signal| {
            assert_eq!(scope, binding.scope);
            assert_eq!(signal, first);
            std::future::ready(Err(failure("native admission unavailable")))
        })
        .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("native admission unavailable")
    );
    assert_eq!(
        binding.open().unwrap().pending(10).unwrap(),
        vec![first.clone()]
    );
    let reopened = HostSignalBinding::decode(&binding.encode().unwrap()).unwrap();
    assert_eq!(
        reopened
            .drain_pending(std::num::NonZeroU32::new(10).unwrap(), |scope, signal| {
                assert_eq!(scope, binding.scope);
                assert_eq!(signal, first);
                std::future::ready(Ok(admitted("native-delivery-1")))
            })
            .await
            .unwrap(),
        DrainProgress {
            acknowledged: 1,
            deferred: None
        }
    );
    assert!(binding.open().unwrap().pending(10).unwrap().is_empty());
}

#[tokio::test]
async fn cancelling_a_drain_keeps_unacknowledged_work() {
    let temp = tempfile::tempdir().unwrap();
    let binding = binding(&temp);
    let first = enqueue(&binding, "event-1");
    let started = tokio::sync::Notify::new();
    let drain = binding.drain_pending(std::num::NonZeroU32::new(10).unwrap(), |_, _| {
        started.notify_one();
        std::future::pending::<Result<meerkat_contracts::WireContinuationReceipt>>()
    });
    {
        tokio::pin!(drain);
        tokio::select! {
            result = &mut drain => panic!("admission must still be pending: {result:?}"),
            () = started.notified() => {}
        }
    }
    assert_eq!(binding.open().unwrap().pending(10).unwrap(), vec![first]);
}

#[tokio::test]
async fn drain_releases_database_locks_and_preserves_new_work() {
    let temp = tempfile::tempdir().unwrap();
    let binding = binding(&temp);
    let first = enqueue(&binding, "event-1");
    let (entered, admission) = tokio::sync::oneshot::channel();
    let (finish, release) = tokio::sync::oneshot::channel();
    let sender_binding = binding.clone();
    let first_copy = first.clone();
    let drain = tokio::spawn(async move {
        let mut entered = Some(entered);
        let mut release = Some(release);
        sender_binding
            .drain_pending(std::num::NonZeroU32::new(1).unwrap(), |scope, signal| {
                assert_eq!(scope, sender_binding.scope);
                assert_eq!(signal, first_copy);
                entered.take().unwrap().send(()).unwrap();
                let release = release.take().unwrap();
                async move {
                    release.await.unwrap();
                    Ok(admitted("native-delivery-1"))
                }
            })
            .await
    });
    admission.await.unwrap();
    let second = enqueue(&binding, "event-2");
    finish.send(()).unwrap();
    assert_eq!(
        drain.await.unwrap().unwrap(),
        DrainProgress {
            acknowledged: 1,
            deferred: None
        }
    );
    assert_eq!(binding.open().unwrap().pending(10).unwrap(), vec![second]);
}

#[tokio::test]
async fn drain_acknowledgment_failure_retries_the_original_admission_key() {
    let temp = tempfile::tempdir().unwrap();
    let binding = binding(&temp);
    let first = enqueue(&binding, "event-1");
    let journal = binding.open().unwrap();
    journal.connection.execute_batch(
        "CREATE TRIGGER fail_ack BEFORE UPDATE ON deliveries BEGIN SELECT RAISE(ABORT, 'ack unavailable'); END;"
    ).unwrap();
    drop(journal);
    let result = binding
        .drain_pending(std::num::NonZeroU32::new(1).unwrap(), |_, signal| {
            assert_eq!(signal, first);
            std::future::ready(Ok(admitted("native-delivery-1")))
        })
        .await;
    assert!(result.unwrap_err().to_string().contains("ack unavailable"));
    let journal = binding.open().unwrap();
    assert_eq!(journal.pending(10).unwrap(), vec![first.clone()]);
    journal
        .connection
        .execute_batch("DROP TRIGGER fail_ack;")
        .unwrap();
    drop(journal);
    assert_eq!(
        binding
            .drain_pending(std::num::NonZeroU32::new(1).unwrap(), |_, signal| {
                assert_eq!(signal, first);
                std::future::ready(Ok(admitted("native-delivery-1")))
            })
            .await
            .unwrap(),
        DrainProgress {
            acknowledged: 1,
            deferred: None
        }
    );
    assert!(binding.open().unwrap().pending(10).unwrap().is_empty());
}

#[tokio::test]
async fn drain_refuses_empty_receipts_and_does_not_retarget_saved_work() {
    let temp = tempfile::tempdir().unwrap();
    let binding = binding(&temp);
    let first = enqueue(&binding, "event-1");
    let result = binding
        .drain_pending(std::num::NonZeroU32::new(1).unwrap(), |_, _| {
            std::future::ready(Ok(admitted(" ")))
        })
        .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("native delivery receipt is empty")
    );
    let mut other = binding.clone();
    other.scope.mob_id = "another-repository".into();
    let result = other
        .drain_pending(
            std::num::NonZeroU32::new(1).unwrap(),
            |_, _| -> std::future::Ready<Result<meerkat_contracts::WireContinuationReceipt>> {
                panic!("a changed target must be refused before native admission");
            },
        )
        .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("another repository, runtime or tracker scope")
    );
    assert_eq!(binding.open().unwrap().pending(10).unwrap(), vec![first]);
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
