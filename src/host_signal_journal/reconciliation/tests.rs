use super::*;
use crate::host_signal_journal::tests::{admitted, binding, enqueue};

fn save(binding: &HostSignalBinding, event: &str) -> AdmittedSignal {
    let signal = enqueue(binding, event);
    let receipt = admitted(event);
    binding
        .open()
        .unwrap()
        .acknowledge(&signal.key, &receipt)
        .unwrap();
    AdmittedSignal {
        key: signal.key,
        receipt,
    }
}

fn queued(signal: &AdmittedSignal) -> StoredInputState {
    let WireContinuationStatus::Admitted { input_id, .. } = &signal.receipt.status else {
        panic!("admission")
    };
    let mut input = StoredInputState::new_accepted(meerkat_core::lifecycle::InputId(
        uuid::Uuid::parse_str(input_id).unwrap(),
    ));
    input.seed.phase = InputLifecycleState::Queued;
    input
}

fn stopped(signal: &AdmittedSignal) -> StoredInputState {
    let mut input = queued(signal);
    input.seed.phase = InputLifecycleState::Abandoned;
    input.seed.terminal_outcome = Some(InputTerminalOutcome::Abandoned {
        reason: InputAbandonReason::Stopped,
    });
    input
}

fn present(input: StoredInputState) -> NativeInputObservation {
    NativeInputObservation::Present(Box::new(input))
}

#[test]
fn admission_receipt_survives_acknowledgement_and_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let binding = binding(&temp);
    let signal = save(&binding, "first");
    let mut journal = binding.open().unwrap();
    assert!(journal.pending(1).unwrap().is_empty());
    assert_eq!(
        journal.admitted_for_reconciliation(1).unwrap(),
        vec![signal.clone()]
    );
    let mut conflicting = signal.receipt.clone();
    conflicting.committed_at_ms += 1;
    assert!(journal.acknowledge(&signal.key, &conflicting).is_err());
    assert_eq!(
        journal.admitted_for_reconciliation(1).unwrap(),
        vec![signal]
    );
}

#[test]
fn stopped_replacement_is_atomic_durable_and_idempotent_after_new_admission() {
    let temp = tempfile::tempdir().unwrap();
    let binding = binding(&temp);
    let signal = save(&binding, "first");
    let observation = present(stopped(&signal));
    assert!(matches!(
        binding
            .open()
            .unwrap()
            .reconcile_input(&signal, &observation)
            .unwrap(),
        ObservedInput::Requeued
    ));
    let replacement = binding.open().unwrap().pending(1).unwrap().remove(0);
    assert_ne!(replacement.key, signal.key);
    assert_eq!(replacement.envelope.content, "first");
    let proof: String = binding
        .open()
        .unwrap()
        .connection
        .query_row(
            "SELECT terminal_receipt FROM replacements WHERE previous_key = ?1",
            [&signal.key.0],
            |row| row.get(0),
        )
        .unwrap();
    let proof: AbandonedAdmission = serde_json::from_str(&proof).unwrap();
    assert_eq!(proof.admission, signal.receipt);
    assert!(stopped_before_run(&proof.input));
    let second = admitted("second-native-admission");
    binding
        .open()
        .unwrap()
        .acknowledge(&replacement.key, &second)
        .unwrap();
    assert!(matches!(
        binding
            .open()
            .unwrap()
            .reconcile_input(&signal, &observation)
            .unwrap(),
        ObservedInput::Requeued
    ));
    let saved = binding
        .open()
        .unwrap()
        .admitted_for_reconciliation(10)
        .unwrap();
    assert_eq!(
        saved,
        vec![AdmittedSignal {
            key: replacement.key,
            receipt: second
        }]
    );
    assert!(binding.open().unwrap().pending(1).unwrap().is_empty());
}

#[test]
fn failed_replacement_transaction_retains_original_admission_and_no_successor() {
    let temp = tempfile::tempdir().unwrap();
    let binding = binding(&temp);
    let signal = save(&binding, "first");
    let mut journal = binding.open().unwrap();
    journal.connection.execute_batch("CREATE TRIGGER reject_rekey BEFORE UPDATE OF delivery_key ON deliveries BEGIN SELECT RAISE(ABORT, 'fixture'); END;").unwrap();
    assert!(
        journal
            .reconcile_input(&signal, &present(stopped(&signal)))
            .is_err()
    );
    assert_eq!(
        journal.admitted_for_reconciliation(10).unwrap(),
        vec![signal]
    );
    assert_eq!(
        journal
            .connection
            .query_row("SELECT count(*) FROM replacements", [], |row| row
                .get::<_, u32>(0))
            .unwrap(),
        0
    );
    assert!(journal.pending(1).unwrap().is_empty());
}

#[test]
fn uncertain_or_previously_executed_inputs_never_gain_a_successor() {
    let temp = tempfile::tempdir().unwrap();
    let binding = binding(&temp);
    let signal = save(&binding, "first");
    let mut cases = Vec::new();
    for phase in [
        InputLifecycleState::Accepted,
        InputLifecycleState::Queued,
        InputLifecycleState::Staged,
        InputLifecycleState::Applied,
        InputLifecycleState::AppliedPendingConsumption,
    ] {
        let mut state = queued(&signal);
        state.seed.phase = phase;
        cases.push(state);
    }
    for reason in [
        InputAbandonReason::Cancelled,
        InputAbandonReason::Retired,
        InputAbandonReason::Reset,
        InputAbandonReason::Destroyed,
        InputAbandonReason::NeverExecuted,
        InputAbandonReason::ResumeRefused,
        InputAbandonReason::RequeueExpired,
        InputAbandonReason::ToolProcessInterrupted,
    ] {
        let mut state = stopped(&signal);
        state.seed.terminal_outcome = Some(InputTerminalOutcome::Abandoned { reason });
        cases.push(state);
    }
    let mut state = stopped(&signal);
    state.seed.last_run_id = Some(meerkat_core::lifecycle::RunId::new());
    cases.push(state);
    let mut state = stopped(&signal);
    state.seed.attempt_count = 1;
    cases.push(state);
    let mut state = stopped(&signal);
    state.seed.last_boundary_sequence = Some(1);
    cases.push(state);
    let mut state = stopped(&signal);
    state.state.stop_requeue = Some(meerkat_runtime::input_state::StopRequeueMark {
        stopped_at: chrono::Utc::now(),
        max_age_ms: 1000,
    });
    cases.push(state);
    let mut state = stopped(&signal);
    state
        .state
        .history
        .push(meerkat_runtime::InputStateHistoryEntry {
            timestamp: chrono::Utc::now(),
            from: InputLifecycleState::Applied,
            to: InputLifecycleState::Queued,
            reason: Some("ResolveHeldTailRequeue".into()),
        });
    cases.push(state);
    for state in cases {
        assert!(!matches!(
            binding
                .open()
                .unwrap()
                .reconcile_input(&signal, &present(state))
                .unwrap(),
            ObservedInput::Requeued
        ));
        assert!(binding.open().unwrap().pending(1).unwrap().is_empty());
    }
    assert_eq!(
        binding
            .open()
            .unwrap()
            .connection
            .query_row("SELECT count(*) FROM replacements", [], |row| row
                .get::<_, u32>(0))
            .unwrap(),
        0
    );
}

#[test]
fn consumed_input_settles_and_mismatched_input_cannot_change_receipt() {
    let temp = tempfile::tempdir().unwrap();
    let binding = binding(&temp);
    let signal = save(&binding, "first");
    let other = save(&binding, "other");
    assert!(
        binding
            .open()
            .unwrap()
            .reconcile_input(&signal, &present(stopped(&other)))
            .is_err()
    );
    let mut input = queued(&signal);
    input.seed.phase = InputLifecycleState::Consumed;
    input.seed.terminal_outcome = Some(InputTerminalOutcome::Consumed);
    assert!(matches!(
        binding
            .open()
            .unwrap()
            .reconcile_input(&signal, &present(input))
            .unwrap(),
        ObservedInput::Consumed
    ));
    assert_eq!(
        binding
            .open()
            .unwrap()
            .admitted_for_reconciliation(10)
            .unwrap(),
        vec![other]
    );
    assert!(binding.open().unwrap().pending(1).unwrap().is_empty());
}

#[tokio::test]
async fn unresolvable_input_is_persisted_and_not_polled_forever() {
    let temp = tempfile::tempdir().unwrap();
    let binding = binding(&temp);
    let signal = save(&binding, "first");
    let report = binding
        .reconcile_admitted(std::num::NonZeroU32::MIN, |_, _| {
            std::future::ready(Ok(NativeInputObservation::Unresolvable(
                InputUnavailable::MissingInput,
            )))
        })
        .await
        .unwrap();
    assert_eq!(report.unresolvable[0].admission, signal);
    assert_eq!(
        report.unresolvable[0].reason,
        InputUnavailable::MissingInput
    );
    let report = binding
        .reconcile_admitted(
            std::num::NonZeroU32::MIN,
            |_, _| -> std::future::Ready<Result<NativeInputObservation>> {
                panic!("a permanent missing input must not be queried again");
            },
        )
        .await
        .unwrap();
    assert_eq!(report.checked, 0);
    assert_eq!(report.unresolvable[0].admission, signal);
    let next = enqueue(&binding, "new-notification");
    let drained = binding
        .drain_pending(std::num::NonZeroU32::MIN, |_, saved| {
            assert_eq!(saved.key, next.key);
            std::future::ready(Ok(admitted("new-notification")))
        })
        .await
        .unwrap();
    assert_eq!(drained.acknowledged, 1);
}

#[tokio::test]
async fn transient_errors_rotate_fairly_and_native_reads_hold_no_sqlite_transaction() {
    let temp = tempfile::tempdir().unwrap();
    let binding = binding(&temp);
    let signals = [
        save(&binding, "first"),
        save(&binding, "second"),
        save(&binding, "third"),
    ];
    for signal in &signals {
        let result = binding
            .reconcile_admitted(std::num::NonZeroU32::MIN, |_, receipt| {
                assert_eq!(receipt, signal.receipt);
                enqueue(&binding, "arrived-during-observation");
                std::future::ready(Err(failure("transient native read failure")))
            })
            .await;
        assert!(result.is_err());
    }
    assert_eq!(
        binding
            .open()
            .unwrap()
            .admitted_for_reconciliation(1)
            .unwrap(),
        vec![signals[0].clone()]
    );
    assert_eq!(
        binding.open().unwrap().pending(1).unwrap()[0]
            .envelope
            .content,
        "arrived-during-observation"
    );
}

#[tokio::test]
async fn in_flight_input_does_not_block_later_notifications_or_reconciliation() {
    let temp = tempfile::tempdir().unwrap();
    let binding = binding(&temp);
    let first = save(&binding, "first");
    let later = enqueue(&binding, "completion-notification");
    let report = binding
        .reconcile_admitted(std::num::NonZeroU32::MIN, |_, receipt| {
            assert_eq!(receipt, first.receipt);
            std::future::ready(Ok(present(queued(&first))))
        })
        .await
        .unwrap();
    assert_eq!(report.consumed, 0);
    assert_eq!(
        binding
            .drain_pending(std::num::NonZeroU32::MIN, |_, signal| {
                assert_eq!(signal.key, later.key);
                std::future::ready(Ok(admitted("completion-notification")))
            })
            .await
            .unwrap()
            .acknowledged,
        1
    );
    // The older input may receive one more observation before the newly
    // admitted receipt, but cannot retain the head of the queue indefinitely.
    binding
        .reconcile_admitted(std::num::NonZeroU32::MIN, |_, receipt| {
            assert_eq!(receipt, first.receipt);
            std::future::ready(Ok(present(queued(&first))))
        })
        .await
        .unwrap();
    let observed = binding
        .open()
        .unwrap()
        .admitted_for_reconciliation(1)
        .unwrap();
    assert_eq!(
        observed[0].key, later.key,
        "a newer admitted input is not starved by old in-flight work"
    );
}

#[tokio::test]
async fn sustained_new_admissions_cannot_starve_an_observed_input() {
    let temp = tempfile::tempdir().unwrap();
    let binding = binding(&temp);
    let older = save(&binding, "older");
    binding
        .reconcile_admitted(std::num::NonZeroU32::MIN, |_, receipt| {
            assert_eq!(receipt, older.receipt);
            std::future::ready(Ok(present(queued(&older))))
        })
        .await
        .unwrap();
    let limit = std::num::NonZeroU32::new(32).unwrap();
    let mut recovered = 0;
    let mut arrivals = Vec::new();
    for pass in 0..4 {
        arrivals.extend(
            (0..limit.get()).map(|index| save(&binding, &format!("arrival-{pass}-{index}"))),
        );
        let report = binding
            .reconcile_admitted(limit, |_, receipt| {
                let state = if receipt == older.receipt {
                    stopped(&older)
                } else {
                    let signal = arrivals
                        .iter()
                        .find(|signal| signal.receipt == receipt)
                        .unwrap();
                    let mut state = queued(signal);
                    state.seed.phase = InputLifecycleState::Consumed;
                    state.seed.terminal_outcome = Some(InputTerminalOutcome::Consumed);
                    state
                };
                std::future::ready(Ok(present(state)))
            })
            .await
            .unwrap();
        recovered += report.requeued;
    }
    assert_eq!(
        recovered, 1,
        "older input must recover under sustained arrivals"
    );
    let pending = binding.open().unwrap().pending(32).unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].envelope.content, "older");
}

#[tokio::test]
async fn failure_or_cancellation_does_not_rotate_unvisited_rows() {
    for cancel in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let binding = binding(&temp);
        let first = save(&binding, "first");
        let second = save(&binding, "second");
        let third = save(&binding, "third");
        let limit = std::num::NonZeroU32::new(32).unwrap();
        let attempt = binding.reconcile_admitted(limit, |_, receipt| {
            assert_eq!(receipt, first.receipt);
            async move {
                if cancel {
                    std::future::pending::<()>().await;
                }
                Err(failure("unavailable native read"))
            }
        });
        if cancel {
            assert!(
                tokio::time::timeout(Duration::from_millis(20), attempt)
                    .await
                    .is_err()
            );
        } else {
            assert!(attempt.await.is_err());
        }
        let rows = binding
            .open()
            .unwrap()
            .admitted_for_reconciliation(32)
            .unwrap();
        assert_eq!(rows, vec![second, third, first]);
    }
}

#[test]
fn a_late_observation_cannot_reopen_consumed_or_unresolvable_work() {
    for missing in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let binding = binding(&temp);
        let signal = save(&binding, "first");
        let terminal = if missing {
            NativeInputObservation::Unresolvable(InputUnavailable::MissingInput)
        } else {
            let mut input = queued(&signal);
            input.seed.phase = InputLifecycleState::Consumed;
            input.seed.terminal_outcome = Some(InputTerminalOutcome::Consumed);
            present(input)
        };
        binding
            .open()
            .unwrap()
            .reconcile_input(&signal, &terminal)
            .unwrap();
        let stale = present(stopped(&signal));
        assert!(!matches!(
            binding
                .open()
                .unwrap()
                .reconcile_input(&signal, &stale)
                .unwrap(),
            ObservedInput::Requeued
        ));
        assert!(binding.open().unwrap().pending(1).unwrap().is_empty());
        assert!(
            binding
                .open()
                .unwrap()
                .admitted_for_reconciliation(1)
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn superseded_and_coalesced_are_explicit_terminals_without_resend() {
    let temp = tempfile::tempdir().unwrap();
    let binding = binding(&temp);
    let signal = save(&binding, "first");
    for (phase, outcome) in [
        (
            InputLifecycleState::Superseded,
            InputTerminalOutcome::Superseded {
                superseded_by: meerkat_core::lifecycle::InputId::new(),
            },
        ),
        (
            InputLifecycleState::Coalesced,
            InputTerminalOutcome::Coalesced {
                aggregate_id: meerkat_core::lifecycle::InputId::new(),
            },
        ),
    ] {
        let mut input = queued(&signal);
        input.seed.phase = phase;
        input.seed.terminal_outcome = Some(outcome);
        assert!(matches!(
            binding
                .open()
                .unwrap()
                .reconcile_input(&signal, &present(input))
                .unwrap(),
            ObservedInput::Attention
        ));
        assert!(binding.open().unwrap().pending(1).unwrap().is_empty());
    }
}
