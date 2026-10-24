//! Reconcile host delivery intent against canonical native input observations.
//! This is not an execution scheduler: only a proven never-run stop permits a
//! new admission key. Native inputs retain their own lifecycle authority.

use super::*;
use meerkat_contracts::{WireContinuationReceipt, WireContinuationStatus};
use meerkat_runtime::input_state::StoredInputState;
use meerkat_runtime::{InputAbandonReason, InputLifecycleState, InputTerminalOutcome};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmittedSignal {
    pub key: DeliveryKey,
    pub receipt: WireContinuationReceipt,
}

#[derive(Debug)]
pub struct InputAttention {
    pub admission: AdmittedSignal,
    pub phase: InputLifecycleState,
    pub outcome: Option<InputTerminalOutcome>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
pub enum InputUnavailable {
    #[error("native session service is unavailable")]
    MissingSessionService,
    #[error("native runtime adapter is unavailable")]
    MissingRuntimeAdapter,
    #[error("admitted native input is missing")]
    MissingInput,
    #[error("admitted native runtime is missing")]
    MissingRuntime,
    #[error("admitted native runtime was destroyed")]
    DestroyedRuntime,
}

#[derive(Debug)]
pub enum NativeInputObservation {
    Present(Box<StoredInputState>),
    Unresolvable(InputUnavailable),
}

#[derive(Debug)]
pub struct UnresolvableSignal {
    pub admission: AdmittedSignal,
    pub reason: InputUnavailable,
}

#[derive(Debug, Default)]
pub struct ReconciliationProgress {
    pub checked: u32,
    pub consumed: u32,
    pub requeued: u32,
    pub attention: Vec<InputAttention>,
    pub unresolvable: Vec<UnresolvableSignal>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AbandonedAdmission {
    admission: WireContinuationReceipt,
    input: StoredInputState,
}

enum ObservedInput {
    Pending,
    Consumed,
    Requeued,
    Attention,
    Unresolvable,
}

pub(super) fn validate_admission(receipt: &WireContinuationReceipt) -> Result<()> {
    if receipt.delivery_id.trim().is_empty() {
        return Err(failure("native delivery receipt is empty"));
    }
    let WireContinuationStatus::Admitted {
        session_id,
        input_id,
    } = &receipt.status
    else {
        return Err(failure("expected a native admission receipt"));
    };
    meerkat_core::SessionId::parse(session_id)
        .map_err(|_| failure("native admission has an invalid session id"))?;
    uuid::Uuid::parse_str(input_id)
        .map_err(|_| failure("native admission has an invalid input id"))?;
    Ok(())
}

fn stopped_before_run(input: &StoredInputState) -> bool {
    input.seed.phase == InputLifecycleState::Abandoned
        && input.seed.terminal_outcome == Some(InputTerminalOutcome::Abandoned {
            reason: InputAbandonReason::Stopped,
        })
        && input.seed.last_run_id.is_none()
        && input.seed.last_boundary_sequence.is_none()
        && input.seed.attempt_count == 0
        && input.state.stop_requeue.is_none()
        // The native repair operation clears run association for a held tail.
        // It can therefore resemble a never-run input after a later stop.
        // This is the exact native transition marker, not task prose.
        && !input.state.history().iter().any(|entry|
            entry.reason.as_deref() == Some("ResolveHeldTailRequeue"))
}

impl HostSignalJournal {
    /// Snapshot a bounded fair page. Only an actually attempted read rotates
    /// its position; unvisited rows must keep priority when an earlier read
    /// fails or hangs. No database connection crosses a native read's await.
    pub fn admitted_for_reconciliation(&self, limit: u32) -> Result<Vec<AdmittedSignal>> {
        let mut query = self
            .connection
            .prepare(
                "SELECT delivery_key, admitted_receipt, accepted_delivery_id FROM deliveries
             WHERE accepted_delivery_id IS NOT NULL AND consumed = 0 AND unresolvable IS NULL
             ORDER BY last_checked, sequence LIMIT ?1",
            )
            .map_err(failure)?;
        let rows = query
            .query_map([limit], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(failure)?;
        rows.map(|row| {
            let (key, saved, delivery_id) = row.map_err(failure)?;
            let receipt: WireContinuationReceipt = serde_json::from_str(&saved)
                .map_err(|_| failure("saved native admission is invalid"))?;
            validate_admission(&receipt)?;
            if key.is_empty() || receipt.delivery_id != delivery_id {
                return Err(failure(
                    "saved native admission does not match its delivery",
                ));
            }
            Ok(AdmittedSignal {
                key: DeliveryKey(key),
                receipt,
            })
        })
        .collect()
    }

    /// A scheduling cursor for observation only. This is deliberately committed
    /// before await so a hung read loses priority without claiming consumption.
    fn mark_admission_checked(&mut self, signal: &AdmittedSignal) -> Result<bool> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(failure)?;
        let revision: i64 = transaction
            .query_row(
                "SELECT reconciliation_revision FROM metadata WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(failure)?;
        let next = revision
            .checked_add(1)
            .ok_or_else(|| failure("reconciliation revision exhausted"))?;
        let changed = transaction.execute(
            "UPDATE deliveries SET last_checked = ?1 WHERE delivery_key = ?2 AND admitted_receipt = ?3
             AND consumed = 0 AND unresolvable IS NULL", params![next, signal.key.0, encode(&signal.receipt)?],
        ).map_err(failure)?;
        if changed == 0 {
            return Ok(false);
        }
        transaction
            .execute(
                "UPDATE metadata SET reconciliation_revision = ?1 WHERE singleton = 1",
                [next],
            )
            .map_err(failure)?;
        transaction.commit().map_err(failure)?;
        Ok(true)
    }

    fn reconcile_input(
        &mut self,
        signal: &AdmittedSignal,
        observation: &NativeInputObservation,
    ) -> Result<ObservedInput> {
        validate_admission(&signal.receipt)?;
        if let NativeInputObservation::Present(input) = observation {
            let WireContinuationStatus::Admitted { input_id, .. } = &signal.receipt.status else {
                return Err(failure("expected native admission"));
            };
            if input.state.input_id.to_string() != *input_id {
                return Err(failure("native input observation does not match admission"));
            }
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(failure)?;
        let saved: Option<(Option<String>, bool, Option<String>)> = transaction
            .query_row(
                "SELECT admitted_receipt, consumed, unresolvable FROM deliveries WHERE delivery_key = ?1",
                [&signal.key.0],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(failure)?;
        match saved {
            Some((Some(saved), consumed, unresolvable)) if saved == encode(&signal.receipt)? => {
                if consumed {
                    return Ok(ObservedInput::Consumed);
                }
                if unresolvable.is_some() {
                    return Ok(ObservedInput::Unresolvable);
                }
            }
            None => {
                // A concurrent observer may already have committed the same
                // replacement. It cannot erase or redirect that evidence.
                let evidence: Option<String> = transaction
                    .query_row(
                        "SELECT terminal_receipt FROM replacements WHERE previous_key = ?1",
                        [&signal.key.0],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(failure)?;
                if let Some(evidence) = evidence {
                    let proof: AbandonedAdmission = serde_json::from_str(&evidence)
                        .map_err(|_| failure("saved abandoned admission proof is invalid"))?;
                    if proof.admission == signal.receipt && stopped_before_run(&proof.input) {
                        return Ok(ObservedInput::Requeued);
                    }
                }
                return Err(failure("admission key is no longer recorded"));
            }
            _ => return Err(failure("native observation conflicts with saved admission")),
        }
        let input = match observation {
            NativeInputObservation::Present(input) => input,
            NativeInputObservation::Unresolvable(reason) => {
                transaction
                    .execute(
                        "UPDATE deliveries SET unresolvable = ?1 WHERE delivery_key = ?2",
                        params![encode(reason)?, signal.key.0],
                    )
                    .map_err(failure)?;
                transaction.commit().map_err(failure)?;
                return Ok(ObservedInput::Unresolvable);
            }
        };
        if stopped_before_run(input) {
            use sha2::{Digest, Sha256};
            let identity = encode(&(&signal.key.0, &signal.receipt.delivery_id))?;
            let replacement = format!("fugue-retry-{:x}", Sha256::digest(identity.as_bytes()));
            let proof = encode(&AbandonedAdmission {
                admission: signal.receipt.clone(),
                input: input.as_ref().clone(),
            })?;
            transaction.execute(
                "INSERT INTO replacements (previous_key, replacement_key, terminal_receipt) VALUES (?1, ?2, ?3)",
                params![signal.key.0, replacement, proof],
            ).map_err(failure)?;
            transaction.execute(
                "UPDATE deliveries SET delivery_key = ?1, accepted_delivery_id = NULL, admitted_receipt = NULL,
                 consumed = 0, terminal_input_state = NULL, unresolvable = NULL, last_checked = 0 WHERE delivery_key = ?2",
                params![replacement, signal.key.0],
            ).map_err(failure)?;
            transaction.commit().map_err(failure)?;
            return Ok(ObservedInput::Requeued);
        }
        let consumed = input.seed.phase == InputLifecycleState::Consumed
            && input.seed.terminal_outcome == Some(InputTerminalOutcome::Consumed);
        let nonterminal = matches!(
            input.seed.phase,
            InputLifecycleState::Accepted
                | InputLifecycleState::Queued
                | InputLifecycleState::Staged
                | InputLifecycleState::Applied
                | InputLifecycleState::AppliedPendingConsumption
        );
        if nonterminal && input.seed.terminal_outcome.is_none() {
            return Ok(ObservedInput::Pending);
        }
        // Retain the canonical terminal observation, including all refusals to
        // retry. Consumed is prompt consumption, not a claim of task success.
        transaction.execute(
            "UPDATE deliveries SET consumed = ?1, terminal_input_state = ?2 WHERE delivery_key = ?3",
            params![consumed, encode(input)?, signal.key.0],
        ).map_err(failure)?;
        transaction.commit().map_err(failure)?;
        Ok(if consumed {
            ObservedInput::Consumed
        } else {
            ObservedInput::Attention
        })
    }

    fn unresolvable_admissions(&self, limit: u32) -> Result<Vec<UnresolvableSignal>> {
        let mut query = self
            .connection
            .prepare(
                "SELECT delivery_key, admitted_receipt, unresolvable FROM deliveries
             WHERE unresolvable IS NOT NULL ORDER BY sequence LIMIT ?1",
            )
            .map_err(failure)?;
        let rows = query
            .query_map([limit], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(failure)?;
        rows.map(|row| {
            let (key, receipt, reason) = row.map_err(failure)?;
            let receipt = serde_json::from_str(&receipt)
                .map_err(|_| failure("saved admission is invalid"))?;
            validate_admission(&receipt)?;
            Ok(UnresolvableSignal {
                admission: AdmittedSignal {
                    key: DeliveryKey(key),
                    receipt,
                },
                reason: serde_json::from_str(&reason)
                    .map_err(|_| failure("saved unresolvable reason is invalid"))?,
            })
        })
        .collect()
    }
}

impl HostSignalBinding {
    /// Read native input state independently of admission so an in-flight turn
    /// never blocks newer notifications. The caller supplies the canonical
    /// runtime read for this exact repository and original receipt IDs.
    pub async fn reconcile_admitted<F, Fut>(
        &self,
        limit: std::num::NonZeroU32,
        mut observe: F,
    ) -> Result<ReconciliationProgress>
    where
        F: FnMut(HostSignalScope, WireContinuationReceipt) -> Fut,
        Fut: std::future::Future<Output = Result<NativeInputObservation>>,
    {
        let admitted = self.open()?.admitted_for_reconciliation(limit.get())?;
        let mut progress = ReconciliationProgress::default();
        for signal in admitted {
            if !self.open()?.mark_admission_checked(&signal)? {
                continue;
            }
            let input = observe(self.scope.clone(), signal.receipt.clone()).await?;
            let outcome = self.open()?.reconcile_input(&signal, &input)?;
            progress.checked += 1;
            match outcome {
                ObservedInput::Pending => {}
                ObservedInput::Consumed => progress.consumed += 1,
                ObservedInput::Requeued => progress.requeued += 1,
                ObservedInput::Unresolvable => {}
                ObservedInput::Attention => {
                    if let NativeInputObservation::Present(input) = input {
                        progress.attention.push(InputAttention {
                            admission: signal,
                            phase: input.seed.phase,
                            outcome: input.seed.terminal_outcome,
                        });
                    }
                }
            }
        }
        progress.unresolvable = self.open()?.unresolvable_admissions(limit.get())?;
        Ok(progress)
    }
}

#[cfg(test)]
mod tests;
