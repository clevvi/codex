use super::*;
use codex_protocol::error::CodexErrorDetails;

#[cfg(test)]
#[path = "start_if_idle_tests.rs"]
mod tests;

impl TurnRequestProcessor {
    pub(crate) async fn turn_start_if_idle(
        &self,
        request_id: &ConnectionRequestId,
        params: TurnStartIfIdleParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        validate_user_input_image_urls(&params.input)?;
        self.turn_start_if_idle_inner(request_id, params)
            .await
            .map(|response| Some(ClientResponsePayload::TurnStartIfIdle(response)))
    }

    async fn turn_start_if_idle_inner(
        &self,
        request_id: &ConnectionRequestId,
        params: TurnStartIfIdleParams,
    ) -> Result<TurnStartResponse, JSONRPCErrorError> {
        let (thread_id, thread) =
            self.load_thread(&params.thread_id)
                .await
                .inspect_err(|error| {
                    self.track_error_response(request_id, error, /*error_type*/ None);
                })?;
        self.ensure_direct_input_allowed(request_id, thread.as_ref())
            .await?;
        if let Err(error) = Self::validate_v2_input_limit(&params.input) {
            self.track_error_response(
                request_id,
                &error,
                Some(AnalyticsJsonRpcError::Input(InputError::TooLarge)),
            );
            return Err(error);
        }

        let mapped_items: Vec<CoreInputItem> = params
            .input
            .into_iter()
            .map(V2UserInput::into_core)
            .collect();
        let turn_has_input = !mapped_items.is_empty();
        let preconditions = StartIfIdlePreconditions::default()
            .with_expected_cwd(params.expected_cwd)
            .require_persistence()
            .disallow_plan_mode();

        let submission = thread
            .start_turn_if_idle_with_preconditions(
                TurnInputRequest::new(TurnInput::UserInput {
                    content: mapped_items,
                    client_id: params.client_user_message_id,
                })
                .with_trace(self.request_trace_context(request_id).await),
                preconditions,
            )
            .await
            .map_err(|err| {
                let error = match err.details() {
                    CodexErrorDetails::InvalidRequest(message) => invalid_request(message.clone()),
                    _ => internal_error(format!("failed to start idle-only turn: {err}")),
                };
                self.track_error_response(request_id, &error, /*error_type*/ None);
                error
            })?;

        let turn_id = match submission {
            StartIfIdlePreconditionSubmission::Started { turn_id } => turn_id,
            StartIfIdlePreconditionSubmission::NotSubmitted { reason } => {
                let error = Self::start_if_idle_rejection_error(reason);
                self.track_error_response(request_id, &error, /*error_type*/ None);
                return Err(error);
            }
            // `StartIfIdlePreconditionSubmission` is `#[non_exhaustive]` outside its
            // defining crate; `Started`/`NotSubmitted` above are its only known variants.
            _ => {
                let error = internal_error("idle-only admission returned an unrecognized result");
                self.track_error_response(request_id, &error, /*error_type*/ None);
                return Err(error);
            }
        };

        if turn_has_input {
            let config_snapshot = thread.config_snapshot().await;
            codex_memories_write::start_memories_startup_task(
                Arc::clone(&self.thread_manager),
                Arc::clone(&self.auth_manager),
                thread_id,
                Arc::clone(&thread),
                thread.config().await,
                config_snapshot.permission_profile,
                &config_snapshot.session_source,
            );
        }

        self.outgoing
            .record_request_turn_id(request_id, &turn_id)
            .await;
        Ok(Self::in_progress_turn_response(turn_id))
    }

    /// Maps every currently-known idle-start rejection reason explicitly. The trailing
    /// `_` arm exists only because `StartIfIdlePreconditionNotSubmittedReason` is
    /// `#[non_exhaustive]` outside its defining crate; it is unreachable for the
    /// preconditions this request sets. `EnvironmentNotReady` remains explicit because
    /// Core evaluates a starting primary environment before the cwd comparison.
    fn start_if_idle_rejection_error(
        reason: StartIfIdlePreconditionNotSubmittedReason,
    ) -> JSONRPCErrorError {
        match reason {
            StartIfIdlePreconditionNotSubmittedReason::NotIdle => invalid_request("busy"),
            StartIfIdlePreconditionNotSubmittedReason::PendingTriggerTurn => {
                invalid_request("pending trigger turn")
            }
            StartIfIdlePreconditionNotSubmittedReason::PlanMode => invalid_request("plan mode"),
            StartIfIdlePreconditionNotSubmittedReason::ExpectedCwdMismatch => {
                invalid_request("expectedCwd mismatch")
            }
            StartIfIdlePreconditionNotSubmittedReason::PersistenceDisabled => {
                invalid_request("persistence disabled")
            }
            // Preconditions never request field-level thread-settings overrides, so Core
            // cannot reject on this reason for this request.
            StartIfIdlePreconditionNotSubmittedReason::ThreadSettingsUnsupported => {
                internal_error("idle-only admission unexpectedly rejected thread settings")
            }
            StartIfIdlePreconditionNotSubmittedReason::EnvironmentNotReady => {
                invalid_request("environment not ready")
            }
            _ => internal_error(format!(
                "idle-only admission rejected for an unrecognized reason: {reason:?}"
            )),
        }
    }
}
