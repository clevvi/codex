use super::*;
use crate::session::tests::make_session_and_context_with_rx;
use crate::state::TaskKind;
use crate::tasks::SessionTask;
use crate::tasks::SessionTaskResult;
use codex_exec_server::LOCAL_ENVIRONMENT_ID;
use codex_exec_server::REMOTE_ENVIRONMENT_ID;
use codex_protocol::AgentPath;
use codex_protocol::config_types::CollaborationMode;
use codex_protocol::config_types::ModeKind;
use codex_protocol::config_types::Settings;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::ThreadSettingsOverrides;
use codex_protocol::protocol::TurnAbortReason;
use codex_protocol::protocol::TurnEnvironmentSelection;
use codex_protocol::protocol::TurnEnvironmentSelections;
use codex_protocol::turn_input::StartIfIdlePreconditionNotSubmittedReason;
use codex_protocol::turn_input::StartIfIdlePreconditionSubmission;
use codex_protocol::turn_input::StartIfIdlePreconditions;
use codex_protocol::turn_input::TurnInput as SubmittedTurnInput;
use codex_protocol::user_input::UserInput;
use codex_utils_absolute_path::AbsolutePathBuf;
use codex_utils_path_uri::PathUri;
use core_test_support::test_codex::local_selections;
use pretty_assertions::assert_eq;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::time::sleep;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy)]
struct NeverEndingTask {
    kind: TaskKind,
    listen_to_cancellation_token: bool,
}

impl SessionTask for NeverEndingTask {
    fn kind(&self) -> TaskKind {
        self.kind
    }

    fn span_name(&self) -> &'static str {
        "session_task.turn_input_test"
    }

    async fn run(
        self: Arc<Self>,
        _session: Arc<Session>,
        _ctx: Arc<TurnContext>,
        _input: Vec<TurnInput>,
        cancellation_token: CancellationToken,
    ) -> SessionTaskResult {
        if self.listen_to_cancellation_token {
            cancellation_token.cancelled().await;
            return Ok(None);
        }
        loop {
            sleep(std::time::Duration::from_secs(60)).await;
        }
    }
}

fn user_message(text: &str) -> ResponseItem {
    ResponseItem::Message {
        id: None,
        role: "user".to_string(),
        content: vec![ContentItem::InputText {
            text: text.to_string(),
        }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    }
}

async fn submit_start_only(
    session: &Arc<Session>,
    input: SubmittedTurnInput,
) -> TurnInputSubmission {
    handle(
        session,
        TurnInputRequest::new(input),
        TurnInputMode::StartIfIdle,
        "test-submission".to_string(),
    )
    .await
    .expect("start-only submission should be valid")
}

async fn submit_start_only_with_preconditions(
    session: &Arc<Session>,
    input: SubmittedTurnInput,
    preconditions: StartIfIdlePreconditions,
) -> StartIfIdlePreconditionSubmission {
    submit_start_only_request_with_preconditions(
        session,
        TurnInputRequest::new(input),
        preconditions,
    )
    .await
}

async fn submit_start_only_request_with_preconditions(
    session: &Arc<Session>,
    request: TurnInputRequest,
    preconditions: StartIfIdlePreconditions,
) -> StartIfIdlePreconditionSubmission {
    start_if_idle_with_preconditions(
        session,
        request,
        preconditions,
        "test-submission".to_string(),
    )
    .await
    .expect("preconditioned start-only submission should be valid")
}

async fn submit_steer_only(
    session: &Arc<Session>,
    input: Vec<UserInput>,
    expected_turn_id: &str,
) -> TurnInputSubmission {
    handle(
        session,
        TurnInputRequest::new(SubmittedTurnInput::UserInput {
            content: input,
            client_id: None,
        }),
        TurnInputMode::Steer {
            expected_turn_id: expected_turn_id.to_string(),
        },
        "test-submission".to_string(),
    )
    .await
    .expect("steer-only submission should be valid")
}

#[tokio::test]
async fn accepted_input_applies_thread_settings() {
    let (session, turn_context, _rx) = make_session_and_context_with_rx().await;
    let config = session.get_config().await;
    handle(
        &session,
        TurnInputRequest::user_input(vec![UserInput::Text {
            text: "hello".to_string(),
            text_elements: Vec::new(),
        }])
        .with_thread_settings(ThreadSettingsOverrides {
            environments: Some(local_selections(config.cwd.clone())),
            approval_policy: Some(config.permissions.approval_policy.value()),
            approvals_reviewer: Some(codex_config::types::ApprovalsReviewer::AutoReview),
            sandbox_policy: Some(config.legacy_sandbox_policy()),
            summary: config.model_reasoning_summary,
            personality: config.personality,
            collaboration_mode: Some(CollaborationMode {
                mode: ModeKind::Default,
                settings: Settings {
                    model: turn_context.model_info.slug.clone(),
                    reasoning_effort: config.model_reasoning_effort.clone(),
                    developer_instructions: None,
                },
            }),
            ..Default::default()
        }),
        TurnInputMode::StartOrSteer,
        "sub-1".to_string(),
    )
    .await
    .expect("submit user turn");

    let state = session.state.lock().await;
    assert_eq!(
        state.session_configuration.approvals_reviewer,
        codex_config::types::ApprovalsReviewer::AutoReview
    );
    assert!(
        session.mcp_refresh.is_pending(),
        "server elicitation authority changes must refresh MCP state"
    );
}

#[tokio::test]
async fn start_only_rejects_active_turn_without_injecting() {
    let (session, turn_context, _rx) = make_session_and_context_with_rx().await;
    session
        .spawn_task(
            Arc::clone(&turn_context),
            Vec::new(),
            NeverEndingTask {
                kind: TaskKind::Regular,
                listen_to_cancellation_token: true,
            },
        )
        .await;

    let input = SubmittedTurnInput::ResponseItem(user_message("synthetic idle input"));
    let submission = submit_start_only(&session, input).await;

    assert_eq!(
        TurnInputSubmission::NotSubmitted {
            reason: NotSubmittedReason::NotIdle,
        },
        submission
    );
    assert_eq!(
        (Vec::<TurnInput>::new(), None, None),
        session
            .input_queue
            .get_pending_input(&session.active_turn)
            .await
    );

    session.abort_all_tasks(TurnAbortReason::Interrupted).await;
}

#[tokio::test]
async fn recovery_rejects_active_turn_without_injecting_or_applying_settings() {
    let (session, turn_context, _rx) = make_session_and_context_with_rx().await;
    let original_approval_policy = session
        .get_config()
        .await
        .permissions
        .approval_policy
        .value();
    session
        .spawn_task(
            Arc::clone(&turn_context),
            Vec::new(),
            NeverEndingTask {
                kind: TaskKind::Regular,
                listen_to_cancellation_token: true,
            },
        )
        .await;

    let submission = handle_recovery(
        &session,
        ThreadSettingsOverrides {
            approval_policy: Some(AskForApproval::Never),
            ..Default::default()
        },
        "recovered-turn".to_string(),
    )
    .await
    .expect("recovery should return a typed rejection");

    assert_eq!(
        submission,
        TurnInputSubmission::NotSubmitted {
            reason: NotSubmittedReason::NotIdle,
        }
    );
    assert_eq!(
        session
            .get_config()
            .await
            .permissions
            .approval_policy
            .value(),
        original_approval_policy
    );
    assert_eq!(
        session
            .input_queue
            .get_pending_input(&session.active_turn)
            .await,
        (Vec::<TurnInput>::new(), None, None)
    );

    session.abort_all_tasks(TurnAbortReason::Interrupted).await;
}

#[tokio::test]
async fn start_only_rejects_plan_mode_without_injecting() {
    let (session, _turn_context, _rx) = make_session_and_context_with_rx().await;
    let mut collaboration_mode = session.collaboration_mode().await;
    collaboration_mode.mode = ModeKind::Plan;
    {
        let mut state = session.state.lock().await;
        state.session_configuration.collaboration_mode = collaboration_mode;
    }

    let submission = submit_start_only(
        &session,
        SubmittedTurnInput::ResponseItem(user_message("synthetic idle input")),
    )
    .await;

    assert_eq!(
        TurnInputSubmission::NotSubmitted {
            reason: NotSubmittedReason::PlanMode,
        },
        submission
    );
    assert!(session.active_turn.lock().await.is_none());
    assert_eq!(
        (Vec::<TurnInput>::new(), None, None),
        session
            .input_queue
            .get_pending_input(&session.active_turn)
            .await
    );
}

#[tokio::test]
async fn start_only_accepts_user_input_in_plan_mode() {
    let (session, _turn_context, _rx) = make_session_and_context_with_rx().await;
    let mut collaboration_mode = session.collaboration_mode().await;
    collaboration_mode.mode = ModeKind::Plan;
    {
        let mut state = session.state.lock().await;
        state.session_configuration.collaboration_mode = collaboration_mode;
        state.merge_connector_selection(["calendar".to_string()]);
    }

    let submission = submit_start_only(
        &session,
        SubmittedTurnInput::UserInput {
            content: vec![UserInput::Text {
                text: "queued user input".to_string(),
                text_elements: Vec::new(),
            }],
            client_id: Some("queued-user-message".to_string()),
        },
    )
    .await;
    assert!(matches!(submission, TurnInputSubmission::Started { .. }));
    assert!(
        session
            .state
            .lock()
            .await
            .get_connector_selection()
            .is_empty()
    );

    session.abort_all_tasks(TurnAbortReason::Interrupted).await;
}

#[tokio::test]
async fn start_only_with_preconditions_defaults_preserve_idle_user_input_admission() {
    let (session, _turn_context, _rx) = make_session_and_context_with_rx().await;

    let submission = submit_start_only_with_preconditions(
        &session,
        SubmittedTurnInput::UserInput {
            content: vec![UserInput::Text {
                text: "default preconditions admit user input".to_string(),
                text_elements: Vec::new(),
            }],
            client_id: None,
        },
        StartIfIdlePreconditions::default(),
    )
    .await;

    assert_eq!(
        StartIfIdlePreconditionSubmission::Started {
            turn_id: "test-submission".to_string(),
        },
        submission
    );

    session.abort_all_tasks(TurnAbortReason::Interrupted).await;
}

#[tokio::test]
async fn start_only_requires_persistence_without_reserving_or_injecting() {
    let (session, _turn_context, rx) = make_session_and_context_with_rx().await;

    let submission = submit_start_only_with_preconditions(
        &session,
        SubmittedTurnInput::UserInput {
            content: vec![UserInput::Text {
                text: "persistence required".to_string(),
                text_elements: Vec::new(),
            }],
            client_id: None,
        },
        StartIfIdlePreconditions::default().require_persistence(),
    )
    .await;

    assert_eq!(
        StartIfIdlePreconditionSubmission::NotSubmitted {
            reason: StartIfIdlePreconditionNotSubmittedReason::PersistenceDisabled,
        },
        submission
    );
    assert!(session.active_turn.lock().await.is_none());
    assert_eq!(
        (Vec::<TurnInput>::new(), None, None),
        session
            .input_queue
            .get_pending_input(&session.active_turn)
            .await
    );
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn start_only_disallows_plan_mode_for_user_input_without_reserving_or_injecting() {
    let (session, _turn_context, rx) = make_session_and_context_with_rx().await;
    let mut collaboration_mode = session.collaboration_mode().await;
    collaboration_mode.mode = ModeKind::Plan;
    {
        let mut state = session.state.lock().await;
        state.session_configuration.collaboration_mode = collaboration_mode;
    }

    let submission = submit_start_only_with_preconditions(
        &session,
        SubmittedTurnInput::UserInput {
            content: vec![UserInput::Text {
                text: "plan mode is disallowed".to_string(),
                text_elements: Vec::new(),
            }],
            client_id: None,
        },
        StartIfIdlePreconditions::default().disallow_plan_mode(),
    )
    .await;

    assert_eq!(
        StartIfIdlePreconditionSubmission::NotSubmitted {
            reason: StartIfIdlePreconditionNotSubmittedReason::PlanMode,
        },
        submission
    );
    assert!(session.active_turn.lock().await.is_none());
    assert_eq!(
        (Vec::<TurnInput>::new(), None, None),
        session
            .input_queue
            .get_pending_input(&session.active_turn)
            .await
    );
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn start_only_clears_cwd_mismatch_reservation_before_matching_request() {
    let (session, _turn_context, _rx) = make_session_and_context_with_rx().await;
    let matching_cwd = session.thread_config_snapshot().await.cwd().clone();
    let mismatched_cwd =
        AbsolutePathBuf::from_absolute_path("/tmp/other-cwd").expect("absolute cwd");

    let rejected = submit_start_only_with_preconditions(
        &session,
        SubmittedTurnInput::UserInput {
            content: vec![UserInput::Text {
                text: "wrong cwd".to_string(),
                text_elements: Vec::new(),
            }],
            client_id: None,
        },
        StartIfIdlePreconditions::default().with_expected_cwd(mismatched_cwd),
    )
    .await;

    assert_eq!(
        StartIfIdlePreconditionSubmission::NotSubmitted {
            reason: StartIfIdlePreconditionNotSubmittedReason::ExpectedCwdMismatch,
        },
        rejected
    );
    assert!(session.active_turn.lock().await.is_none());

    let accepted = submit_start_only_with_preconditions(
        &session,
        SubmittedTurnInput::UserInput {
            content: vec![UserInput::Text {
                text: "matching cwd".to_string(),
                text_elements: Vec::new(),
            }],
            client_id: None,
        },
        StartIfIdlePreconditions::default().with_expected_cwd(matching_cwd),
    )
    .await;

    assert_eq!(
        StartIfIdlePreconditionSubmission::Started {
            turn_id: "test-submission".to_string(),
        },
        accepted
    );

    session.abort_all_tasks(TurnAbortReason::Interrupted).await;
}

#[tokio::test]
async fn start_only_rejects_unknown_first_environment_when_ready_primary_cwd_differs() {
    let (session, _turn_context, rx) = make_session_and_context_with_rx().await;
    let workspace = tempfile::tempdir().expect("create workspace");
    let unknown_cwd = AbsolutePathBuf::try_from(workspace.path().join("unknown"))
        .expect("unknown cwd is absolute");
    let ready_cwd =
        AbsolutePathBuf::try_from(workspace.path().join("ready")).expect("ready cwd is absolute");
    std::fs::create_dir_all(ready_cwd.as_path()).expect("create ready cwd");
    let selections = TurnEnvironmentSelections::new(
        ready_cwd.clone(),
        vec![
            TurnEnvironmentSelection {
                environment_id: "unknown-first-environment".to_string(),
                cwd: PathUri::from_abs_path(&unknown_cwd),
                workspace_roots: Vec::new(),
            },
            TurnEnvironmentSelection {
                environment_id: LOCAL_ENVIRONMENT_ID.to_string(),
                cwd: PathUri::from_abs_path(&ready_cwd),
                workspace_roots: Vec::new(),
            },
        ],
    );
    session
        .update_settings(SessionSettingsUpdate {
            environments: Some(selections.clone()),
            ..Default::default()
        })
        .await
        .expect("configure selected environments");
    session.services.turn_environments.snapshot().await;

    let rejected = submit_start_only_request_with_preconditions(
        &session,
        TurnInputRequest::user_input(vec![UserInput::Text {
            text: "the unknown selection must not be treated as primary".to_string(),
            text_elements: Vec::new(),
        }]),
        StartIfIdlePreconditions::default().with_expected_cwd(unknown_cwd),
    )
    .await;

    assert_eq!(
        StartIfIdlePreconditionSubmission::NotSubmitted {
            reason: StartIfIdlePreconditionNotSubmittedReason::ExpectedCwdMismatch,
        },
        rejected
    );
    assert!(session.active_turn.lock().await.is_none());
    assert_eq!(
        (Vec::<TurnInput>::new(), None, None),
        session
            .input_queue
            .get_pending_input(&session.active_turn)
            .await
    );
    assert_eq!(
        selections.environments,
        session
            .thread_config_snapshot()
            .await
            .environment_selections()
    );
    assert!(rx.try_recv().is_err());

    let accepted = submit_start_only_request_with_preconditions(
        &session,
        TurnInputRequest::user_input(vec![UserInput::Text {
            text: "the ready selection must become the turn primary".to_string(),
            text_elements: Vec::new(),
        }]),
        StartIfIdlePreconditions::default().with_expected_cwd(ready_cwd.clone()),
    )
    .await;

    assert_eq!(
        StartIfIdlePreconditionSubmission::Started {
            turn_id: "test-submission".to_string(),
        },
        accepted
    );
    let active_turn = session.active_turn.lock().await;
    let turn_context = &active_turn
        .as_ref()
        .and_then(|active_turn| active_turn.task.as_ref())
        .expect("accepted turn should be active")
        .turn_context;
    assert_eq!(
        Some(ready_cwd),
        turn_context
            .environments
            .primary()
            .and_then(|environment| environment.cwd().to_abs_path().ok())
    );
    drop(active_turn);
    session.abort_all_tasks(TurnAbortReason::Interrupted).await;
}

/// Registers a remote environment selected first (so it starts `Starting`)
/// ahead of a local environment selected second (which resolves `Ready`
/// immediately), and accepts the remote's initial connection so it stays
/// pending rather than failing outright. Shared by every test that must
/// observe the earlier selection's readiness as still ambiguous.
struct StartingBeforeReadyEnvironments {
    _workspace: tempfile::TempDir,
    ready_cwd: AbsolutePathBuf,
    remote_stream: TcpStream,
}

async fn configure_remote_starting_before_local_ready(
    session: &Arc<Session>,
) -> StartingBeforeReadyEnvironments {
    let workspace = tempfile::tempdir().expect("create workspace");
    let starting_cwd = AbsolutePathBuf::try_from(workspace.path().join("starting"))
        .expect("starting cwd is absolute");
    let ready_cwd =
        AbsolutePathBuf::try_from(workspace.path().join("ready")).expect("ready cwd is absolute");
    std::fs::create_dir_all(ready_cwd.as_path()).expect("create ready cwd");
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind starting remote listener");
    session
        .services
        .turn_environments
        .environment_manager()
        .upsert_environment(
            REMOTE_ENVIRONMENT_ID.to_string(),
            format!(
                "ws://{}",
                listener
                    .local_addr()
                    .expect("starting remote listener address")
            ),
            /*connect_timeout*/ None,
        )
        .expect("register starting remote environment");
    let selections = TurnEnvironmentSelections::new(
        ready_cwd.clone(),
        vec![
            TurnEnvironmentSelection {
                environment_id: REMOTE_ENVIRONMENT_ID.to_string(),
                cwd: PathUri::from_abs_path(&starting_cwd),
                workspace_roots: Vec::new(),
            },
            TurnEnvironmentSelection {
                environment_id: LOCAL_ENVIRONMENT_ID.to_string(),
                cwd: PathUri::from_abs_path(&ready_cwd),
                workspace_roots: Vec::new(),
            },
        ],
    );
    session
        .update_settings(SessionSettingsUpdate {
            environments: Some(selections),
            ..Default::default()
        })
        .await
        .expect("configure starting and ready environments");
    let (remote_stream, _) = timeout(std::time::Duration::from_secs(1), listener.accept())
        .await
        .expect("remote should begin connecting")
        .expect("accept starting remote connection");
    StartingBeforeReadyEnvironments {
        _workspace: workspace,
        ready_cwd,
        remote_stream,
    }
}

#[tokio::test]
async fn start_only_rejects_starting_primary_environment_before_admission() {
    let (session, _turn_context, rx) = make_session_and_context_with_rx().await;
    let environments = configure_remote_starting_before_local_ready(&session).await;

    let submission = submit_start_only_with_preconditions(
        &session,
        SubmittedTurnInput::UserInput {
            content: vec![UserInput::Text {
                text: "a pending primary must not admit the ready secondary cwd".to_string(),
                text_elements: Vec::new(),
            }],
            client_id: None,
        },
        StartIfIdlePreconditions::default().with_expected_cwd(environments.ready_cwd),
    )
    .await;

    assert_eq!(
        StartIfIdlePreconditionSubmission::NotSubmitted {
            reason: StartIfIdlePreconditionNotSubmittedReason::EnvironmentNotReady,
        },
        submission
    );
    assert!(session.active_turn.lock().await.is_none());
    assert_eq!(
        (Vec::<TurnInput>::new(), None, None),
        session
            .input_queue
            .get_pending_input(&session.active_turn)
            .await
    );
    assert!(rx.try_recv().is_err());
    drop(environments.remote_stream);
}

/// Requirement 8 (fail closed on Starting-first promotion ambiguity) must
/// hold even when the request never asks for `expected_cwd`: the admitted
/// turn context always builds from this same captured snapshot, so an
/// unresolved earlier selection is ambiguous for every precondition shape.
#[tokio::test]
async fn start_only_rejects_starting_primary_environment_for_default_preconditions() {
    let (session, _turn_context, rx) = make_session_and_context_with_rx().await;
    let environments = configure_remote_starting_before_local_ready(&session).await;

    let submission = submit_start_only_with_preconditions(
        &session,
        SubmittedTurnInput::UserInput {
            content: vec![UserInput::Text {
                text: "no expected_cwd should still fail closed on ambiguity".to_string(),
                text_elements: Vec::new(),
            }],
            client_id: None,
        },
        StartIfIdlePreconditions::default(),
    )
    .await;

    assert_eq!(
        StartIfIdlePreconditionSubmission::NotSubmitted {
            reason: StartIfIdlePreconditionNotSubmittedReason::EnvironmentNotReady,
        },
        submission
    );
    assert!(session.active_turn.lock().await.is_none());
    assert_eq!(
        (Vec::<TurnInput>::new(), None, None),
        session
            .input_queue
            .get_pending_input(&session.active_turn)
            .await
    );
    assert!(rx.try_recv().is_err());
    drop(environments.remote_stream);
}

#[tokio::test]
async fn start_only_with_preconditions_rejects_thread_settings_before_reserving_or_injecting() {
    let (session, _turn_context, rx) = make_session_and_context_with_rx().await;

    let submission = submit_start_only_request_with_preconditions(
        &session,
        TurnInputRequest::user_input(vec![UserInput::Text {
            text: "preconditioned starts do not apply thread settings".to_string(),
            text_elements: Vec::new(),
        }])
        .with_thread_settings(ThreadSettingsOverrides {
            permission_profile: Some(codex_protocol::models::PermissionProfile::read_only()),
            ..Default::default()
        }),
        StartIfIdlePreconditions::default(),
    )
    .await;

    assert_eq!(
        StartIfIdlePreconditionSubmission::NotSubmitted {
            reason: StartIfIdlePreconditionNotSubmittedReason::ThreadSettingsUnsupported,
        },
        submission
    );
    assert!(session.active_turn.lock().await.is_none());
    assert_eq!(
        (Vec::<TurnInput>::new(), None, None),
        session
            .input_queue
            .get_pending_input(&session.active_turn)
            .await
    );
    assert!(rx.try_recv().is_err());
}

/// When a request simultaneously violates both the thread-settings and the
/// persistence precondition, the ordered contract requires
/// `ThreadSettingsUnsupported` to win: settings are rejected before Core
/// ever consults persistence state.
#[tokio::test]
async fn start_only_with_preconditions_prioritizes_thread_settings_over_persistence_violation() {
    let (session, _turn_context, rx) = make_session_and_context_with_rx().await;

    let submission = submit_start_only_request_with_preconditions(
        &session,
        TurnInputRequest::user_input(vec![UserInput::Text {
            text: "both violations must resolve to thread settings".to_string(),
            text_elements: Vec::new(),
        }])
        .with_thread_settings(ThreadSettingsOverrides {
            permission_profile: Some(codex_protocol::models::PermissionProfile::read_only()),
            ..Default::default()
        }),
        StartIfIdlePreconditions::default().require_persistence(),
    )
    .await;

    assert_eq!(
        StartIfIdlePreconditionSubmission::NotSubmitted {
            reason: StartIfIdlePreconditionNotSubmittedReason::ThreadSettingsUnsupported,
        },
        submission
    );
    assert!(session.active_turn.lock().await.is_none());
    assert_eq!(
        (Vec::<TurnInput>::new(), None, None),
        session
            .input_queue
            .get_pending_input(&session.active_turn)
            .await
    );
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn start_only_rejects_empty_user_input_in_plan_mode() {
    let (session, _turn_context, _rx) = make_session_and_context_with_rx().await;
    let mut collaboration_mode = session.collaboration_mode().await;
    collaboration_mode.mode = ModeKind::Plan;
    {
        let mut state = session.state.lock().await;
        state.session_configuration.collaboration_mode = collaboration_mode;
    }

    let submission = submit_start_only(
        &session,
        SubmittedTurnInput::UserInput {
            content: Vec::new(),
            client_id: Some("empty-queued-user-message".to_string()),
        },
    )
    .await;

    assert_eq!(
        TurnInputSubmission::NotSubmitted {
            reason: NotSubmittedReason::PlanMode,
        },
        submission
    );
    assert!(session.active_turn.lock().await.is_none());
}

#[tokio::test]
async fn start_only_rejects_pending_trigger_turn_without_injecting() {
    let (session, _turn_context, _rx) = make_session_and_context_with_rx().await;
    enqueue_pending_trigger_turn(&session, "pending trigger").await;

    let submission = submit_start_only(
        &session,
        SubmittedTurnInput::ResponseItem(user_message("synthetic idle input")),
    )
    .await;

    assert_eq!(
        TurnInputSubmission::NotSubmitted {
            reason: NotSubmittedReason::PendingTriggerTurn,
        },
        submission
    );
    assert!(session.active_turn.lock().await.is_none());
    assert!(session.input_queue.has_trigger_turn_mailbox_items().await);
}

async fn enqueue_pending_trigger_turn(session: &Arc<Session>, message: &str) {
    session
        .input_queue
        .enqueue_mailbox_communication(
            InterAgentCommunication::new(
                AgentPath::root(),
                AgentPath::root(),
                Vec::new(),
                message.to_string(),
                /*trigger_turn*/ true,
            ),
            /*parent_turn_id*/ None,
            /*root_turn_id*/ None,
        )
        .await;
}

/// Direct coverage of the NEW sibling path's `NotIdle` guard: the old
/// path's fixture is not proof here because the handler is separate.
#[tokio::test]
async fn start_only_with_preconditions_rejects_active_turn_without_injecting() {
    let (session, turn_context, _rx) = make_session_and_context_with_rx().await;
    session
        .spawn_task(
            Arc::clone(&turn_context),
            Vec::new(),
            NeverEndingTask {
                kind: TaskKind::Regular,
                listen_to_cancellation_token: true,
            },
        )
        .await;

    let submission = submit_start_only_with_preconditions(
        &session,
        SubmittedTurnInput::ResponseItem(user_message("synthetic idle input")),
        StartIfIdlePreconditions::default(),
    )
    .await;

    assert_eq!(
        StartIfIdlePreconditionSubmission::NotSubmitted {
            reason: StartIfIdlePreconditionNotSubmittedReason::NotIdle,
        },
        submission
    );
    assert_eq!(
        (Vec::<TurnInput>::new(), None, None),
        session
            .input_queue
            .get_pending_input(&session.active_turn)
            .await
    );

    session.abort_all_tasks(TurnAbortReason::Interrupted).await;
}

/// Direct coverage of the NEW sibling path's pending-trigger guard as
/// observed before any reservation is attempted.
#[tokio::test]
async fn start_only_with_preconditions_rejects_pending_trigger_turn_before_reservation() {
    let (session, _turn_context, rx) = make_session_and_context_with_rx().await;
    enqueue_pending_trigger_turn(&session, "pending trigger").await;

    let submission = submit_start_only_with_preconditions(
        &session,
        SubmittedTurnInput::ResponseItem(user_message("synthetic idle input")),
        StartIfIdlePreconditions::default(),
    )
    .await;

    assert_eq!(
        StartIfIdlePreconditionSubmission::NotSubmitted {
            reason: StartIfIdlePreconditionNotSubmittedReason::PendingTriggerTurn,
        },
        submission
    );
    assert!(session.active_turn.lock().await.is_none());
    assert!(session.input_queue.has_trigger_turn_mailbox_items().await);
    assert!(rx.try_recv().is_err());
}

/// Direct coverage of the NEW sibling path's second pending-trigger guard,
/// which exists for the race window between reserving the idle slot and
/// re-checking the mailbox. Holding `active_turn`'s lock delays the
/// function's own reservation until after the mailbox item is enqueued,
/// deterministically routing it through the second check instead of the
/// first: the function observes an empty mailbox, blocks acquiring the
/// lock, and only reserves once this test releases it.
#[tokio::test]
async fn start_only_with_preconditions_rejects_pending_trigger_turn_after_reservation_race() {
    let (session, _turn_context, _rx) = make_session_and_context_with_rx().await;
    let reservation_guard = session.active_turn.lock().await;

    let submission_task = tokio::spawn({
        let session = Arc::clone(&session);
        async move {
            submit_start_only_with_preconditions(
                &session,
                SubmittedTurnInput::ResponseItem(user_message("synthetic idle input")),
                StartIfIdlePreconditions::default(),
            )
            .await
        }
    });
    tokio::task::yield_now().await;
    enqueue_pending_trigger_turn(&session, "race trigger").await;
    drop(reservation_guard);

    let submission = timeout(std::time::Duration::from_secs(5), submission_task)
        .await
        .expect("submission should complete")
        .expect("submission task should not panic");

    assert_eq!(
        StartIfIdlePreconditionSubmission::NotSubmitted {
            reason: StartIfIdlePreconditionNotSubmittedReason::PendingTriggerTurn,
        },
        submission
    );
    // The rejected reservation was cleared, but yielding to the pending
    // trigger turn appropriately starts a new turn for it rather than
    // leaving the thread idle with mailbox work still waiting.
    let active_turn = session.active_turn.lock().await;
    assert!(
        active_turn
            .as_ref()
            .and_then(|active_turn| active_turn.task.as_ref())
            .is_some(),
        "the pending trigger turn should have started its own turn"
    );
    drop(active_turn);

    session.abort_all_tasks(TurnAbortReason::Interrupted).await;
}

#[tokio::test]
async fn steer_only_requires_active_turn() {
    let (session, _turn_context, _rx) = make_session_and_context_with_rx().await;
    let submission = submit_steer_only(
        &session,
        vec![UserInput::Text {
            text: "steer".to_string(),
            text_elements: Vec::new(),
        }],
        "missing-turn-id",
    )
    .await;

    assert_eq!(
        TurnInputSubmission::NotSubmitted {
            reason: NotSubmittedReason::NoActiveTurn,
        },
        submission
    );
}

#[tokio::test]
async fn steer_only_enforces_expected_turn_id() {
    let (session, turn_context, _rx) = make_session_and_context_with_rx().await;
    session
        .spawn_task(
            Arc::clone(&turn_context),
            vec![TurnInput::UserInput {
                content: vec![UserInput::Text {
                    text: "hello".to_string(),
                    text_elements: Vec::new(),
                }],
                client_id: None,
            }],
            NeverEndingTask {
                kind: TaskKind::Regular,
                listen_to_cancellation_token: false,
            },
        )
        .await;

    let submission = submit_steer_only(
        &session,
        vec![UserInput::Text {
            text: "steer".to_string(),
            text_elements: Vec::new(),
        }],
        "different-turn-id",
    )
    .await;
    assert_eq!(
        TurnInputSubmission::NotSubmitted {
            reason: NotSubmittedReason::ExpectedTurnMismatch {
                expected: "different-turn-id".to_string(),
                actual: turn_context.sub_id.clone(),
            },
        },
        submission
    );
}

#[tokio::test]
async fn rejects_non_regular_turns() {
    for (task_kind, turn_kind) in [
        (TaskKind::Review, NonSteerableTurnKind::Review),
        (TaskKind::Compact, NonSteerableTurnKind::Compact),
    ] {
        let (session, incoming_turn_context, _rx) = make_session_and_context_with_rx().await;
        incoming_turn_context
            .turn_metadata_state
            .set_root_turn_id("incoming-root".to_string());
        let turn_context = session
            .new_default_turn_with_sub_id("turn".to_string())
            .await;
        turn_context
            .turn_metadata_state
            .set_root_turn_id("active-root".to_string());
        session
            .spawn_task(
                Arc::clone(&turn_context),
                vec![TurnInput::UserInput {
                    content: vec![UserInput::Text {
                        text: "hello".to_string(),
                        text_elements: Vec::new(),
                    }],
                    client_id: None,
                }],
                NeverEndingTask {
                    kind: task_kind,
                    listen_to_cancellation_token: true,
                },
            )
            .await;

        let steer_input = vec![UserInput::Text {
            text: "steer".to_string(),
            text_elements: Vec::new(),
        }];
        let steer_submission = submit_steer_only(&session, steer_input.clone(), "turn").await;
        assert_eq!(
            TurnInputSubmission::NotSubmitted {
                reason: NotSubmittedReason::ActiveTurnNotSteerable { turn_kind },
            },
            steer_submission
        );
        let start_or_steer_submission = handle(
            &session,
            TurnInputRequest::user_input(steer_input),
            TurnInputMode::StartOrSteer,
            "test-submission".to_string(),
        )
        .await
        .expect("start-or-steer submission should be valid");
        assert_eq!(
            TurnInputSubmission::NotSubmitted {
                reason: NotSubmittedReason::ActiveTurnNotSteerable { turn_kind },
            },
            start_or_steer_submission
        );
        assert_eq!(
            turn_context.turn_metadata_state.root_turn_id().as_deref(),
            Some("active-root")
        );

        session.abort_all_tasks(TurnAbortReason::Interrupted).await;
    }
}
