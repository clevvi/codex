use anyhow::Context;
use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use app_test_support::create_final_assistant_message_sse_response;
use app_test_support::create_mock_responses_server_sequence_unchecked;
use codex_app_server_protocol::JSONRPCError;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadSettingsUpdateParams;
use codex_app_server_protocol::ThreadSettingsUpdateResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::TurnCompletedNotification;
use codex_app_server_protocol::TurnStartIfIdleParams;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::TurnStartedNotification;
use codex_app_server_protocol::UserInput as V2UserInput;
use codex_features::Feature;
use codex_protocol::config_types::CollaborationMode;
use codex_protocol::config_types::ModeKind;
use codex_protocol::config_types::Settings;
use codex_utils_absolute_path::AbsolutePathBuf;
use codex_utils_absolute_path::test_support::PathExt;
use core_test_support::responses;
use core_test_support::streaming_sse::StreamingSseChunk;
use core_test_support::streaming_sse::start_streaming_sse_server;
use pretty_assertions::assert_eq;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::oneshot;
use tokio::time::timeout;
use wiremock::MockServer;
use wiremock::Request;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
const INVALID_REQUEST: i64 = -32600;

fn text_input(text: &str) -> V2UserInput {
    V2UserInput::Text {
        text: text.to_string(),
        text_elements: Vec::new(),
    }
}

fn params(thread_id: &str, expected_cwd: AbsolutePathBuf, text: &str) -> TurnStartIfIdleParams {
    TurnStartIfIdleParams {
        thread_id: thread_id.to_string(),
        expected_cwd,
        client_user_message_id: Some(format!("client-{text}")),
        input: vec![text_input(text)],
    }
}

async fn read_error(mcp: &mut TestAppServer, request_id: i64) -> Result<JSONRPCError> {
    timeout(
        DEFAULT_TIMEOUT,
        mcp.read_stream_until_error_message(RequestId::Integer(request_id)),
    )
    .await?
}

async fn response_requests(server: &MockServer) -> Result<Vec<Request>> {
    Ok(server
        .received_requests()
        .await
        .context("failed to fetch received requests")?
        .into_iter()
        .filter(|request| request.url.path().ends_with("/responses"))
        .collect())
}

/// Covers matrix item 2: a wrong `expectedCwd` rejects atomically (zero provider
/// requests), and a subsequent matching-cwd call admits the actual turn id shared
/// by the response, `turn/started`, and `turn/completed`, with exactly one
/// provider POST containing only the accepted input.
#[tokio::test]
async fn wrong_cwd_rejects_then_matching_cwd_admits_exact_turn() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(vec![
        create_final_assistant_message_sse_response("done")?,
    ])
    .await;
    let codex_home = TempDir::new()?;
    let wrong_dir = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;
    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let expected_cwd = mcp.auto_env()?.cwd().clone();
    let thread = mcp
        .start_thread(ThreadStartParams {
            model: Some("mock-model".to_string()),
            ..Default::default()
        })
        .await?
        .thread;

    let rejected_id = mcp
        .send_turn_start_if_idle_request(params(&thread.id, wrong_dir.path().abs(), "wrong-cwd"))
        .await?;
    let error = read_error(&mut mcp, rejected_id).await?;
    assert_eq!(error.error.code, INVALID_REQUEST);
    assert_eq!(error.error.message, "expectedCwd mismatch");
    assert!(response_requests(&server).await?.is_empty());

    let admitted_id = mcp
        .send_turn_start_if_idle_request(params(&thread.id, expected_cwd, "accepted"))
        .await?;
    let TurnStartResponse { turn } =
        timeout(DEFAULT_TIMEOUT, mcp.read_response(admitted_id)).await??;
    let started: TurnStartedNotification =
        timeout(DEFAULT_TIMEOUT, mcp.read_notification("turn/started")).await??;
    assert_eq!(started.turn.id, turn.id);
    let completed: TurnCompletedNotification =
        timeout(DEFAULT_TIMEOUT, mcp.read_notification("turn/completed")).await??;
    assert_eq!(completed.turn.id, turn.id);

    let requests = response_requests(&server).await?;
    assert_eq!(requests.len(), 1);
    let body = String::from_utf8_lossy(&requests[0].body);
    assert!(body.contains("accepted"));
    assert!(!body.contains("wrong-cwd"));
    Ok(())
}

/// Covers matrix item 3: a held ordinary turn wins; the conditional idle-start
/// input is rejected as `busy`, never steers, and never reaches the provider.
#[tokio::test]
async fn ordinary_turn_queued_first_wins_and_conditional_input_is_not_sent() -> Result<()> {
    let (release_response, response_gate) = oneshot::channel();
    let (server, _completions) = start_streaming_sse_server(vec![vec![StreamingSseChunk {
        gate: Some(response_gate),
        body: responses::sse(vec![
            responses::ev_response_created("resp-1"),
            responses::ev_completed("resp-1"),
        ]),
    }]])
    .await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(server.uri()).write(codex_home.path())?;
    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let expected_cwd = mcp.auto_env()?.cwd().clone();
    let thread = mcp
        .start_thread(ThreadStartParams {
            model: Some("mock-model".to_string()),
            ..Default::default()
        })
        .await?
        .thread;

    let ordinary_id = mcp
        .send_turn_start_request(TurnStartParams {
            thread_id: thread.id.clone(),
            input: vec![text_input("ordinary-first")],
            ..Default::default()
        })
        .await?;
    let conditional_id = mcp
        .send_turn_start_if_idle_request(params(
            &thread.id,
            expected_cwd,
            "conditional-must-not-steer",
        ))
        .await?;
    let _: TurnStartResponse = timeout(DEFAULT_TIMEOUT, mcp.read_response(ordinary_id)).await??;
    timeout(DEFAULT_TIMEOUT, server.wait_for_request_count(1)).await?;
    let error = read_error(&mut mcp, conditional_id).await?;
    assert_eq!(error.error.code, INVALID_REQUEST);
    assert_eq!(error.error.message, "busy");

    let requests = server.requests().await;
    assert_eq!(requests.len(), 1);
    let body = String::from_utf8_lossy(&requests[0]);
    assert!(body.contains("ordinary-first"));
    assert!(!body.contains("conditional-must-not-steer"));

    release_response.send(()).expect("release fake response");
    timeout(
        DEFAULT_TIMEOUT,
        mcp.read_stream_until_notification_message("turn/completed"),
    )
    .await??;
    server.shutdown().await;
    Ok(())
}

/// Covers matrix item 4: Plan mode rejects atomically with zero provider requests.
#[tokio::test]
async fn plan_mode_rejects_without_provider_work() -> Result<()> {
    let server = responses::start_mock_server().await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .enable_feature(Feature::CollaborationModes)
        .write(codex_home.path())?;
    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let expected_cwd = mcp.auto_env()?.cwd().clone();
    let thread = mcp
        .start_thread(ThreadStartParams {
            model: Some("mock-model".to_string()),
            ..Default::default()
        })
        .await?
        .thread;

    let settings_id = mcp
        .send_thread_settings_update_request(ThreadSettingsUpdateParams {
            thread_id: thread.id.clone(),
            collaboration_mode: Some(CollaborationMode {
                mode: ModeKind::Plan,
                settings: Settings {
                    model: "mock-model".to_string(),
                    reasoning_effort: None,
                    developer_instructions: None,
                },
            }),
            ..Default::default()
        })
        .await?;
    let _: ThreadSettingsUpdateResponse =
        timeout(DEFAULT_TIMEOUT, mcp.read_response(settings_id)).await??;
    timeout(
        DEFAULT_TIMEOUT,
        mcp.read_stream_until_notification_message("thread/settings/updated"),
    )
    .await??;

    let request_id = mcp
        .send_turn_start_if_idle_request(params(&thread.id, expected_cwd, "not-in-plan"))
        .await?;
    let error = read_error(&mut mcp, request_id).await?;
    assert_eq!(error.error.code, INVALID_REQUEST);
    assert_eq!(error.error.message, "plan mode");
    assert!(response_requests(&server).await?.is_empty());
    Ok(())
}

/// Covers matrix item 5: an ephemeral (persistence-disabled) thread rejects
/// atomically with zero provider requests.
#[tokio::test]
async fn ephemeral_thread_rejects_without_provider_work() -> Result<()> {
    let server = responses::start_mock_server().await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;
    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let expected_cwd = mcp.auto_env()?.cwd().clone();
    let thread = mcp
        .start_thread(ThreadStartParams {
            model: Some("mock-model".to_string()),
            ephemeral: Some(true),
            ..Default::default()
        })
        .await?
        .thread;

    let request_id = mcp
        .send_turn_start_if_idle_request(params(&thread.id, expected_cwd, "ephemeral"))
        .await?;
    let error = read_error(&mut mcp, request_id).await?;
    assert_eq!(error.error.code, INVALID_REQUEST);
    assert_eq!(error.error.message, "persistence disabled");
    assert!(response_requests(&server).await?.is_empty());
    Ok(())
}

/// An admitted non-empty conditional turn preserves the ordinary turn/start
/// memory-startup side effect.
#[tokio::test]
async fn admitted_turn_starts_memory_pipeline() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(vec![
        create_final_assistant_message_sse_response("done")?,
    ])
    .await;
    let codex_home = TempDir::new()?;
    let memory_root = codex_home.path().join("memories");
    MockResponsesConfig::new(&server.uri())
        .enable_feature(Feature::MemoryTool)
        .write(codex_home.path())?;
    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let expected_cwd = mcp.auto_env()?.cwd().clone();
    let thread = mcp
        .start_thread(ThreadStartParams {
            model: Some("mock-model".to_string()),
            ..Default::default()
        })
        .await?
        .thread;

    let request_id = mcp
        .send_turn_start_if_idle_request(params(&thread.id, expected_cwd, "remember-this"))
        .await?;
    let _: TurnStartResponse = timeout(DEFAULT_TIMEOUT, mcp.read_response(request_id)).await??;
    timeout(DEFAULT_TIMEOUT, async {
        while !memory_root.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;

    Ok(())
}
