use super::*;
use crate::CHANNEL_CAPACITY;
use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::OutgoingEnvelope;
use crate::outgoing_message::OutgoingMessage;
use crate::outgoing_message::OutgoingMessageSender;
use anyhow::Result;
use anyhow::bail;
use codex_app_server_protocol::CodexErrorInfo as V2CodexErrorInfo;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ThreadStatus;
use codex_app_server_protocol::TurnCompletedNotification;
use codex_app_server_protocol::TurnError;
use codex_app_server_protocol::TurnStatus;
use codex_core::CodexAppsToolsCache;
use codex_core::StartThreadOptions;
use codex_core::test_support::EmptyUserInstructionsProvider;
use codex_core::test_support::auth_manager_from_auth;
use codex_core::test_support::models_manager_with_provider;
use codex_exec_server::EnvironmentManager;
use codex_login::CodexAuth;
use codex_protocol::ThreadId;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::TurnStartedEvent;
use codex_thread_store::AppendThreadItemsParams;
use codex_thread_store::ArchiveThreadParams;
use codex_thread_store::CreateThreadParams;
use codex_thread_store::DeleteThreadParams;
use codex_thread_store::InMemoryThreadStore;
use codex_thread_store::ListThreadsParams;
use codex_thread_store::LoadThreadHistoryParams;
use codex_thread_store::PersistContext;
use codex_thread_store::ReadThreadByRolloutPathParams;
use codex_thread_store::ReadThreadParams;
use codex_thread_store::ResumeThreadParams;
use codex_thread_store::StoredThread;
use codex_thread_store::StoredThreadHistory;
use codex_thread_store::ThreadPage;
use codex_thread_store::ThreadStore;
use codex_thread_store::ThreadStoreError;
use codex_thread_store::ThreadStoreFuture;
use codex_thread_store::ThreadStoreResult;
use codex_thread_store::UpdateThreadMetadataParams;
use core_test_support::load_default_config_for_test;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use tempfile::TempDir;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::Duration;
use tokio::time::timeout;

const TEST_TURN_COMPLETED_AT: i64 = 1_716_000_456;
const TEST_TURN_DURATION_MS: i64 = 1_234;
const FLUSH_FAILURE: &str = "injected rollout flush failure";

enum NextFlush {
    Delegate,
    Hold {
        entered: oneshot::Sender<()>,
        release: oneshot::Receiver<ThreadStoreResult<()>>,
    },
    Fail(ThreadStoreError),
}

struct ControlledFlushStore {
    inner: Arc<dyn ThreadStore>,
    next_flush: Mutex<NextFlush>,
    flush_calls: AtomicUsize,
}

impl ControlledFlushStore {
    fn new(inner: Arc<dyn ThreadStore>) -> Self {
        Self {
            inner,
            next_flush: Mutex::new(NextFlush::Delegate),
            flush_calls: AtomicUsize::new(0),
        }
    }

    async fn hold_next_flush(
        &self,
    ) -> (
        oneshot::Receiver<()>,
        oneshot::Sender<ThreadStoreResult<()>>,
    ) {
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        *self.next_flush.lock().await = NextFlush::Hold {
            entered: entered_tx,
            release: release_rx,
        };
        (entered_rx, release_tx)
    }

    async fn fail_next_flush(&self) {
        *self.next_flush.lock().await = NextFlush::Fail(ThreadStoreError::Internal {
            message: FLUSH_FAILURE.to_string(),
        });
    }

    fn flush_calls(&self) -> usize {
        self.flush_calls.load(Ordering::SeqCst)
    }
}

impl ThreadStore for ControlledFlushStore {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn default_history_mode(&self) -> codex_protocol::protocol::ThreadHistoryMode {
        self.inner.default_history_mode()
    }

    fn create_thread(&self, params: CreateThreadParams) -> ThreadStoreFuture<'_, ()> {
        self.inner.create_thread(params)
    }

    fn resume_thread(&self, params: ResumeThreadParams) -> ThreadStoreFuture<'_, ()> {
        self.inner.resume_thread(params)
    }

    fn append_items(&self, params: AppendThreadItemsParams) -> ThreadStoreFuture<'_, ()> {
        self.inner.append_items(params)
    }

    fn persist_thread(
        &self,
        thread_id: ThreadId,
        context: PersistContext,
    ) -> ThreadStoreFuture<'_, ()> {
        self.inner.persist_thread(thread_id, context)
    }

    fn flush_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            self.flush_calls.fetch_add(1, Ordering::SeqCst);
            match std::mem::replace(&mut *self.next_flush.lock().await, NextFlush::Delegate) {
                NextFlush::Delegate => self.inner.flush_thread(thread_id).await,
                NextFlush::Hold { entered, release } => {
                    let _ = entered.send(());
                    release.await.map_err(|_| ThreadStoreError::Internal {
                        message: "rollout flush test release dropped".to_string(),
                    })??;
                    self.inner.flush_thread(thread_id).await
                }
                NextFlush::Fail(err) => Err(err),
            }
        })
    }

    fn shutdown_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        self.inner.shutdown_thread(thread_id)
    }

    fn discard_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        self.inner.discard_thread(thread_id)
    }

    fn load_history(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreFuture<'_, StoredThreadHistory> {
        self.inner.load_history(params)
    }

    fn read_thread(&self, params: ReadThreadParams) -> ThreadStoreFuture<'_, StoredThread> {
        self.inner.read_thread(params)
    }

    fn read_thread_by_rollout_path(
        &self,
        params: ReadThreadByRolloutPathParams,
    ) -> ThreadStoreFuture<'_, StoredThread> {
        self.inner.read_thread_by_rollout_path(params)
    }

    fn list_threads(&self, params: ListThreadsParams) -> ThreadStoreFuture<'_, ThreadPage> {
        self.inner.list_threads(params)
    }

    fn update_thread_metadata(
        &self,
        params: UpdateThreadMetadataParams,
    ) -> ThreadStoreFuture<'_, StoredThread> {
        self.inner.update_thread_metadata(params)
    }

    fn archive_thread(&self, params: ArchiveThreadParams) -> ThreadStoreFuture<'_, ()> {
        self.inner.archive_thread(params)
    }

    fn unarchive_thread(&self, params: ArchiveThreadParams) -> ThreadStoreFuture<'_, StoredThread> {
        self.inner.unarchive_thread(params)
    }

    fn delete_thread(&self, params: DeleteThreadParams) -> ThreadStoreFuture<'_, ()> {
        self.inner.delete_thread(params)
    }
}

struct CompletionHarness {
    _codex_home: TempDir,
    conversation_id: ThreadId,
    conversation: Arc<CodexThread>,
    thread_manager: Arc<ThreadManager>,
    store: Arc<ControlledFlushStore>,
    outgoing: ThreadScopedOutgoingMessageSender,
    outgoing_rx: mpsc::Receiver<OutgoingEnvelope>,
    thread_state: Arc<Mutex<ThreadState>>,
    thread_watch_manager: ThreadWatchManager,
}

impl CompletionHarness {
    async fn new(ephemeral: bool) -> Result<Self> {
        let codex_home = TempDir::new()?;
        let mut config = load_default_config_for_test(&codex_home).await;
        config.ephemeral = ephemeral;
        let auth_manager =
            auth_manager_from_auth(CodexAuth::create_dummy_chatgpt_auth_for_testing());
        let models_manager = models_manager_with_provider(
            config.codex_home.to_path_buf(),
            auth_manager.clone(),
            config.model_provider.clone(),
        );
        let store = Arc::new(ControlledFlushStore::new(Arc::new(
            InMemoryThreadStore::default(),
        )));
        let thread_store: Arc<dyn ThreadStore> = store.clone();
        let thread_manager = Arc::new(ThreadManager::new(
            &config,
            auth_manager,
            models_manager,
            CodexAppsToolsCache::default(),
            SessionSource::Exec,
            Arc::new(EnvironmentManager::default_for_tests()),
            codex_extension_api::empty_extension_registry(),
            Arc::new(EmptyUserInstructionsProvider),
            /*analytics_events_client*/ None,
            thread_store,
            /*agent_graph_store*/ None,
            "turn-completion-persistence-test".to_string(),
            /*attestation_provider*/ None,
            /*external_time_provider*/ None,
        ));
        let codex_core::NewThread {
            thread_id: conversation_id,
            thread: conversation,
            ..
        } = thread_manager
            .start_thread(StartThreadOptions::new(config))
            .await?;
        let (outgoing_tx, outgoing_rx) = mpsc::channel(CHANNEL_CAPACITY);
        let outgoing = Arc::new(OutgoingMessageSender::new(
            outgoing_tx,
            codex_analytics::AnalyticsEventsClient::disabled(),
        ));
        let outgoing = ThreadScopedOutgoingMessageSender::new(
            outgoing,
            vec![ConnectionId(1)],
            conversation_id,
        );
        let thread_state = Arc::new(Mutex::new(ThreadState::default()));
        let thread_watch_manager = ThreadWatchManager::new();
        thread_watch_manager
            .note_turn_started(&conversation_id.to_string())
            .await;
        thread_state.lock().await.track_current_turn_event(
            "turn-persistence",
            &EventMsg::TurnStarted(TurnStartedEvent {
                turn_id: "turn-persistence".to_string(),
                trace_id: None,
                started_at: Some(42),
                model_context_window: None,
                collaboration_mode_kind: Default::default(),
            }),
        );

        Ok(Self {
            _codex_home: codex_home,
            conversation_id,
            conversation,
            thread_manager,
            store,
            outgoing,
            outgoing_rx,
            thread_state,
            thread_watch_manager,
        })
    }

    async fn spawn_turn_complete(&self) -> tokio::task::JoinHandle<()> {
        let event = turn_complete_event();
        self.thread_state
            .lock()
            .await
            .track_current_turn_event("turn-persistence", &EventMsg::TurnComplete(event.clone()));
        let conversation_id = self.conversation_id;
        let conversation = self.conversation.clone();
        let thread_manager = self.thread_manager.clone();
        let outgoing = self.outgoing.clone();
        let thread_state = self.thread_state.clone();
        let thread_watch_manager = self.thread_watch_manager.clone();
        tokio::spawn(async move {
            apply_bespoke_event_handling(
                Event {
                    id: "turn-persistence".to_string(),
                    msg: EventMsg::TurnComplete(event),
                },
                conversation_id,
                conversation,
                thread_manager,
                outgoing,
                thread_state,
                thread_watch_manager,
                Arc::new(tokio::sync::Semaphore::new(/*permits*/ 1)),
                "test-provider".to_string(),
            )
            .await;
        })
    }

    async fn apply_turn_complete(&self) {
        self.spawn_turn_complete()
            .await
            .await
            .expect("terminal handler should not panic");
    }

    async fn recv_terminal(&mut self) -> Result<TurnCompletedNotification> {
        let envelope = timeout(Duration::from_secs(2), self.outgoing_rx.recv())
            .await
            .map_err(|_| anyhow::anyhow!("terminal notification timed out"))?
            .ok_or_else(|| anyhow::anyhow!("terminal notification channel closed"))?;
        let message = match envelope {
            OutgoingEnvelope::Broadcast { message }
            | OutgoingEnvelope::ToConnection { message, .. } => message,
        };
        let OutgoingMessage::AppServerNotification(envelope) = message else {
            bail!("unexpected outgoing message: {message:?}");
        };
        let ServerNotification::TurnCompleted(notification) = envelope.notification else {
            bail!("unexpected notification: {:?}", envelope.notification);
        };
        Ok(notification)
    }

    async fn assert_watch_idle(&self) {
        assert_eq!(
            self.thread_watch_manager
                .loaded_status_for_thread(&self.conversation_id.to_string())
                .await,
            ThreadStatus::Idle,
        );
        assert_eq!(self.thread_watch_manager.running_turn_count().await, 0);
    }

    fn assert_no_more_notifications(&mut self) {
        assert!(
            self.outgoing_rx.try_recv().is_err(),
            "exactly one terminal notification should be emitted"
        );
    }
}

fn turn_complete_event() -> TurnCompleteEvent {
    TurnCompleteEvent {
        turn_id: "turn-persistence".to_string(),
        started_at: Some(42),
        last_agent_message: None,
        error: None,
        completed_at: Some(TEST_TURN_COMPLETED_AT),
        duration_ms: Some(TEST_TURN_DURATION_MS),
        time_to_first_token_ms: None,
    }
}

#[tokio::test]
async fn turn_complete_persistence_waits_for_rollout_flush_before_success_notification()
-> Result<()> {
    let mut harness = CompletionHarness::new(/*ephemeral*/ false).await?;
    let (mut flush_entered, release_flush) = harness.store.hold_next_flush().await;
    let mut terminal_task = harness.spawn_turn_complete().await;

    tokio::select! {
        entered = &mut flush_entered => {
            entered.expect("rollout flush should enter its durability barrier");
        }
        result = &mut terminal_task => {
            result.expect("terminal handler should not panic");
            let leaked_notification = harness.recv_terminal().await?;
            let leaked_status = harness.thread_watch_manager
                .loaded_status_for_thread(&harness.conversation_id.to_string())
                .await;
            panic!(
                "terminal boundary escaped before rollout flush: watch={leaked_status:?}, notification={leaked_notification:?}"
            );
        }
    }

    assert_eq!(
        harness
            .thread_watch_manager
            .loaded_status_for_thread(&harness.conversation_id.to_string())
            .await,
        ThreadStatus::Active {
            active_flags: Vec::new(),
        },
    );
    assert_eq!(harness.thread_watch_manager.running_turn_count().await, 1);
    assert!(
        harness.outgoing_rx.try_recv().is_err(),
        "successful v2 completion must remain behind rollout flush"
    );

    release_flush
        .send(Ok(()))
        .expect("release held rollout flush");
    timeout(Duration::from_secs(2), terminal_task)
        .await
        .expect("terminal handler should finish after flush release")
        .expect("terminal handler should not panic");

    let completed = harness.recv_terminal().await?;
    assert_eq!(completed.turn.status, TurnStatus::Completed);
    harness.assert_watch_idle().await;
    assert_eq!(harness.store.flush_calls(), 1);
    harness.assert_no_more_notifications();
    Ok(())
}

#[tokio::test]
async fn turn_complete_persistence_failure_emits_failed_notification() -> Result<()> {
    let mut harness = CompletionHarness::new(/*ephemeral*/ false).await?;
    harness.store.fail_next_flush().await;

    harness.apply_turn_complete().await;

    let completed = harness.recv_terminal().await?;
    assert_eq!(completed.turn.status, TurnStatus::Failed);
    let error = completed
        .turn
        .error
        .expect("flush failure should become the terminal error");
    assert!(
        error.message.contains(FLUSH_FAILURE),
        "terminal error should identify the failed persistence barrier: {error:?}"
    );
    harness.assert_watch_idle().await;
    assert_eq!(harness.store.flush_calls(), 1);
    harness.assert_no_more_notifications();
    Ok(())
}

#[tokio::test]
async fn turn_complete_persistence_failure_preserves_primary_turn_error() -> Result<()> {
    let mut harness = CompletionHarness::new(/*ephemeral*/ false).await?;
    let primary_error = TurnError {
        message: "primary model failure".to_string(),
        codex_error_info: Some(V2CodexErrorInfo::Other),
        additional_details: None,
    };
    handle_error(
        harness.conversation_id,
        primary_error.clone(),
        &harness.thread_state,
    )
    .await;
    harness.store.fail_next_flush().await;

    harness.apply_turn_complete().await;

    let completed = harness.recv_terminal().await?;
    assert_eq!(completed.turn.status, TurnStatus::Failed);
    assert_eq!(completed.turn.error, Some(primary_error));
    harness.assert_watch_idle().await;
    assert_eq!(
        harness.store.flush_calls(),
        1,
        "secondary persistence failure must still be observed"
    );
    harness.assert_no_more_notifications();
    Ok(())
}

#[tokio::test]
async fn turn_complete_persistence_ephemeral_completion_remains_unfenced() -> Result<()> {
    let mut harness = CompletionHarness::new(/*ephemeral*/ true).await?;
    harness.store.fail_next_flush().await;

    harness.apply_turn_complete().await;

    let completed = harness.recv_terminal().await?;
    assert_eq!(completed.turn.status, TurnStatus::Completed);
    assert_eq!(completed.turn.error, None);
    harness.assert_watch_idle().await;
    assert_eq!(
        harness.store.flush_calls(),
        0,
        "ephemeral completion has no persistent rollout barrier"
    );
    harness.assert_no_more_notifications();
    Ok(())
}
