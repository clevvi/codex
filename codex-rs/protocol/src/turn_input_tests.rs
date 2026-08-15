use super::*;

#[test]
fn turn_input_request_literals_remain_source_compatible() {
    let _request = TurnInputRequest {
        input: TurnInput::UserInput {
            content: Vec::new(),
            client_id: None,
        },
        thread_settings: ThreadSettingsOverrides::default(),
        start: TurnStartOptions::default(),
        additional_context: BTreeMap::new(),
        responsesapi_client_metadata: None,
        trace: None,
    };
}

#[test]
fn start_if_idle_preconditions_construct_through_public_builder_surface() {
    let workspace = tempfile::tempdir().expect("create workspace");
    let expected_cwd =
        AbsolutePathBuf::try_from(workspace.path().join("expected-cwd")).expect("absolute cwd");
    let defaults = StartIfIdlePreconditions::default();
    assert_eq!(defaults.expected_cwd(), None);
    assert!(!defaults.requires_persistence());
    assert!(!defaults.disallows_plan_mode());

    let preconditions = StartIfIdlePreconditions::default()
        .with_expected_cwd(expected_cwd.clone())
        .require_persistence()
        .disallow_plan_mode();

    assert_eq!(preconditions.expected_cwd(), Some(&expected_cwd));
    assert!(preconditions.requires_persistence());
    assert!(preconditions.disallows_plan_mode());
}
