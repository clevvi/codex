use super::*;
use crate::error_code::INVALID_REQUEST_ERROR_CODE;
use pretty_assertions::assert_eq;

#[test]
fn environment_not_ready_is_an_invalid_request() {
    let error = TurnRequestProcessor::start_if_idle_rejection_error(
        StartIfIdlePreconditionNotSubmittedReason::EnvironmentNotReady,
    );

    assert_eq!(
        error,
        JSONRPCErrorError {
            code: INVALID_REQUEST_ERROR_CODE,
            message: "environment not ready".to_string(),
            data: None,
        }
    );
}
