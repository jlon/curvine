use curvine_model::TransferTaskState;

#[test]
fn stale_transfer_task_is_recoverable() {
    assert!(!TransferTaskState::Stale.is_terminal());
}
