use curvine_config::MasterConf;

#[test]
fn master_runtime_limits_require_positive_capacity() {
    let mut conf = MasterConf {
        actor_threads: 0,
        ..Default::default()
    };
    assert!(conf.init().is_err());

    let mut conf = MasterConf {
        executor_threads: 0,
        ..Default::default()
    };
    assert!(conf.init().is_err());

    let mut conf = MasterConf {
        executor_channel_size: 0,
        ..Default::default()
    };
    assert!(conf.init().is_err());
}
