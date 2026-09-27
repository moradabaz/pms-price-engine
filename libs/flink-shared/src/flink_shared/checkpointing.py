from pyflink.common import Configuration


def configure_checkpointing(
    env,
    checkpoint_interval_ms: int,
    checkpoint_storage_path: str,
    s3_endpoint_url: str | None = None,
) -> None:
    """Enables EXACTLY_ONCE checkpointing on RocksDB + S3. Shared between
    every Flink job in this project (flink_jobs, market_pulse_job) so their
    checkpointing behavior never drifts apart by copy-paste."""
    env.enable_checkpointing(checkpoint_interval_ms)
    config = Configuration()
    config.set_string("state.backend.type", "rocksdb")
    config.set_string("state.backend.incremental", "true")
    config.set_string("state.checkpoints.dir", checkpoint_storage_path)
    config.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    if s3_endpoint_url:
        config.set_string("s3.endpoint", s3_endpoint_url)
        config.set_string("s3.path.style.access", "true")
        config.set_string("s3.access-key", "test")
        config.set_string("s3.secret-key", "test")
    env.configure(config)
