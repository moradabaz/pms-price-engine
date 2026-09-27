from unittest.mock import MagicMock

from flink_shared.checkpointing import configure_checkpointing


def test_enables_checkpointing_with_given_interval():
    env = MagicMock()
    configure_checkpointing(env, checkpoint_interval_ms=60_000, checkpoint_storage_path="s3://x/")
    env.enable_checkpointing.assert_called_once_with(60_000)


def test_configures_rocksdb_incremental_exactly_once():
    env = MagicMock()
    configure_checkpointing(env, checkpoint_interval_ms=60_000, checkpoint_storage_path="s3://x/")
    config = env.configure.call_args[0][0]
    assert config.get_string("state.backend.type", "") == "rocksdb"
    assert config.get_string("state.backend.incremental", "") == "true"
    assert config.get_string("execution.checkpointing.mode", "") == "EXACTLY_ONCE"
    assert config.get_string("state.checkpoints.dir", "") == "s3://x/"


def test_sets_s3_credentials_only_when_endpoint_given():
    env = MagicMock()
    configure_checkpointing(
        env,
        checkpoint_interval_ms=60_000,
        checkpoint_storage_path="s3://x/",
        s3_endpoint_url=None,
    )
    config = env.configure.call_args[0][0]
    assert config.get_string("s3.endpoint", "") == ""


def test_sets_s3_endpoint_when_given():
    env = MagicMock()
    configure_checkpointing(
        env,
        checkpoint_interval_ms=60_000,
        checkpoint_storage_path="s3://x/",
        s3_endpoint_url="localhost:4566",
    )
    config = env.configure.call_args[0][0]
    assert config.get_string("s3.endpoint", "") == "localhost:4566"
    assert config.get_string("s3.access-key", "") == "test"
