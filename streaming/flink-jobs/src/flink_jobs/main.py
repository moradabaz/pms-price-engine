from common import configure_logging, get_logger
from pyflink.datastream import StreamExecutionEnvironment

from flink_jobs.job import build_job
from flink_jobs.settings import FlinkJobSettings


def main() -> None:
    settings = FlinkJobSettings()
    configure_logging("INFO")
    logger = get_logger(__name__)

    env = StreamExecutionEnvironment.get_execution_environment()
    build_job(env, settings)

    logger.info("flink_job_starting")
    env.execute("pms-price-engine")


if __name__ == "__main__":
    main()
