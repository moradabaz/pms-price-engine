import time

import boto3
from common import configure_logging, get_logger
from kafka import KafkaProducer

from kinesis_kafka_bridge.bridge import latest_iterator, list_shard_ids, poll_shard, republish
from kinesis_kafka_bridge.settings import BridgeSettings


def run_forever(kinesis: object, producer: KafkaProducer, settings: BridgeSettings) -> None:
    logger = get_logger(__name__)
    shard_ids = list_shard_ids(kinesis, settings.kinesis_stream_name)
    iterators = {sid: latest_iterator(kinesis, settings.kinesis_stream_name, sid) for sid in shard_ids}
    logger.info("bridge_starting", shards=len(shard_ids), topic=settings.kafka_topic)

    while True:
        for shard_id, iterator in list(iterators.items()):
            if iterator is None:
                continue
            records, next_iterator = poll_shard(kinesis, iterator)
            if records:
                count = republish(producer, settings.kafka_topic, records)
                logger.info("republished", shard_id=shard_id, count=count)
            iterators[shard_id] = next_iterator
        time.sleep(settings.poll_interval_seconds)


def main() -> None:
    settings = BridgeSettings()
    configure_logging(settings.log_level)

    kinesis = boto3.client(
        "kinesis", region_name=settings.aws_region, endpoint_url=settings.kinesis_endpoint_url
    )
    producer = KafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)

    run_forever(kinesis, producer, settings)


if __name__ == "__main__":
    main()
