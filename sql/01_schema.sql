CREATE DATABASE IF NOT EXISTS homework;

CREATE TABLE IF NOT EXISTS homework.transactions
(
    row_id UInt64,
    transaction_time DateTime('UTC'),
    state String,
    category String,
    amount Decimal(18, 2)
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS homework.transactions_kafka
(
    row_id UInt64,
    transaction_time DateTime('UTC'),
    state String,
    category String,
    amount Decimal(18, 2)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'transactions',
    kafka_group_name = 'homework_ingest',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 0,
    kafka_flush_interval_ms = 1000;
