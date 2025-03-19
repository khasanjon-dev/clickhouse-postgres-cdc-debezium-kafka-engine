CREATE TABLE kafka_all_tables
(
    before String,
    after  String,
    source String
) ENGINE = Kafka
      SETTINGS kafka_broker_list = 'localhost:9092',
          kafka_topic_list = 'all_tables',
          kafka_group_name = 'clickhouse_consumer',
          kafka_format = 'JSONEachRow',
          kafka_flush_interval_ms = 1000;
