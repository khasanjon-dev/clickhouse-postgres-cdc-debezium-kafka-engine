CREATE TABLE users
(
    id         UInt32,
    name       String,
    email      String,
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
      ORDER BY id;



CREATE TABLE kafka_all_tables
(
    before String,
    after  String,
    source String
) ENGINE = Kafka
      SETTINGS kafka_broker_list = 'kafka_broker:29092',
          kafka_topic_list = 'all_tables',
          kafka_group_name = 'clickhouse_consumer',
          kafka_format = 'JSONEachRow',
          kafka_flush_interval_ms = 1000;



CREATE MATERIALIZED VIEW users_mv
    TO users
AS
SELECT JSONExtractString(after, 'id')                          AS id,
       JSONExtractString(after, 'name')                        AS name,
       JSONExtractString(after, 'email')                       AS email,
       toDateTime64(JSONExtractString(after, 'created_at'), 3) AS created_at
FROM kafka_all_tables
WHERE JSONExtractString(source, 'table') = 'users';



drop table kafka_all_tables;

drop view users_mv;


select count(*)
from users;


select *
from users
limit 10;