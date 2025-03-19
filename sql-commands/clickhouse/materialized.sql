CREATE MATERIALIZED VIEW users_mv
    TO users
AS
SELECT JSONExtractString(after, 'id')           AS id,
       JSONExtractString(after, 'name')         AS name,
       JSONExtractString(after, 'email')        AS email,
       JSONExtractDateTime(after, 'created_at') AS created_at
FROM kafka_all_tables
WHERE JSONExtractString(source, 'table') = 'users';