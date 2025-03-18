import json
import logging
import os
import time
from datetime import datetime, timezone

import clickhouse_connect
from confluent_kafka import Consumer, KafkaError

KAFKA_HOST = os.environ.get("KAFKA_HOST")
KAFKA_PORT = os.environ.get("KAFKA_PORT")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = os.environ.get("CLICKHOUSE_PORT")
CLICKHOUSE_USERNAME = os.environ.get("CLICKHOUSE_USERNAME")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE")
ALL_TABLE_TOPIC = os.environ.get("ALL_TABLE_TOPIC")
SCHEMA_KEYWORD = os.environ.get("SCHEMA_KEYWORD")

# Kafka and ClickHouse configuration
conf = {
    "bootstrap.servers": f"{KAFKA_HOST}:{KAFKA_PORT}",
    "group.id": "all_tables_group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,  # disable auto-commit
}

consumer = Consumer(conf)
CLICKHOUSE_CONF = {
    "host": CLICKHOUSE_HOST,
    "port": CLICKHOUSE_PORT,
    "username": CLICKHOUSE_USERNAME,
    "password": CLICKHOUSE_PASSWORD,
    "database": CLICKHOUSE_DATABASE,
}
clickhouse_client = clickhouse_connect.get_client(**CLICKHOUSE_CONF)

# Kafka topic
consumer.subscribe([ALL_TABLE_TOPIC])

# Configure the logger
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.DEBUG,  # Capture all levels from DEBUG and above
    handlers=[
        logging.FileHandler("synchronizer.log"),  # Log to file
        logging.StreamHandler(),  # Log to console
    ],
)

logger = logging.getLogger("KafkaToClickHouse")


def consume_and_insert():
    """
    Consume messages from Kafka and insert them into ClickHouse.
    """
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                handle_kafka_error(msg)
            else:
                process_message_until_success(msg)
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
    finally:
        consumer.close()


def handle_kafka_error(msg):
    """
    Handle Kafka message errors.
    """
    if msg.error().code() == KafkaError._PARTITION_EOF:
        logger.info(f"Reached end of partition: {msg.topic()} [{msg.partition()}]")
    else:
        logger.error(f"Kafka error: {msg.error()}")


def process_message_until_success(msg):
    """
    Process Kafka message until successful.
    """
    retry_count = 0
    max_retries = 5

    while retry_count < max_retries:
        try:
            process_message(msg)
            consumer.commit()  # Commit the message only if processing is successful
            logger.info(f"Successfully processed message with offset {msg.offset()}.")
            break  # Break out of the loop once processed successfully
        except Exception as e:
            logger.error(
                f"Message processing error: {e}. Retrying... ({retry_count + 1}/{max_retries})"
            )
            retry_count += 1
            time.sleep(2)  # Wait before retrying

    if retry_count == max_retries:
        logger.error(
            f"Failed to process message after {max_retries} retries: {msg.value()}"
        )
        # Handle the message according to your retry policy (e.g., dead letter queue or log)


def process_message(msg):
    """
    Deserialize and process Kafka message.
    """
    try:
        value = json.loads(msg.value().decode("utf-8"))
        insert_data_into_clickhouse(value)
    except json.JSONDecodeError as e:
        logger.error(
            f"JSON deserialization error: {e}. Message: {msg.value().decode('utf-8')}"
        )
        # Do not commit the message, so it will be reprocessed
    except Exception as e:
        raise RuntimeError(f"Error processing the message: {e}")


def insert_data_into_clickhouse(data):
    """
    Insert data into ClickHouse, dynamically inserting all fields from the payload.
    """
    try:
        payload = data.get("payload", {})
        source = payload.get("source", {})
        table_name = source.get("table", "")
        schema_name = source.get("schema", "")

        # Ensure table_name is not empty
        logger.info(data)
        if not table_name:
            raise ValueError("Table name is empty in the source payload.")

        # Determine if the record is deleted
        if payload.get("after") is None:
            payload_data = payload.get("before", {})
            if "deleted_at" not in payload_data or payload_data["deleted_at"] is None:
                payload_data["deleted_at"] = datetime.now(timezone.utc)
        else:
            payload_data = payload.get("after", {})

        # Dynamically convert any field ending with 'at' to datetime if it's a timestamp
        for field, value in payload_data.items():
            if field.endswith("_at"):
                payload_data[field] = convert_timestamp_to_datetime(value)

        payload_data[SCHEMA_KEYWORD] = schema_name
        columns = list(payload_data.keys())
        values = list(payload_data.values())

        # Log columns and values for debugging
        logger.debug(f"Inserting data into table: {table_name}")
        logger.debug(f"Columns: {columns}")
        logger.debug(f"Values: {values}")

        # Check if columns and values match
        if len(columns) != len(values):
            raise ValueError("Number of columns and values do not match.")

        # Insert into ClickHouse
        clickhouse_client.insert(
            table_name,
            [values],
            column_names=columns,
        )
        logger.info(
            f"Processed data for table {table_name} with ID {payload_data.get('id')}"
        )
    except Exception as e:
        logger.error(f"Error inserting data into ClickHouse: {e}")
        if "Unrecognized column" in str(e):
            # Handle case where new columns are introduced dynamically
            logger.info("Handling dynamic columns. Retrying insert...")
            insert_data_into_clickhouse(data)  # Retry the insertion
        else:
            raise


def convert_timestamp_to_datetime(timestamp):
    """
    Convert a microsecond timestamp to a Python datetime object.
    """
    if timestamp is None:
        return None
    try:
        # Check if the timestamp is already a datetime object
        if isinstance(timestamp, datetime):
            return timestamp  # If it's already a datetime, return it as-is

        # Convert from microseconds to seconds (if it's not a datetime object)
        if isinstance(timestamp, (int, float)):
            timestamp_seconds = timestamp / 1_000_000
            return datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)

        # If timestamp is neither datetime nor a valid number, log an error
        logger.error(f"Invalid timestamp format: {timestamp}")
        return None

    except Exception as e:
        logger.error(f"Error converting timestamp {timestamp}: {e}")
        return None


if __name__ == "__main__":
    consume_and_insert()
