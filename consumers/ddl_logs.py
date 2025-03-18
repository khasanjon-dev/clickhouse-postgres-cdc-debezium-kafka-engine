import json
import logging
import os

import clickhouse_connect
from confluent_kafka import Consumer, KafkaError

KAFKA_HOST = os.environ.get("KAFKA_HOST")
KAFKA_PORT = os.environ.get("KAFKA_PORT")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = os.environ.get("CLICKHOUSE_PORT")
CLICKHOUSE_USERNAME = os.environ.get("CLICKHOUSE_USERNAME")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE")
DDL_LOGS_TOPIC = os.environ.get("DDL_LOGS_TOPIC")

# Logger configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("ddl_logs_consumer.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Kafka and ClickHouse configuration
KAFKA_CONF = {
    "bootstrap.servers": f"{KAFKA_HOST}:{KAFKA_PORT}",
    "group.id": "ddl_logs_group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

logger.info(KAFKA_CONF)
CLICKHOUSE_CONF = {
    "host": CLICKHOUSE_HOST,
    "port": CLICKHOUSE_PORT,
    "username": CLICKHOUSE_USERNAME,
    "password": CLICKHOUSE_PASSWORD,
    "database": CLICKHOUSE_DATABASE,
}
logger.info(CLICKHOUSE_CONF)

consumer = Consumer(KAFKA_CONF)
clickhouse_client = clickhouse_connect.get_client(**CLICKHOUSE_CONF)

# Kafka topic
consumer.subscribe([DDL_LOGS_TOPIC])


def consume_and_process():
    logger.info("Kafka consumer started.")
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
        logger.info("Consumer stopped by user.")
    except Exception as e:
        logger.error(f"Global error: {e}", exc_info=True)
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")


def handle_kafka_error(msg):
    if msg.error().code() == KafkaError._PARTITION_EOF:
        logger.warning(
            f"Reached the end of partition: {msg.topic()} [{msg.partition()}]"
        )
    else:
        logger.error(f"Kafka error: {msg.error()}")


def process_message_until_success(msg):
    while True:
        try:
            process_message(msg)
            consumer.commit()  # Commit offset only if successful
            break
        except Exception as e:
            logger.error(f"Error processing message: {e}. Retrying...", exc_info=True)


def process_message(msg):
    try:
        value = json.loads(msg.value().decode("utf-8"))
        logger.info(f"Message received: {value}")
        apply_ddl_action(value)
    except json.JSONDecodeError as e:
        logger.error(
            f"JSON deserialization error: {e}. Message: {msg.value().decode('utf-8')}"
        )
    except Exception as e:
        raise RuntimeError(f"Error processing the message: {e}")


def map_type_for_clickhouse(field_type):
    type_mapping = {
        "smallint": "Int16",
        "integer": "Int32",
        "bigint": "Int64",
        "numeric": "Decimal(38, 18)",
        "double precision": "Float64",
        "varchar": "String",
        "text": "String",
        "date": "Date",
        "time": "String",
        "timestamp": "DateTime",
        "interval": "String",
        "boolean": "UInt8",
        "uuid": "UUID",
        "array": "Array(String)",
        "json": "String",
        "jsonb": "String",
        "bytea": "String",
        "point": "String",
        "linestring": "String",
        "polygon": "String",
    }
    return type_mapping.get(field_type.lower(), "String")


def apply_ddl_action(data):
    try:
        after = data.get("payload", {}).get("after", {})
        action = after.get("action")
        table_name = after.get("current_table_name")
        column_name = after.get("current_column_name")
        column_type = after.get("current_column_type")
        old_column_name = after.get("old_column_name")

        if action == "column.add":
            mapped_column_type = map_type_for_clickhouse(column_type)
            if not column_exists(table_name, column_name):
                query = f"ALTER TABLE {table_name} ADD COLUMN {column_name} Nullable({mapped_column_type})"
                clickhouse_client.command(query)
                logger.info(
                    f"Added column {column_name} ({mapped_column_type}) to table {table_name}."
                )
            else:
                logger.warning(
                    f"Column {column_name} already exists in table {table_name}. Skipping..."
                )

        elif action == "column.rename":
            if column_exists(table_name, old_column_name):
                query = f"ALTER TABLE {table_name} RENAME COLUMN {old_column_name} TO {column_name}"
                clickhouse_client.command(query)
                logger.info(
                    f"Renamed column {old_column_name} to {column_name} in table {table_name}."
                )
            else:
                logger.warning(
                    f"Column {old_column_name} does not exist in table {table_name}. Skipping rename."
                )

        elif action == "column.type":
            if column_exists(table_name, column_name):
                mapped_column_type = map_type_for_clickhouse(column_type)
                # Ensure that the column is nullable when modifying its type
                query = f"ALTER TABLE {table_name} MODIFY COLUMN {column_name} Nullable({mapped_column_type})"
                clickhouse_client.command(query)
                logger.info(
                    f"Changed column {column_name} type to {mapped_column_type} with Nullable in table {table_name}."
                )
            else:
                logger.warning(
                    f"Column {column_name} does not exist in table {table_name}. Skipping type change."
                )

        elif action == "column.drop":
            if column_exists(table_name, column_name):
                query = f"ALTER TABLE {table_name} DROP COLUMN {column_name}"
                clickhouse_client.command(query)
                logger.info(f"Dropped column {column_name} from table {table_name}.")
            else:
                logger.warning(
                    f"Column {column_name} does not exist in table {table_name}. Skipping drop."
                )

        else:
            logger.warning(f"Unrecognized action: {action}")

    except Exception as e:
        logger.error(f"Error applying DDL action: {e}", exc_info=True)


def column_exists(table_name, column_name):
    try:
        existing_columns = clickhouse_client.query(
            f"DESCRIBE TABLE {table_name}"
        ).result_rows
        return any(column[0] == column_name for column in existing_columns)
    except Exception as e:
        logger.error(
            f"Error checking column existence in table {table_name}: {e}", exc_info=True
        )
        return False


if __name__ == "__main__":
    consume_and_process()
