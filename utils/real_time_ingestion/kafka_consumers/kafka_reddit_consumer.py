# reddit_consumer.py
from kafka import KafkaConsumer, errors as KafkaErrors
import json
import logging
import os
from dotenv import load_dotenv

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("RedditConsumer")

# --- Load Environment Variables ---
load_dotenv() # Load .env file if present
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_REDDIT_TOPIC", "reddit_posts_topic")
CONSUMER_GROUP_ID = os.getenv("KAFKA_REDDIT_CONSUMER_GROUP", "reddit_consumer_group")
# Use .jsonl for JSON Lines format convention
OUTPUT_FILE_PATH = os.getenv("OUTPUT_FILE_PATH", "./landing_zone/streaming/reddit.jsonl")

logger.info("Reddit Consumer starting...")
logger.info(f"   - Kafka Broker: {KAFKA_BROKER}")
logger.info(f"   - Kafka Topic: {KAFKA_TOPIC}")
logger.info(f"   - Consumer Group ID: {CONSUMER_GROUP_ID}")
logger.info(f"   - Output File: {OUTPUT_FILE_PATH}")


# --- Kafka Consumer Configuration ---
try:
    logger.info(f"Attempting to connect to Kafka broker at {KAFKA_BROKER}...")
    consumer = KafkaConsumer(
        KAFKA_TOPIC, # Topic(s) to subscribe to
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='latest',  # Start reading at the end of the log
        enable_auto_commit=True,    # Commit offsets automatically
        group_id=CONSUMER_GROUP_ID, # Consumer group ID
        # Use lambda to handle potential decoding errors gracefully
        value_deserializer=lambda m: json.loads(m.decode('utf-8', errors='replace'))
    )
    logger.info(f"✅ Kafka Consumer initialized. Subscribed to topic '{KAFKA_TOPIC}'. Waiting for messages...")

except KafkaErrors.NoBrokersAvailable as e:
    logger.error(f"❌ Failed to connect to Kafka brokers at {KAFKA_BROKER}: {e}")
    logger.error("   Please ensure Kafka is running and accessible.")
    exit(1)
except Exception as e:
    logger.error(f"❌ An unexpected error occurred during Kafka Consumer initialization: {e}")
    exit(1)


# --- Prepare Output File ---
try:
    # Ensure the output directory exists
    output_dir = os.path.dirname(OUTPUT_FILE_PATH)
    if output_dir: # Check if dirname returned anything (it might be empty if file is in root)
         os.makedirs(output_dir, exist_ok=True)
         logger.info(f"Ensured output directory exists: {output_dir}")

    # Optionally clear the file on startup, or just append if it exists
    # Clear the file:
    # with open(OUTPUT_FILE_PATH, "w") as f:
    #     f.truncate(0)
    # logger.info(f"Cleared content (if any) from {OUTPUT_FILE_PATH}")
    # OR let it append (more common for streaming consumers):
    logger.info(f"Consumer will append messages to {OUTPUT_FILE_PATH}")

except OSError as e:
    logger.error(f"❌ Error preparing output file path {OUTPUT_FILE_PATH}: {e}")
    exit(1)


# --- Poll for messages and append to file ---
message_count = 0
try:
    for message in consumer:
        # message value holds the data from the producer
        post_data = message.value

        # Basic check if data looks like a dictionary (JSON object)
        if isinstance(post_data, dict):
            post_id = post_data.get('id', 'N/A')
            subreddit = post_data.get('subreddit', 'N/A')
            title_preview = post_data.get('title', 'N/A')[:50] # Preview title

            logger.info(f"Received message: Offset={message.offset}, ID={post_id}, Subreddit={subreddit}, Title='{title_preview}...'")

            # Append message as a JSON line to the output file
            try:
                with open(OUTPUT_FILE_PATH, "a", encoding='utf-8') as f:
                    # Use ensure_ascii=False to preserve non-ASCII characters correctly
                    f.write(json.dumps(post_data, ensure_ascii=False) + "\n")
                message_count += 1
            except IOError as e:
                logger.error(f"❌ Error writing message (ID: {post_id}) to file {OUTPUT_FILE_PATH}: {e}")
                # Consider adding a retry mechanism or stopping the consumer
            except Exception as e:
                 logger.error(f"❌ Unexpected error processing/writing message (ID: {post_id}): {e}")

        else:
            # Log if the message value isn't the expected dictionary format
            logger.warning(f"Received message with unexpected format: Offset={message.offset}, Type={type(post_data)}")
            logger.warning(f"Message Value: {post_data}")


except KeyboardInterrupt:
    logger.info("🛑 Consumer stopped manually.")
except Exception as e:
    logger.error(f"❌ Consumer failed unexpectedly: {e}", exc_info=True) # Log traceback
finally:
    if consumer:
        consumer.close()
        logger.info("📴 Kafka Consumer connection closed.")
    logger.info(f"Total messages processed in this session: {message_count}")