# reddit_producer.py (Simulates Climate Change Posts + Sends Heartbeat)
import time
import logging
import os
# import praw # <-- No longer needed
from kafka import KafkaProducer, errors as KafkaErrors
import json
from dotenv import load_dotenv
import datetime
import random # Still useful for simulated data and heartbeat

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("SimulatedClimateProducer") # Updated logger name

# --- Load Environment Variables ---
load_dotenv()
logger.info("Loading environment variables for Simulated Producer...")

# --- Remove Reddit Configuration ---
# REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
# ... etc ...

# --- Kafka Configuration (Keep this) ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_REDDIT_TOPIC", "reddit_posts_topic") # Keep same topic name

# --- Timing Configuration (Keep this) ---
# Controls how often a *simulated* post and heartbeat are sent
POLL_INTERVAL_SECONDS = int(os.getenv("SIMULATION_INTERVAL_SECONDS", 5)) # Send simulated post every 5s default

# --- Basic Kafka Validation ---
missing_vars = []
if not KAFKA_BROKER: missing_vars.append("KAFKA_BROKER")
if not KAFKA_TOPIC: missing_vars.append("KAFKA_REDDIT_TOPIC")

if missing_vars:
    logger.error(f"❌ Missing required Kafka environment variables: {', '.join(missing_vars)}")
    exit(1)
else:
    logger.info("✅ Successfully loaded Kafka configuration.")
    logger.info(f"   - Kafka Broker: {KAFKA_BROKER}")
    logger.info(f"   - Kafka Topic: {KAFKA_TOPIC}")
    logger.info(f"   - Simulation Interval: {POLL_INTERVAL_SECONDS}s")

# --- Initialize Kafka Producer (Keep this) ---
logger.info(f"Attempting to connect to Kafka broker at {KAFKA_BROKER}...")
producer = None
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5,
        acks='all',
    )
    logger.info("✅ Kafka Producer initialized successfully.")
except KafkaErrors.NoBrokersAvailable as e:
    logger.error(f"❌ Failed to connect to Kafka brokers at {KAFKA_BROKER}: {e}")
    exit(1)
except KafkaErrors.KafkaError as e:
     logger.error(f"❌ Kafka error during producer initialization: {e}")
     exit(1)
except Exception as e:
    logger.error(f"❌ An unexpected error occurred during Kafka initialization: {e}", exc_info=True)
    exit(1)


# --- Reddit Login Function --- (REMOVED)
# def login_to_reddit(): ...

# --- State for Seen Posts --- (REMOVED)
# seen_post_ids = set()

# --- Sample Climate Change Posts Data ---
# Add more variety or realistic text as needed
SAMPLE_CLIMATE_POSTS = [
    {
        "subreddit": "climatechange",
        "author": "EcoWarrior88",
        "title": "Study shows alarming acceleration in Arctic ice melt",
        "selftext": "Just read a new paper in Nature Climate Change detailing how much faster the Arctic is warming compared to previous models. The feedback loops are kicking in stronger than expected. We need urgent action now! #climatecrisis",
        "url": "https://example.com/arctic_melt_study"
    },
    {
        "subreddit": "environment",
        "author": "GreenThumb2",
        "title": "Tips for reducing your carbon footprint at home?",
        "selftext": "Hey everyone, looking for practical ways to lower my household's carbon emissions. Already recycling and composting, but what else works? Thinking about solar panels but they seem expensive. Any advice?",
        "url": "https://reddit.com/r/environment/comments/abcde"
    },
    {
        "subreddit": "airquality",
        "author": "CityDweller_AQ",
        "title": "Air quality index hitting unhealthy levels again this week.",
        "selftext": "Anyone else notice the haze? My app shows PM2.5 levels are really high, especially during rush hour. Seems linked to the ongoing wildfires hundreds of miles away combined with local traffic. Stay safe, wear masks if sensitive.",
        "url": "https://example.com/air_quality_alert"
    },
    {
        "subreddit": "climatechange",
        "author": "PolicyWonk_CC",
        "title": "Analysis of the latest proposed carbon tax legislation",
        "selftext": "Diving deep into the economic impacts and potential effectiveness of the new carbon pricing bill. While it has some good points, there are concerns about equity and potential loopholes for major polluters. Full analysis linked.",
        "url": "https://example.com/carbon_tax_analysis"
    },
    {
        "subreddit": "environment",
        "author": "SustainableLife",
        "title": "The impact of fast fashion on the planet",
        "selftext": "It's shocking how much water and energy goes into making cheap clothes that are only worn a few times. Let's discuss alternatives like thrifting, clothing swaps, and supporting sustainable brands.",
        "url": "https://example.com/fast_fashion_impact"
    }
]
logger.info(f"Loaded {len(SAMPLE_CLIMATE_POSTS)} sample posts for simulation.")

# --- Simulate and Send Post Function ---
def simulate_and_send_post(producer_instance, sample_posts, message_index, simulated_id_counter):
    """Selects a sample post, adds dynamic data, and sends it."""
    if not producer_instance:
        logger.warning("Producer not available, cannot send simulated post.")
        return message_index, simulated_id_counter

    # Cycle through the sample posts list
    post_template = sample_posts[message_index % len(sample_posts)]
    current_time = datetime.datetime.now(datetime.timezone.utc)

    # Create the simulated post data, mimicking the original structure
    post_data = {
        'message_type': 'SIMULATED_REDDIT_POST', # Indicate this is simulated
        'subreddit': post_template['subreddit'],
        'id': f"sim_{simulated_id_counter}", # Create a unique simulated ID
        'title': post_template['title'],
        'score': random.randint(1, 500), # Simulate score
        'num_comments': random.randint(0, 250), # Simulate comments
        'created_utc': current_time.timestamp(), # Simulate creation time
        'created_iso': current_time.isoformat(),
        'url': post_template['url'],
        'selftext': post_template.get('selftext', ""), # Use get for optional selftext
        'author': post_template['author'],
        'fetch_time_utc': current_time.isoformat() # Represents simulation time
    }

    # Send the simulated data to Kafka
    try:
        logger.info(f"-> Sending SIMULATED post ID {post_data['id']} (Index: {message_index % len(sample_posts)})...")
        producer_instance.send(KAFKA_TOPIC, value=post_data)
        # Rely on main loop flush
    except KafkaErrors.KafkaError as kafka_err:
         logger.error(f"Kafka error sending simulated post {post_data['id']}: {kafka_err}")
    except Exception as send_err:
         logger.error(f"Unexpected error sending simulated post {post_data['id']}: {send_err}", exc_info=True)

    # Return the next index and incremented ID counter
    return message_index + 1, simulated_id_counter + 1


# --- Send Heartbeat Function (Keep this) ---
def send_heartbeat(producer_instance):
    """Sends a simple heartbeat message to Kafka."""
    # (Function content remains the same as before)
    if not producer_instance:
        logger.warning("Cannot send heartbeat, producer is not available.")
        return

    heartbeat_message = {
        'message_type': 'HEARTBEAT',
        'producer_id': 'simulated_climate_producer', # Update ID
        'timestamp_iso': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'status': 'ALIVE_SIMULATING',
        'random_val': random.randint(1, 100)
    }
    try:
        logger.info("-> Sending HEARTBEAT message...")
        producer_instance.send(KAFKA_TOPIC, value=heartbeat_message)
    except KafkaErrors.KafkaError as kafka_err:
        logger.error(f"Kafka error sending HEARTBEAT: {kafka_err}")
    except Exception as send_err:
        logger.error(f"Unexpected error sending HEARTBEAT: {send_err}", exc_info=True)


# --- Main Loop (Modified for Simulation) ---
if __name__ == "__main__":
    # reddit_instance = None # Removed
    simulated_message_index = 0 # Index for cycling through SAMPLE_CLIMATE_POSTS
    simulated_id_counter = 1 # Counter to create unique simulated post IDs
    heartbeat_counter = 0

    logger.info("🚀 Starting Simulated Climate Post Producer with Heartbeat...")

    while True:
        try:
            # --- 1. Simulate and Send One Post ---
            simulated_message_index, simulated_id_counter = simulate_and_send_post(
                producer, SAMPLE_CLIMATE_POSTS, simulated_message_index, simulated_id_counter
            )

            # --- 2. Send Heartbeat Message ---
            send_heartbeat(producer)
            heartbeat_counter += 1

            # --- 3. Flush Kafka Producer ---
            try:
                logger.debug("Flushing Kafka producer...")
                producer.flush(timeout=10)
                logger.debug("Producer flushed.")
            except KafkaErrors.KafkaError as flush_err:
                 logger.warning(f"Kafka error during flush: {flush_err}")
            except Exception as flush_exc:
                 logger.error(f"Unexpected error during flush: {flush_exc}", exc_info=True)

            # --- 4. Wait for Next Cycle ---
            logger.info(f"Cycle complete (Simulated Post Index: {simulated_message_index}, Heartbeat #{heartbeat_counter}). Waiting {POLL_INTERVAL_SECONDS} seconds...")
            time.sleep(POLL_INTERVAL_SECONDS)

        # --- Error Handling (No PRAW errors needed) ---
        except KafkaErrors.KafkaError as e:
            logger.error(f"Kafka error occurred in main loop: {e}. Check Kafka status. Will attempt to continue.")
            time.sleep(60)
        except KeyboardInterrupt:
            logger.info("🛑 SIGINT received, stopping producer manually...")
            break
        except Exception as e:
            logger.error(f"❌ An unexpected fatal error occurred in the main loop: {e}", exc_info=True)
            logger.error("   Waiting 60 seconds before attempting to continue...")
            time.sleep(60)

    # --- Cleanup (Keep this) ---
    if producer:
        logger.info("Flushing final messages and closing Kafka producer...")
        try:
            producer.flush(timeout=10)
            producer.close(timeout=10)
            logger.info("Kafka producer closed successfully.")
        except Exception as close_err:
             logger.error(f"Error during final flush/close: {close_err}", exc_info=True)

    logger.info("Simulated Climate Producer script finished.")