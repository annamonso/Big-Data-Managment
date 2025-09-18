import time
import logging
import os
from dotenv import load_dotenv
from atproto import Client
from kafka import KafkaProducer
import json
from datetime import datetime, timezone

# Load environment variables from a .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Mantener la última fecha vista por autor
last_seen = {}

def login_to_client():
    """
    Log in to the AT Proto client using environment variables.
    Returns:
        Client: Logged-in AT Proto client instance.
        Profile: Profile object for the logged-in user.
    """
    try:
        email = os.getenv("BLUESKY_EMAIL")
        password = os.getenv("BLUESKY_PASSWORD")
        if not email or not password:
            raise ValueError("Missing BLUESKY_EMAIL or BLUESKY_PASSWORD in environment variables.")

        client = Client()
        profile = client.login(email, password)
        logger.info("Login successful. Welcome, %s!", profile.display_name)
        return client, profile
    except Exception as e:
        logger.error("Failed to log in: %s", e)
        raise

# Lista de autores a monitorizar
authors = [os.getenv("AUTHOR_1"), os.getenv("AUTHOR_2")]

# Inicializar last_seen para cada autor al momento actual
now = datetime.now(timezone.utc)
for author in authors:
    last_seen[author] = now

# Keywords para filtrar
topic_keywords = ["temperature", "climate", "air quality","Barcelona","paris","Madrid","valencia","seville","malaga","bilbao"]

def parse_created_at(ts_str: str) -> datetime:
    """
    Parsea la fecha ISO y devuelve un datetime con zona UTC.
    """
    # asume formato ISO 8601
    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    return dt

def fetch_and_send_posts(client, authors, keywords):
    """
    Fetch posts de los autores; filtra sólo los nuevos según last_seen y por keywords.
    """
    global last_seen

    for author in authors:
        try:
            profile_feed = client.get_author_feed(actor=author)
            max_ts = last_seen[author]

            for feed_view in profile_feed.feed:
                created_at_str = feed_view.post.record.created_at
                created_at = parse_created_at(created_at_str)
                text = feed_view.post.record.text.lower()

                # Sólo posts posteriores a last_seen
                if created_at > last_seen[author]:
                    # Filtrar por keyword
                    if any(kw.lower() in text for kw in keywords):
                        post_data = {
                            'author': author,
                            'created_at': created_at_str,
                            'text': feed_view.post.record.text
                        }
                        producer.send('bluesky_topic', post_data)
                        logger.info(f"Sent to Kafka: {post_data}")

                    # Actualizar max_ts local
                    if created_at > max_ts:
                        max_ts = created_at

            # Actualizamos last_seen[author] al mayor creado
            last_seen[author] = max_ts

        except Exception as e:
            logger.error(f"Error fetching/sending posts for author {author}: {e}")

def listen_for_new_posts(client, authors, keywords, interval=5):
    """
    Cada `interval` segundos busca posts nuevos.
    """
    while True:
        logger.info("Checking for new posts...")
        fetch_and_send_posts(client, authors, keywords)
        time.sleep(interval)

if __name__ == "__main__":
    try:
        client, profile = login_to_client()
        listen_for_new_posts(client, authors, topic_keywords, interval=1)
    except Exception as e:
        logger.error("Fatal error: %s", e)
