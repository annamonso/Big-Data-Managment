from kafka import KafkaConsumer 
import json
import logging
import os
from pymongo import MongoClient
from datetime import datetime

from transformers import pipeline

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("KafkaBlueskyConsumer")

# --- Kafka Consumer Configuration ---
consumer = KafkaConsumer(
    'bluesky_topic',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='bluesky_consumer_group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

logger.info("📡 Kafka Bluesky Consumer started. Listening for messages...")

# --- MongoDB connection ---
mongo_client = MongoClient("mongodb://mongo:27017")  # Cambia "mongo" si tu servicio se llama diferente
db = mongo_client["bluesky_db"]
collection = db["city_sentiment_stats"]

# --- Output File Path ---
output_file = "./landing_zone/streaming/bluesky.json"
output_file2 = "./exploitation_zone/sentiment_analysis.json"
# # Ensure the directory exists
# os.makedirs(os.path.dirname(output_file), exist_ok=True)

# # Erase all content in the JSON file before starting to consume
# if os.path.exists(output_file):
#     with open(output_file, "w") as f:
#         f.truncate(0)  


#1) NER to find city/GPE mentions
ner = pipeline(
    "ner",
    model="Davlan/xlm-roberta-large-ner-hrl",  # multiligüe, reconoce LOC/GPE mejor
    tokenizer="Davlan/xlm-roberta-large-ner-hrl",
    grouped_entities=True
)

# 2) Zero-Shot Classification to decide "good" vs "bad"
#    You can swap to sentiment-analysis if you prefer a simple positive/negative.
classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")

# Candidate labels for whether a post is beneficial to a city
candidate_labels = ["good", "bad", "neutral"]



# --- Poll for messages and append to file ---
try:
    for message in consumer:
        post_data = message.value
        author = post_data.get('author')
        created_at = post_data.get('created_at')
        text = post_data.get('text')

        logger.info(f"✍️ Author: {author} | 🕒 Time: {created_at} | 📄 Post: {text}")

        # Append message as a JSON line
        with open(output_file, "a", encoding='utf-8') as f:
            f.write(json.dumps(post_data, ensure_ascii=False) + "\n")

        # 1) Extract any GPE/LOC entities (cities, countries, etc.)
        ner_results = ner(text)
        # logger.info(f" !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! NER Results: {ner_results}")
        # Keep only GPE or LOC, normalize text
        cities = {
            ent["word"].strip()
            for ent in ner_results
            if ent["entity_group"] in ("GPE", "LOC")
        }

        if not cities:
            logger.info("No city found in post, skipping.")
            continue

        # 2) Classify overall stance
        cls = classifier(text, candidate_labels)
        # pick highest‐scoring label
        best_label = cls["labels"][0]
        score = cls["scores"][0]

        # 3) Build enriched record(s)
        enriched = []
        for city in cities:
            enriched.append({
                "created_at": created_at,
                "city": city,
                "assessment": best_label,      # "good for city" or "not good for city"
                "score": score,
                "text": text
            })
            logger.info(f"Enriched record for {city}: {enriched[-1]}")
        
        with open(output_file2, "a", encoding="utf-8") as f:
            for record in enriched:
                # 1. Escribir el análisis enriquecido al archivo JSON
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

                # 2. Actualizar MongoDB con stats
                city = record["city"]
                assessment = record["assessment"]

                # Convertir a campo contable: good → positive, etc.
                update_field = {
                    "good": "positive",
                    "bad": "negative",
                    "neutral": "neutral"
                }.get(assessment, "neutral")  # fallback por seguridad

                collection.update_one(
                    {"city": city},
                    {
                        "$inc": {update_field: 1}
                    },
                    upsert=True
                )
                logger.info(f"✅ database updated for {city}: {update_field} incremented.")


        logger.info(f"✅ Processed post for {cities}: {best_label} ({score:.2f})")


except KeyboardInterrupt:
    logger.info("🛑 Consumer stopped manually.")
except Exception as e:
    logger.error(f"❌ Consumer failed: {e}")
finally:
    consumer.close()
    logger.info("📴 Kafka Consumer connection closed.")

