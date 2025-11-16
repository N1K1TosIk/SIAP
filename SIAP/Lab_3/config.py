import os

# Kafka
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")  
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "etl_topic")

POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://user:pass@localhost:5432/dbname")


