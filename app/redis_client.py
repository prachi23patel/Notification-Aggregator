import redis.asyncio as aioredis
import os
from dotenv import load_dotenv

load_dotenv()

db_url = os.getenv("REDIS_URL")
# Format: redis://[[user]:password@]host[:port]/db_number


# Initialize using from_url instead of the normal constructor
redis_client = aioredis.from_url(
    db_url,
    decode_responses=True  # Keeps strings clean instead of raw bytes
)