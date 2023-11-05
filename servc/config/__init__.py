import os
import socket

redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
cache_url = os.getenv("CACHE_URL", redis_url)

cloudamqp_url = os.getenv("CLOUDAMQP_URL", "amqp://localhost:5672")
bus_url = os.getenv("BUS_URL", cloudamqp_url)

postgres_url = os.getenv("POSTGRES_URL", "postgresql://localhost:5432")
db_url = os.getenv("DATABASE_URL", postgres_url)

port = int(os.getenv("PORT", 3000))
instance_id = os.getenv("INSTANCE_ID", socket.gethostname())
