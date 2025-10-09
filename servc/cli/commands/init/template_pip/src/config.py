import os

COMPONENT_NAME = "test-service"
PREFIX = os.environ.get("PREFIX", "servc")
QUEUE_NAME = os.environ.get("QUEUE_NAME", f"{PREFIX}-{COMPONENT_NAME}")