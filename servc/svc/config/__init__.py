import json
import os
import socket
from typing import Any

import yaml

defaults = {
    "conf.http.port": int(os.getenv("PORT", 3000)),
    "conf.instanceid": os.getenv("INSTANCE_ID", socket.gethostname()),
    "conf.cache.url": os.getenv(
        "CACHE_URL", os.getenv("REDIS_URL", "redis://localhost:6379")
    ),
    "conf.bus.url": os.getenv(
        "BUS_URL", os.getenv("CLOUDAMQP_URL", "amqp://localhost:5672")
    ),
    "conf.bus.route": os.getenv("CONF__BUS__QUEUE", os.getenv("QUEUE_NAME", "test")),
    "conf.bus.routemap": json.loads(os.getenv("CONF__BUS__ROUTEMAP", json.dumps({}))),
    "conf.bus.prefix": "",
    "conf.bus.bindtoeventexchange": True,
}


class Config:
    _configDictionary: dict = {}

    def __init__(self, config_path: str | None = None):
        if config_path is None:
            config_path = os.getenv("CONF__FILE", "/config/config.yaml")
        elif not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        self.setValue("conf.file", config_path)
        if os.path.exists(config_path):
            with open(config_path) as stream:
                self._configDictionary = yaml.safe_load(stream)
                if self._configDictionary is None:
                    self._configDictionary = {}
            if self.get("conf.file") is None:
                self.setValue("conf.file", config_path)

        # set certain defaults
        for key, value in defaults.items():
            if self.get(key) is None:
                self.setValue(key, value)

        # parse the environment variables and override the configuration file
        for key, value in os.environ.items():
            if key.startswith("CONF__") and key not in (
                "CONF__FILE",
                "CONF__BUS__ROUTEMAP",
            ):
                self.setValue(key.replace("__", ".").lower(), value)

        # validate conf.file matches config_path. Otherwise, raise an exception because we are not able to load the configuration file
        if self.get("conf.file") != config_path:
            raise Exception("Configuration file does not match the configuration path")

    def get(self, key: str) -> Any:
        keys = key.lower().split(".")
        subconfig = self._configDictionary

        for index, subkey in enumerate(keys):
            is_last = index == len(keys) - 1

            if is_last:
                return subconfig.get(subkey)
            else:
                subconfig = subconfig.get(subkey, {})

    def setValue(self, key: str, value: Any):
        keys = key.lower().split(".")
        subconfig = self._configDictionary

        for index, subkey in enumerate(keys):
            is_last = index == len(keys) - 1
            if is_last:
                subconfig[subkey] = value
            elif subkey not in subconfig:
                subconfig[subkey] = {}

            subconfig = subconfig[subkey]

    def getAll(self) -> dict:
        return self._configDictionary

    def setAll(self, config: dict):
        self._configDictionary = config
