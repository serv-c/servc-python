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
    "conf.worker.bindtoeventexchange": True,
    "conf.worker.exiton5xx": True,
    "conf.worker.exiton4xx": False,
}

BOOLEAN_CONFIGS = os.getenv(
    "SERVC_BOOLEAN_CONFIGS",
    ",".join(
        [
            "conf.worker.exiton400",
            "conf.worker.exiton404",
            "conf.worker.exiton401",
            "conf.worker.exiton422",
            "conf.worker.exiton4xx",
            "conf.worker.exiton5xx",
            "conf.worker.bindtoeventexchange",
        ]
    ),
).split(",")
DOT_MARKER = os.getenv("SERVC_DOT_MARKER", "_DOT_")
DASH_MARKER = os.getenv("SERVC_DASH_MARKER", "_DASH_")


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
                newkey = key.replace(DASH_MARKER, "-").replace("__", ".")
                if newkey.lower() in BOOLEAN_CONFIGS:
                    value = value.lower() in ("yes", "true", "t", "1")
                self.setValue(newkey, value)

        self.setValue("conf.bus.instanceid", self.get("conf.instanceid"))

        # validate conf.file matches config_path. Otherwise, raise an exception because we are not able to load the configuration file
        if self.get("conf.file") != config_path:
            raise Exception("Configuration file does not match the configuration path")

    def get(self, key: str) -> Any:
        keys = [x.replace(DOT_MARKER, ".") for x in key.split(".")]
        subconfig = self._configDictionary

        for index, subkey in enumerate(keys):
            is_last = index == len(keys) - 1

            if is_last:
                return subconfig.get(subkey)
            else:
                subconfig = subconfig.get(subkey, {})

    def setValue(self, key: str, value: Any):
        key = key.lower().replace(DOT_MARKER.lower(), DOT_MARKER)
        keys = [x.replace(DOT_MARKER, ".") for x in key.split(".")]
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
