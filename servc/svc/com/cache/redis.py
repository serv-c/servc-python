import datetime
import decimal
import json
from typing import Any

import simplejson
from redis import Redis

from servc.svc.com.cache import CacheComponent
from servc.svc.config import Config


def decimal_default(obj: Any) -> None | str | float:
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    return None


class CacheRedis(CacheComponent):
    _redisClient: Redis

    _url: str

    def __init__(self, config: Config):
        super().__init__(config)
        self._url = str(config.get("url"))

    @property
    def conn(self):
        return self._redisClient

    def _connect(self):
        if self.isOpen:
            return None
        self._redisClient = Redis.from_url(self._url)
        self._isReady = self._redisClient.ping()
        self._isOpen = self._redisClient.ping()
        return None

    def _close(self):
        if self._isOpen:
            self._redisClient.close()
            self._isReady = False
            self._isOpen = False
            return True
        return False

    def setKey(self, id: str, value: Any) -> str:
        if not self.isReady:
            self.connect()
            return self.setKey(id, value)
        self._redisClient.set(
            id, simplejson.dumps(value, default=decimal_default, ignore_nan=True)
        )
        return id

    def getKey(self, id: str) -> Any | None:
        if not self.isReady:
            self.connect()
            return self.getKey(id)
        value = self._redisClient.get(id)
        if value:
            return json.loads(value)  # type: ignore
        return value

    def deleteKey(self, id: str) -> bool:
        if not self.isReady:
            self.connect()
            return self.deleteKey(id)
        return self.conn.delete(id) > 0
