import datetime
import decimal
import json
from typing import Any, Union

import simplejson
from redis import Redis

from servc.com.cache import CacheComponent


def decimal_default(obj: Any):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    return


class CacheRedis(CacheComponent):
    _redisClient: Union[Redis.client, None] = None

    def _connect(self):
        if self.isOpen:
            return None
        self._redisClient = Redis.from_url(self._url)
        self.isReady = self._redisClient.ping()
        self.isOpen = self._redisClient.ping()
        return None

    def _close(self):
        if self._isOpen:
            self._redisClient.close()
            self.isReady = False
            self.isOpen = False
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
            return json.loads(value)
        return value

    def deleteKey(self, id: str) -> bool:
        if not self.isReady:
            self.connect()
            return self.deleteKey(id)
        return self._redisClient.delete(id) > 0
