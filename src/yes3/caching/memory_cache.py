from datetime import datetime, UTC
from typing import Any, Optional, Self

from yes3.caching.base import CacheCore, CachedItemMeta, raise_not_found, _NotSpecified


class MemoryCache(CacheCore):
    def __init__(self, active=True, read_only=False, log_level=None):
        super().__init__(active=active, read_only=read_only, log_level=log_level)
        self._data: dict[str, Any] = {}
        self._meta: dict[str, CachedItemMeta] = {}

    def __contains__(self, key: str):
        if not self.is_active():
            return False
        found = key in self._data
        if found and key not in self._meta:
            raise RuntimeError(f"data exists, but no metadata found, for key '{key}' in {type(self).__name__}")
        return found

    def get(self, key: str, default=_NotSpecified):
        if not self.is_active() or key not in self:
            if default is _NotSpecified:
                raise_not_found(key)
            else:
                return default
        return self._data[key]

    def get_meta(self, key: str) -> CachedItemMeta:
        if not self.is_active() or key not in self:
            raise_not_found(key)
        return self._meta[key]

    def put(
            self,
            key: str,
            obj,
            *,
            update=False,
            meta: Optional[CachedItemMeta] = None,
            log_msg: Optional[str] = None,
    ) -> Self:
        if self.is_read_only():
            raise TypeError('Cache is in read only mode')
        if self.is_active():
            if key in self and not update:
                raise ValueError(f"key '{key}' already exists in cache; use 'update' to overwrite")
            if meta is None:
                meta = CachedItemMeta(key=key, timestamp=datetime.now(UTC), path=None, size=None)
            self._meta[key] = meta
            self._data[key] = obj
        else:
            self.logger.warning(f"WARNING: {type(self).__name__} is not active")
        if log_msg:
            self.logger.warn("Log messages are not persisted in MemoryCache")
            self.logger.info(log_msg)
        return self

    def remove(self, key: str, log_msg: Optional[str] = None) -> Self:
        if self.is_active():
            if key in self:
                if self.is_read_only():
                    raise TypeError('Cache is in read only mode')
                self._data.pop(key)
                self._meta.pop(key)
        else:
            self.logger.warning(f"WARNING: {type(self).__name__} is not active")
        return self

    def keys(self) -> list[Any]:
        if not self.is_active():
            return []
        else:
            return list(self._data.keys())

    def clear(self, force=False) -> Self:
        if self.is_active():
            if len(self.keys()) > 0:
                if not force:
                    raise RuntimeError(f'Clearing this {type(self).__name__} requires specifying force=True')
                self._data: dict[str, Any] = {}
                self._meta: dict[str, CachedItemMeta] = {}
        else:
            self.logger.warning(f"WARNING: {type(self).__name__} is not active")
        return self
