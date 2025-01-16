from typing import Iterator, Self

from yes3.caching.base import CacheCore, raise_not_found, UNSPECIFIED


class MultiCache(CacheCore):
    def __init__(self, caches: list[CacheCore], left_to_right_priority=True, sync_all=False, active=True,
                 read_only=False):
        super().__init__(active=active, read_only=read_only)
        if left_to_right_priority:
            self._caches = list(caches)
        else:
            self._caches = list(caches[::-1])
        self._sync_all = sync_all

    def __iter__(self) -> Iterator[CacheCore]:
        return iter(self._caches)

    def activate(self) -> Self:
        super().activate()
        for cache in self:
            cache.activate()
        return self

    def deactivate(self) -> Self:
        super().deactivate()
        for cache in self:
            cache.deactivate()
        return self

    def is_active(self) -> bool:
        return super().is_active() and any(cache.is_active() for cache in self)

    def is_read_only(self) -> bool:
        return super().is_read_only() or all(cache.is_read_only() for cache in self)

    def add_cache(self, cache: CacheCore, index=-1) -> Self:
        if index is not None and index >= 0:
            self._caches.insert(index, cache)
        else:
            self._caches.append(cache)
        return self

    def subcache(self, *args, **kwargs) -> Self:
        subcaches = [cache.subcache(*args, **kwargs) for cache in self]
        return type(self)(subcaches, sync_all=self._sync_all, active=self.is_active(), read_only=self.is_read_only())

    def __contains__(self, key: str):
        for cache in self:
            if key in cache:
                return True
        return False

    def get(self, key: str, default=UNSPECIFIED, sync=None):
        if sync is None:
            sync = self._sync_all
        result = UNSPECIFIED
        for cache in self:
            if key in cache:
                result = cache.get(key)
                break
        if result is UNSPECIFIED:
            if default is UNSPECIFIED:
                raise_not_found(key)
            else:
                result = default
        elif sync:
            for cache in self:
                if cache.is_read_only():
                    continue
                if key not in cache:
                    cache.put(key, result)
        return result

    def put(self, key: str, obj, *, update=False) -> Self:
        for cache in self:
            if cache.is_read_only():
                continue
            cache.put(key, obj, update=update)
            if not self._sync_all:
                break
        return self

    def update(self, key: str, obj) -> Self:
        if key not in self:
            raise_not_found(key)
        return self.put(key, obj, update=True)

    def remove(self, key) -> Self:
        for cache in self:
            if key in cache:
                cache.remove(key)
        return self

    def pop(self, key, default=UNSPECIFIED):
        item = self.get(key, default=default)
        if key in self:
            self.remove(key)
        return item

    def keys(self) -> list[str]:
        if not self.is_active():
            return []
        else:
            keys = []
            for cache in self:
                keys.extend(cache.keys())
            return list(set(keys))

    def sync_now(self) -> Self:
        for key in self.keys():
            obj = None
            for cache in self:
                if key not in cache:
                    if obj is None:
                        obj = self.get(key, sync=False)
                    cache.put(key, obj)
        return self

    def sync_always(self):
        self._sync_all = True
        self.sync_now()

    def __repr__(self):
        return f"{type(self).__name__}({', '.join([str(cache) for cache in self])})"
