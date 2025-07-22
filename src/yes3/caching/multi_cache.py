from collections import defaultdict
from typing import Iterator, Optional, Self

from tqdm import tqdm

from yes3 import S3Location
from yes3.caching.base import CacheCore, CachedItemMeta, check_meta_mismatches, raise_not_found, _NotSpecified


class MultiCache(CacheCore):
    def __init__(
            self,
            caches: list[CacheCore],
            left_to_right_priority=True,
            sync_all=False,
            active=True,
            read_only=False,
            log_level=None,
    ):
        super().__init__(active=active, read_only=read_only, log_level=log_level)
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

    def set_read_only(self, value: bool) -> Self:
        super().set_read_only(value)
        for cache in self:
            cache.set_read_only(value)
        return self

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

    def get(self, key: str, default=_NotSpecified, sync=None):
        if sync is None:
            sync = self._sync_all
        result = _NotSpecified
        for cache in self:
            if key in cache:
                result = cache.get(key)
                break
        if result is _NotSpecified:
            if default is _NotSpecified:
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

    def get_meta(self, key) -> CachedItemMeta:
        meta = None
        for cache in self:
            if key in cache:
                c_meta = cache.get_meta(key)
                if meta is None:
                    meta = c_meta
                elif meta != c_meta:
                    self.logger.warning(f"WARNING: meta data mismatch in caches for '{key}'")
        if meta is None:
            raise_not_found(key)
        return meta

    def check_meta_mismatches(self, key=None) -> dict[str, tuple[CachedItemMeta, ...]]:
        return check_meta_mismatches(self._caches, key=key)

    def compare_all_metadata(self) -> dict[str, dict[str, dict]]:
        metadata = defaultdict(dict)
        for key in self.keys():
            for i, cache in enumerate(self):
                cache_key = f'Cache {i + 1}'
                if hasattr(cache, 'path'):
                    if isinstance(cache.path, S3Location):
                        cache_key += f' ({cache.path.s3_uri})'
                    else:
                        cache_key += f' ({cache.path})'
                metadata[key][cache_key] = cache.get_meta(key).to_dict() if key in cache else None
        return dict(metadata)

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
        if not self.is_active():
            self.logger.warning(f"WARNING: {type(self).__name__} is not active")
            return self
        for cache in self:
            if cache.is_read_only():
                continue
            cache.put(key, obj, update=update, meta=meta, log_msg=log_msg)
            meta = cache.get_meta(key)
            mismatch = self.check_meta_mismatches(key)
            if mismatch and not update:
                self.logger.warning(f"WARNING: Metadata mismatch for '{key}'. Use update=True to sync across caches.")
            if not self._sync_all and not mismatch:
                break
        return self

    def remove(self, key: str, log_msg: Optional[str] = None) -> Self:
        if self.is_active():
            for cache in self:
                if key in cache:
                    cache.remove(key, log_msg=log_msg)
        else:
            self.logger.warning(f"WARNING: {type(self).__name__} is not active")
        return self

    def keys(self) -> list[str]:
        if not self.is_active():
            return []
        else:
            keys = []
            for cache in self:
                keys.extend(cache.keys())
            return list(set(keys))

    def sync_now(self) -> Self:
        for key in tqdm(self.keys(), desc='Syncing caches', disable=(not self.is_active())):
            obj = None
            meta = None
            for cache in self:
                if key not in cache:
                    if obj is None:
                        obj = self.get(key, sync=False)
                        meta = self.get_meta(key)
                    cache.put(key, obj, meta=meta)
                else:
                    mismatch = self.check_meta_mismatches(key)
                    if mismatch:
                        raise RuntimeError(f"Metadata mismatch for '{key}'")
        return self

    def sync_always(self):
        self._sync_all = True
        self.sync_now()

    def rebuild(self) -> Self:
        for cache in self:
            if hasattr(cache, 'rebuild'):
                cache.rebuild()
        return self

    def __repr__(self):
        return f"{type(self).__name__}({', '.join([str(cache) for cache in self])})"
