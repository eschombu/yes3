import traceback
from abc import ABC, ABCMeta, abstractmethod
from copy import deepcopy
from pathlib import Path
from types import MappingProxyType
from typing import Iterator, Mapping, Optional, Self, Iterable

CatalogType = dict[str, Path]

class _UnspecifiedParamType:
    pass

UNSPECIFIED = _UnspecifiedParamType()

class CacheNotInitializedError(Exception):
    pass


def raise_not_found(key) -> KeyError:
    raise KeyError(f"key '{key}' not found in cache")


class CacheCoreMethods(metaclass=ABCMeta):
    @abstractmethod
    def __contains__(self, key):
        pass

    @abstractmethod
    def put(self, key, obj):
        pass

    @abstractmethod
    def get(self, key):
        pass


class Cache(CacheCoreMethods, metaclass=ABCMeta):
    def __init__(self, active=True, read_only=False):
        super().__init__()
        self._read_only = read_only
        self._active = active

    def is_active(self) -> bool:
        return self._active

    def activate(self):
        self._active = True
        return self

    def deactivate(self):
        self._active = False
        return self

    def is_read_only(self) -> bool:
        return self._read_only

    def set_read_only(self, value: bool) -> Self:
        self._read_only = value
        return self

    def initialize(self) -> Self:
        return self

    def is_initialized(self) -> bool:
        return True

    @abstractmethod
    def _get(self, key: str):
        pass

    @abstractmethod
    def _put(self, key: str, obj):
        pass

    @abstractmethod
    def _remove(self, key: str) -> None:
        pass

    @abstractmethod
    def _contains(self, key: str) -> bool:
        pass

    @abstractmethod
    def _keys(self):
        pass

    def get(self, key: str, default=UNSPECIFIED):
        if key not in self or not self.is_active():
            if default is UNSPECIFIED:
                raise_not_found(key)
            else:
                return default
        return self._get(key)

    def put(self, key: str, obj, *, update=False):
        if self.is_read_only():
            raise TypeError('Cache is in read only mode')
        if self.is_active():
            if key in self and not update:
                raise ValueError(f"key '{key}' already exists in cache; use 'update' to overwrite")
            self._put(key, obj)

    def update(self, key: str, obj):
        if key not in self:
            raise_not_found(key)
        self.put(key, obj, update=True)

    def remove(self, key: str) -> Self:
        if self.is_active() and key in self:
            if self.is_read_only():
                raise TypeError('Cache is in read only mode')
            self._remove(key)
        return self

    def __contains__(self, key: str) -> bool:
        if not self.is_active():
            return False
        return self._contains(key)

    def __getitem__(self, key: str):
        return self.get(key)

    def keys(self) -> list[str]:
        if not self.is_active():
            return []
        else:
            return list(self._keys())

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())

    def __setitem__(self, key: str, obj) -> None:
        self.put(key, obj)

    def __delitem__(self, key: str) -> None:
        self.remove(key)

    def pop(self, key: str, default=UNSPECIFIED):
        obj = self.get(key, default=default)
        self.remove(key)
        return obj


class CatalogCache(Cache, metaclass=ABCMeta):
    def __init__(
            self,
            catalog: Optional[CatalogType] = None,
            active=True,
            read_only=False,
            auto_init=False,
    ):
        super().__init__(active=active, read_only=read_only)
        self._catalog = catalog
        self._auto_init = auto_init

    @abstractmethod
    def initialize(self):
        pass

    def is_initialized(self) -> bool:
        return self._catalog is not None

    def _check_initialized(self):
        if not self.is_initialized():
            if self._auto_init:
                self.initialize()
            else:
                raise CacheNotInitializedError

    def with_key_prefix(self, prefix: Optional[str]) -> Self:
        copied = deepcopy(self)
        copied._key_prefix = prefix
        return copied

    def _keys(self) -> Iterable[str]:
        return self._catalog.keys()

    @property
    def catalog(self) -> Mapping[str, Path]:
        return MappingProxyType(self._catalog)  # immutable

    def _contains(self, key) -> bool:
        self._check_initialized()
        return key in self._catalog

    def get(self, key, default=UNSPECIFIED):
        self._check_initialized()
        return super().get(key, default=default)

    def put(self, key, obj, *, update=False) -> Self:
        self._check_initialized()
        return super().put(key, obj, update=update)

    def keys(self) -> list[str]:
        self._check_initialized()
        return super().keys()

    @abstractmethod
    def _build_catalog(self) -> CatalogType:
        pass

    def _repr_params(self) -> list[str]:
        params = []
        if self.is_initialized():
            params.append(f'{len(self.keys())} items')
        else:
            params.append('UNINITIALIZED')
        if not self.is_active():
            params.append('NOT ACTIVE')
        if self.is_read_only():
            params.append('READ-ONLY')
        return params

    def __repr__(self):
        return f"{type(self).__name__}({', '.join(self._repr_params())})"


class MultiCache(CacheCoreMethods):
    def __init__(self, caches: list[Cache], left_to_right_priority=True, sync_all=False):
        if left_to_right_priority:
            self._caches = list(caches)
        else:
            self._caches = list(caches[::-1])
        self._sync_all = sync_all

    def __iter__(self) -> Iterator[Cache]:
        return iter(self._caches)

    def initialize(self, reinit=False) -> Self:
        for cache in self:
            if not cache.is_initialized() or reinit:
                cache.initialize()
        return self

    def is_initialized(self) -> bool:
        return all(cache.is_initialized() for cache in self)

    def add_cache(self, cache: Cache, index=-1) -> Self:
        if self.is_initialized() and not cache.is_initialized():
            cache.initialize()
        if index is not None and index >= 0:
            self._caches.insert(index, cache)
        else:
            self._caches.append(cache)
        return self

    def __contains__(self, key):
        for cache in self:
            if key in cache:
                return True
        return False

    def get(self, key, default=UNSPECIFIED):
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
        elif self._sync_all:
            for cache in self:
                if cache.is_read_only():
                    continue
                if key not in cache:
                    cache.put(key, result)
        return result

    def put(self, key, obj, *, update=False) -> Self:
        for cache in self:
            if cache.is_read_only():
                continue
            cache.put(key, obj, update=update)
            if not self._sync_all:
                break
        return self

    def __repr__(self):
        return f"{type(self).__name__}({', '.join([str(c) for c in self._caches])})"


class Serializer(metaclass=ABCMeta):
    ext = None

    @abstractmethod
    def read(self, path):
        pass

    @abstractmethod
    def write(self, obj, path):
        pass
