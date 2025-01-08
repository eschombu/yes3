from abc import ABCMeta, abstractmethod
from typing import Any, Iterator, Optional, Self

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
    def get(self, key):
        pass

    @abstractmethod
    def put(self, key, obj):
        pass

    @abstractmethod
    def remove(self, key):
        pass

    @abstractmethod
    def update(self, key, obj):
        pass

    def __getitem__(self, key: str):
        return self.get(key)

    def __setitem__(self, key: str, obj) -> None:
        self.put(key, obj)

    def __delitem__(self, key: str) -> None:
        self.remove(key)


class CacheReaderWriter(metaclass=ABCMeta):
    @abstractmethod
    def read(self, key: str):
        pass

    @abstractmethod
    def write(self, key: str, obj) -> str:
        pass

    @abstractmethod
    def delete(self, key: str):
        pass


class CacheCatalog(metaclass=ABCMeta):
    @abstractmethod
    def contains(self, key: str):
        pass

    @abstractmethod
    def add(self, key: str, value):
        pass

    @abstractmethod
    def remove(self, key: str):
        pass

    @abstractmethod
    def keys(self):
        pass

    @abstractmethod
    def items(self):
        pass

    def initialize(self) -> Self:
        assert self.is_initialized()
        return self

    def is_initialized(self) -> bool:
        return True


class CachePathDictCatalog(CacheCatalog):
    def __init__(self, catalog: Optional[dict[str, Any]] = None):
        self._catalog = catalog

    def initialize(self, catalog: Optional[dict[str, Any]] = None) -> Self:
        if catalog is None:
            catalog: dict[str, Any] = {}
        self._catalog = catalog
        return self

    def is_initialized(self) -> bool:
        return self._catalog is not None

    def contains(self, key: str):
        return key in self._catalog

    def add(self, key: str, value: Any):
        self._catalog[key] = value

    def remove(self, key: str):
        del self._catalog[key]

    def keys(self):
        return list(self._catalog.keys())

    def items(self):
        return iter(self._catalog.items())


class Cache(CacheCoreMethods, metaclass=ABCMeta):
    def __init__(self, catalog: CacheCatalog, reader_writer: CacheReaderWriter, active=True, read_only=False):
        super().__init__()
        self._catalog = catalog
        self._reader_writer = reader_writer
        self._read_only = read_only
        self._active = active

    @classmethod
    @abstractmethod
    def create(cls, *args, **kwargs):
        pass

    def __contains__(self, key: str) -> bool:
        if not self.is_active():
            return False
        return self._catalog.contains(key)

    def get(self, key: str, default=UNSPECIFIED):
        if not self.is_active() or key not in self:
            if default is UNSPECIFIED:
                raise_not_found(key)
            else:
                return default
        return self._reader_writer.read(key)

    def put(self, key: str, obj, *, update=False) -> Self:
        if self.is_read_only():
            raise TypeError('Cache is in read only mode')
        if self.is_active():
            if key in self and not update:
                raise ValueError(f"key '{key}' already exists in cache; use 'update' to overwrite")
            path = self._reader_writer.write(key, obj)
            self._catalog.add(key, path)
        return self

    def remove(self, key: str) -> Self:
        if self.is_active() and key in self:
            if self.is_read_only():
                raise TypeError('Cache is in read only mode')
            self._catalog.remove(key)
            self._reader_writer.delete(key)
        return self

    def initialize(self) -> Self:
        self._catalog.initialize()
        return self

    def is_initialized(self) -> bool:
        return self._catalog.is_initialized()

    def is_active(self) -> bool:
        return self._active and self.is_initialized()

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

    def keys(self) -> list[str]:
        if not self.is_active():
            return []
        else:
            return list(self._catalog.keys())

    def items(self) -> Iterator[tuple[str, Any]]:
        if not self.is_active():
            return iter([])
        else:
            return self._catalog.items()

    def pop(self, key: str, default=UNSPECIFIED):
        obj = self.get(key, default=default)
        self.remove(key)
        return obj

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
