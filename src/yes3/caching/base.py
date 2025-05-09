import logging
from abc import ABCMeta, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Iterable, Iterator, Optional, Self

from yes3.utils.logs import check_level, get_logger

logger = get_logger('caching', level=logging.WARNING)

_NotSpecified = object()


def raise_not_found(key) -> KeyError:
    raise KeyError(f"key '{key}' not found in cache")


@dataclass
class CachedItemMeta:
    key: str
    path: Optional[str]
    size: Optional[int]
    timestamp: Optional[datetime]

    _ts_format = '%Y-%m-%d %H:%M:%S.%f %z'

    def __post_init__(self):
        if isinstance(self.timestamp, float):
            self.timestamp = datetime.fromtimestamp(self.timestamp, UTC)
        if isinstance(self.timestamp, str):
            self.timestamp = datetime.strptime(self.timestamp, self._ts_format)

    def to_dict(self) -> dict:
        return {
            'key': self.key,
            'path': self.path,
            'size': self.size,
            'timestamp': self.timestamp.strftime(self._ts_format) if self.timestamp else None,
        }


class CacheCore(metaclass=ABCMeta):
    def __init__(self, active=True, read_only=False, log_level=None):
        self._read_only = read_only
        self._active = active
        self._log_level = None
        self.set_log_level(log_level)

    def set_log_level(self, level) -> Self:
        if level != self._log_level:
            logger.info(f"Setting log level to {level}")
        self._log_level = level
        if level is not None:
            logger.setLevel(check_level(level))
        return self

    def get_log_level(self) -> Optional[int]:
        return self._log_level

    @abstractmethod
    def __contains__(self, key):
        pass

    @abstractmethod
    def get(self, key, default=_NotSpecified):
        pass

    @abstractmethod
    def get_meta(self, key) -> CachedItemMeta:
        pass

    @abstractmethod
    def put(self, key, obj, update=False, meta: Optional[CachedItemMeta] = None):
        pass

    @abstractmethod
    def remove(self, key):
        pass

    @abstractmethod
    def keys(self):
        pass

    def __getitem__(self, key: str):
        return self.get(key)

    def __setitem__(self, key: str, obj) -> None:
        self.put(key, obj)

    def __delitem__(self, key: str) -> None:
        self.remove(key)

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

    def update(self, key: str, obj):
        if key not in self:
            raise_not_found(key)
        self.put(key, obj, update=True)

    def pop(self, key: str, default=_NotSpecified):
        obj = self.get(key, default=default)
        self.remove(key)
        return obj

    def list(self) -> dict[str, CachedItemMeta]:
        items_meta = {}
        for key in self.keys():
            items_meta[key] = self.get_meta(key)
        return items_meta

    def subcache(self, *args, **kwargs) -> Self:
        raise NotImplementedError(f"`subcache` method is not defined for class {type(self).__name__}")


class CacheReaderWriter(metaclass=ABCMeta):
    @abstractmethod
    def read(self, key: str):
        pass

    @abstractmethod
    def get_meta(self, key: str) -> CachedItemMeta:
        pass

    @abstractmethod
    def write(self, key: str, obj, meta=None) -> CachedItemMeta:
        pass

    @abstractmethod
    def delete(self, key: str, meta_only=False):
        pass


class CacheCatalog(metaclass=ABCMeta):
    @abstractmethod
    def contains(self, key: str):
        pass

    @abstractmethod
    def add(self, key: str, info: CachedItemMeta):
        pass

    @abstractmethod
    def get(self, key: str) -> CachedItemMeta:
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

    @abstractmethod
    def rebuild(self):
        pass


_CatalogT = dict[str, CachedItemMeta]
_CatalogBuilderT = Callable[[], _CatalogT]


class CacheDictCatalog(CacheCatalog):
    def __init__(
            self,
            catalog: Optional[dict[str, CachedItemMeta]] = None,
            catalog_builder: Optional[_CatalogBuilderT] = None,
    ):
        self._catalog = catalog
        if catalog_builder is None:
            catalog_builder = dict
        self._build_catalog = catalog_builder
        if self._catalog is None:
            self.rebuild()

    def rebuild(self):
        self._catalog = self._build_catalog().copy()

    def contains(self, key: str):
        return str(key) in self._catalog

    def add(self, key: str, meta: CachedItemMeta):
        self._catalog[str(key)] = meta

    def get(self, key: str) -> CachedItemMeta:
        return self._catalog[str(key)]

    def remove(self, key: str):
        self._catalog.pop(str(key))

    def keys(self):
        return list(self._catalog.keys())

    def items(self) -> Iterator[tuple[str, CachedItemMeta]]:
        return iter(self._catalog.items())


class Cache(CacheCore, metaclass=ABCMeta):
    def __init__(
            self,
            catalog: CacheCatalog,
            reader_writer: CacheReaderWriter,
            active=True, read_only=False,
            log_level=None,
    ):
        super().__init__(active=active, read_only=read_only, log_level=log_level)
        self._catalog = catalog
        self._reader_writer = reader_writer

    @classmethod
    @abstractmethod
    def create(cls, *args, **kwargs):
        pass

    def __contains__(self, key: str) -> bool:
        if not self.is_active():
            return False
        return self._catalog.contains(key)

    def get(self, key: str, default=_NotSpecified):
        if not self.is_active() or key not in self:
            if default is _NotSpecified:
                raise_not_found(key)
            else:
                return default
        return self._reader_writer.read(key)

    def get_meta(self, key: str) -> CachedItemMeta:
        if not self.is_active() or key not in self:
            raise_not_found(key)
        return self._catalog.get(key)

    def put(self, key: str, obj, *, update=False, meta: Optional[CachedItemMeta] = None) -> Self:
        if self.is_read_only():
            raise TypeError('Cache is in read only mode')
        if self.is_active():
            if key in self and not update:
                raise ValueError(f"key '{key}' already exists in cache; use 'update' to overwrite")
            meta = self._reader_writer.write(key, obj, meta=meta)
            self._catalog.add(key, meta)
        else:
            logger.info(f'WARNING: {type(self).__name__} is not active')
        return self

    def remove(self, key: str, meta_only=False) -> Self:
        if self.is_active() and key in self:
            if self.is_read_only():
                raise TypeError('Cache is in read only mode')
            self._catalog.remove(key)
            self._reader_writer.delete(key, meta_only=meta_only)
        return self

    def remove_meta(self, key: str) -> Self:
        return self.remove(key, meta_only=True)

    def keys(self) -> list[str]:
        if not self.is_active():
            return []
        else:
            return list(self._catalog.keys())

    def rebuild(self) -> Self:
        self._catalog.rebuild()
        return self

    def _repr_params(self) -> list[str]:
        params = [f'{len(self.keys())} items']
        if not self.is_active():
            params.append('NOT ACTIVE')
        if self.is_read_only():
            params.append('READ ONLY')
        return params

    def __repr__(self):
        return f"{type(self).__name__}({', '.join(self._repr_params())})"


def check_meta_mismatches(caches: Iterable[CacheCore], key=None) -> dict[str, tuple[CachedItemMeta, ...]]:
    if key is not None and not isinstance(key, str):
        raise TypeError('key is not a string')
    for cache in caches:
        if not isinstance(cache, CacheCore):
            raise TypeError('caches must be an iterable containing Cache instances')
    mismatches = {}
    if key is None:
        keys = set(k for cache in caches for k in cache.keys())
    else:
        keys = [key]
    for k in keys:
        metas = [cache.get_meta(k) for cache in caches if k in cache]
        if len(metas) > 1:
            first_meta = metas[0]
            if any(meta != first_meta for meta in metas[1:]):
                mismatches[k] = tuple(metas)
    return mismatches


class Serializer(metaclass=ABCMeta):
    default_ext = None

    def __init__(self, ext=None):
        if ext is None:
            self.ext = self.default_ext
        else:
            self.ext = ext

    @abstractmethod
    def read(self, path):
        pass

    @abstractmethod
    def write(self, path, obj):
        pass
