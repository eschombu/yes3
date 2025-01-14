from abc import ABCMeta, abstractmethod
from collections.abc import Callable
from typing import Any, Iterator, Optional, Self


class _UnspecifiedParamType:
    pass


UNSPECIFIED = _UnspecifiedParamType()


class CacheNotInitializedError(Exception):
    pass


def raise_not_found(key) -> KeyError:
    raise KeyError(f"key '{key}' not found in cache")


class CacheCore(metaclass=ABCMeta):
    def __init__(self, active=True, initialize=True, read_only=False):
        self._read_only = read_only
        self._active = active
        if initialize:
            self.initialize()

    @abstractmethod
    def __contains__(self, key):
        pass

    @abstractmethod
    def get(self, key, default=UNSPECIFIED):
        pass

    @abstractmethod
    def put(self, key, obj, update=False):
        pass

    @abstractmethod
    def remove(self, key):
        pass

    @abstractmethod
    def update(self, key, obj):
        pass

    @abstractmethod
    def keys(self):
        pass

    def pop(self, key: str, default=UNSPECIFIED):
        obj = self.get(key, default=default)
        self.remove(key)
        return obj

    def __getitem__(self, key: str):
        return self.get(key)

    def __setitem__(self, key: str, obj) -> None:
        self.put(key, obj)

    def __delitem__(self, key: str) -> None:
        self.remove(key)

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

    def initialize(self) -> Self:
        return self

    def is_initialized(self) -> bool:
        return True

    def subcache(self, *args, **kwargs) -> Self:
        raise NotImplementedError(f"`subcache` method is not defined for class {type(self).__name__}")


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
        return self

    def is_initialized(self) -> bool:
        return True


CatalogBuilderT = Callable[[], dict]


class CachePathDictCatalog(CacheCatalog):
    def __init__(
            self,
            catalog: Optional[dict[str, Any]] = None,
            initialize=True,
            catalog_builder: Optional[CatalogBuilderT] = None,
    ):
        self._catalog = catalog
        if catalog_builder is None:
            catalog_builder = lambda: dict()
        self._build_catalog = catalog_builder
        if initialize:
            self.initialize()

    def initialize(self, catalog: Optional[dict[str, Any]] = None) -> Self:
        if catalog is not None:
            self._catalog = catalog
        elif self._catalog is None:
            self._catalog = self._build_catalog().copy()
        return self

    def is_initialized(self) -> bool:
        return self._catalog is not None

    def contains(self, key: str):
        return key in self._catalog

    def add(self, key: str, value: Any):
        self._catalog[key] = value

    def remove(self, key: str):
        self._catalog.pop(key)

    def keys(self):
        return list(self._catalog.keys())

    def items(self):
        return iter(self._catalog.items())


class Cache(CacheCore, metaclass=ABCMeta):
    def __init__(self, catalog: CacheCatalog, reader_writer: CacheReaderWriter, active=True, initialize=True,
                 read_only=False):
        super().__init__(active=active, initialize=False, read_only=read_only)
        self._catalog = catalog
        self._reader_writer = reader_writer
        if initialize:
            self.initialize()

    @classmethod
    @abstractmethod
    def create(cls, *args, **kwargs):
        pass

    def _check_initialized(self):
        if not self.is_initialized():
            if hasattr(self._reader_writer, 'path'):
                msg = f"{type(self)} at {self._reader_writer.path} not yet initialized"
            else:
                msg = f"{type(self)} not yet initialized"
            raise CacheNotInitializedError(msg)

    def __contains__(self, key: str) -> bool:
        self._check_initialized()
        if not self.is_active():
            return False
        return self._catalog.contains(key)

    def get(self, key: str, default=UNSPECIFIED):
        self._check_initialized()
        if not self.is_active() or key not in self:
            if default is UNSPECIFIED:
                raise_not_found(key)
            else:
                return default
        return self._reader_writer.read(key)

    def put(self, key: str, obj, *, update=False) -> Self:
        self._check_initialized()
        if self.is_read_only():
            raise TypeError('Cache is in read only mode')
        if self.is_active():
            if key in self and not update:
                raise ValueError(f"key '{key}' already exists in cache; use 'update' to overwrite")
            path = self._reader_writer.write(key, obj)
            self._catalog.add(key, path)
        return self

    def update(self, key: str, obj):
        self._check_initialized()
        if key not in self:
            raise_not_found(key)
        self.put(key, obj, update=True)

    def remove(self, key: str) -> Self:
        self._check_initialized()
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


class Serializer(metaclass=ABCMeta):
    ext = None

    @abstractmethod
    def read(self, path):
        pass

    @abstractmethod
    def write(self, obj, path):
        pass
