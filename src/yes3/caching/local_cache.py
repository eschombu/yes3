import os
import pickle
import shutil
from functools import cached_property
from pathlib import Path
from typing import Optional, Self

from yes3.caching.base import Cache, CachePathDictCatalog, Serializer, CacheReaderWriter


class PickleSerializer(Serializer):
    ext = '.pkl'

    def read(self, path):
        with open(path, 'rb') as f:
            return pickle.load(f)

    def write(self, path, obj):
        with open(path, 'wb') as f:
            pickle.dump(obj, f)


def _get_serializer(serializer: str | Serializer) -> Serializer:
    if isinstance(serializer, str):
        if serializer.lstrip('.').lower() in {'pkl', 'pickle'}:
            return PickleSerializer()
        else:
            raise NotImplementedError(f"Serializer not implemented for file type '{serializer}'")
    elif isinstance(serializer, Serializer):
        return serializer
    else:
        raise TypeError(
            f'file_type must be a string or a Serializer subclass, but got type {type(serializer).__name__}')


def _with_ext(path, ext: Optional[str]):
    if ext is None:
        return path
    if not ext.startswith('.'):
        ext = f'.{ext}'
    path_str = str(path)
    if path_str.endswith(ext):
        return path
    else:
        return type(path)(path_str + ext)


class LocalReaderWriter(CacheReaderWriter):
    def __init__(self, path: str | Path, serializer: str | Serializer = 'pkl'):
        self.path = Path(path)
        self._serializer = _get_serializer(serializer)

    def key2path(self, key: str) -> Path:
        return self.path / _with_ext(key, self._serializer.ext)

    def path2key(self, path: str | Path) -> str:
        path = Path(path)
        rel_path = path.relative_to(self.path)
        key, ext = os.path.splitext(rel_path)
        return key

    def read(self, key: str):
        path = self.key2path(key)
        self._log(f"Reading cached item '{key}' at {path}")
        return self._serializer.read(path)

    def write(self, key: str, obj):
        path = self.key2path(key)
        self._log(f"Caching item '{key}' at {path}")
        self._serializer.write(path, obj)

    def delete(self, key: str):
        path = self.key2path(key)
        self._log(f"Deleting cached item '{key}' at {path}")
        os.remove(path)

    def _log(self, *args, **kwargs):
        print(*args, **kwargs)


class LocalDiskCache(Cache):
    @staticmethod
    def _build_catalog_dict(reader_writer: LocalReaderWriter) -> dict:
        catalog_dict = {}
        for (dirpath, dirnames, filenames) in os.walk(reader_writer.path):
            for fname in filenames:
                fpath = Path(dirpath) / fname
                key = reader_writer.path2key(fpath)
                if key in catalog_dict:
                    raise KeyError(f"Key already in cache catalog: '{key}'")
                catalog_dict[key] = fpath
        return catalog_dict

    @classmethod
    def create(cls, path: str | Path, serializer: str | Serializer = 'pkl', **kwargs):
        reader_writer = LocalReaderWriter(path, serializer)
        catalog_dict = cls._build_catalog_dict(reader_writer)
        catalog = CachePathDictCatalog(catalog_dict or None)
        return cls(catalog, reader_writer, **kwargs)

    @cached_property
    def path(self) -> Path:
        return self._reader_writer.path

    def _log(self, *args, **kwargs):
        print(*args, **kwargs)

    def initialize(self) -> Self:
        self._log(f"Initializing cache at {self.path}")
        if not self.path.exists():
            os.makedirs(self.path, exist_ok=True)
        catalog_dict = self._build_catalog_dict(self._reader_writer)
        self._catalog = CachePathDictCatalog(catalog_dict)
        if len(self.keys()) > 0:
            self._log(f'{len(self.keys())} cached items discovered')
        return self

    def is_initialized(self) -> bool:
        return super().is_initialized() and self.path.exists()

    def clear(self, force=False, initialize=False) -> Self:
        if self.is_active() and self._catalog and self.path.exists():
            if not force:
                raise RuntimeError(f'Clearing this cache ({self.path}) requires specifying force=True')
            else:
                self._log(f'Deleting {len(self.keys())} from cache at {self.path}')
                shutil.rmtree(self.path)
                catalog_dict = self._build_catalog_dict(self._reader_writer)
                self._catalog = CachePathDictCatalog(catalog_dict or None)
        if initialize:
            self.initialize()
        return self

    def _repr_params(self) -> list[str]:
        params = super()._repr_params()
        params.insert(0, str(self.path))
        return params
