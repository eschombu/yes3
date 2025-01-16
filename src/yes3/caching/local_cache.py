import os
import pickle
import shutil
from functools import partial
from pathlib import Path
from typing import Optional, Self

from yes3.caching.base import Cache, CachePathDictCatalog, Serializer, CacheReaderWriter


class PickleSerializer(Serializer):
    ext = '.pkl'

    def read(self, path):
        with open(path, 'rb') as f:
            return pickle.load(f)

    def write(self, path, obj):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
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
        print(f"Reading cached item '{key}' at {path}")
        return self._serializer.read(path)

    def write(self, key: str, obj) -> Path:
        path = self.key2path(key)
        print(f"Caching item '{key}' at {path}")
        self._serializer.write(path, obj)
        return path

    def delete(self, key: str):
        path = self.key2path(key)
        print(f"Deleting cached item '{key}' at {path}")
        os.remove(path)


class LocalDiskCache(Cache):
    @staticmethod
    def _build_catalog_dict(reader_writer: LocalReaderWriter) -> dict:
        catalog_dict = {}
        if os.path.exists(reader_writer.path):
            for (dirpath, dirnames, filenames) in os.walk(reader_writer.path):
                for fname in filenames:
                    fpath = Path(dirpath) / fname
                    key = reader_writer.path2key(fpath)
                    if key in catalog_dict:
                        raise KeyError(f"Key already in cache catalog: '{key}'")
                    catalog_dict[key] = fpath
        if len(catalog_dict.keys()) > 0:
            print(f'{len(catalog_dict.keys())} cached items discovered at {reader_writer.path}')
        return catalog_dict

    @classmethod
    def create(cls, path: str | Path, serializer: str | Serializer = 'pkl', **kwargs):
        reader_writer = LocalReaderWriter(path, serializer)
        catalog_builder = partial(cls._build_catalog_dict, reader_writer=reader_writer)
        catalog = CachePathDictCatalog(catalog_builder=catalog_builder)
        return cls(catalog, reader_writer, **kwargs)

    @property
    def path(self) -> Path:
        return self._reader_writer.path

    def subcache(self, rel_path: str) -> Self:
        path = self.path / rel_path
        kwargs = dict(active=self.is_active(), read_only=self.is_read_only())
        return type(self).create(path, self._reader_writer._serializer, **kwargs)

    def clear(self, force=False) -> Self:
        if self.is_active() and self._catalog and self.path.exists():
            if not force:
                raise RuntimeError(f'Clearing this cache ({self.path}) requires specifying force=True')
            print(f'Deleting {len(self.keys())} item(s) from cache at {self.path}')
            shutil.rmtree(self.path)
            new_cache = type(self).create(self.path, self._reader_writer._serializer)
            self.__init__(new_cache._catalog, new_cache._reader_writer, active=self._active, read_only=self._read_only)
        return self

    def _repr_params(self) -> list[str]:
        params = super()._repr_params()
        params.insert(0, str(self.path))
        return params
