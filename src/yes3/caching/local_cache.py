import json
import os
import pickle
import sys
from datetime import datetime, UTC
from functools import partial
from glob import glob
from pathlib import Path
from typing import Optional, Self

from yes3.caching.base import Cache, CachedItemMeta, CacheDictCatalog, Serializer, CacheReaderWriter


class PickleSerializer(Serializer):
    default_ext = 'pkl'

    def read(self, path):
        with open(path, 'rb') as f:
            return pickle.load(f)

    def write(self, path, obj):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, 'wb') as f:
            pickle.dump(obj, f)


class JsonSerializer(Serializer):
    default_ext = 'json'

    def read(self, path) -> dict:
        with open(path, 'r') as f:
            return json.load(f)

    def write(self, path, meta: dict):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, 'w') as f:
            json.dump(meta, f)


class JsonMetaSerializer(JsonSerializer):
    default_ext = 'meta'

    def read(self, path) -> CachedItemMeta:
        meta_dict = super().read(path)
        return CachedItemMeta(**meta_dict)

    def write(self, path, meta: CachedItemMeta):
        super().write(path, meta.to_dict())


def _get_serializer(serializer: str | Serializer, ext=None) -> Serializer:
    if isinstance(serializer, type):
        serializer = serializer(ext)

    if isinstance(serializer, str):
        if serializer.lstrip('.').lower() in {'pkl', 'pickle'}:
            return PickleSerializer(ext)
        elif serializer.lstrip('.').lower() == 'json':
            return JsonSerializer(ext)
        else:
            raise NotImplementedError(f"Serializer not implemented for file type '{serializer}'")
    elif isinstance(serializer, Serializer):
        if ext is not None:
            serializer.ext = ext
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
    def __init__(
            self, path: str | Path,
            object_serializer: str | Serializer = PickleSerializer(),
            meta_serializer: str | Serializer = JsonMetaSerializer(),
    ):
        self.path = Path(path)
        self.obj_serializer = _get_serializer(object_serializer)
        self.meta_serializer = _get_serializer(meta_serializer)

    def key2path(self, key: str, meta=False) -> Path:
        if meta:
            return self.path / _with_ext(key, self.meta_serializer.ext)
        else:
            return self.path / _with_ext(key, self.obj_serializer.ext)

    def path2key(self, path: str | Path) -> str:
        path = Path(path)
        rel_path = path.relative_to(self.path)
        return rel_path.stem

    def read(self, key: str):
        path = self.key2path(key)
        print(f"Reading cached item '{key}' at {path}")
        return self.obj_serializer.read(path)

    def get_info(self, key: str) -> CachedItemMeta:
        path = self.key2path(key, meta=True)
        return self.meta_serializer.read(path)

    def write(self, key: str, obj, meta=None) -> CachedItemMeta:
        path = self.key2path(key)
        print(f"Caching item '{key}' at {path}")
        self.obj_serializer.write(path, obj)

        meta_path = self.key2path(key, meta=True)
        if meta is None:
            rel_path = path.relative_to(self.path)
            meta = CachedItemMeta(
                key=key,
                path=str(rel_path),
                size=sys.getsizeof(obj, -1),
                timestamp=datetime.now(UTC).timestamp(),
            )
        self.meta_serializer.write(meta_path, meta)
        return meta

    def delete(self, key: str):
        path = self.key2path(key)
        meta_path = self.key2path(key, meta=True)
        print(f"Deleting cached item '{key}' at {path}")
        os.remove(path)
        os.remove(meta_path)


class LocalDiskCache(Cache):
    @staticmethod
    def _build_catalog_dict(reader_writer: LocalReaderWriter) -> dict:
        catalog_dict = {}
        if os.path.exists(reader_writer.path):
            data_ext = reader_writer.obj_serializer.ext.lstrip('.')
            meta_ext = reader_writer.meta_serializer.ext.lstrip('.')
            data_files = glob(str(reader_writer.path / f'*.{data_ext}'))
            meta_files = glob(str(reader_writer.path / f'*.{meta_ext}'))
            data_map = {Path(p).stem: p for p in data_files}
            meta_map = {Path(p).stem: p for p in meta_files}
            if data_map.keys() != meta_map.keys():
                raise RuntimeError(f'data and metadata files are not aligned for a valid cache at {reader_writer.path}')
            for key, meta_path in meta_map.items():
                catalog_dict[key] = reader_writer.get_info(key)
        if len(catalog_dict.keys()) > 0:
            print(f'{len(catalog_dict.keys())} cached items discovered at {reader_writer.path}')
        return catalog_dict

    @classmethod
    def create(
            cls,
            path: str | Path,
            obj_serializer: str | Serializer = PickleSerializer(),
            meta_serializer: str | Serializer = JsonMetaSerializer(),
            reader_writer: Optional[CacheReaderWriter] = None,
            **kwargs,
    ):
        if reader_writer is None:
            reader_writer = LocalReaderWriter(path, obj_serializer, meta_serializer)
        catalog_builder = partial(cls._build_catalog_dict, reader_writer=reader_writer)
        catalog = CacheDictCatalog(catalog_builder=catalog_builder)
        return cls(catalog, reader_writer, **kwargs)

    @property
    def path(self) -> Path:
        return self._reader_writer.path

    def subcache(self, rel_path: str) -> Self:
        path = self.path / rel_path
        kwargs = dict(active=self.is_active(), read_only=self.is_read_only())
        return type(self).create(path, reader_writer=self._reader_writer, **kwargs)

    def clear(self, force=False) -> Self:
        if self.is_active() and len(self.keys()) > 0:
            if not force:
                raise RuntimeError(f'Clearing this cache ({self.path}) requires specifying force=True')
            print(f'Deleting {len(self.keys())} item(s) from cache at {self.path}')
            for key in self.keys():
                self.remove(key)
            new_cache = type(self).create(self.path, reader_writer=self._reader_writer)
            self.__init__(new_cache._catalog, new_cache._reader_writer, active=self._active, read_only=self._read_only)
        return self

    def _repr_params(self) -> list[str]:
        params = super()._repr_params()
        params.insert(0, str(self.path))
        return params
