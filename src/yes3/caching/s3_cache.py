import os
from functools import cached_property, partial
from typing import Optional, Self

from yes3 import s3
from yes3.caching.base import Cache, CachePathDictCatalog, CacheReaderWriter
from yes3.s3 import S3Location


def _with_ext(path: S3Location, ext: Optional[str]) -> S3Location:
    if ext is None:
        return path
    if not ext.startswith('.'):
        ext = f'.{ext}'
    key = path.key
    if key.endswith(ext):
        return path
    else:
        return type(path)(path.bucket, key + ext, path.region)


class S3ReaderWriter(CacheReaderWriter):
    def __init__(self, path: str | S3Location, file_type: str = 'pkl'):
        self.path = S3Location(path)
        self._file_type = file_type

    def key2path(self, key: str) -> S3Location:
        return _with_ext(self.path.join(key), self._file_type)

    def path2key(self, path: str | S3Location) -> str:
        path = S3Location(path)
        filename = path.s3_uri.split(self.path.s3_uri, maxsplit=1)[-1].lstrip('/')
        base, ext = os.path.splitext(filename)
        if ext.endswith(self._file_type):
            key = base
        else:
            key = filename
        return key

    def read(self, key: str, file_type=None):
        file_type = file_type or self._file_type
        path = self.key2path(key)
        print(f"Reading cached item '{key}' at {path.s3_uri}")
        return s3.read(path, file_type=file_type)

    def write(self, key: str, obj, file_type=None) -> S3Location:
        file_type = file_type or self._file_type
        path = self.key2path(key)
        print(f"Caching item '{key}' at {path.s3_uri}")
        s3.write_to_s3(obj, path, file_type=file_type)
        return path

    def delete(self, key: str):
        path = self.key2path(key)
        print(f"Deleting cached item '{key}' at {path.s3_uri}")
        s3.delete(path)


class S3Cache(Cache):
    @staticmethod
    def _build_catalog_dict(reader_writer: S3ReaderWriter) -> dict:
        catalog_dict = {}
        locations = s3.list_objects(reader_writer.path)
        for loc in locations:
            key = reader_writer.path2key(loc)
            if key in catalog_dict:
                raise KeyError(f"Key already in cache catalog: '{key}'")
            catalog_dict[key] = loc
        if len(catalog_dict.keys()) > 0:
            print(f'{len(catalog_dict.keys())} cached items discovered at {reader_writer.path.s3_uri}')
        return catalog_dict

    @classmethod
    def create(cls, path: str | S3Location, file_type=None, **kwargs):
        rw_kwargs = {}
        if file_type is not None:
            rw_kwargs['file_type'] = file_type
        reader_writer = S3ReaderWriter(path, **rw_kwargs)
        catalog_builder = partial(cls._build_catalog_dict, reader_writer)
        catalog = CachePathDictCatalog(catalog_builder=catalog_builder)
        return cls(catalog, reader_writer, **kwargs)

    @property
    def path(self) -> S3Location:
        return self._reader_writer.path

    def subcache(self, rel_path: str) -> Self:
        path = self.path / rel_path
        kwargs = dict(active=self.is_active(), read_only=self.is_read_only())
        return self.create(path, file_type=self._reader_writer._file_type, **kwargs)

    def clear(self, force=False) -> 'S3Cache':
        if self.is_active() and self._catalog and self.path.exists():
            if not force:
                raise RuntimeError(f'Clearing this cache ({self.path.s3_uri}) requires specifying force=True')
            print(f'Deleting {len(self.keys())} item(s) from cache at {self.path.s3_uri}')
            s3.delete(self.path, recursive=True)
            new_cache = type(self).create(self.path, self._reader_writer._file_type)
            self.__init__(new_cache._catalog, new_cache._reader_writer, active=self._active, read_only=self._read_only)
        return self

    def _repr_params(self) -> list[str]:
        params = super()._repr_params()
        params.insert(0, self.path.s3_uri)
        return params
