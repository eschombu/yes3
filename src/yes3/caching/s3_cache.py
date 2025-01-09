from functools import cached_property
from typing import Self

from yes3 import s3
from yes3.caching.base import Cache, CachePathDictCatalog, CacheReaderWriter
from yes3.s3 import S3Location


class S3ReaderWriter(CacheReaderWriter):
    def __init__(self, path: str | S3Location, file_type: str = 'pkl'):
        self.path = S3Location(path)
        self._file_type = file_type

    def key2path(self, key: str) -> S3Location:
        return self.path.join(key)

    def path2key(self, path: str | S3Location) -> str:
        path = S3Location(path)
        key = path.s3_uri.split(self.path.s3_uri, maxsplit=1)[-1].lstrip('/')
        # key, ext = os.path.splitext(rel_path)
        return key

    def read(self, key: str, file_type=None):
        file_type = file_type or self._file_type
        path = self.key2path(key)
        self._log(f"Reading cached item '{key}' at {path.s3_uri}")
        return s3.read(path, file_type=file_type)

    def write(self, key: str, obj, file_type=None):
        file_type = file_type or self._file_type
        path = self.key2path(key)
        self._log(f"Caching item '{key}' at {path.s3_uri}")
        s3.write_to_s3(obj, path, file_type=file_type)

    def delete(self, key: str):
        path = self.key2path(key)
        self._log(f"Deleting cached item '{key}' at {path.s3_uri}")
        s3.delete(path)

    def _log(self, *args, **kwargs):
        print(*args, **kwargs)


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
        return catalog_dict

    @classmethod
    def create(cls, path: str | S3Location, file_type=None, **kwargs):
        rw_kwargs = {}
        if file_type is not None:
            rw_kwargs['file_type'] = file_type
        reader_writer = S3ReaderWriter(path, **rw_kwargs)
        catalog_dict = cls._build_catalog_dict(reader_writer)
        catalog = CachePathDictCatalog(catalog_dict or None)
        return cls(catalog, reader_writer, **kwargs)

    @cached_property
    def path(self) -> S3Location:
        return self._reader_writer.path

    def _log(self, *args, **kwargs):
        print(*args, **kwargs)

    def initialize(self) -> Self:
        self._log(f"Initializing cache at {self.path.s3_uri}")
        catalog_dict = self._build_catalog_dict(self._reader_writer)
        self._catalog = CachePathDictCatalog(catalog_dict)
        if len(self.keys()) > 0:
            self._log(f'{len(self.keys())} cached items discovered')
        return self

    def clear(self, force=False, initialize=False) -> 'S3Cache':
        if self.is_active() and self._catalog and self.path.exists():
            if not force:
                raise RuntimeError(f'Clearing this cache ({self.path.s3_uri}) requires specifying force=True')
            else:
                self._log(f'Deleting {len(self.keys())} from cache at {self.path.s3_uri}')
                s3.delete(self.path, recursive=True)
                catalog_dict = self._build_catalog_dict(self._reader_writer)
                self._catalog = CachePathDictCatalog(catalog_dict or None)
        if initialize:
            self.initialize()
        return self

    def _repr_params(self) -> list[str]:
        params = super()._repr_params()
        params.insert(0, self.path.s3_uri)
        return params
