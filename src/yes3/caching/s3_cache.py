import os
from datetime import datetime, UTC
from functools import partial
from pathlib import Path
from time import sleep
from typing import Optional, Self

from yes3 import s3
from yes3.caching import base_logger
from yes3.caching.base import Cache, CacheDictCatalog, CachedItemMeta, CacheReaderWriter
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
    def __init__(
            self,
            path: str | S3Location,
            file_type: str = 'pkl',
            meta_file_type: str = 'json',
            meta_ext: str = 'meta',
            progress=None,
    ):
        self.path = S3Location(path)
        self._file_type = file_type
        self._meta_file_type = meta_file_type
        self._meta_ext = meta_ext
        self._progress = progress

    def clone(self, *args, **kwargs) -> Self:
        params = {
            'path': self.path,
            'file_type': self._file_type,
            'meta_file_type': self._meta_file_type,
            'meta_ext': self._meta_ext,
            'progress': self._progress,
        }
        for value, key in zip(args, params.keys()):
            if value is not None:
                params[key] = value
        for key, value in kwargs.items():
            if key not in params:
                raise TypeError(f'Unexpected parameter {key} for {type(self).__name__}')
            params[key] = value
        return type(self)(**params)

    def key2path(self, key: str, meta=False) -> S3Location:
        if meta:
            return _with_ext(self.path.join(key), self._meta_ext)
        else:
            return _with_ext(self.path.join(key), self._file_type)

    def path2key(self, path: str | S3Location) -> str:
        path = S3Location(path)
        filename = path.s3_uri.split(self.path.s3_uri, maxsplit=1)[-1].lstrip('/')
        key, ext = os.path.splitext(filename)
        return key

    def read(self, key: str, file_type=None):
        file_type = file_type or self._file_type
        path = self.key2path(key)
        self.logger.info(f"Reading cached item '{key}' at {path.s3_uri}")
        return s3.read(path, file_type=file_type)

    def _build_meta(self, path, key=None) -> CachedItemMeta:
        if key is None:
            key = self.path2key(path)
        rel_path = Path(path.key).relative_to(Path(self.path.key))
        obj_meta = path.get_object_metadata()
        return CachedItemMeta(
            key=key,
            path=str(rel_path),
            size=obj_meta.size,
            timestamp=obj_meta.last_modified,
        )

    def get_meta(self, key: str, rebuild=False, file_type=None) -> CachedItemMeta:
        if rebuild:
            obj_path = self.key2path(key)
            meta_path = self.key2path(key, meta=True)
            meta = self._build_meta(path=obj_path, key=key)
            s3.write_to_s3(meta.to_dict(), meta_path, file_type=self._meta_file_type)
        else:
            meta_path = self.key2path(key, meta=True)
            meta_dict = s3.read(meta_path, file_type=self._meta_file_type, progress=False)
            meta = CachedItemMeta(**meta_dict)
        return meta

    def write(self, key: str, obj, meta: Optional[CachedItemMeta] = None, file_type=None) -> CachedItemMeta:
        file_type = file_type or self._file_type
        obj_path = self.key2path(key)
        self.logger.info(f"Caching item '{key}' at {obj_path.s3_uri}")
        s3.write_to_s3(obj, obj_path, file_type=file_type, progress=self._progress)

        meta_path = self.key2path(key, meta=True)
        if meta is None:
            meta = self._build_meta(path=obj_path, key=key)
        s3.write_to_s3(meta.to_dict(), meta_path, file_type=self._meta_file_type)
        return meta

    def delete(self, key: str, meta_only=False):
        path = self.key2path(key)
        meta_path = self.key2path(key, meta=True)
        if meta_only:
            self.logger.info(f"Deleting cached item '{key}' metadata at {meta_path.s3_uri}")
        else:
            self.logger.info(f"Deleting cached item '{key}' at {path.s3_uri}")
            s3.delete(path)
        s3.delete(meta_path)


class S3Cache(Cache):
    @staticmethod
    def _build_catalog_dict(
            reader_writer: S3ReaderWriter, rebuild_missing_meta=False, retries=1, retry_sec=0.5,
    ) -> dict:
        catalog_dict = {}
        locations = sorted(s3.list_objects(reader_writer.path), key=lambda loc: loc.key)
        meta_locs = [loc for loc in locations if loc.key.endswith(reader_writer._meta_ext)]
        data_locs = [loc for loc in locations if loc not in meta_locs]
        data_map = {reader_writer.path2key(loc): loc for loc in data_locs}
        meta_map = {reader_writer.path2key(loc): loc for loc in meta_locs}
        if data_map.keys() != meta_map.keys():
            if retries:
                # During parallel processing, there may be a temporary misalignment of data and metadata files,
                #  retry in case the situation is quickly resolved
                base_logger.warning(
                    f'WARNING: data and metadata files are not aligned for cache at {reader_writer.path}, '
                    f'retrying in {retry_sec} seconds')
                sleep(retry_sec)
                return S3Cache._build_catalog_dict(
                    reader_writer, rebuild_missing_meta, retries - 1, retry_sec=retry_sec)
            else:
                if rebuild_missing_meta:
                    base_logger.warning(
                        f'WARNING: data and metadata files are not aligned for cache at {reader_writer.path}, '
                        'rebuilding missing metadata files')
                else:
                    raise RuntimeError(f'data and metadata files are not aligned for cache at {reader_writer.path}')
        for key in data_map.keys():
            if key not in meta_map and rebuild_missing_meta:
                catalog_dict[key] = reader_writer.get_meta(key, rebuild=True)
            else:
                meta_path = reader_writer.key2path(key, meta=True)
                catalog_dict[key] = CachedItemMeta(load_path=meta_path)
        if len(catalog_dict.keys()) > 0:
            base_logger.info(f'{len(catalog_dict.keys())} cached items discovered at {reader_writer.path.s3_uri}')
        return catalog_dict

    @classmethod
    def create(
            cls,
            path: str | S3Location,
            file_type=None,
            meta_file_type=None,
            meta_ext=None,
            reader_writer: Optional[CacheReaderWriter] = None,
            rebuild_missing_meta=False,
            **kwargs,
    ):
        if reader_writer is None:
            rw_kwargs = {}
            if file_type is not None:
                rw_kwargs['file_type'] = file_type
            if meta_file_type is not None:
                rw_kwargs['meta_file_type'] = meta_file_type
            if meta_ext is not None:
                rw_kwargs['meta_ext'] = meta_ext
            reader_writer = S3ReaderWriter(path, **rw_kwargs)
        elif not isinstance(reader_writer, S3ReaderWriter):
            raise TypeError(f'`reader_writer` must be a {S3ReaderWriter.__name__} instance')
        elif reader_writer.path != path:
            reader_writer = reader_writer.clone(path)
        catalog_builder = partial(cls._build_catalog_dict, reader_writer=reader_writer,
                                  rebuild_missing_meta=rebuild_missing_meta)
        catalog = CacheDictCatalog(catalog_builder=catalog_builder)
        return cls(catalog, reader_writer, **kwargs)

    @property
    def path(self) -> S3Location:
        return self._reader_writer.path

    def subcache(self, rel_path: str) -> Self:
        path = self.path / rel_path
        kwargs = dict(active=self.is_active(), read_only=self.is_read_only())
        return self.create(path, reader_writer=self._reader_writer, **kwargs)

    def clear(self, force=False, log_msg: Optional[str] = None) -> Self:
        if self.is_active() and len(self.keys()) > 0:
            if not force:
                raise RuntimeError(f'Clearing this {type(self).__name__} ({self.path.s3_uri}) requires specifying '
                                   'force=True')
            self.logger.info(f'Deleting {len(self.keys())} item(s) from cache at {self.path.s3_uri}')
            for key in self.keys():
                self.remove(key)
            if log_msg:
                self.write_log_msg(log_msg)
            new_cache = type(self).create(self.path, reader_writer=self._reader_writer)
            self.__init__(new_cache._catalog, new_cache._reader_writer, active=self._active, read_only=self._read_only,
                          log_level=self._log_level)
        return self

    def clear_meta(self, force=False, log_msg: Optional[str] = None) -> Self:
        if self.is_active() and len(self.keys()) > 0:
            if not force:
                raise RuntimeError(f'Clearing this {type(self).__name__} metadata ({self.path.s3_uri}) requires '
                                   'specifying force=True')
            self.logger.info(f'Deleting {len(self.keys())} item(s) from cache at {self.path.s3_uri}')
            for key in self.keys():
                self.remove(key, meta_only=True)
            if log_msg:
                self.write_log_msg(log_msg)
        return self

    def _repr_params(self) -> list[str]:
        params = super()._repr_params()
        params.insert(0, self.path.s3_uri)
        return params

    def read_log(self) -> list[dict]:
        path = self.path / self._log_filename
        if not path.exists():
            self.logger.debug(f'Log file {path.s3_uri} not found')
            return []
        log = s3.read(path, file_type='json')
        if log:
            self.logger.debug(f'Reading {len(log)} messages from Log file {path.s3_uri}')
            return log
        else:
            self.logger.debug(f'Log file {path.s3_uri} is empty')
            return []

    def write_log_msg(self, msg: str):
        path = self.path / self._log_filename
        log = self.read_log()
        entry = {'timestamp': datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S %Z'), 'message': str(msg)}
        log.append(entry)
        s3.write_to_s3(path, log, file_type='json')
        self.logger.debug(f'Logged message to {path.s3_uri}')
