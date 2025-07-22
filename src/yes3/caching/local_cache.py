import json
import os
import pickle
from datetime import datetime, UTC
from functools import partial
from glob import glob
from pathlib import Path
from time import sleep
from typing import Optional, Self

from yes3.caching import base_logger
from yes3.caching.base import Cache, CacheDictCatalog, CachedItemMeta, Serializer, CacheReaderWriter


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

    def write(self, path, obj: dict):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, 'w') as f:
            json.dump(obj, f)


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
        try:
            return type(path)(path_str + ext)
        except (ValueError, TypeError):
            return Path(path_str + ext)


class LocalReaderWriter(CacheReaderWriter):
    def __init__(
            self, path: str | Path,
            object_serializer: str | Serializer = PickleSerializer(),
            meta_serializer: str | Serializer = JsonMetaSerializer(),
    ):
        self.path = Path(path)
        self.obj_serializer = _get_serializer(object_serializer)
        self.meta_serializer = _get_serializer(meta_serializer)

    def clone(self, *args, **kwargs) -> Self:
        params = {
            'path': self.path,
            'object_serializer': self.obj_serializer,
            'meta_serializer': self.meta_serializer,
        }
        for value, key in zip(args, params.keys()):
            if value is not None:
                params[key] = value
        for key, value in kwargs.items():
            if key not in params:
                raise TypeError(f'Unexpected parameter {key} for {type(self).__name__}')
            params[key] = value
        return type(self)(**params)

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
        self.logger.info(f"Reading cached item '{key}' at {path}")
        return self.obj_serializer.read(path)

    def _build_meta(self, path, key=None) -> CachedItemMeta:
        if key is None:
            key = self.path2key(path)
        file_stat = os.stat(path)
        rel_path = path.relative_to(self.path)
        return CachedItemMeta(
            key=key,
            path=str(rel_path),
            size=file_stat.st_size,
            timestamp=datetime.fromtimestamp(file_stat.st_mtime, UTC),
        )

    def get_meta(self, key: str, rebuild=False) -> CachedItemMeta:
        if rebuild:
            obj_path = self.key2path(key)
            meta_path = self.key2path(key, meta=True)
            meta = self._build_meta(path=obj_path, key=key)
            self.meta_serializer.write(meta_path, meta)
        else:
            meta_path = self.key2path(key, meta=True)
            meta = self.meta_serializer.read(meta_path)
        return meta

    def write(self, key: str, obj, meta: Optional[CachedItemMeta] = None) -> CachedItemMeta:
        obj_path = self.key2path(key)
        self.logger.info(f"Caching item '{key}' at {obj_path}")
        self.obj_serializer.write(obj_path, obj)

        meta_path = self.key2path(key, meta=True)
        if meta is None:
            meta = self._build_meta(path=obj_path, key=key)
        self.meta_serializer.write(meta_path, meta)
        return meta

    def delete(self, key: str, meta_only=False):
        path = self.key2path(key)
        meta_path = self.key2path(key, meta=True)
        if meta_only:
            self.logger.info(f"Deleting cached item '{key}' metadata at {meta_path}")
        else:
            self.logger.info(f"Deleting cached item '{key}' at {path}")
            os.remove(path)
        os.remove(meta_path)


class LocalDiskCache(Cache):
    @staticmethod
    def _build_catalog_dict(
            reader_writer: LocalReaderWriter, rebuild_missing_meta=False, retries=1, retry_sec=0.5,
    ) -> dict:
        catalog_dict = {}
        if os.path.exists(reader_writer.path):
            data_ext = reader_writer.obj_serializer.ext.lstrip('.')
            meta_ext = reader_writer.meta_serializer.ext.lstrip('.')
            data_files = glob(str(reader_writer.path / f'*.{data_ext}'))
            meta_files = glob(str(reader_writer.path / f'*.{meta_ext}'))
            data_map = {Path(p).stem: p for p in data_files}
            meta_map = {Path(p).stem: p for p in meta_files}
            if data_map.keys() != meta_map.keys():
                if retries:
                    # During parallel processing, there may be a temporary misalignment of data and metadata files,
                    #  retry in case the situation is quickly resolved
                    base_logger.warning(
                        f'WARNING: data and metadata files are not aligned for cache at {reader_writer.path}, '
                        f'retrying in {retry_sec} seconds')
                    sleep(retry_sec)
                    return LocalDiskCache._build_catalog_dict(
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
            base_logger.info(f'{len(catalog_dict.keys())} cached items discovered at {reader_writer.path}')
        return catalog_dict

    @classmethod
    def create(
            cls,
            path: str | Path,
            obj_serializer: str | Serializer = PickleSerializer(),
            meta_serializer: str | Serializer = JsonMetaSerializer(),
            reader_writer: Optional[CacheReaderWriter] = None,
            rebuild_missing_meta=False,
            **kwargs,
    ):
        if reader_writer is None:
            reader_writer = LocalReaderWriter(path, obj_serializer, meta_serializer)
        elif not isinstance(reader_writer, LocalReaderWriter):
            raise TypeError(f'`reader_writer` must be a {LocalReaderWriter.__name__} instance')
        elif reader_writer.path != path:
            reader_writer = reader_writer.clone(path)
        catalog_builder = partial(cls._build_catalog_dict, reader_writer=reader_writer,
                                  rebuild_missing_meta=rebuild_missing_meta)
        catalog = CacheDictCatalog(catalog_builder=catalog_builder)
        return cls(catalog, reader_writer, **kwargs)

    @property
    def path(self) -> Path:
        return self._reader_writer.path

    def subcache(self, rel_path: str) -> Self:
        path = self.path / rel_path
        kwargs = dict(active=self.is_active(), read_only=self.is_read_only())
        return type(self).create(path, reader_writer=self._reader_writer, **kwargs)

    def clear(self, force=False, log_msg: Optional[str] = None) -> Self:
        if self.is_active() and len(self.keys()) > 0:
            if not force:
                raise RuntimeError(f'Clearing this {type(self).__name__} ({self.path}) requires specifying force=True')
            self.logger.info(f'Deleting {len(self.keys())} item(s) from cache at {self.path}')
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
                raise RuntimeError(f'Clearing this {type(self).__name__} metadata ({self.path}) requires specifying '
                                   'force=True')
            self.logger.info(f'Deleting {len(self.keys())} item(s) from cache at {self.path}')
            for key in self.keys():
                self.remove(key, meta_only=True)
            if log_msg:
                self.write_log_msg(log_msg)
        return self

    def _repr_params(self) -> list[str]:
        params = super()._repr_params()
        params.insert(0, str(self.path))
        return params

    def read_log(self) -> list[dict]:
        serializer = JsonSerializer()
        path = self.path / self._log_filename
        if not path.exists():
            self.logger.debug(f'Log file {path} not found')
            return []
        log = serializer.read(path)
        if log:
            self.logger.debug(f'Reading {len(log)} messages from Log file {path}')
            return log
        else:
            self.logger.debug(f'Log file {path} is empty')
            return []

    def write_log_msg(self, msg: str):
        serializer = JsonSerializer()
        path = self.path / self._log_filename
        log = self.read_log()
        entry = {'timestamp': datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S %Z'), 'message': str(msg)}
        log.append(entry)
        serializer.write(path, log)
        self.logger.info(f'Logged message to {path}')
