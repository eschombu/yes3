import os
import pickle
import shutil
from pathlib import Path
from typing import Optional, Self

from yes3.caching.base import CatalogCache, Serializer


class PickleSerializer(Serializer):
    ext = '.pkl'

    def read(self, path):
        with open(path, 'rb') as f:
            return pickle.load(f)

    def write(self, obj, path):
        with open(path, 'wb') as f:
            pickle.dump(obj, f)


def _get_reader_writer(serializer: str | Serializer) -> Serializer:
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


class LocalDiskCache(CatalogCache):
    def __init__(
            self,
            local_path: Path | str,
            serializer: str | Serializer = 'pkl',
            read_only=False,
            auto_init=False,
            active=True,
    ):
        super().__init__(
            active=active,
            read_only=read_only,
            auto_init=auto_init,
        )
        local_path = Path(local_path).resolve()
        self.local_path = local_path
        self._serializer = _get_reader_writer(serializer)

    def _log(self, *args, **kwargs):
        print(*args, **kwargs)

    def initialize(self) -> Self:
        self._log(f"Initializing cache at {self.local_path}")
        if not self.local_path.exists():
            os.makedirs(self.local_path, exist_ok=True)
        self._catalog = self._build_catalog()
        if len(self._catalog) > 0:
            self._log(f'{len(self._catalog)} cached items discovered')
        return self

    def is_initialized(self) -> bool:
        return self.local_path.exists() and super().is_initialized()

    def _key2path(self, key: str) -> Path:
        return self.local_path / _with_ext(key, self._serializer.ext)

    def _path2key(self, path: str | Path) -> str:
        path = Path(path)
        rel_path = path.relative_to(self.local_path)
        key, ext = os.path.splitext(rel_path)
        return key

    def _build_catalog(self) -> dict[str, Path]:
        catalog = {}
        for (dirpath, dirnames, filenames) in os.walk(self.local_path):
            for fname in filenames:
                path = Path(dirpath) / fname
                key = self._path2key(path)
                if key in catalog:
                    raise KeyError(f"Key already in cache catalog: '{key}'")
                catalog[key] = path
        return catalog

    def _get(self, key: str):
        path = self._catalog[key]
        self._log(f'Loading cached item at {path}')
        try:
            return self._serializer.read(path)
        except EOFError:
            self._remove(key)
            return None

    def _put(self, key: str, obj):
        path = self._key2path(key)
        self._log(f'Caching item at {path}')
        os.makedirs(path.parent, exist_ok=True)  # Allow for key to include subdirectories
        self._serializer.write(obj, path)
        self._catalog[key] = path

    def _remove(self, key: str):
        path = self._catalog.pop(key, None)
        if path:
            self._log(f'Removing file from cache: {path}')
            os.remove(path)

    def clear(self, force=False, initialize=False) -> Self:
        if self.is_active() and self._catalog and self.local_path.exists():
            if not force:
                raise RuntimeError(f'Clearing this cache ({self.local_path}) requires specifying force=True')
            else:
                self._log(f'Deleting {len(self._catalog)} from cache at {self.local_path}')
                shutil.rmtree(self.local_path)
                self._catalog = self._build_catalog()
        if initialize:
            self.initialize()
        return self

    def _repr_params(self) -> list[str]:
        params = super()._repr_params()
        params.insert(0, str(self.local_path))
        params.append(f"'{self._serializer.ext}'")
        return params
