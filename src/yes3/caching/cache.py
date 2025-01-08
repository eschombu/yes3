import os
from typing import Optional

from yes3 import s3
from yes3.caching.base import CatalogCache
from yes3.s3 import S3Location


class S3Cache(CatalogCache):
    def __init__(
            self,
            bucket_or_location: str | S3Location,
            prefix: Optional[str] = None,
            file_type: str = '.pkl',
            active=True,
            read_only=False,
            auto_init=False,
    ):
        super().__init__(
            read_only=read_only,
            auto_init=auto_init,
            active=active,
        )
        s3_location = S3Location(bucket_or_location, prefix)
        self.s3_location = s3_location
        self._file_type = f".{file_type.lstrip('.').lower()}"

    def _log(self, *args, **kwargs):
        print(*args, **kwargs)

    def initialize(self) -> None:
        self._log(f"Initializing cache at {self.s3_location}")
        self._catalog = self._build_catalog()
        if len(self._catalog) > 0:
            self._log(f'{len(self._catalog)} cached items discovered')

    def _key2path(self, key: str) -> S3Location:
        return self.s3_location.join(key + self._file_type)

    def _path2key(self, path: str | S3Location) -> str:
        path = S3Location(path)
        key_ext = path.s3_uri.split(self.s3_location.s3_uri)[1].lstrip('/')
        key, ext = os.path.splitext(key_ext)
        return key

    def _build_catalog(self) -> dict[str, S3Location]:
        catalog = {}
        for obj in s3.list_objects(self.s3_location) or []:
            loc = obj.location
            key = self._path2key(loc)
            if key in catalog:
                raise KeyError(f"Key already in cache catalog: '{key}'")
            catalog[key] = loc
        return catalog

    def _get(self, key: str):
        path = self._catalog[key]
        self._log(f'Loading cached item at {path}')
        try:
            return s3.read_from_s3(path)
        except EOFError:
            self._remove(key)
            return None

    def _put(self, obj, key: str):
        path = self._key2path(key)
        self._log(f'Caching item at {path}')
        s3.write_to_s3(obj, path, overwrite=True)
        self._catalog[key] = path

    def _remove(self, key: str):
        path = self._catalog.pop(key, None)
        if path:
            self._log(f'Removing from cache: {path}')
            s3.delete(path)

    def clear(self, force=False, initialize=False) -> 'S3Cache':
        if self.is_active() and self._catalog and self.s3_location.exists():
            if not force:
                self._log(f'WARNING: Deleting {len(self._catalog)} items from this cache ({self.s3_location}) requires '
                          'specifying force=True. Skipping this step.')
            else:
                self._log(f'Deleting {len(self._catalog)} from cache at {self.s3_location}')
                s3.delete(self.s3_location, recursive=True)
                self._catalog = self._build_catalog()
        if initialize:
            self.initialize()
        return self

    def _repr_params(self) -> list[str]:
        params = super()._repr_params()
        params.insert(0, str(self.s3_location))
        params.append(f"'{self._file_type}'")
        return params
