from pathlib import Path

from yes3.s3 import is_s3_url, S3Location
from yes3.caching import CacheCore, LocalDiskCache, MultiCache, S3Cache


def setup_single_cache(path: str | Path | S3Location | CacheCore, rebuild_missing_metadata=False) -> CacheCore:
    if isinstance(path, CacheCore):
        cache = path
    elif isinstance(path, S3Location) or (isinstance(path, str) and is_s3_url(path)):
        cache = S3Cache.create(path, rebuild_missing_meta=rebuild_missing_metadata)
    elif isinstance(path, Path) or isinstance(path, str):
        cache = LocalDiskCache.create(path, rebuild_missing_meta=rebuild_missing_metadata)
    else:
        raise TypeError('`path` must be a Cache, local path, or s3 location')
    return cache


def setup_cache(*paths, sync=False, rebuild_missing_metadata=False) -> CacheCore | None:
    caches = [setup_single_cache(path, rebuild_missing_metadata=rebuild_missing_metadata)
              for path in paths if path is not None]
    if len(caches) == 0:
        return None
    elif len(caches) == 1:
        return caches[0]
    else:
        return MultiCache(caches, sync_all=sync)
