from pathlib import Path
from typing import Optional

from yes3.s3 import is_s3_url, S3Location
from yes3.caching import CacheCore, LocalDiskCache, MemoryCache, MultiCache, S3Cache


def setup_single_cache(
        path: Optional[str | Path | S3Location | CacheCore] = None,
        in_memory=False,
        rebuild_missing_metadata=False,
        log_level=None,
) -> CacheCore:
    if isinstance(path, CacheCore):
        cache = path
    elif isinstance(path, S3Location) or (isinstance(path, str) and is_s3_url(path)):
        cache = S3Cache.create(path, rebuild_missing_meta=rebuild_missing_metadata)
    elif isinstance(path, Path) or isinstance(path, str):
        cache = LocalDiskCache.create(path, rebuild_missing_meta=rebuild_missing_metadata)
    elif not in_memory:
        raise TypeError('`path` must be a Cache, local path, or s3 location')
    else:
        cache = MemoryCache()
    if log_level is not None:
        cache.set_log_level(log_level)
    return cache


def setup_cache(
        *paths,
        in_memory=False,
        sync=False,
        rebuild_missing_metadata=False,
        log_level=None,
) -> CacheCore | None:
    caches = []
    if in_memory:
        caches.append(setup_single_cache(in_memory=in_memory))
    for path in paths:
        if path is not None:
            if not (isinstance(path, str) or isinstance(path, CacheCore)) and hasattr(path, '__len__'):
                for p in path:
                    caches.append(setup_single_cache(p, rebuild_missing_metadata=rebuild_missing_metadata))
            else:
                caches.append(setup_single_cache(path, rebuild_missing_metadata=rebuild_missing_metadata))
    if len(caches) == 0:
        cache = None
    elif len(caches) == 1:
        cache = caches[0]
    else:
        cache = MultiCache(caches, sync_all=sync)
    if cache is not None and log_level is not None:
        cache.set_log_level(log_level)
    return cache
