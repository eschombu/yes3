import gzip
import io
import json
import os
import pickle
import subprocess
from dataclasses import dataclass
from datetime import datetime
from glob import glob
from pathlib import Path
from typing import Iterable, List, Optional, Self
from urllib.parse import quote, unquote, urlparse

import numpy as np
import pandas as pd
from boto3.s3.transfer import TransferConfig
from tqdm import tqdm

from .client import get_client as _get_client
from .config import YeS3Config
from .utils.decorators import timeit_opt
from .utils.logs import get_logger

log = get_logger()
_client = _get_client()

# Don't overwrite the value of YES3_CONFIG if it has already been set
global YES3_CONFIG  # noqa: F824
try:
    YES3_CONFIG
except NameError:
    YES3_CONFIG = YeS3Config()

# TODO:
#  1. Add overwrite protection options
#  2. Add some more shortcut methods to S3Location, perhaps some aliases


def config(**config_params):
    global YES3_CONFIG  # noqa: F824
    for param, value in config_params.items():
        setattr(YES3_CONFIG, param, value)
    return YES3_CONFIG


def is_s3_url(s: str) -> bool:
    return s.startswith('s3://') or s.startswith('https://s3.')


class S3Location:
    def __init__(self, bucket: str | Self, key: Optional[str] = None, region: Optional[str] = None):
        if isinstance(bucket, S3Location):
            loc = bucket
            if key:
                loc = loc.join(key)
            self.bucket = loc.bucket
            self.key = loc.key
        elif is_s3_url(bucket):
            loc = type(self).parse(bucket)
            if key is not None:
                loc = loc.join(key)
            self.bucket = loc.bucket
            self.key = loc.key
        else:
            self.bucket = bucket
            self.key = key
        self.region = region

    def __repr__(self) -> str:
        params = {"bucket": self.bucket, "key": self.key}
        if self.region:
            params["region"] = self.region
        return f"{type(self).__name__}({', '.join([f'{k}={v}' for k, v in params.items()])})"

    def __eq__(self, other) -> bool:
        if isinstance(other, S3Location):  # not using type(self) in case comparing different subclasses of S3Location
            return ((self.bucket == other.bucket) and
                    ((self.key is None and other.key is None) or (self.key.rstrip('/') == other.key.rstrip('/'))))
        elif isinstance(other, str):
            return self == type(self).parse(other)
        else:
            return False

    @property
    def s3_uri(self) -> str:
        s = f's3://{self.bucket}'
        if self.key is not None:
            s += f'/{self.key}'
        return s

    @property
    def https_url(self, region=None) -> str:
        region = region or (self.region or YES3_CONFIG.default_region)
        s = f'https://s3.{region}.amazonaws.com/{quote(self.bucket)}'
        if self.key is not None:
            s += f'/{quote(self.key)}'
        return s

    @classmethod
    def parse(cls, url: str) -> Self:
        parsed_url = urlparse(url)
        if parsed_url.scheme == 's3':
            bucket = unquote(parsed_url.netloc)
            key = unquote(parsed_url.path[1:])
            region = None
        elif parsed_url.scheme.startswith('http'):
            bucket, *key_parts = unquote(parsed_url.path[1:]).split('/')
            key = '/'.join(key_parts)
            region = parsed_url.netloc.split('.')[1]
        else:
            raise ValueError(f'Unrecognized URL scheme: {parsed_url.scheme}')
        return cls(bucket=bucket, key=key, region=region)

    def join(self, *parts) -> Self:
        key_parts = self.key.split('/') if self.key else []
        if any(not isinstance(part, str) or is_s3_url(part) for part in parts):
            raise TypeError('All arguments to `join` must be non-url strings.')
        for part in parts:
            key_parts += part.split('/')
        new_key = '/'.join(key_parts)
        while '//' in new_key:
            new_key = new_key.replace('//', '/')
        return type(self)(self.bucket, new_key, self.region)

    def exists(self) -> bool:
        return len(list_objects(self, limit=1)) > 0

    def is_bucket(self) -> bool:
        return self.key is None or len(self.key) == 0

    def is_object(self) -> bool:
        objects = list_objects(self, limit=2)
        return len(objects) == 1 and objects[0].key == self.key

    def is_dir(self) -> bool:
        if not self.exists():
            return False
        if self.is_object():
            return False
        first_obj = list_objects(self, limit=1)[0]
        suffix = first_obj.s3_uri.split(self.s3_uri.rstrip('/'))[1]
        if suffix.startswith('/'):
            return True
        return False

    def is_dir_path(self) -> bool:
        if self.is_bucket():
            return True
        if self.key.endswith('/'):
            return True
        return self.is_dir()

    def split_key(self) -> tuple[str, str]:
        if self.key:
            key = self.key.rstrip('/')
            if '/' not in key:
                return '', key
            else:
                return tuple(key.rsplit('/', maxsplit=1))
        else:
            return '', ''

    @property
    def parent(self) -> Self:
        parent_key = self.split_key()[0]
        if not parent_key:
            parent_key = None
        return type(self)(self.bucket, parent_key, self.region)

    def get_object_metadata(self) -> 'S3Object' | List['S3Object']:
        objects = list_objects(self, return_metadata=True)
        if len(objects) == 1 and objects[0].location.key == self.key:
            return objects[0]
        else:
            return objects

    def __truediv__(self, other):
        return self.join(other)


S3LocationLike = str | S3Location
LocalPathLike = str | os.PathLike


def get_size_str(bytes: int) -> str:
    if bytes < 1024:
        return f'{bytes} B'
    elif bytes < 1024 ** 2:
        return f'{bytes / 1024:.2f} KB'
    elif bytes < 1024 ** 3:
        return f'{bytes / 1024 ** 2:.2f} MB'
    else:
        return f'{bytes / 1024 ** 3:.2f} GB'


@dataclass
class S3Object:
    location: S3Location
    last_modified: datetime
    e_tag: str
    size: int
    storage_class: str

    @classmethod
    def from_dict(cls, bucket: str, d: dict) -> Self:
        return cls(
            location=as_s3_location(bucket, d['Key']),
            last_modified=d['LastModified'],
            e_tag=d['ETag'],
            size=d['Size'],
            storage_class=d['StorageClass'],
        )

    def __repr__(self) -> str:
        params = [
            self.location.s3_uri,
            get_size_str(self.size),
            self.last_modified.isoformat(),
        ]
        return f"{type(self).__name__}({', '.join(params)})"


@dataclass
class S3Prefix:
    location: S3Location
    key_count: int = -1  # -1 means unknown number of objects with this prefix, 0 means no objects have this prefix

    def count_objects(self) -> int:
        objs = list_objects(self.location)
        self.key_count = len(objs)
        return self.key_count

    def __repr__(self) -> str:
        params = [self.location.s3_uri]
        if self.key_count >= 0:
            params.append(f'{self.key_count} objects')
        return f"{type(self).__name__}({', '.join(params)})"


def as_s3_location(bucket_or_location: S3LocationLike, key: Optional[str] = None) -> S3Location:
    if isinstance(bucket_or_location, S3Location):
        location = bucket_or_location
        if key is not None:
            location.key = key
    elif bucket_or_location.startswith('s3://') or bucket_or_location.startswith('https://'):
        location = S3Location.parse(bucket_or_location)
        if key is not None:
            location = location.join(key)
    else:
        location = S3Location(bucket_or_location, key)
    return location


def list_objects(
        bucket_or_location: S3LocationLike,
        prefix: Optional[str] = None,
        limit: Optional[int] = None,
        return_metadata: bool = False,
) -> list[S3Location | S3Object]:
    location = as_s3_location(bucket_or_location, prefix)

    def get_next_page(cont_token=None) -> [Optional[str], list[S3Object]]:
        args = dict(Bucket=location.bucket, Prefix=location.key)
        if limit is not None and limit >= 0:
            args['MaxKeys'] = int(limit)
        if cont_token is not None:
            args['ContinuationToken'] = cont_token
        resp = _client.list_objects_v2(**args)
        next_token = resp.get('NextContinuationToken')
        contents = resp.get('Contents', [])
        parsed_contents = [S3Object.from_dict(location.bucket, d) for d in contents]
        return next_token, parsed_contents

    token, results = get_next_page()
    while token is not None and (limit is None or len(results) < limit):
        token, next_results = get_next_page(token)
        results += next_results

    if limit is not None and limit >= 0:
        results = results[:limit]

    if return_metadata:
        return results
    else:
        return [r.location for r in results]


@timeit_opt
def list_dir(
        bucket_or_location: S3LocationLike,
        prefix: Optional[str] = None,
        depth=None,
        recursive=False,
        limit: Optional[int] = None,
        return_metadata: bool = False,
        count_only: bool = False,
) -> list[S3Location | S3Object | S3Prefix] | int:
    location = as_s3_location(bucket_or_location, prefix)
    if location.is_dir_path():
        location = location.join('')

    if depth is None:
        if recursive:
            depth = -1
        else:
            depth = 1

    paginator = _client.get_paginator('list_objects_v2')
    paginator_args = {'Bucket': location.bucket, 'Prefix': location.key, 'Delimiter': '/'}
    if limit is not None and limit >= 0:
        paginator_args['PaginationConfig'] = {'MaxItems': int(limit)}

    if count_only:
        results = 0
    else:
        results = []
    if depth == 0:
        return results

    for page in paginator.paginate(**paginator_args):
        # CommonPrefixes and Contents might not be included in a page if there
        # are no items, so use .get() to return an empty list in that case
        for item in page.get('CommonPrefixes', []):
            if count_only:
                results += 1
            else:
                results.append(S3Prefix(S3Location(location.bucket, item['Prefix'])))
                if depth > 1 or depth < 0:
                    results.extend(list_dir(location.bucket, item['Prefix'], depth - 1, return_metadata=True))
        for item in page.get('Contents', []):
            if count_only:
                results += 1
            else:
                results.append(S3Object.from_dict(location.bucket, item))

    if limit is not None and limit >= 0:
        if count_only:
            return min(results, limit)
        else:
            results = results[:limit]

    if count_only or return_metadata:
        return results
    else:
        return [r.location for r in results]


def PathExt(*args, **kwargs) -> os.PathLike:
    p = Path(*args, **kwargs)
    cls = type(p)

    class UnmadeDirPath(cls):
        _is_unmade_dir = True

    if not p.exists() and str(args[-1]).endswith('/'):
        return UnmadeDirPath(p)
    else:
        return p


def is_unmade_dir(path: os.PathLike) -> bool:
    p = PathExt(path)
    try:
        return p._is_unmade_dir
    except AttributeError:
        return False


def _parse_progress_arg(progress_arg) -> tuple[str, int | float]:
    if isinstance(progress_arg, str):
        progress_mode = YES3_CONFIG.check_progress_mode(progress_arg)
        progress_size_threshold = YES3_CONFIG.progress_size_threshold
    elif progress_arg is False:
        progress_mode = YES3_CONFIG.check_progress_mode('off')
        progress_size_threshold = YES3_CONFIG.progress_size_threshold
    elif progress_arg is None:
        progress_mode = YES3_CONFIG.progress_mode
        progress_size_threshold = YES3_CONFIG.progress_size_threshold
    else:
        try:
            progress_size_threshold = float(progress_arg)
            progress_mode = YES3_CONFIG.check_progress_mode('large')
        except (ValueError, TypeError):
            raise ValueError(f'Invalid progress arg value: {progress_arg}')

    return progress_mode, progress_size_threshold


def _get_upload_prog_callback(progress_arg, filesize: int, path: Path):
    # Parse progress arg
    if callable(progress_arg):
        return progress_arg
    progress_mode, progress_size_threshold = _parse_progress_arg(progress_arg)

    if progress_mode == 'off':
        return

    if progress_mode == 'large' and filesize < progress_size_threshold:
        return

    pbar = tqdm(total=filesize, desc=f"Uploading {path.name}")

    def f(size):
        pbar.update(size)

    return f


def _upload_file(local_path: LocalPathLike, location: S3Location, progress=None) -> S3Location:
    local_path = Path(local_path).resolve()
    if not local_path.exists():
        raise FileNotFoundError(str(local_path))
    if location.is_dir_path():
        filename = local_path.name
        location = location.join(filename)
    log.info(f'Uploading {local_path} to {location.s3_uri}... ')
    filesize = local_path.stat().st_size
    transfer_config = YES3_CONFIG.upload_transfer_config or TransferConfig()
    if filesize < transfer_config.multipart_threshold:
        with open(local_path, 'rb') as f:
            _client.put_object(Bucket=location.bucket, Key=location.key, Body=f)
    else:
        opt_prog_callback = _get_upload_prog_callback(progress, filesize, local_path)
        _client.upload_file(
            Filename=str(local_path),
            Bucket=location.bucket,
            Key=location.key,
            Config=transfer_config,
            Callback=opt_prog_callback,
        )
    log.info(f'Uploaded {local_path} to {location.s3_uri}')
    return location


def _highest_common_dir(paths: Iterable[LocalPathLike], resolve=True) -> os.PathLike:
    if resolve:
        paths = [Path(p).resolve() for p in paths]
    else:
        paths = [Path(p) for p in paths]
    if len(paths) == 0:
        raise ValueError('At least one path is required')
    next_path = paths.pop(0)
    common_dir = next_path.parent
    while paths:
        next_path = paths.pop(0)
        while not str(next_path).startswith(str(common_dir)):
            common_dir = common_dir.parent
    return common_dir


def upload(
        local_path: LocalPathLike,
        bucket_or_location: S3LocationLike,
        prefix: Optional[str] = None,
        recursive: bool = False,
        base_dir=None,
        progress=None,
        # workers: int = 1,
        # threads: int = 1,
) -> S3Location | list[S3Location]:
    local_path = Path(local_path).resolve()
    location = as_s3_location(bucket_or_location, prefix)

    if not local_path.exists():
        raise FileNotFoundError(f'No paths found matching `{local_path}`')
    elif local_path.is_file():
        return _upload_file(local_path, location, progress=progress)
    else:
        if not recursive:
            raise ValueError(f'Must set `recursive=True` for directories or paths with wildcards (*): {local_path}')

        if '*' in str(local_path):
            local_paths = [Path(p) for p in glob(str(local_path))]
        elif local_path.is_dir():
            local_paths = []
            for (dir_path, _, filenames) in os.walk(local_path):
                for filename in filenames:
                    local_paths.append(Path(dir_path) / filename)
        else:
            local_paths = []

        if len(local_paths) == 0:
            raise FileNotFoundError(f'No paths found matching `{local_path}`')

        common_dir = _highest_common_dir(local_paths)
        if base_dir is None:
            base_dir = common_dir
        else:
            base_dir = Path(base_dir).resolve()
            if not str(common_dir).startswith(str(base_dir)):
                raise ValueError(f'Local files are not in subdirectories of the given base dir `{base_dir}`')

        # if workers == -1 or workers > 1:
        #     rel_paths = [location.join(str(p.relative_to(base_dir))) for p in local_paths]
        #     with Pool(workers) as pool:
        #         locations = pool.map(_upload_file, local_paths, rel_paths)
        # elif threads > 1:
        #     rel_paths = [location.join(str(p.relative_to(base_dir))) for p in local_paths]
        #     with ThreadPoolExecutor(threads) as pool:
        #         locations = list(pool.map(_upload_file, local_paths, rel_paths))
        # else:
        locations = []
        for path in local_paths:
            rel_path = path.relative_to(base_dir)
            loc = _upload_file(path, location.join(str(rel_path)), progress=progress)
            locations.append(loc)

        return locations


def _get_download_prog_callback(progress_arg, location: S3Location):
    if callable(progress_arg):
        return progress_arg
    progress_mode, progress_size = _parse_progress_arg(progress_arg)

    if progress_mode == 'off':
        return

    obj_meta = list_objects(location, return_metadata=True)[0]
    obj_size = obj_meta.size
    if progress_mode == 'large' and obj_size < progress_size:
        return

    pbar = tqdm(total=obj_size, desc=f"Downloading {obj_meta.location.key.rsplit('/', 1)[-1]}")

    def f(size):
        pbar.update(size)

    return f


def _download_object(location: S3Location, local_path: LocalPathLike, progress=None) -> os.PathLike:
    input_local_path = local_path
    local_path = Path(local_path).resolve()
    if local_path.is_dir() or is_unmade_dir(input_local_path):
        os.makedirs(local_path, exist_ok=True)
        filename = location.split_key()[1]
        local_path = local_path / filename
    else:
        os.makedirs(local_path.parent, exist_ok=True)
    log.info(f'Downloading {location.s3_uri} to {local_path}... ')
    callback = _get_download_prog_callback(progress, location)
    _client.download_file(location.bucket, location.key, str(local_path), Callback=callback)
    log.info(f'Downloaded {location.s3_uri} to {local_path}')
    return local_path


def download(
        bucket_or_location: S3LocationLike,
        prefix: str | LocalPathLike,
        local_path: Optional[LocalPathLike] = None,
        recursive: bool = False,
        base_dir=None,
        progress=None,
) -> str | list[str]:
    if local_path is None:
        local_path = prefix
        prefix = None

    location = as_s3_location(bucket_or_location, prefix)

    if is_unmade_dir(local_path):
        os.makedirs(local_path)
    local_path = Path(local_path).resolve()

    if not location.exists():
        raise FileNotFoundError(f'No object(s) present at {location.s3_uri}')
    elif not location.is_object() and not recursive:
        raise ValueError(f'{location.s3_uri} is not a uri for a single object; '
                         'use `recursive=True` to download all objects with this prefix')

    if location.is_object():
        p = _download_object(location, local_path, progress=progress)
        return str(p)
    else:
        objects = list_objects(location)
        common_dir = _highest_common_dir([loc.key for loc in objects], resolve=False)
        if base_dir is None:
            base_dir = common_dir
        else:
            base_dir = Path(base_dir).resolve()
            if not str(common_dir).startswith(str(base_dir)):
                raise ValueError(f'Local files are not in subdirectories of the given base dir `{base_dir}`')

        local_paths = []
        for loc in objects:
            rel_dir = Path(loc.key).relative_to(base_dir).parent
            rel_path = local_path / str(rel_dir) / loc.split_key()[1]
            p = _download_object(loc, rel_path, progress=progress)
            local_paths.append(str(p))
        return local_paths


def _delete_object(location: S3Location):
    log.info(f'Deleting {location.s3_uri}... ')
    _client.delete_object(Bucket=location.bucket, Key=location.key)
    log.info(f'Deleted {location.s3_uri}')


def delete(
        bucket_or_location: S3LocationLike,
        prefix: Optional[str] = None,
        recursive: bool = False,
):
    location = as_s3_location(bucket_or_location, prefix)
    if not location.exists():
        raise FileNotFoundError(f'No object(s) at {location.s3_uri}')
    elif not location.is_object():
        if not recursive:
            raise ValueError(f'Multiple objects with prefix {location.s3_uri}, set `recursive=True` to delete them all')
        for loc in list_objects(location):
            _delete_object(loc)
    else:
        _delete_object(location)


def read(
        bucket_or_location: S3LocationLike,
        prefix: Optional[str] = None,
        file_type: Optional[str] = None,
        local_temp_file=None,
        progress=None,
        **kwargs,
):
    def read_body(body):
        if file_type == 'pkl':
            return pickle.load(body, **kwargs)
        elif file_type == 'json':
            return json.load(body, **kwargs)
        elif file_type == 'csv':
            return pd.read_csv(body, **kwargs)
        elif file_type == 'npy':
            return np.load(body, **kwargs)
        elif file_type == 'parquet':
            return pd.read_parquet(body, **kwargs)
        elif file_type in ('txt', 'text'):
            return body.read().decode()
        else:
            return body

    location = as_s3_location(bucket_or_location, prefix)
    if not location.exists():
        raise FileNotFoundError(f'No object(s) present at {location.s3_uri}')
    elif not location.is_object():
        raise ValueError(f'{location.s3_uri} is not a uri for a single object, cannot use `read` method.')

    if callable(progress):
        with_progress = True
    else:
        if progress is False:
            with_progress = False
        else:
            progress_mode, progress_size = _parse_progress_arg(progress)
            if progress_mode == 'off':
                with_progress = False
            elif progress_mode == 'all':
                with_progress = True
            else:
                obj_size = list_objects(location, limit=1, return_metadata=True)[0].size
                with_progress = (obj_size >= progress_size)

    ext = Path(location.key).suffix
    if file_type is None and ext:
        file_type = ext.lstrip('.')
    if file_type is not None:
        file_type = file_type.lstrip('.').lower()

    if with_progress or local_temp_file:
        if not local_temp_file:
            local_temp_file = f"TMPFILE.{datetime.now().strftime('%Y%m%dT%H%M%S.%f')}"
        log.info(f"Reading to local temp {file_type} file: {local_temp_file}")
        body = download(location, local_temp_file, progress=progress)
        with open(body, 'rb') as f:
            obj = read_body(f)
        log.info(f'Deleting {local_temp_file}')
        os.remove(local_temp_file)
        return obj
    else:
        body = _client.get_object(Bucket=location.bucket, Key=location.key)['Body']
        return read_body(body)


def write_to_s3(
        obj,
        bucket_or_path: str | S3Location,
        key: Optional[str] = None,
        local_temp_file=None,
        file_type: Optional[str] = None,
        progress=None,
        **kwargs
) -> S3Location:
    s3_loc = S3Location(bucket_or_path, key)

    if not local_temp_file:
        local_temp_file = f"TMPFILE.{datetime.now().strftime('%Y%m%dT%H%M%S.%f')}"

    if file_type is None:
        file_type = os.path.splitext(s3_loc.key)[1].lstrip('.').lower()
    if not file_type:
        file_type = 'pkl'

    log.info(f"Writing local temp {file_type} file: {local_temp_file}")
    if file_type == 'json':
        with open(local_temp_file, 'w') as f:
            json.dump(obj, f, **kwargs)
    elif file_type in ('pkl', 'pickle'):
        with open(local_temp_file, 'wb') as f:
            pickle.dump(obj, f, **kwargs)
    elif file_type == 'parquet':
        obj.to_parquet(local_temp_file, **kwargs)
    elif file_type in ('gz', 'gzip'):
        with open(local_temp_file, 'wb') as f:
            pickle.dump(io.BytesIO(gzip.compress(obj)), f, **kwargs)
    elif file_type == 'txt':
        with open(local_temp_file, 'w') as f:
            f.write(str(obj))
    else:
        raise ValueError(f"Unrecognized file_type: '{file_type}'")

    written_loc = upload(local_temp_file, s3_loc, progress=progress)
    log.info(f'Deleting {local_temp_file}')
    os.remove(local_temp_file)
    return written_loc


def touch(bucket_or_location: S3LocationLike, prefix: Optional[str] = None):
    location = as_s3_location(bucket_or_location, prefix)
    _client.put_object(Bucket=location.bucket, Key=location.key, Body=b'')
    log.info(f'Created empty object at {location.s3_uri}')


@timeit_opt(default=True)
def large_recursive_delete(bucket_or_location: S3LocationLike, prefix: Optional[str] = None, timeit=True):
    """See https://serverfault.com/a/1123717"""
    location = as_s3_location(bucket_or_location, prefix)
    if not location.exists():
        raise FileNotFoundError(f'No object(s) at {location.s3_uri}')
    elif location.is_object():
        raise ValueError(f'{location.s3_uri} is an object, not a prefix; use `delete` instead')
    else:
        cmd = ("""aws s3api list-objects-v2 --bucket {bucket} --prefix {prefix} --output text --query """
               """'Contents[].[Key]' | grep -v -e "'" | tr '\\n' '\\0' | xargs -0 -P2 -n500 bash -c """
               """'aws s3api delete-objects --bucket {bucket} --delete """
               """"Objects=[$(printf "{{Key=%q}}," "$@")],Quiet=true"' _ """
               ).format(bucket=location.bucket, prefix=location.key)
    if timeit:
        print(f'Starting recursive delete with prefix {location.s3_uri} at {datetime.now().isoformat()}')
    os.system(cmd)


@timeit_opt(default=True)
def list_many_objects(bucket_or_location: S3LocationLike, prefix: Optional[str] = None, timeit=True) -> list[str]:
    location = as_s3_location(bucket_or_location, prefix)
    if not location.exists():
        raise FileNotFoundError(f'No object(s) at {location.s3_uri}')
    elif location.is_object():
        return [location.s3_uri]
    else:
        cmd = ("aws s3api list-objects-v2 --bucket {bucket} --prefix {prefix} --output text --query 'Contents[].[Key]'"
               ).format(bucket=location.bucket, prefix=location.key)
    if timeit:
        print(f'Starting list-objects call with prefix {location.s3_uri} at {datetime.now().isoformat()}')
    output = subprocess.run(cmd, shell=True, capture_output=True)
    keys = output.stdout.decode().split()
    return [as_s3_location(location.bucket, k).s3_uri for k in keys]
