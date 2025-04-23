# `yes3`

## A library for intuitive reading, writing, listing, and caching with AWS S3 (Simple Storage Service).

This library wraps the `boto3` S3 API boilerplate with a simple and intuitive interface, path flexibility, and powerful
utilities for easily listing, reading, and writing data on/from/to S3.

## Installation

Using a virtual environment is recommended. The simplest way to install is with pip:
latest
```
pip install yes3
```

Alternatively, you can install the latest version from github:
```
pip install git+https://github.com/eschombu/yes3.git
```

To run tests and test scripts, and/or contribute to `yes3`, clone this repository from
https://github.com/eschombu/yes3.git, and install the dev requirements:
```
git clone https://github.com/eschombu/yes3.git
cd yes3
# Optionally create a virtual environment:
# python3.1x -m venv .venv/yes3
# source .venv/yes3/bin/activate
pip install -e .[dev]
pytest
```

## TODO

1. Documentation
2. Replace message printing with loggers

## Usage

### S3 Locations and Paths

The `boto3` APIs for S3 typically consider the 'bucket' and 'key' of an S3 object:
```
import boto3
s3_client = boto3.client('s3')
s3_client.download_file('my-bucket', 'key/to/object', 'path/to/local/file')
```

The awscli uses urls:
```
aws s3 cp s3://my-bucket/key/to/object path/to/local/file
```

In `yes3`, we accept either, attempting to flexibly interpret input arguments as S3 locations and local paths,
converting S3 locations into `S3Location` objects:
```
from yes3 import s3, S3Location

# The following download calls are equivalent
s3.download('s3://my-bucket/key/to/object', 'path/to/local/file')
s3.download('my-bucket', 'key/to/object', 'path/to/local/file')

s3_loc = S3Location('s3://my-bucket/key/to/object')
print(s3_loc.bucket)  # 'my-bucket'
print(s3_loc.key)  # 'key/to/object'
print(s3_loc.exists())  # True
print(s3_loc.is_bucket())  # False
print(s3_loc.is_dir())  # False
print(s3_loc.is_object())  # True
s3.download(s3_loc, 'path/to/local/file')
```

If the local path is to a directory, the object will be downloaded with the filename inferred from the S3 path.
Recursive downloads are also supported.
```
s3_dir = S3Location('s3://my-bucket/path/to/dir')
print(s3_dir.is_dir())  # True
print(s3_dir.is_object())  # False
s3.download(s3_dir, 'local_dir/')  # raises ValueError because s3_dir is not a single S3 object
s3.download(s3_dir, 'local_dir/', recursive=True)  # downloads all objects to the `local_dir` directory (which is created if it does not already exist)
```

Direct read/write functions are also available: `s3.read`, `s3.write_to_s3` (which actually creates a local temp file,
which is removed afterwards), and `s3.touch`.

Convenient object and directory listing methods are available:
* `s3.list_objects`: list all objects with the specified prefix
* `s3.list_dir`: List objects and directories only up to the specified depth (default: 1). S3 does not actually have a
directory structure, but this function works as if it does.

### Easy key-based caching utilities, for local, S3, and multi-location caches

To quickly and easily cache data, and allow for such a cache to be synced across devices, this package includes `Cache`
classes, which include `LocalDiskCache` and `S3Cache` subclasses, as well as a `MultiCache` which can utilize multiple
cache locations. Caching is key-value based, with customizable serializers that can store objects with `pickle` or
alternative data/file formats.

A helper function, `setup_cache`, provides a simple interface to create a `Cache` object with the default
`PickleSerializer` serializer:

```
from yes3.caching import setup_cache

local_cache = setup_cache('path/to/cache/dir')
s3_cache = setup_cache('s3://my-bucket/cache/dir/prefix')

if 'data' in s3_cache:
    data = s3_cache['data']
else:
    data = expensive_data_processing(args)
if 'data' not in local_cache:
    local_cache['data'] = data

multi_cache = MultiCache([local_cache, s3_cache])
multi_cache.sync_now()  # Add any data missing found in either cache to the one in which it is missing
multi_cache.sync_always()  # Keep the caches synced moving forward

new_data = get_more_data()
multi_cache.put('new_data', new_data)
print('new_data' in local_cache)  # True
print('new_data' in s3_cache)  # True

from yes3 import s3
for loc in s3.list_objects(s3_cache.path):
    print(loc.s3_uri)
# 's3://my-bucket/cache/dir/prefix/data.meta'
# 's3://my-bucket/cache/dir/prefix/data.pkl'
# 's3://my-bucket/cache/dir/prefix/new_data.meta'
# 's3://my-bucket/cache/dir/prefix/new_data.pkl'
```
