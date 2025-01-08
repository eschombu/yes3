import shutil
import sys
import unittest
from pathlib import Path

from moto import mock_aws

this_dir = Path(__file__).resolve().parent
repo_root = this_dir.parent.parent

try:
    from tests import get_arg_parser, run_tests
except ModuleNotFoundError:
    sys.path.insert(0, str(repo_root))
    from tests import get_arg_parser, run_tests

try:
    from yes3.caching import CacheNotInitializedError, LocalDiskCache, MultiCache, S3Cache
except ModuleNotFoundError:
    sys.path.insert(0, str(repo_root / 'src'))
    from yes3.caching import CacheNotInitializedError, LocalDiskCache, MultiCache, S3Cache
from yes3 import s3
from yes3.caching.base import CatalogCache

TEST_LOCAL_DIR = Path('_tmp_test_dir_')
TEST_BUCKET = 'mock-bucket'
TEST_S3_DIR = f's3://{TEST_BUCKET}/unit-tests/'
VERBOSE = False


def _vprint(*args, **kwargs):
    if VERBOSE:
        print(*args, **kwargs)


def _cleanup_local():
    def rm(path):
        path = Path(path)
        if path.exists():
            _vprint(f'Deleting {path}')
            shutil.rmtree(path)
        assert path.exists() is False

    _vprint('----- Cleaning up local files -----')
    rm(TEST_LOCAL_DIR)
    _vprint()


data = {'i': 42, 's': 'hello world'}
updated_data = data.copy()
updated_data['i'] = 43
key = 'test_data'
path = TEST_LOCAL_DIR / (key + '.pkl')


class TestLocalDiskCache(unittest.TestCase):
    def _test_cache_state(self, cache: CatalogCache):
        # Test cache state (active & initialized)
        self.assertFalse(cache.is_active())
        cache.activate()
        self.assertTrue(cache.is_active())
        self.assertFalse(cache.is_initialized())

        with self.assertRaises(CacheNotInitializedError):
            _ = key in cache
        cache.initialize()
        self.assertTrue(cache.is_initialized())

    def _test_missing_data(self, cache: CatalogCache):
        # Test checking and getting data that is not present
        self.assertFalse(key in cache)
        self.assertFalse(path.exists())
        with self.assertRaises(KeyError):
            _ = cache.get(key)
        with self.assertRaises(KeyError):
            _ = cache[key]
        retrieved = cache.get(key, default=None)
        self.assertIsNone(retrieved)

    def _test_adding_data(self, cache: CatalogCache):
        # Test adding and retrieving data
        cache.put(key, data)
        self.assertTrue(key in cache)
        retrieved = cache.get(key)
        self.assertEqual(retrieved, data)
        self.assertEqual(list(cache.keys()), [key])
        self.assertEqual(list(cache), [key])
        self.assertTrue(path.exists())

        retrieved = cache[key]
        self.assertEqual(retrieved, data)

    def _test_updating_data(self, cache: CatalogCache):
        # Test updating data for existing key
        with self.assertRaises(ValueError):
            cache.put(key, updated_data)
        cache.update(key, updated_data)
        retrieved = cache.get(key)
        self.assertEqual(retrieved, updated_data)
        self.assertNotEqual(retrieved, data)

        cache.put(key, data, update=True)
        retrieved = cache.get(key)
        self.assertEqual(retrieved, data)
        self.assertNotEqual(retrieved, updated_data)

    def _test_removing_data(self, cache: CatalogCache):
        # Test removing data
        cache.remove(key)
        self.assertFalse(key in cache)
        self.assertIsNone(cache.get(key, None))
        self.assertFalse(path.exists())

        with self.assertRaises(KeyError):
            cache.update(key, data)
        cache[key] = data
        retrieved = cache.pop(key)
        self.assertFalse(key in cache)
        self.assertFalse(path.exists())
        self.assertEqual(retrieved, data)

    def _test_initializing_local_cache_with_data(self, cache: CatalogCache):
        # Test initializing cache when there is existing data in its location
        cache.put(key, data)
        new_cache = LocalDiskCache(TEST_LOCAL_DIR).initialize()
        self.assertTrue(key in new_cache)
        retrieved = new_cache.get(key)
        self.assertEqual(retrieved, data)

    def _test_clearing_local_cache(self, cache: LocalDiskCache):
        # Test clearing cache
        if key not in cache:
            cache.put(key, data)
        second_cache = LocalDiskCache(cache.local_path).initialize()
        self.assertTrue(key in second_cache)
        with self.assertRaises(RuntimeError):
            cache.clear()
        cache.clear(force=True)
        with self.assertRaises(CacheNotInitializedError):
            _ = key in cache
        cache.initialize()
        self.assertFalse(key in cache)
        self.assertEqual(len(cache.keys()), 0)
        retrieved = cache.get(key, None)
        self.assertIsNone(retrieved)
        self.assertFalse(path.exists())
        with self.assertRaises(FileNotFoundError):
            second_cache.get(key)

    def _run_local_tests(self, cache: LocalDiskCache):
        self._test_cache_state(cache)
        self._test_missing_data(cache)
        self._test_adding_data(cache)
        self._test_updating_data(cache)
        self._test_removing_data(cache)
        self._test_initializing_local_cache_with_data(cache)
        self._test_clearing_local_cache(cache)

    def test_local_cache(self):
        cache = LocalDiskCache(TEST_LOCAL_DIR, active=False)
        self._run_local_tests(cache)

    def _test_initializing_s3_cache_with_data(self, cache: S3Cache):
        # Test initializing cache when there is existing data in its location
        cache.put(key, data)
        new_cache = S3Cache(TEST_S3_DIR).initialize()
        self.assertTrue(key in new_cache)
        retrieved = new_cache.get(key)
        self.assertEqual(retrieved, data)

    def _test_clearing_s3_cache(self, cache: S3Cache):
        # Test clearing cache
        if key not in cache:
            cache.put(key, data)
        second_cache = S3Cache(cache.s3_location).initialize()
        self.assertTrue(key in second_cache)
        with self.assertRaises(RuntimeError):
            cache.clear()
        cache.clear(force=True)
        with self.assertRaises(CacheNotInitializedError):
            _ = key in cache
        cache.initialize()
        self.assertFalse(key in cache)
        self.assertEqual(len(cache.keys()), 0)
        retrieved = cache.get(key, None)
        self.assertIsNone(retrieved)
        self.assertFalse(path.exists())
        with self.assertRaises(FileNotFoundError):
            second_cache.get(key)

    def _run_s3_tests(self, cache: S3Cache):
        self._test_cache_state(cache)
        self._test_missing_data(cache)
        self._test_adding_data(cache)
        self._test_updating_data(cache)
        self._test_removing_data(cache)

    @mock_aws
    def test_s3_cache(self):
        # moto (aws mock) requires the bucket be created before use
        s3._client.create_bucket(Bucket=TEST_BUCKET)
        cache = S3Cache(TEST_S3_DIR)


if __name__ == '__main__':
    arg_parser = get_arg_parser()
    args = arg_parser.parse_args()
    if args.verbose:
        VERBOSE = True
    try:
        run_tests(args, TestLocalDiskCache)
    except Exception:
        raise
    finally:
        _cleanup_local()
