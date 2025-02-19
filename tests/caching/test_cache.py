import os.path
import shutil
import unittest
from pathlib import Path
from time import sleep

from moto import mock_aws

from yes3 import s3, S3Location
from yes3.caching import CacheCore, LocalDiskCache, MemoryCache, MultiCache, S3Cache
from yes3.utils.testing import get_arg_parser, run_tests

TEST_LOCAL_DIR = Path('_tmp_cache_test_dir_')
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
updated_data = list(range(10000))
key = 'test_data'
local_obj_path = TEST_LOCAL_DIR / (key + '.pkl')
local_meta_path = TEST_LOCAL_DIR / (key + '.meta')
s3_obj_loc = S3Location(TEST_S3_DIR).join(key)
s3_meta_loc = S3Location(TEST_S3_DIR).join(key + '.meta')


class TestCaches(unittest.TestCase):
    def _check_paths_exists(self, cache, expect_exists: bool):
        if isinstance(cache, MultiCache):
            for c in cache:
                path = s3_obj_loc if isinstance(c.path, S3Location) else local_obj_path
                self.assertIs(path.exists(), expect_exists)
                meta_path = s3_meta_loc if isinstance(c.path, S3Location) else local_meta_path
                self.assertIs(meta_path.exists(), expect_exists)
        elif not isinstance(cache, MemoryCache):
            path = s3_obj_loc if isinstance(cache.path, S3Location) else local_obj_path
            self.assertIs(path.exists(), expect_exists)
            meta_path = s3_meta_loc if isinstance(cache.path, S3Location) else local_meta_path
            self.assertIs(meta_path.exists(), expect_exists)

    def _test_cache_state(self, cache: CacheCore):
        cache.deactivate()
        self.assertFalse(cache.is_active())
        cache.put(key, data)
        self.assertFalse(key in cache)
        cache.activate()
        self.assertTrue(cache.is_active())
        cache.set_read_only(True)
        with self.assertRaises(TypeError):
            _ = cache.put(key, data)
        cache.set_read_only(False)

    def _test_missing_data(self, cache: CacheCore):
        self.assertFalse(key in cache)
        self._check_paths_exists(cache, False)
        with self.assertRaises(KeyError):
            _ = cache.get(key)
        with self.assertRaises(KeyError):
            _ = cache[key]
        retrieved = cache.get(key, default=None)
        self.assertIsNone(retrieved)

    def _test_adding_data(self, cache: CacheCore):
        # Test adding and retrieving data
        cache.put(key, data)
        self.assertTrue(key in cache)
        retrieved = cache.get(key)
        self.assertEqual(retrieved, data)
        self.assertEqual(list(cache.keys()), [key])
        self._check_paths_exists(cache, True)

        retrieved = cache[key]
        self.assertEqual(retrieved, data)

    def _test_updating_data(self, cache: CacheCore):
        with self.assertRaises(ValueError):
            cache.put(key, updated_data)
        start_meta = cache.get_meta(key)
        if isinstance(cache, S3Cache):
            sleep(1)  # S3 object timestamp only has precision to nearest second
        cache.update(key, updated_data)
        retrieved = cache.get(key)
        new_meta = cache.get_meta(key)
        self.assertEqual(retrieved, updated_data)
        self.assertNotEqual(retrieved, data)
        self.assertEqual(start_meta.key, new_meta.key)
        if start_meta.size is None:
            self.assertIsNone(new_meta.size)
        else:
            self.assertNotEqual(start_meta.size, new_meta.size)
        if start_meta.path is None:
            self.assertIsNone(new_meta.path)
        else:
            self.assertEqual(start_meta.path, new_meta.path)
        self.assertNotEqual(start_meta.timestamp, new_meta.timestamp)

        cache.put(key, data, update=True)
        retrieved = cache.get(key)
        new_new_meta = cache.get_meta(key)
        self.assertEqual(retrieved, data)
        self.assertNotEqual(retrieved, updated_data)
        self.assertEqual(start_meta.key, new_new_meta.key)
        self.assertEqual(start_meta.size, new_new_meta.size)
        self.assertEqual(start_meta.path, new_new_meta.path)
        self.assertNotEqual(start_meta.timestamp, new_new_meta.timestamp)

    def _test_removing_data(self, cache: CacheCore):
        cache.remove(key)
        self.assertFalse(key in cache)
        self.assertIsNone(cache.get(key, None))
        self._check_paths_exists(cache, False)

        with self.assertRaises(KeyError):
            cache.update(key, data)
        cache[key] = data
        retrieved = cache.pop(key)
        self.assertFalse(key in cache)
        self._check_paths_exists(cache, False)
        self.assertEqual(retrieved, data)

    def _test_initializing_local_cache_with_data(self, cache: LocalDiskCache):
        cache.put(key, data)
        new_cache = LocalDiskCache.create(TEST_LOCAL_DIR)
        self.assertTrue(key in new_cache)
        retrieved = new_cache.get(key)
        self.assertEqual(retrieved, data)

    def _test_clearing_local_cache(self, cache: LocalDiskCache):
        path = local_obj_path
        if key not in cache:
            cache.put(key, data)
        second_cache = LocalDiskCache.create(cache.path)
        self.assertTrue(key in second_cache)
        with self.assertRaises(RuntimeError):
            cache.clear()
        cache.clear(force=True)
        self.assertFalse(key in cache)
        self.assertEqual(len(cache.keys()), 0)
        retrieved = cache.get(key, None)
        self.assertIsNone(retrieved)
        self.assertFalse(path.exists())
        with self.assertRaises(FileNotFoundError):
            second_cache.get(key)

    def _test_rebuild_missing_meta(self, local=True):
        if local:
            CacheType = LocalDiskCache
            cache_dir = TEST_LOCAL_DIR
            meta_path = local_meta_path
            rm_fun = os.remove
        else:
            CacheType = S3Cache
            cache_dir = TEST_S3_DIR
            meta_path = s3_meta_loc
            rm_fun = s3.delete

        cache = CacheType.create(cache_dir)
        if key not in cache:
            cache.put(key, data)
        self._check_paths_exists(cache, True)
        meta = cache.get_meta(key)
        rm_fun(meta_path)
        self.assertFalse(meta_path.exists())
        with self.assertRaises(RuntimeError):
            new_cache = CacheType.create(cache_dir)
        new_cache = CacheType.create(cache_dir, rebuild_missing_meta=True)
        self._check_paths_exists(new_cache, True)
        new_meta = new_cache.get_meta(key)
        self.assertEqual(new_meta.key, meta.key)
        self.assertEqual(new_meta.size, meta.size)
        self.assertEqual(new_meta.path, meta.path)
        self.assertEqual(new_meta.timestamp, meta.timestamp)

    def _run_local_tests(self, cache: LocalDiskCache):
        self._test_cache_state(cache)
        self._test_missing_data(cache)
        self._test_adding_data(cache)
        self._test_updating_data(cache)
        self._test_removing_data(cache)
        self._test_initializing_local_cache_with_data(cache)
        self._test_rebuild_missing_meta(local=True)
        self._test_clearing_local_cache(cache)

    def _test_local_cache(self):
        cache = LocalDiskCache.create(TEST_LOCAL_DIR)
        self._run_local_tests(cache)

    def _test_clearing_memory_cache(self, cache: MemoryCache):
        cache.clear()
        self.assertEqual(len(cache.keys()), 0)

    def _run_memory_tests(self, cache: MemoryCache):
        self._test_cache_state(cache)
        self._test_missing_data(cache)
        self._test_adding_data(cache)
        self._test_updating_data(cache)
        self._test_removing_data(cache)
        self._test_clearing_memory_cache(cache)

    def _test_memory_cache(self):
        cache = MemoryCache()
        self._run_memory_tests(cache)

    def _test_initializing_s3_cache_with_data(self, cache: S3Cache):
        cache.put(key, data)
        new_cache = S3Cache.create(TEST_S3_DIR)
        self.assertTrue(key in new_cache)
        retrieved = new_cache.get(key)
        self.assertEqual(retrieved, data)

    def _test_clearing_s3_cache(self, cache: S3Cache):
        path = s3_obj_loc
        if key not in cache:
            cache.put(key, data)
        second_cache = S3Cache.create(cache.path)
        self.assertTrue(key in second_cache)
        with self.assertRaises(RuntimeError):
            cache.clear()
        cache.clear(force=True)
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
        self._test_initializing_s3_cache_with_data(cache)
        self._test_rebuild_missing_meta(local=False)
        self._test_clearing_s3_cache(cache)

    @mock_aws
    def _test_s3_cache(self):
        # moto (aws mock) requires the bucket be created before use
        s3._client.create_bucket(Bucket=TEST_BUCKET)
        cache = S3Cache.create(TEST_S3_DIR)
        self._run_s3_tests(cache)

    def _test_multi_cache_sync(self, cache: MultiCache):
        for c in cache:
            self.assertEqual(len(c.keys()), 0)

        cache.put(key, data)
        mismatches = cache.check_meta_mismatches()
        self.assertEqual(len(mismatches), 0)

        cache._caches[0].update(key, data)
        mismatches = cache.check_meta_mismatches()
        self.assertEqual(len(mismatches), 1)
        with self.assertRaises(RuntimeError):
            cache.sync_now()

    def _run_multi_tests(self, cache: MultiCache):
        self._test_cache_state(cache)
        self._test_missing_data(cache)
        self._test_adding_data(cache)
        self._test_updating_data(cache)
        self._test_removing_data(cache)
        self._test_multi_cache_sync(cache)

    @mock_aws
    def _test_multi_cache(self):
        # moto (aws mock) requires the bucket be created before use
        s3._client.create_bucket(Bucket=TEST_BUCKET)
        local_cache = LocalDiskCache.create(TEST_LOCAL_DIR)
        s3_cache = S3Cache.create(TEST_S3_DIR)
        multi_cache = MultiCache([local_cache, s3_cache], sync_all=True)
        self._run_multi_tests(multi_cache)

    def test_all_tests(self):
        try:
            self._test_memory_cache()
            self._test_local_cache()
            self._test_s3_cache()
            self._test_multi_cache()
        except Exception:
            raise
        finally:
            _cleanup_local()


if __name__ == '__main__':
    arg_parser = get_arg_parser()
    args = arg_parser.parse_args()
    if args.verbose:
        VERBOSE = True
    run_tests(args, TestCaches)
