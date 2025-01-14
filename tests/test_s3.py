import os
import shutil
import sys
import unittest
from pathlib import Path
from typing import List

from moto import mock_aws

this_dir = Path(__file__).resolve().parent
repo_root = this_dir.parent

try:
    from tests import get_arg_parser, run_tests
except ModuleNotFoundError:
    sys.path.insert(0, str(repo_root))
    from tests import get_arg_parser, run_tests

try:
    from yes3 import s3
except ModuleNotFoundError:
    sys.path.insert(0, str(repo_root / 'src'))
    from yes3 import s3
from yes3.s3 import S3Location

TEST_LOCAL_DIR = Path('_tmp_test_dir_')
LOCAL_COPY_DIR = str(TEST_LOCAL_DIR) + 'copy_/'
TEST_BUCKET = 'mock-bucket'
TEST_S3_DIR = f's3://{TEST_BUCKET}/unit-tests/'
VERBOSE = False

TEST_FILE_CONTENTS = {
    TEST_LOCAL_DIR / 'empty_file': None,
    TEST_LOCAL_DIR / 'file1.txt': 'file1 contents',
    TEST_LOCAL_DIR / 'dir1/file1.1.txt': 'file1.1 contents',
    TEST_LOCAL_DIR / 'dir1/file1.2': None,
    TEST_LOCAL_DIR / 'dir1/dir1.2/file2.1': None,
    TEST_LOCAL_DIR / 'dir1/dir1.2/file2.2': 'file2.2 contents',
}


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
    rm(LOCAL_COPY_DIR)
    _vprint()


def _create_test_files() -> List[os.PathLike]:
    paths = []
    for path, content in TEST_FILE_CONTENTS.items():
        os.makedirs(path.parent, exist_ok=True)
        if content is not None:
            with open(path, 'w') as f:
                f.write(content)
        else:
            path.touch(exist_ok=True)
        paths.append(path)
    return paths


def _test_single_uploads():
    s3_dir_loc = s3.S3Location(TEST_S3_DIR)

    # Test single file uploads
    file_loc = s3.upload(TEST_LOCAL_DIR / 'empty_file', s3_dir_loc)
    assert file_loc.key == s3_dir_loc.join('empty_file').key
    assert file_loc.exists()
    assert file_loc.is_object()
    assert not file_loc.is_dir()

    file_loc = s3.upload(TEST_LOCAL_DIR / 'dir1/file1.1.txt', s3_dir_loc.join('dir1/file'))
    dir_loc = s3_dir_loc.join('dir1')
    assert file_loc.key == dir_loc.join('file').key
    assert file_loc.exists()
    assert file_loc.is_object()
    assert not file_loc.is_dir()
    assert dir_loc.exists()
    assert dir_loc.is_dir()
    assert not dir_loc.is_object()


def _test_delete():
    s3_dir_loc = s3.S3Location(TEST_S3_DIR)

    file_loc = s3_dir_loc.join('empty_file')
    s3.delete(file_loc)
    assert not file_loc.exists()

    s3.delete(s3_dir_loc.bucket, s3_dir_loc.join('dir1', 'file').key)
    assert len(s3.list_objects(TEST_S3_DIR)) == 0


def _test_recursive_uploads():
    s3_dir_loc = s3.S3Location(TEST_S3_DIR)
    file_locs = s3.upload(TEST_LOCAL_DIR, s3_dir_loc, recursive=True)
    assert len(file_locs) == len(TEST_FILE_CONTENTS)


def _test_list_objects():
    dir12_locs = s3.list_objects(TEST_S3_DIR, 'dir1/dir1.2')
    for obj in dir12_locs:
        _vprint(f'Found S3 object: {obj}')
    assert len(dir12_locs) == 2
    assert set(loc.split_key()[1] for loc in dir12_locs) == {'file2.1', 'file2.2'}


def _test_list_dir():
    dir_objs = s3.list_dir(TEST_S3_DIR, return_metadata=True)
    assert len(dir_objs) == 3
    s3_objs = [o for o in dir_objs if isinstance(o, s3.S3Object)]
    for obj in s3_objs:
        _vprint(f'Found S3 object: {obj}')
    s3_pfxs = [o for o in dir_objs if isinstance(o, s3.S3Prefix)]
    for pfx in s3_pfxs:
        pfx.count_objects()
        _vprint(f'Found S3 prefix: {pfx}')
    assert len(s3_objs) == 2
    assert all(obj.location.is_object() for obj in s3_objs)
    assert len(s3_pfxs) == 1
    assert all(obj.location.is_dir() for obj in s3_pfxs)

    # List two levels down; both dir and its contents listed
    dir_objs = s3.list_dir(TEST_S3_DIR, depth=2)
    assert len(dir_objs) == 6
    _vprint()
    for loc in dir_objs:
        _vprint(f'Found S3 object: {loc}')


def _test_download():
    s3_dir_loc = s3.S3Location(TEST_S3_DIR)

    dwnld_path = s3.download(s3_dir_loc.join('empty_file'), LOCAL_COPY_DIR)
    assert dwnld_path == str(Path(LOCAL_COPY_DIR).resolve() / 'empty_file')
    assert os.path.exists(dwnld_path)
    os.remove(dwnld_path)

    dwnld_paths = s3.download(s3_dir_loc, LOCAL_COPY_DIR, recursive=True)
    assert len(dwnld_paths) == len(TEST_FILE_CONTENTS)

    _vprint(f'Deleting {LOCAL_COPY_DIR}')
    shutil.rmtree(LOCAL_COPY_DIR)
    assert not os.path.exists(LOCAL_COPY_DIR)


def _test_write_read():
    contents = {'a': 42, 'b': list(range(10)), 'c': None}
    s3_path = s3.S3Location(TEST_S3_DIR).join('test_write_file.pkl')
    s3.write_to_s3(contents, s3_path)
    read_contents = s3.read(s3_path)
    assert contents == read_contents


class TestS3Utils(unittest.TestCase):
    def test_s3_location(self):

        # Test with bucket only
        loc = S3Location('test-bucket')
        self.assertEqual(loc.s3_uri, 's3://test-bucket')

        # Test with bucket, prefix
        loc = S3Location('test-bucket', 'test-folder1/test_folder2/', 'us-region-1')
        self.assertEqual(loc.s3_uri, 's3://test-bucket/test-folder1/test_folder2/')
        self.assertEqual(loc.https_url, 'https://s3.us-region-1.amazonaws.com/test-bucket/test-folder1/test_folder2/')

        # Test with bucket, key, region
        loc = S3Location('test-bucket', 'test-folder/test_file.ext', 'us-region-1')
        self.assertEqual(loc.s3_uri, 's3://test-bucket/test-folder/test_file.ext')
        self.assertEqual(loc.https_url, 'https://s3.us-region-1.amazonaws.com/test-bucket/test-folder/test_file.ext')

        # Test manipulations
        loc = S3Location('test-bucket', 'test-folder1/', 'us-region-1').join('test_folder2', 'test_file.ext')
        self.assertEqual(
            loc.https_url,
            'https://s3.us-region-1.amazonaws.com/test-bucket/test-folder1/test_folder2/test_file.ext'
        )
        prefix, name = loc.split_key()
        self.assertEqual(prefix, 'test-folder1/test_folder2')
        self.assertEqual(name, 'test_file.ext')

        self.assertEqual(loc.parent.join(name), loc)
        self.assertEqual(loc.parent / name, loc)

        loc = S3Location('test-bucket', 'test-folder1/', 'us-region-1').join('test_folder2', '', '')
        self.assertEqual(
            loc.https_url,
            'https://s3.us-region-1.amazonaws.com/test-bucket/test-folder1/test_folder2/'
        )

    @mock_aws
    def test_read_write_ops(self):
        # moto (aws mock) requires the bucket be created before use
        s3._client.create_bucket(Bucket=TEST_BUCKET)

        s3.set_verbosity(VERBOSE)
        tests = [
            _create_test_files,
            _test_single_uploads,
            _test_delete,
            _test_recursive_uploads,
            _test_list_objects,
            _test_list_dir,
            _test_download,
            _test_write_read,
        ]
        _vprint('Running tests...\n')
        try:
            for test in tests:
                _vprint(f'----- {test.__name__} -----')
                test()
                _vprint()
        except Exception:
            raise
        finally:
            _cleanup_local()


if __name__ == '__main__':
    arg_parser = get_arg_parser()
    args = arg_parser.parse_args()
    if args.verbose:
        VERBOSE = True
    run_tests(args, TestS3Utils)
