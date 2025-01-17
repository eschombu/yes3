import argparse
import pdb
import sys
import traceback
import unittest
from typing import Type, Union


def get_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action='store_true', help='Run unittest.main with verbosity==2')
    parser.add_argument('-s', '--step', action='store_true', help='Step through with pdb.')
    parser.add_argument('--pdb', action='store_true', help='Enter pdb.post_mortem on failure.')
    return parser


def run_tests(args, test_case: Union[unittest.TestCase, Type[unittest.TestCase]], with_unittest=True) -> None:
    if isinstance(args, argparse.ArgumentParser):
        args = args.parse_args()
    if isinstance(test_case, type):
        test_case = test_case()
    if args.pdb or args.step or not with_unittest:
        try:
            if args.step:
                pdb.set_trace()
            tests = [attr for attr in dir(test_case) if callable(getattr(test_case, attr)) and attr.startswith('test_')]
            for test in tests:
                print(f"TESTING: {test}()... ", end='')
                getattr(test_case, test)()
                print("PASSED")
        except (KeyboardInterrupt, pdb.bdb.BdbQuit):
            sys.exit(1)
        except Exception:
            print("FAILED")
            traceback.print_exc()
            if args.pdb:
                pdb.post_mortem()
    else:
        unittest.main(verbosity=(args.verbose + 1))
