import argparse
import os
import shutil

import pytest

from entropylab.cli.main import init, command


def test_init_with_no_args():
    # arrange
    args = argparse.Namespace()
    args.directory = ""
    # act
    init(args)


def test_init_with_current_dir():
    # arrange
    args = argparse.Namespace()
    args.directory = "."
    # act
    init(args)
    # assert
    assert os.path.exists(".entropy/entropy.db")
    assert os.path.exists(".entropy/hdf5")
    # clean up
    shutil.rmtree(".entropy")


# def test_serve():
#     args = argparse.Namespace()
#     args.directory = "tests_cache"
#     args.host = "localhost"
#     args.port = 9876
#     args.debug = True
#     serve(args)
#     assert False


def test_safe_run_command_with_no_args():
    no_args_func()


def test_safe_run_command_with_one_args():
    one_args_func("foo")


def test_safe_run_command_with_two_args():
    two_args_func("foo", "bar")


def test_safe_run_command_that_raises():
    with pytest.raises(SystemExit) as se:
        two_args_func_that_raises()
    assert se.type == SystemExit
    assert se.value.code == -1


@command
def no_args_func() -> None:
    print("Foo!")


@command
def one_args_func(one: str) -> None:
    print("Foo! " + one)


@command
def two_args_func(one: str, two: str) -> None:
    print("Foo! " + one + " " + two)


@command
def two_args_func_that_raises():
    raise RuntimeError("Foo!")
