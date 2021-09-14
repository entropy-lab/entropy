import argparse

import pytest

from entropylab.cli.main import init, command


def test_init():
    args = argparse.Namespace
    args.directory = "."
    init(args)


# def test_serve():
#     mock_function = create_autospec(serve_results, return_value=None)
#     # mock serve_results
#     # mocker.patch("serve_results")
#     args = argparse.Namespace
#     args.directory = "."
#     args.port = 12345
#     serve(args)
#     mock_function.assert_called_once()


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
