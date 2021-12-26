import pytest

from entropylab.results.dashboard.dashboard import get_added_row


def test_():
    prev_rows = [1, 2, 3]
    curr_rows = [1, 2, 3, 4]
    assert get_added_row(prev_rows, curr_rows) == 4


@pytest.mark.parametrize(
    "prev_rows, curr_rows, expected",
    [
        ([1, 2, 3], [1, 2, 3, 4], 4),
        ([1, 2, 3, 4], [1, 2, 3], None),
        ([1, 2, 3], [1, 2, 3], None),
        ([1, 2, 3], [4, 5, 6], 4),
        ([], [], None),
        ([1, 2, 3], [], None),
        ([], [1, 2, 3], 1),
    ],
)
def test_get_added_row(prev_rows, curr_rows, expected):
    assert get_added_row(prev_rows, curr_rows) == expected
