"""
Basic test to verify pytest is working correctly.
"""

import pytest


def test_simple_addition():
    """Test that basic addition works."""
    assert 2 + 2 == 4


def test_string_concatenation():
    """Test that string concatenation works."""
    result = "hello" + " " + "world"
    assert result == "hello world"


def test_list_operations():
    """Test basic list operations."""
    items = [1, 2, 3]
    items.append(4)
    assert len(items) == 4
    assert items[-1] == 4


@pytest.mark.parametrize("input_value,expected", [
    (0, 0),
    (1, 1),
    (2, 4),
    (3, 9),
    (5, 25),
])
def test_square_function(input_value, expected):
    """Test squaring function with multiple inputs."""
    result = input_value ** 2
    assert result == expected
