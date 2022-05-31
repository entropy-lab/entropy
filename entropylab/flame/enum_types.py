"""
Definitions of enumerated types used on several places
"""

from enum import Enum


class InputType(Enum):
    STATE = 1
    STREAM = 2


class ioFormat(Enum):
    VALUES = 1
    JUST_STRUCTURE = 2
