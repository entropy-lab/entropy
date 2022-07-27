from __future__ import annotations

import os

import jsonpickle as jsonpickle
from tinydb import Storage

from entropylab.logger import logger


class JSONPickleStorage(Storage):
    def __init__(self, filename):
        self.filename = filename

    def read(self):
        if not os.path.isfile(self.filename):
            return None
        with open(self.filename) as handle:
            # noinspection PyBroadException
            try:
                s = handle.read()
                data = jsonpickle.decode(s)
                return data
            except BaseException:
                logger.exception(
                    f"Exception decoding TinyDB JSON file '{self.filename}'"
                )
                return None

    def write(self, data):
        # noinspection PyBroadException
        try:
            with open(self.filename, "w+") as handle:
                s = jsonpickle.encode(data)
                handle.write(s)
        except BaseException:
            logger.exception(f"Exception encoding TinyDB JSON file '{self.filename}'")

    def close(self):
        pass
