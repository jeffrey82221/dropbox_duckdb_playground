"""
Base class of FileSystem and RDB
"""


class Backend:
    def __init__(self, directory: str = '/'):
        self._directory = directory
