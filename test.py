"""Unit tests for main module."""

import unittest
from unittest import mock
from fastapi.testclient import TestClient

import main


class Test(unittest.TestCase):
    """Unit tests for routing and each operation."""

    def setUp(self):
        patcher = mock.patch("main.get_client")
        self.mocked_get_client = patcher.start()
        self.addCleanup(patcher.stop)


if __name__ == "__main__":
    unittest.main()
