"""Unit tests for main module."""

import unittest
from unittest import mock
from fastapi.testclient import TestClient

import main


class Test(unittest.TestCase):
    """Unit tests for routing and each operation."""

    def setUp(self):
        patcher_load_config_file = mock.patch("main.load_config_file")
        patcher_get_client = mock.patch("main.get_client")
        self.mocked_load_config_file = patcher_load_config_file.start()
        self.mocked_get_client = patcher_get_client.start()
        self.addCleanup(patcher_load_config_file.stop)
        self.addCleanup(patcher_get_client.stop)

    @mock.patch.dict("main.configs", {"master_path": "master_path/"})
    def test_list_directory_return(self):
        """Tests if list_directory() returns correctly."""
        test_list = ["dir1/", "dir2/", "file1.py", "file2.py"]
        self.mocked_get_client.return_value.list_directory.return_value = test_list
        with TestClient(main.app) as client:
            response = client.get("/ls/")
        self.mocked_load_config_file.assert_called_once()
        self.mocked_get_client.return_value.list_directory.assert_called_once_with(
            "master_path/"
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), test_list)


if __name__ == "__main__":
    unittest.main()
