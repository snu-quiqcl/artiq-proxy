"""Unit tests for main module."""

import unittest
from unittest import mock
from fastapi.testclient import TestClient

import main


class RoutingTest(unittest.TestCase):
    """Unit tests for routing and each operation."""

    def setUp(self):
        patcher_load_config_file = mock.patch("main.load_config_file")
        patcher_get_client = mock.patch("main.get_client")
        self.mocked_load_config_file = patcher_load_config_file.start()
        self.mocked_get_client = patcher_get_client.start()
        self.addCleanup(patcher_load_config_file.stop)
        self.addCleanup(patcher_get_client.stop)

    @mock.patch.dict("main.configs",
                     {"master_path": "master_path/", "repository_path": "repo_path/"})
    def test_list_directory(self):
        test_list = ["dir1/", "dir2/", "file1.py", "file2.py"]
        mocked_client = self.mocked_get_client.return_value
        mocked_client.list_directory.return_value = test_list
        with TestClient(main.app) as client:
            self.mocked_load_config_file.assert_called_once()
            for query, path in [("", ""), ("?directory=dir1/", "dir1/")]:
                response = client.get(f"/ls/{query}")
                mocked_client.list_directory.assert_called_with(
                    "master_path/repo_path/" + path
                )
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json(), test_list)

    def test_get_experiment_info(self):
        test_info = {
            "ExperimentClass": main.ExperimentInfo(
                name="experiment_name",
                arginfo={
                    "arg1": [{"ty": "StringValue", "default": "DEFAULT"}, None, None],
                    "arg2": [{"ty": "BooleanValue", "default": True}, None, None]
                }
            )
        }
        mocked_client = self.mocked_get_client.return_value
        mocked_client.examine.return_value = test_info
        with TestClient(main.app) as client:
            self.mocked_load_config_file.assert_called_once()
            response = client.get("/experiment/info/?file=experiment.py")
            mocked_client.examine.assert_called_with("experiment.py")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json(), test_info)



if __name__ == "__main__":
    unittest.main()
