"""Unit tests for main module."""

import logging
import json
import posixpath
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
            for params in [{}, {"directory": "dir1/"}]:
                response = client.get(f"/ls/", params=params)
                mocked_client.list_directory.assert_called_with(
                    f"master_path/repo_path/{params.get('directory', '')}")
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
            response = client.get("/experiment/info/", params={'file': 'experiment.py'})
            mocked_client.examine.assert_called_with("experiment.py")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json(), test_info)

    @mock.patch.dict("main.configs", {"repository_path": "repo_path/"})
    def test_submit_experiment(self):
        test_rid = 0
        mocked_client = self.mocked_get_client.return_value
        mocked_client.submit.return_value = test_rid
        with TestClient(main.app) as client:
            self.mocked_load_config_file.assert_called_once()
            for params in [
                {"file": "experiment1.py"},
                {"file": "experiment2.py", "args": '{"k": "v"}'}]:
                response = client.get("/experiment/submit/", params=params)
                mocked_client.submit.assert_called_with(
                    "main", {
                        "log_level": logging.WARNING,
                        "class_name": None,
                        "arguments": json.loads(params.get("args", "{}")),
                        "file": posixpath.join("repo_path", params["file"])
                    }, 0, None, False
                )
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json(), test_rid)


if __name__ == "__main__":
    unittest.main()
