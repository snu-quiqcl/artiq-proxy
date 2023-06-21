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
        self.mocked_client = self.mocked_get_client.return_value
        self.addCleanup(patcher_load_config_file.stop)
        self.addCleanup(patcher_get_client.stop)

    @mock.patch.dict("main.configs",
                     {"master_path": "master_path/", "repository_path": "repo_path/"})
    def test_list_directory(self):
        test_list = ["dir1/", "dir2/", "file1.py", "file2.py"]
        self.mocked_client.list_directory.return_value = test_list
        with TestClient(main.app) as client:
            self.mocked_load_config_file.assert_called_once()
            for params in ({}, {"directory": "dir1/"}):
                directory = params.get('directory', '')
                response = client.get("/ls/", params=params)
                directory = params.get('directory', '')
                self.mocked_client.list_directory.assert_called_with(
                    f"master_path/repo_path/{directory}")
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
        self.mocked_client.examine.return_value = test_info
        with TestClient(main.app) as client:
            self.mocked_load_config_file.assert_called_once()
            response = client.get("/experiment/info/", params={'file': 'experiment.py'})
            self.mocked_client.examine.assert_called_with("experiment.py")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json(), test_info)

    @mock.patch.dict("main.configs", {"repository_path": "repo_path/"})
    def test_submit_experiment(self):
        test_rid = 0
        self.mocked_client.submit.return_value = test_rid
        with TestClient(main.app) as client:
            self.mocked_load_config_file.assert_called_once()
            test_params = (
                {"file": "experiment1.py"},
                {"file": "experiment2.py", "args": '{"k": "v"}'}
            )
            for params in test_params:
                expid = {
                    "log_level": logging.WARNING,
                    "class_name": None,
                    "arguments": json.loads(params.get("args", "{}")),
                    "file": posixpath.join("repo_path", params["file"])
                }
                response = client.get("/experiment/submit/", params=params)
                self.mocked_client.submit.assert_called_with("main", expid, 0, None, False)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json(), test_rid)


class FunctionTest(unittest.TestCase):
    """Unit tests for other functions."""

    @mock.patch("builtins.open")
    @mock.patch("json.load",
                return_value={"master_path": "master_path/", "repository_path": "repo_path/"})
    def test_load_config_file(self, mocked_load, mocked_open):
        main.load_config_file()
        mocked_open.assert_called_once_with("config.json", encoding="utf-8")
        mocked_load.assert_called_once()
        self.assertEqual(main.configs,
                          {"master_path": "master_path/", "repository_path": "repo_path/"})

    @mock.patch("main.rpc.Client")
    def test_get_client(self, mocked_client_cls):
        for target_name in ("name1", "name2"):
            main.get_client(target_name)
            mocked_client_cls.assert_called_with("::1", 3251, target_name)


if __name__ == "__main__":
    unittest.main()
