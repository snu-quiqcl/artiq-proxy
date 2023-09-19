"""Unit tests for main module."""

import json
import logging
import posixpath
import time
import copy
import unittest
from datetime import datetime
from unittest import mock

from fastapi.testclient import TestClient

import main

class RoutingTest(unittest.TestCase):
    """Unit tests for routing and each operation."""

    def setUp(self):
        patcher_load_config_file = mock.patch("main.load_config_file")
        patcher_connect_moninj = mock.patch("main.connect_moninj")
        patcher_mi_connection = mock.patch("main.mi_connection")
        patcher_get_client = mock.patch("main.get_client")
        patcher_load_config_file.start()
        patcher_connect_moninj.start()
        mocked_mi_connection = patcher_mi_connection.start()
        mocked_mi_connection.close = mock.AsyncMock()
        mocked_get_client = patcher_get_client.start()
        self.mocked_client = mocked_get_client.return_value
        self.addCleanup(patcher_load_config_file.stop)
        self.addCleanup(patcher_connect_moninj.stop)
        self.addCleanup(patcher_mi_connection.stop)
        self.addCleanup(patcher_get_client.stop)

    @mock.patch.dict("main.configs",
                     {"master_path": "master_path/", "repository_path": "repo_path/"})
    def test_list_directory(self):
        test_list = ["dir1/", "dir2/", "file1.py", "file2.py"]
        self.mocked_client.list_directory.return_value = test_list
        with TestClient(main.app) as client:
            for params in ({}, {"directory": "dir1/"}):
                directory = params.get('directory', '')
                response = client.get("/ls/", params=params)
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
            response = client.get("/experiment/info/", params={'file': 'experiment.py'})
            self.mocked_client.examine.assert_called_with("experiment.py")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(
                response.json()["ExperimentClass"],
                test_info["ExperimentClass"].model_dump()
            )

    def test_get_experiment_queue(self):
        test_queue = {
            "1": {
                "pipeline": "main",
                "expid": {
                    "log_level": 30,
                    "class_name": None,
                    "arguments": {"user": "QuIQCL", "time": 1.0, "save": False, "color": "r"},
                   "file": "DIRECTORY-PATH"
                },
                "priority": 1,
                "due_date": None,
                "flush": False,
                "status": None,
                "repo_msg": None
            }
        }
        with TestClient(main.app) as client:
            for status in ["pending", "preparing", "running", "run_done", "analyzing", "deleting"]:
                test_queue["1"]["status"] = status
                self.mocked_client.get_status.return_value = copy.deepcopy(test_queue)
                response = client.get("/experiment/queue/")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json(), test_queue)

    def test_delete_experiment(self):
        test_rid = 1
        with TestClient(main.app) as client:
            client.post("/experiment/delete/", params={"rid": test_rid})
            self.mocked_client.delete.assert_called_with(test_rid)

    def test_request_termination_of_experiment(self):
        test_rid = 1
        with TestClient(main.app) as client:
            client.post("/experiment/terminate/", params={"rid": test_rid})
            self.mocked_client.request_termination.assert_called_with(test_rid)

    @mock.patch.dict("main.configs", {"repository_path": "repo_path/"})
    def test_submit_experiment(self):
        test_rid = 0
        self.mocked_client.submit.return_value = test_rid
        with TestClient(main.app) as client:
            test_params = (
                {"file": "experiment1.py"},
                {"file": "experiment2.py", "args": '{"k": "v"}'},
                {"file": "experiment3.py", "pipeline": "minor"},
                {"file": "experiment3.py", "priority": 1},
                {"file": "experiment3.py", "pipeline": "2023-06-26T10:00:00"},
            )
            for params in test_params:
                response = client.get("/experiment/submit/", params=params)
                file = posixpath.join("repo_path", params["file"])
                args = json.loads(params.get("args", "{}"))
                pipeline = params.get("pipeline", "main")
                priority = params.get("priority", 0)
                timed = params.get("timed", None)
                due_date = None if timed is None \
                           else time.mktime(datetime.fromisoformat(timed).timetuple())
                expid = {
                    "log_level": logging.WARNING,
                    "class_name": None,
                    "arguments": args,
                    "file": file
                }
                self.mocked_client.submit.assert_called_with(
                    pipeline,
                    expid,
                    priority,
                    due_date,
                    False
                )
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
        self.assertEqual(
            main.configs,
            {"master_path": "master_path/", "repository_path": "repo_path/"}
        )

    @mock.patch("main.rpc.Client")
    def test_get_client(self, mocked_client_cls):
        for target_name in ("name1", "name2"):
            main.get_client(target_name)
            mocked_client_cls.assert_called_with("::1", 3251, target_name)


if __name__ == "__main__":
    unittest.main()
