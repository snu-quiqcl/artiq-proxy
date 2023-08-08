"""Proxy server to communicate a client to ARTIQ."""

import ast
import json
import logging
import os
import posixpath
import shutil
import time
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import pydantic
from fastapi import FastAPI
from sipyco import pc_rpc as rpc

configs = {}


def load_config_file():
    """Loads config information from the configuration file.

    The file should have the following JSON structure:

      {
        "master_path": {master_path}
        "repository_path": {repository_path}
      }
    """
    with open("config.json", encoding="utf-8") as config_file:
        configs.update(json.load(config_file))


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Lifespan events.

    This function is set as the lifespan of the application.
    """
    load_config_file()
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/ls/")
async def list_directory(directory: str = "") -> List[str]:
    """Gets the list of elements in the given path and returns it.

    The "master_path" and "repository_path" in the configuration file 
    is used for the prefix of the path.

    Args:
        directory: The path of the directory to search for.

    Returns:
        A list with items in the given directory.
        It lists directories before files, sorted in an alphabetical order.
    """
    remote = get_client("master_experiment_db")
    full_path = posixpath.join(configs["master_path"], configs["repository_path"], directory)
    item_list = remote.list_directory(full_path)
    return sorted(
        item_list,
        key=lambda item: (not item.endswith("/"), item)
    )


class ExperimentInfo(pydantic.BaseModel):
    """Experiment information.
    
    This is the return type of get_experiment_info().

    Fields:
        name: The experiment name which is set as the docstring in the experiment file.
        arginfo: The dictionary containing arguments of the experiment.
          Each key is an argument name and its value contains the argument type,
          the default value, and the additional information for the argument.
    """
    name: str
    arginfo: Dict[str, Any]


@app.get("/experiment/info/", response_model=Dict[str, ExperimentInfo])
async def get_experiment_info(file: str) -> Any:
    """Gets information of the given experiment file and returns it.
    
    Args:
        file: The path of the experiment file.

    Returns:
        A dictionary containing only one element of which key is the experiment class name.
        The value is an ExperimentInfo object.
    """
    remote = get_client("master_experiment_db")
    return remote.examine(file)


previous_queue = {}


@app.get("/experiment/queue/")
async def get_experiment_queue() -> Dict[int, Dict[str, Any]]:
    """Gets the list of queued experiment and returns it.

    Returns:
        A dictionary of queued experiments with rid as their keys.
        The value of each corresponding rid is a dictionary with several information:
          "priority", "status", "due_date", "pipeline", "expid", etc.
        It includes the running experiment with different "status" value.
    """
    remote = get_client("master_schedule")
    current_queue = previous_queue
    while current_queue == previous_queue:
        await asyncio.sleep(0)
        current_queue.clear()
        current_queue.update(remote.get_status())
    previous_queue.clear()
    previous_queue.update(current_queue)
    return current_queue


@app.post("/experiment/delete/")
async def delete_experiment(rid: int):
    """Kills the run with the specified RID.

    Args:
        rid: The run identifier value of the target experiment.
    """
    remote = get_client("master_schedule")
    remote.delete(rid)


@app.post("/experiment/terminate/")
async def request_termination_of_experiment(rid: int):
    """Requests graceful termination of the run with the specified RID.

    Args:
        rid: The run identifier value of the target experiment.
    """
    remote = get_client("master_schedule")
    remote.request_termination(rid)


def add_tracking_line(stmt_list: List[ast.stmt]) -> List[ast.stmt]:
    modified_stmt_list = []
    for stmt in stmt_list:
        tracking_line_code = f"rtio_log('line', {stmt.lineno})"
        tracking_line_stmt = ast.parse(tracking_line_code).body
        modified_stmt_list.extend(tracking_line_stmt)
        for attr in ("body", "handlers", "orelse", "finalbody"):
            if hasattr(stmt, attr):
                setattr(stmt, attr, add_tracking_line(getattr(stmt, attr)))
        modified_stmt_list.append(stmt)
    return modified_stmt_list


def modify_experiment_code(code: str, experiment_cls_name: str):
    code_mod = ast.parse(code)
    experiment_cls_stmt = next(
        stmt for stmt in code_mod.body
        if isinstance(stmt, ast.ClassDef) and stmt.name == experiment_cls_name
    )
    run_func_stmt = next(
        stmt for stmt in experiment_cls_stmt.body
        if isinstance(stmt, ast.FunctionDef) and stmt.name == "run"
    )
    # make logs to track each line
    run_func_stmt.body = add_tracking_line(run_func_stmt.body)
    # call write_vcd()
    write_vcd_call_stmt = ast.parse("write_vcd()").body
    run_func_stmt.body.append(write_vcd_call_stmt)
    # define write_vcd()
    write_vcd_func_code = """
def write_vcd(self):
    pass
    """
    write_vcd_func_stmt = ast.parse(write_vcd_func_code).body
    experiment_cls_stmt.body.append(write_vcd_func_stmt)
    return ast.unparse(code_mod)


@app.get("/experiment/submit/")
async def submit_experiment(  # pylint: disable=too-many-arguments
    file: str,
    cls: str,
    args: str = "{}",
    pipeline: str = "main",
    priority: int = 0,
    timed: Optional[str] = None,
    visualize: bool = False
) -> int:
    """Submits the given experiment file.
    
    Args:
        file: The path of the experiment file.
        cls: The class name of the experiment.
        args: The arguments to submit which must be a JSON string of a dictionary.
          Each key is an argument name and its value is the value of the argument.
        pipeline: The pipeline to run the experiment in.
        priority: Higher value means sooner scheduling.
        timed: The due date for the experiment in ISO format.
          None for no due date.
        visualize: If True, the experiment file is modified for visualization.
          The original file and vcd file are saved in the visualize path set in config.json.
    
    Returns:
        The run identifier, an integer which is incremented at each experiment submission.
    """
    if visualize:
        experiment_path = posixpath.join(configs["master_path"], configs["repository_path"], file)
        with open(experiment_path, encoding="utf-8") as experiment_file:
            code = experiment_file.read()
        modified_code = modify_experiment_code(code, cls)
        print(modified_code)
    else:
        # TODO(BECATRUE): The exact experiment path will be assigned in #37.
        pass
    args_dict = json.loads(args)
    expid = {
        "log_level": logging.WARNING,
        "class_name": None,
        "arguments": args_dict,
        "file": posixpath.join(configs["repository_path"], file)
    }
    due_date = None if timed is None else time.mktime(datetime.fromisoformat(timed).timetuple())
    remote = get_client("master_schedule")
    rid = remote.submit(pipeline, expid, priority, due_date, False)
    if visualize:
        visualize_dir_path = posixpath.join(
            configs["master_path"],
            configs["visualize_path"],
            f"{rid}/"
        )
        os.makedirs(visualize_dir_path)
        copied_experiment_path = posixpath.join(visualize_dir_path, "experiment.py")
        shutil.copyfile(experiment_path, copied_experiment_path)
        metadata = {
            "file": experiment_path,
            "args": args_dict,
            "pipeline": pipeline,
            "priority": priority,
            "timed": timed,
            "submission_time": datetime.now().isoformat(timespec="seconds")
        }
        metadata_path = posixpath.join(visualize_dir_path, "metadata.json")
        with open(metadata_path, "w", encoding="utf-8") as metadata_file:
            json.dump(metadata, metadata_file)
    return rid


@app.get("/experiment/running/")
async def get_running_experiment() -> Optional[int]:
    """Gets the running experiment RID.

    It assumes that there is at most one running experiment.
    
    Returns:
        The run identifier of the running experiment.
        If no running experiment, it returns None.
    """
    remote = get_client("master_schedule")
    status = remote.get_status()
    for rid, info in status.items():
        if info["status"] == "running":
            return rid
    return None


def get_client(target_name: str) -> rpc.Client:
    """Creates a client connecting to ARTIQ and returns it.

    The host is a localhost and the port is for ARTIQ master control.

    Args:
        target_name: The name of the target.
          The possible candidates are as follows:
            - master_schedule
            - master_dataset_db
            - master_device_db
            - master_experiment_db
          For details, see main() in artiq.frontend.artiq_client.
    """
    return rpc.Client("::1", 3251, target_name)
