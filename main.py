"""Proxy server to communicate a client to ARTIQ."""

import ast
import json
import logging
import os
import posixpath
import shutil
import time
import copy
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional

import h5py
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
        "result_path": {result_path}
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
    current_queue = copy.deepcopy(previous_queue)
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
    """Returns a new statements list interleaved with rtio logs for tracking each line.

    Adds logs containing the corresponding line number before each line, recursively.
    The given statements list is not modified.

    Args:
        stmt_list: The list of ast statements.

    Returns:
        The created list of ast statements.
    """
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


def modify_experiment_code(code: str, experiment_cls_name: str) -> str:
    """Modifies the given code to leave rtio logs and make the vcd file.

    It performs the following modifications:
        1. Add the rtio logs to track each line.
        2. Make the vcd file into the corresponding path.

    It assumes that the code contains an experiment class and run() method.

    Args:
        code: The experiment code.
        experiment_cls_name: The class name of the experiment.

    Returns:
        The modified experiment code.
    """
    mod = ast.parse(code)
    # import modules
    import_stmt = ast.parse("import subprocess").body
    mod.body = import_stmt + mod.body
    experiment_cls_stmt = next(
        stmt for stmt in mod.body
        if isinstance(stmt, ast.ClassDef) and stmt.name == experiment_cls_name
    )
    run_func_stmt = next(
        stmt for stmt in experiment_cls_stmt.body
        if isinstance(stmt, ast.FunctionDef) and stmt.name == "run"
    )
    # make logs to track each line
    run_func_stmt.body = add_tracking_line(run_func_stmt.body)
    # call write_vcd()
    write_vcd_call_stmt = ast.parse("self.write_vcd()").body
    run_func_stmt.body.append(write_vcd_call_stmt)
    # define write_vcd()
    device_db_path = posixpath.join(configs["master_path"], "device_db.py")
    vcd_path = posixpath.join(configs["master_path"], configs["result_path"], "{rid}/rtio.vcd")
    write_vcd_func_code = f"""
def write_vcd(self):
    result = subprocess.run("curl http://127.0.0.1:8000/experiment/running/",
                            capture_output=True, shell=True, text=True)
    rid = int(result.stdout)
    device_db_path = "{device_db_path}"
    vcd_path = f"{vcd_path}"
    subprocess.run(f"artiq_coreanalyzer --device-db {{device_db_path}} -w {{vcd_path}}",
                   capture_output=True, shell=True)
    """
    write_vcd_func_stmt = ast.parse(write_vcd_func_code).body
    experiment_cls_stmt.body.append(write_vcd_func_stmt)
    return ast.unparse(mod)


@app.get("/experiment/submit/")
async def submit_experiment(  # pylint: disable=too-many-arguments, too-many-locals
    file: str,
    args: str = "{}",
    pipeline: str = "main",
    priority: int = 0,
    timed: Optional[str] = None,
    visualize: bool = False,
    cls: str = ""
) -> int:
    """Submits the given experiment file.
    
    Args:
        file: The path of the experiment file.
        args: The arguments to submit which must be a JSON string of a dictionary.
          Each key is an argument name and its value is the value of the argument.
        pipeline: The pipeline to run the experiment in.
        priority: Higher value means sooner scheduling.
        timed: The due date for the experiment in ISO format.
          None for no due date.
        visualize: If True, the experiment file is modified for visualization.
          The original file and vcd file are saved in the visualize path set in config.json.
        cls: The class name of the experiment.
          It is only necessary when the visualize argument is True.
    
    Returns:
        The run identifier, an integer which is incremented at each experiment submission.
    """
    submission_time = datetime.now().isoformat()
    result_dir_path = posixpath.join(configs["master_path"], configs["result_path"])
    if visualize:
        src_experiment_path = posixpath.join(
            configs["master_path"], configs["repository_path"], file
        )
        modified_experiment_path = posixpath.join(
            result_dir_path,
            f"experiment_{submission_time}.py"
        )
        # modify the experiment code
        with open(src_experiment_path, encoding="utf-8") as experiment_file:
            code = experiment_file.read()
        modified_code = modify_experiment_code(code, cls)
        # save the modified experiment code
        with open(modified_experiment_path, "w", encoding="utf-8") as modified_experiment_file:
            modified_experiment_file.write(modified_code)
        submission_file_path = modified_experiment_path
    else:
        submission_file_path = posixpath.join(configs["repository_path"], file)
    args_dict = json.loads(args)
    expid = {
        "log_level": logging.WARNING,
        "class_name": None,
        "arguments": args_dict,
        "file": submission_file_path
    }
    due_date = None if timed is None else time.mktime(datetime.fromisoformat(timed).timetuple())
    remote = get_client("master_schedule")
    rid = remote.submit(pipeline, expid, priority, due_date, False)
    # make the RID directory
    rid_dir_path = posixpath.join(result_dir_path, f"_{rid}/")
    os.makedirs(rid_dir_path)
    # save the metadata
    metadata = {
        "submission_time": submission_time,
        "visualize": visualize
    }
    metadata_path = posixpath.join(rid_dir_path, "metadata.json")
    with open(metadata_path, "w", encoding="utf-8") as metadata_file:
        json.dump(metadata, metadata_file)
    if visualize:
        # copy the original experiment file
        dst_experiment_path = posixpath.join(rid_dir_path, "experiment.py")
        shutil.copyfile(src_experiment_path, dst_experiment_path)
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


# pylint: disable=too-many-locals
def organize_result_directory(result_dir_path: str, rid: str) -> bool:
    """Organizes the result directory.
    
    It performs the following:
        1. Read the metadata file from the RID directory.
        2. Move the modified experiment to the RID directory.
        3. Find the h5 result file and copy it to the RID directory.
        4. Dump the metadata on the copied result file.
        5. Remove the metadata file.

    Args:
        result_dir_path: The full path of the result directory.
        rid: The run identifier value of the experiment in string.

    Returns:
        True if the h5 result file exists.
        Otherwise, stop organizing the result directory and return False. 
    """
    rid_dir_path = posixpath.join(result_dir_path, f"_{rid}/")
    # read the metadata
    metadata_path = posixpath.join(rid_dir_path, "metadata.json")
    try:
        with open(metadata_path, encoding="utf-8") as metadata_file:
            metadata = json.load(metadata_file)
    except OSError:  # no chance of happening
        return False
    submission_time_str, visualize = metadata["submission_time"], metadata["visualize"]
    submission_time = datetime.fromisoformat(submission_time_str)
    date = submission_time.date().isoformat()
    hour = submission_time.hour
    # find the result file
    datetime_result_dir_path = posixpath.join(result_dir_path, f"{date}/{hour}/")
    padded_rid = rid.zfill(9)
    for item in os.listdir(datetime_result_dir_path):
        if item.startswith(padded_rid):
            break
    else:  # the experiment has not yet been run
        return False
    # copy the result file to the RID directory
    src_result_path = posixpath.join(datetime_result_dir_path, item)
    dst_result_path = posixpath.join(rid_dir_path, "result.h5")
    shutil.copyfile(src_result_path, dst_result_path)
    # modify the result file
    with h5py.File(dst_result_path, "a") as result_file:
        # start time
        start_time = result_file["start_time"][()]
        del result_file["start_time"]
        start_time_str = datetime.fromtimestamp(start_time).isoformat()
        start_time_dataset = result_file.create_dataset(
            "start_time", (1,), dtype=h5py.string_dtype(encoding="utf-8")
        )
        start_time_dataset[0] = start_time_str
        # run time
        run_time = result_file["run_time"][()]
        del result_file["run_time"]
        run_time_str = datetime.fromtimestamp(run_time).isoformat()
        run_time_dataset = result_file.create_dataset(
            "run_time", (1,), dtype=h5py.string_dtype(encoding="utf-8")
        )
        run_time_dataset[0] = run_time_str
        # submission time
        submission_time_dataset = result_file.create_dataset(
            "submission_time", (1,), dtype=h5py.string_dtype(encoding="utf-8")
        )
        submission_time_dataset[0] = submission_time_str
        # visualize option
        visualize_dataset = result_file.create_dataset("visualize", (1,), dtype="bool")
        visualize_dataset[0] = visualize
    # move the modified experiment file to the RID directory
    if visualize:
        src_experiment_path = posixpath.join(
            result_dir_path, f"experiment_{submission_time_str}.py"
        )
        dst_experiment_path = posixpath.join(rid_dir_path, "modified_experiment.py")
        shutil.move(src_experiment_path, dst_experiment_path)
    # remove the metadata file
    os.remove(metadata_path)
    return True


@app.get("/result/")
async def list_result_directory() -> List[int]:
    """Post-processes the executed experiments and returns the RID list.

    It performs the following:
        1. Organize the result directory.
        2. Return the RID list.

    Returns:
        A list with RIDs of the executed experiments, sorted in an ascending order.
    """
    result_dir_path = posixpath.join(configs["master_path"], configs["result_path"])
    # navigate to each RID directory not organized yet
    for item in os.listdir(result_dir_path):
        item_path = posixpath.join(result_dir_path, item)
        if posixpath.isdir(item_path) and item.startswith("_") and item[1:].isdigit():
            rid = item[1:]
            if organize_result_directory(result_dir_path, rid):
                rid_dir_path = posixpath.join(result_dir_path, f"{rid}/")
                os.rename(item_path, rid_dir_path)
    # find all organized RID directories
    rid_list = []
    for item in os.listdir(result_dir_path):
        item_path = posixpath.join(result_dir_path, item)
        if posixpath.isdir(item_path) and item.isdigit():
            rid_list.append(int(item))
    rid_list.sort()
    return rid_list


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
