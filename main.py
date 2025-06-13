# pylint: disable=too-many-lines
"""Proxy server to communicate a client to ARTIQ."""

# pylint: disable=unused-import, import-error
import asyncio
import glob
import importlib.util
import json
import logging
import os
import posixpath
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Optional, Union, Literal

import h5py
import numpy as np
import pydantic
import websockets
from artiq.coredevice.comm_moninj import TTLOverride
from fastapi import FastAPI, WebSocket
from pydantic_settings import BaseSettings
from sipyco import pc_rpc as rpc
from sipyco.sync_struct import Subscriber

import dataset as dset
import schedule as schd
import tracker as trck
import ttl

logger = logging.getLogger(__name__)

configs = {}
device_db = {}
ttl_device_channel_mapping: ttl.DeviceChannelMapping

dataset_tracker: Optional[dset.DatasetTracker] = None
schedule_tracker: Optional[schd.ScheduleTracker] = None
ttl_manager: Optional[ttl.TTLManager] = None

class Setting(BaseSettings):  # pylint: disable=too-few-public-methods
    """Setting to specify the target config file path."""
    config_path: str = "config.json"


setting = Setting()


def load_configs():
    """Loads config information from the configuration file.

    The file should have the following JSON structure:

      {
        "master_path": {master_path},
        "repository_path": {repository_path},
        "result_path": {result_path},
        "core_addr": {core_ip},
        "master_addr": {artiq_master_ip},
        "nofity_port": {nofity_port},
        "dataset_tracker": {
            "maxlen": {maxlen}
        }
      }
    """
    with open(setting.config_path, encoding="utf-8") as config_file:
        configs.update(json.load(config_file))


async def run_subscriber(subscriber: Subscriber):
    """Runs the subscriber's receiving task and closes it finally.
    
    Args:
        subscriber: Target subscriber.
    """
    try:
        await subscriber.receive_task
    finally:
        await subscriber.close()


async def create_subscriber_task(notifier_name: str, tracker: trck.Tracker) -> asyncio.Task:
    """Creates a subscriber task and returns it.
    
    Args:
        notifier_name, tracker.target_builder, and tracker.notify_callback are passed to
        sipyco.sync_struct.Subscriber.__init__().
    """
    subscriber = Subscriber(notifier_name, tracker.target_builder, tracker.notify_callback)
    host, port = configs["master_addr"].split(":")
    await subscriber.connect(host, int(port))
    return asyncio.create_task(run_subscriber(subscriber))


async def init_schedule_tracker() -> asyncio.Task:
    """Initializes the schedule tracker and runs the subscriber.
    
    This should be called after loading config.
    """
    global schedule_tracker  # pylint: disable=global-statement
    schedule_tracker = schd.ScheduleTracker()
    return await create_subscriber_task("schedule", schedule_tracker)


async def init_dataset_tracker() -> asyncio.Task:
    """Initializes the dataset tracker and runs the subscriber.
    
    This should be called after loading config.
    """
    global dataset_tracker  # pylint: disable=global-statement
    maxlen = configs["dataset_tracker"].get("maxlen", 1 << 16)
    dataset_tracker = dset.DatasetTracker(maxlen)
    return await create_subscriber_task("datasets", dataset_tracker)

@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Lifespan events.

    This function is set as the lifespan of the application.
    """
    load_configs()
    _schedule_task = await init_schedule_tracker()
    _dataset_task = await init_dataset_tracker()
    
    yield
    if configs["control_system"] == "lolenc":
        await ttl_manager.connection.close()
    elif configs["control_system"] == "artiq":
        await ttl_manager.connection.close()
    elif configs["control_system"] == "qumare":
        pass
    else:
        logging.critical("Control system is not defined.")


app = FastAPI(lifespan=lifespan)


@app.get("/ls/")
async def list_directory(directory: str = "") -> list[str]:
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
    return_list =  sorted(
        item_list,
        key=lambda item: (not item.endswith("/"), item)
    )
    return return_list

@app.get("/ls_config/")
async def list_config_directory(directory: str = "") -> list[str]:
    """Gets the list of elements in the given path and returns it.

    The "master_path" and "repository_path" in the configuration file 
    is used for the prefix of the path.

    Args:
        directory: The path of the directory to search for.

    Returns:
        A list with items in the given directory.
        It lists directories before files, sorted in an alphabetical order.
    """
    remote = get_client("master_schedule")
    current_config_file = remote.get_configuration()
    config_dir = os.path.dirname(current_config_file)

    remote = get_client("master_experiment_db")
    item_list = remote.list_directory(config_dir)
    return_list =  sorted(
        item_list,
        key=lambda item: (not item.endswith("/"), item)
    )
    return return_list

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
    arginfo: dict[str, Any]

class ConfigurationInfo(pydantic.BaseModel):
    """Configuartion Information for QuMaRE system."""
    common_path: str
    ip: str
    port: str
    exp_file_path: str
    log_path: str


@app.get("/experiment/info/", response_model=dict[str, ExperimentInfo])
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

@app.get("/configuration/info/", response_model=dict[str, ConfigurationInfo])
async def get_configuration_info(file: str) -> Any:
    """Gets configuration of current lolenc system
    
    Args:
        file: The path of the experiment file.

    Returns:
        A dictionary containing only one element of which key is the experiment class name.
        The value is an ExperimentInfo object.
    """
    remote = get_client("master_schedule")
    current_config_file = remote.get_configuration()
    config_dir = os.path.dirname(current_config_file)
    config_name = os.path.basename(current_config_file)
    with open(os.path.join(config_dir, file), "r", encoding="utf-8") as file:
        try:
            json_data = json.load(file)
            return {config_name: json_data}
        except json.JSONDecodeError:
            logger.error("JSON Decode Error")
            return {}
@app.get("/configuration/submit/")
async def submit_configuration(  # pylint: disable=too-many-arguments
    file: str,
    cls: Optional[str] = None,
    args: str = "{}",
) -> None:
    """Submits the given experiment file.
    
    Args:
        file: The path of the experiment file.
        cls: The class name of the experiment to be submitted.
        args: The arguments to submit which must be a JSON string of a dictionary.
          Each key is an argument name and its value is the value of the argument.
        pipeline: The pipeline to run the experiment in.
        priority: Higher value means sooner scheduling.
        timed: The due date for the experiment in ISO format.
          None for no due date.
    
    Returns:
        The run identifier, an integer which is incremented at each experiment submission.
    """
    args_dict = json.loads(args)
    remote = get_client("master_schedule")
    current_config_file = remote.get_configuration()
    config_dir = os.path.dirname(current_config_file)
    file_path = os.path.join(config_dir,file)
    print(os.path.join(config_dir,file))
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(args_dict, file, ensure_ascii=False, indent=4)
    remote.set_configuration(file_path)

@app.websocket("/schedule/")
async def get_schedule(websocket: WebSocket):
    """Sends the schedule whenever it is modified.

    After accepted, it sends the current schedule immediately.
    Then, it sends the schedule every time it is modified.

    Args:
        websocket: The web socket object.
    """
    await websocket.accept()
    try:
        schedule = schedule_tracker.get()
        await websocket.send_json(schedule)
        while True:
            await schedule_tracker.modifed.wait()
            schedule = schedule_tracker.get()
            await websocket.send_json(schedule)
    except websockets.exceptions.ConnectionClosedError:
        logger.info("The connection for sending the schedule is closed.")
    except websockets.exceptions.WebSocketException:
        logger.exception("Failed to send the schedule.")


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


@app.get("/experiment/submit/")
async def submit_experiment(  # pylint: disable=too-many-arguments
    file: str,
    cls: Optional[str] = None,
    args: str = "{}",
    pipeline: str = "main",
    priority: int = 0,
    timed: Optional[str] = None,
) -> int:
    """Submits the given experiment file.
    
    Args:
        file: The path of the experiment file.
        cls: The class name of the experiment to be submitted.
        args: The arguments to submit which must be a JSON string of a dictionary.
          Each key is an argument name and its value is the value of the argument.
        pipeline: The pipeline to run the experiment in.
        priority: Higher value means sooner scheduling.
        timed: The due date for the experiment in ISO format.
          None for no due date.
    
    Returns:
        The run identifier, an integer which is incremented at each experiment submission.
    """
    submission_file_path = posixpath.join(configs["repository_path"], file)
    args_dict = json.loads(args)
    expid = {
        "log_level": logging.WARNING,
        "class_name": cls,
        "arguments": args_dict,
        "file": submission_file_path
    }
    due_date = None if timed is None else time.mktime(datetime.fromisoformat(timed).timetuple())
    remote = get_client("master_schedule")
    rid = remote.submit(pipeline, expid, priority, due_date, False)
    return rid


@app.get("/experiment/status/")
async def get_status(rid: int) -> Optional[dict]:
    """Gets the current status of the given RID.
    
    Args:
        rid: The run identifier value of the experiment.
    
    Returns:
        A status dictionary with "pipeline", "expid", "priority", "due_date", "status", etc.
        If the experiment is done or cancelled, it returns None.
        For details, see notification in artiq.master.scheduler.Run.__init__().
    """
    remote = get_client("master_schedule")
    status = remote.get_status()
    return status.get(rid, None)


@app.get("/rid/list/")
async def list_rid_from_date_hour(date: str, hour: Optional[int] = None) -> list[int]:
    """Returns the list of RIDs corresponding the given date and hour.
    
    Args:
        date: Target date with the format "yyyy-mm-dd".
        hour: Target hour. If None, it searches for all hours.
    """
    result_dir_path = posixpath.join(configs["master_path"], configs["result_path"])
    result_file_path = posixpath.join(result_dir_path, date,
                                      "*" if hour is None else str(hour), "*.h5")
    result_file_list = glob.glob(result_file_path)
    rid_list = sorted([int(os.path.basename(result_file)[:9]) for result_file in result_file_list])
    return rid_list


def get_result_file_from_rid(rid: int) -> Optional[str]:
    """Returns the result file corresponding to the given RID.
    
    Args:
        rid: Target run identifier value.

    Returns:
        If the result file does not exist, it returns None.
    """
    result_dir_path = posixpath.join(configs["master_path"], configs["result_path"])
    result_file_path = posixpath.join(result_dir_path, "*", "*", f"{str(rid).zfill(9)}*.h5")
    result_file_list = glob.glob(result_file_path)
    if len(result_file_list) != 1:
        return None
    return result_file_list[0]


@app.get("/dataset/rid/")
async def get_rid_dataset(rid: int, key: str) -> Optional[Union[int, float, list]]:
    """Returns the dataset in the result file of the given RID.
    
    Args:
        rid: Target run identifier value.
        key: Target dataset key.
    
    Returns:
        If the dataset does not exist, it returns None.
    """
    result_file = get_result_file_from_rid(rid)
    if result_file is None:
        return None
    with h5py.File(result_file, "r") as result_file:
        if key not in result_file["datasets"].keys():
            return None
        data = result_file["datasets"][key][()]
        if isinstance(data, np.ndarray):
            data = data.tolist()
        return data


@app.get("/dataset/master/")
async def get_master_dataset(key: str) -> Union[int, float, list, tuple]:
    """Returns the dataset broadcast to artiq master.

    Args:
        key: The key of the target dataset.

    Returns:
        If the dataset is not initialized or does not exist, it returns an empty tuple.
    """
    _, data = dataset_tracker.get(key)
    if isinstance(data, np.ndarray):
        data = data.tolist()
    return data


@app.get("/dataset/rid/list/")
async def list_dataset_from_rid(rid: int) -> list[str]:
    """Returns the list of dataset names in the result file of the given RID.
    
    Args:
        rid: Target run identifier value.
    """
    result_file = get_result_file_from_rid(rid)
    if result_file is None:
        return []
    with h5py.File(result_file, "r") as result_file:
        return sorted(list(result_file["datasets"].keys()))


@app.websocket("/dataset/master/list/")
async def list_dataset(websocket: WebSocket):
    """Sends the list of datasets available in artiq master whenever it is modified.
    
    After accepted, it sends the current dataset list immediately.
    Then, it sends the dataset list every time it is modified.

    Args:
        websocket: The web socket object.
    """
    await websocket.accept()
    try:
        await websocket.send_json(dataset_tracker.datasets())
        while True:
            await dataset_tracker.list_modified.wait()
            await websocket.send_json(dataset_tracker.datasets())
    except websockets.exceptions.ConnectionClosedError:
        logger.info("The connection for sending the dataset list is closed.")
    except websockets.exceptions.WebSocketException:
        logger.exception("Failed to send the dataset list.")


@app.websocket("/dataset/master/modification/")
async def get_dataset_modification(websocket: WebSocket):
    """Sends the specific dataset modification whenever it is modified.

    After accepted, it receives the target dataset name and the period fetching the dataset.
    Then, it sends the current dataset, parameters, and units immediately.
    Finally, it sends the dataset modificiation at least a second apart, every time it is modified.

    For details about dataset modificiation, see dataset.DatasetTracker.since().

    Args:
        websocket: The web socket object.
    """
    await websocket.accept()
    try:
        info = await websocket.receive_json()
        name, period = tuple(map(info.get, ("name", "period")))
        latest, dataset = dataset_tracker.get(name)
        await websocket.send_json(dataset)
        _, parameters = dataset_tracker.get(f"{name}.parameters")
        await websocket.send_json(parameters)
        _, units = dataset_tracker.get(f"{name}.units")
        await websocket.send_json(units)
        while True:
            latest, modifications = dataset_tracker.since(name, latest)
            if latest < 0:  # dataset is overwritten or removed
                await websocket.send_json(None)
                break
            if not modifications:  # no modification:
                await dataset_tracker.modified[name].wait()
                continue
            await websocket.send_json(modifications)
            await asyncio.sleep(period)
    except websockets.exceptions.ConnectionClosedError:
        logger.info("The connection for sending the dataset modification is closed.")
    except websockets.exceptions.WebSocketException:
        logger.exception("Failed to send the dataset modification.")


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
