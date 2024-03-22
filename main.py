# pylint: disable=too-many-lines
"""Proxy server to communicate a client to ARTIQ."""

import ast
import asyncio
import dataclasses
import enum
import glob
import importlib.util
import json
import logging
import os
import posixpath
import shutil
import time
from contextlib import asynccontextmanager
from datetime import datetime
from enum import Enum
from typing import Any, Optional, Union

import h5py
import numpy as np
import pydantic
import websockets
from artiq.coredevice.comm_moninj import CommMonInj, TTLOverride, TTLProbe
from fastapi import FastAPI, WebSocket
from fastapi.responses import FileResponse
from sipyco import pc_rpc as rpc
from sipyco.sync_struct import Subscriber

import dataset as dset
import schedule as schd
import tracker as trck
from protocols import SortedQueue

logger = logging.getLogger(__name__)

configs = {}
device_db = {}

dataset_tracker: Optional[dset.DatasetTracker] = None
schedule_tracker: Optional[schd.ScheduleTracker] = None

class MonInj:
    """Manages the connection to ARTIQ moninj proxy and TTL status.

    Variables:
        Modifications: Type of modifications.
          It is a dictionary whose key is one of MonitorType values. Each value is a dictionary
          whose key is a TTL channel number and value is the modified value.
        ModificationQueue: SortedQueue for modifications of TTL status.
        device_to_channel, channel_to_device:
          Maps a TTL device name with its channel number, and vice versa.
    
    Attributes:
        queue: SortedQueue with modified StatusType.
        values: Dictionary whose keys are StatusType and values are modified values.
        modified: Event set when any value is modified.
    """

    @enum.unique
    class MonitorType(enum.Enum):
        """Monitoring value type."""
        PROBE = "probe"
        LEVEL = "level"
        OVERRIDE = "override"

    @dataclasses.dataclass
    class StatusType:
        """Monitoring status type.
        
        Fields:
            channel: Monitoring TTL channel number.
            monitor_type: Monitoring value type.
        """
        channel: int
        monitor_type: "MonInj.MonitorType"

        def __hash__(self) -> int:
            """Overridden."""
            return hash(f"{str(self.channel)}_{self.monitor_type.value}")

    Modifications = dict[str, dict[str, int]]
    ModificationQueue = SortedQueue[float, "MonInj.StatusType"]
    device_to_channel: dict[str, int]
    channel_to_device: dict[int, str]

    def __init__(self):
        self.connection = CommMonInj(self.monitor_cb, self.injection_status_cb)
        self.queue = MonInj.ModificationQueue()
        self.values: dict[MonInj.StatusType, int] = {}
        self.modified = asyncio.Event()

    @classmethod
    def map_device_channel(cls):
        """Maps TTL devices and channels."""
        for device in configs["ttl_devices"]:
            channel = device_db[device]["arguments"]["channel"]
            cls.device_to_channel[device] = channel
            cls.channel_to_device[channel] = device

    async def connect(self):
        """Connects to ARTIQ moninj proxy."""
        await self.connection.connect(configs["core_addr"])
        for device in configs["ttl_devices"]:
            channel = MonInj.device_to_channel[device]
            self.connection.monitor_probe(1, channel, TTLProbe.level.value)
            self.connection.monitor_injection(1, channel, TTLOverride.level.value)
            self.connection.monitor_injection(1, channel, TTLOverride.en.value)

    def current_status(self, devices: list[str]) -> tuple[float, "MonInj.Modifications"]:
        """Returns the current timestamp and status.
        
        Args:
            devices: List of target TTL device names.
        
        Returns:
            See Modifications in the variables section for detailed structure of the status.
        """
        modifications = {ty.value: {} for ty in MonInj.MonitorType}
        for device in devices:
            channel = MonInj.device_to_channel[device]
            for ty in MonInj.MonitorType:
                modifications[ty.value][device] = self.values[MonInj.StatusType(channel, ty)]
        return time.time(), modifications

    def modifications_since(
        self, devices: list[str], timestamp: float
    ) -> tuple[float, "MonInj.Modifications"]:
        """Returns the latest timestamp and modifications since the given timestamp.
        
        Args:
            devices: List of target TTL device names.
            timestamp: Timestamp of the latest update.

        Returns:
            See Modifications in the variables section for detailed structure of the modifications.
        """
        modifications = {ty.value: {} for ty in MonInj.MonitorType}
        latest, modification_types = self.queue.tail(timestamp)
        filtered_modification_types = filter(
            lambda ty: MonInj.channel_to_device[ty.channel] in devices, set(modification_types)
        )
        for ty in filtered_modification_types:
            device = MonInj.channel_to_device[ty.channel]
            modifications[ty.monitor_type][device] = self.values[ty]
        return latest, modifications

    def _notify_modified(self):
        """Sets and clears the modified event for the queue."""
        self.modified.set()
        self.modified.clear()

    def monitor_cb(self, channel: int, _ty: int, value: int):
        """Callback function called when any monitoring value is modified.
        
        Args:
            channel: TTL channel number.
            _ty: Type of monitoring value. See artiq.coredevice.comm_moninj.TTLProbe.
              It monitors only "TTLProbe.level", hence this is not used.
            value: Modified monitoring value.
        """
        status_type = MonInj.StatusType(channel, MonInj.MonitorType.PROBE)
        self.values[status_type] = value
        self.queue.push(time.time(), status_type)
        self._notify_modified()

    def injection_status_cb(self, channel: int, ty: int, value: int):
        """Callback function called when any injection status is modified.
        
        Args:
            channel: TTL channel number.
            ty: Type of injection status. See artiq.coredevice.comm_moninj.TTLOverride.
            value: Modified injection status.
        """
        ty_to_monitor_type = {
            TTLOverride.level.value: MonInj.MonitorType.LEVEL,
            TTLOverride.en.value: MonInj.MonitorType.OVERRIDE
        }
        status_type = MonInj.StatusType(channel, ty_to_monitor_type[ty])
        self.values[status_type] = value
        self.queue.push(time.time(), status_type)
        self._notify_modified()


mi: MonInj


def load_configs():
    """Loads config information from the configuration file.

    The file should have the following JSON structure:

      {
        "master_path": {master_path},
        "repository_path": {repository_path},
        "result_path": {result_path},
        "device_db_path": {device_db_path},
        "core_addr": {core_ip},
        "master_addr": {artiq_master_ip},
        "nofity_port": {nofity_port},
        "ttl_devices": [{ttl_device0}, {ttl_device1}, ... ],
        "dac_devices": {
            {dac_device0}: [{dac_device0_channel0}, {dac_device0_channel1}, ... ],
            {dac_device1}: [{dac_device1_channel0}, {dac_device1_channel1}, ... ],
            ...
        },
        "dds_devices": {
            {dds_device0}: [{dds_device0_channel0}, {dds_device0_channel1}, ... ],
            {dds_device1}: [{dds_device1_channel0}, {dds_device1_channel1}, ... ],
            ...
        },
        "dataset_tracker": {
            "maxlen": {maxlen}
        }
      }
    """
    with open("config.json", encoding="utf-8") as config_file:
        configs.update(json.load(config_file))


def load_device_db():
    """Loads device DB from the device DB file."""
    device_db_full_path = posixpath.join(configs["master_path"], configs["device_db_path"])
    module_name = "device_db"
    spec = importlib.util.spec_from_file_location(module_name, device_db_full_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    device_db.update(module.device_db)


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
    await subscriber.connect(configs["master_addr"], configs["notify_port"])
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


async def init_moninj():
    """Initializes a MonInj object connecting to ARTIQ moninj proxy."""
    global mi  # pylint: disable=global-statement
    mi = MonInj()
    await mi.connect()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Lifespan events.

    This function is set as the lifespan of the application.
    """
    load_configs()
    load_device_db()
    _schedule_task = await init_schedule_tracker()
    _dataset_task = await init_dataset_tracker()
    await init_moninj()
    yield
    await mi.connection.close()


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
    arginfo: dict[str, Any]


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


def add_tracking_line(stmt_list: list[ast.stmt]) -> list[ast.stmt]:
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
    vcd_path = posixpath.join(configs["master_path"], configs["result_path"], "_{rid}/rtio.vcd")
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
    cls: Optional[str] = None,
    args: str = "{}",
    pipeline: str = "main",
    priority: int = 0,
    timed: Optional[str] = None,
    visualize: bool = False,
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
        visualize: If True, the experiment file is modified for visualization.
          The original file and vcd file are saved in the visualize path set in config.json.
          The cls argument should be given.
    
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
        "class_name": cls,
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
        logger.exception("The RID %s directory has no metadata file.", rid)
        return False
    submission_time_str, visualize = metadata["submission_time"], metadata["visualize"]
    submission_time = datetime.fromisoformat(submission_time_str)
    date = submission_time.date().isoformat()
    hour = str(submission_time.hour).zfill(2)
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
async def list_result_directory() -> list[int]:
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

    After accepted, it receives the target dataset name.
    Then, it sends the current dataset, parameters, and units immediately.
    Finally, it sends the dataset modificiation at least a second apart, every time it is modified.

    For details about dataset modificiation, see dataset.DatasetTracker.since().

    Args:
        websocket: The web socket object.
    """
    await websocket.accept()
    try:
        name = await websocket.receive_json()
        latest, dataset = dataset_tracker.get(name)
        await websocket.send_json(dataset)
        _, parameters = dataset_tracker.get(f"{name}.parameters")
        await websocket.send_json(parameters)
        _, units = dataset_tracker.get(f"{name}.units")
        await websocket.send_json(units)
        while True:
            await dataset_tracker.modified[name].wait()
            latest, modifications = dataset_tracker.since(name, latest)
            if latest < 0:  # dataset is overwritten or removed
                await websocket.send_json(None)
                break
            await websocket.send_json(modifications)
            await asyncio.sleep(1)
    except websockets.exceptions.ConnectionClosedError:
        logger.info("The connection for sending the dataset modification is closed.")
    except websockets.exceptions.WebSocketException:
        logger.exception("Failed to send the dataset modification.")


class ResultFileType(str, Enum):
    """Enum class for describing the result file type.
    
    H5: The H5 format result file, result.h5.
    CODE: The original experiment file, experiment.py.
    VCD: The VCD format file, rtio.vcd.
    """
    H5 = "h5"
    CODE = "code"
    VCD = "vcd"


@app.get("/result/{rid}/{result_file_type}/")
async def get_result(rid: str, result_file_type: ResultFileType) -> FileResponse:
    """Gets the result file of the given RID for the given result file type and returns it.
    
    Args:
        rid: The run identifier value in string.
        result_file_type: The type of the requested result file in ResultFileType.
    """
    if result_file_type is ResultFileType.H5:
        result_path = "result.h5"
    elif result_file_type is ResultFileType.CODE:
        result_path = "experiment.py"
    else:
        result_path = "rtio.vcd"
    full_result_path = posixpath.join(
        configs["master_path"], configs["result_path"], rid, result_path
    )
    return FileResponse(full_result_path)


class TTLControlInfo(pydantic.BaseModel):
    """TTL control information.
    
    Fields:
        devices, values: List of TTL device name in the device DB and value to be modified,
          repectively. The lengths of these lists should be identical. 
    """
    devices: list[str]
    values: list[bool]


@app.websocket("/ttl/status/modification/")
async def get_ttl_status_modification(websocket: WebSocket):
    """Sends the modifications of TTL status whenever it is modified.
    
    After accepted, it receives the target TTL list.
    Then, it sends the current TTL status immediately.
    Finally, it sends the modifications of TTL status everty time it is modified.

    See Modifications in the variables section of MonInj for modifications structure.

    Args:
        websocket: The web socket object.
    """
    await websocket.accept()
    try:
        devices = await websocket.receive_json()
        latest, status = mi.current_status(devices)
        await websocket.send_json(status)
        while True:
            latest, modifications = mi.modifications_since(devices, latest)
            if not sum(len(m) for m in modifications.values()):  # no modification
                await mi.modified.wait()
                continue
            await websocket.send_json(modifications)
            await asyncio.sleep(0.5)
    except websockets.exceptions.ConnectionClosedError:
        logger.info("The connection for sending the modifications of TTL status is closed.")
    except websockets.exceptions.WebSocketException:
        logger.exception("Failed to send the modifications of TTL status.")


@app.post("/ttl/level/")
async def set_ttl_level(control_info: TTLControlInfo):
    """Sets the overriding values of the given TTL channels.
    
    This only sets the value to be output when overridden, but does not turn on overriding.

    Args:
        control_info: Request body. See the fields section in TTLControlInfo.
    """
    for device, value in zip(control_info.devices, control_info.values):
        if device not in configs["ttl_devices"]:
            logger.error("The TTL device %s is not defined in config.json.", device)
            continue
        channel = MonInj.device_to_channel[device]
        mi.connection.inject(channel, TTLOverride.level.value, value)


@app.post("/ttl/override/")
async def set_ttl_override(control_info: TTLControlInfo):
    """Turns on or off overriding of the given TTL channels.

    Args:
        control_info: Request body. See the fields section in TTLControlInfo.
    """
    for device, value in zip(control_info.devices, control_info.values):
        if device not in configs["ttl_devices"]:
            logger.error("The TTL device %s is not defined in config.json.", device)
            continue
        channel = MonInj.device_to_channel[device]
        mi.connection.inject(channel, TTLOverride.en.value, value)


@app.post("/dac/voltage/")
async def set_dac_voltage(device: str, channel: int, value: float):
    """Sets the voltage of the given DAC channel.
    
    Args:
        device: The DAC device name described in device_db.py.
        channel: The DAC channel number. For Zotino, there are 32 channels, from 0 to 31.
        value: The voltage to set. For Zotino, the valid range is from -10V to +10V.
    """
    if device not in configs["dac_devices"] or channel not in configs["dac_devices"][device]:
        logger.error("The DAC device %s CH %d is not defined in config.json.", device, channel)
        return
    class_name = "SetDACVoltage"
    content = f"""
from artiq.experiment import *

class {class_name}(EnvExperiment):
    def build(self):
        self.setattr_device("core")
        self.dac = self.get_device("{device}")

    @kernel
    def run(self):
        self.core.reset()
        self.dac.init()
        delay(200*us)
        self.dac.set_dac([{value}], [{channel}])
"""
    expid = {
        "log_level": logging.WARNING,
        "content": content,
        "class_name": class_name,
        "arguments": {},
    }
    remote = get_client("master_schedule")
    rid = remote.submit("main", expid, 0, None, False)
    return rid


@app.post("/dds/profile/")
async def set_dds_profile(
    device: str,
    channel: int,
    frequency: float,
    amplitude: float,
    phase: float,
    switching: bool
):  # pylint: disable=too-many-arguments
    """Sets the default profile of the given DDS channel.
    
    Args:
        device: The DDS device name described in device_db.py.
        channel: The DDS channel number. For Urukul, there are 4 channels, from 0 to 3.
        frequency: The frequency to set. For Urukul, the valid range is from 1HHz to 400MHz.
        amplitude: The amplitude to set. For Urukul, the valid range is from 0 to 1.
        phase: The phase to set. For Urukul, the valid range is from 0 to 1.
        switching: If True, the current profile is switched to the default profile.
    """
    if device not in configs["dds_devices"] or channel not in configs["dds_devices"][device]:
        logger.error("The DDS device %s CH %d is not defined in config.json.", device, channel)
        return
    class_name = "SetDDSProfile"
    profile_switching_code = "self.dds.cpld.set_profile(7)"
    content = f"""
from artiq.experiment import *

class {class_name}(EnvExperiment):
    def build(self):
        self.setattr_device("core")
        self.dds = self.get_device("{device}_ch{channel}")

    @kernel
    def run(self):
        self.core.reset()
        self.dds.cpld.init()
        self.dds.init()
        self.dds.set(frequency={frequency}, amplitude={amplitude}, phase={phase})
        {profile_switching_code if switching else ""}
"""
    expid = {
        "log_level": logging.WARNING,
        "content": content,
        "class_name": class_name,
        "arguments": {},
    }
    remote = get_client("master_schedule")
    rid = remote.submit("main", expid, 0, None, False)
    return rid


@app.post("/dds/att/")
async def set_dds_attenuation(device: str, channel: int, value: float) -> int:
    """Sets the attenuation of the given DDS channel.

    Args:
        device: The DDS device name described in device_db.py.
        channel: The DDS channel number. For Urukul, there are 4 channels, from 0 to 3.
        value: The attenuation to set. For Urukul, the valid range is from 0dB to -31.5dB.
          The value is the absolute value of the actual attenuation, e.g., 10 for -10dB.

    Returns:
        The run identifier, an integer which is incremented at each experiment submission.
        If there is an error, it returns -1.
    """
    if device not in configs["dds_devices"] or channel not in configs["dds_devices"][device]:
        logger.error("The DDS device %s CH %d is not defined in config.json.", device, channel)
        return -1
    class_name = "SetDDSAttenuation"
    content = f"""
from artiq.experiment import *

class {class_name}(EnvExperiment):
    def build(self):
        self.setattr_device("core")
        self.dds = self.get_device("{device}_ch{channel}")

    @kernel
    def run(self):
        self.core.reset()
        self.dds.cpld.init()
        self.dds.init()
        self.dds.set_att({value})
"""
    expid = {
        "log_level": logging.WARNING,
        "content": content,
        "class_name": class_name,
        "arguments": {},
    }
    remote = get_client("master_schedule")
    rid = remote.submit("main", expid, 0, None, False)
    return rid


@app.post("/dds/switch/")
async def set_dds_switch(device: str, channel: int, on: bool) -> int:
    """Turns on and off the TTL switch, which controls the given DDS channel.
    
    Args:
        device: The DDS device name described in device_db.py.
        channel: The DDS channel number. For Urukul, there are 4 channels, from 0 to 3.
        on: If True, this turns on the TTL switch. Otherwise, this turns off it.

    Returns:
        The run identifier, an integer which is incremented at each experiment submission.
        If there is an error, it returns -1.
    """
    if device not in configs["dds_devices"] or channel not in configs["dds_devices"][device]:
        logger.error("The DDS device %s CH %d is not defined in config.json.", device, channel)
        return -1
    class_name = "SetDDSSwitch"
    if on:
        setting_switch_code = "self.dds.sw.on()"
    else:
        setting_switch_code = "self.dds.sw.off()"
    content = f"""
from artiq.experiment import *

class {class_name}(EnvExperiment):
    def build(self):
        self.setattr_device("core")
        self.dds = self.get_device("{device}_ch{channel}")

    @kernel
    def run(self):
        self.core.reset()
        self.dds.cpld.init()
        self.dds.init()
        {setting_switch_code}
"""
    expid = {
        "log_level": logging.WARNING,
        "content": content,
        "class_name": class_name,
        "arguments": {},
    }
    remote = get_client("master_schedule")
    rid = remote.submit("main", expid, 0, None, False)
    return rid


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
