# pylint: disable=too-many-lines
"""Proxy server to communicate a client to ARTIQ."""

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
from typing import Any, Optional, Union

import h5py
import numpy as np
import pydantic
import websockets
from artiq.coredevice.comm_moninj import TTLOverride
from fastapi import FastAPI, WebSocket, HTTPException, Body
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

import threading
system_mode = "user"
system_mode_lock = threading.Lock()

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
    with open(setting.config_path, encoding="utf-8") as config_file:
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


async def init_ttl_manager():
    """Initializes the TTL manager connecting to ARTIQ moninj proxy.
    
    This should be called after loading config.
    """
    global ttl_device_channel_mapping, ttl_manager  # pylint: disable=global-statement
    ttl_device_channel_mapping = ttl.DeviceChannelMapping(configs["ttl_devices"], device_db)
    ttl_manager = ttl.TTLManager(ttl_device_channel_mapping)
    await ttl_manager.connect(configs["core_addr"], configs["ttl_devices"])


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Lifespan events.

    This function is set as the lifespan of the application.
    """
    load_configs()
    load_device_db()
    _schedule_task = await init_schedule_tracker()
    _dataset_task = await init_dataset_tracker()
    await init_ttl_manager()
    yield
    await ttl_manager.connection.close()


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
    file: Optional[str] = None,
    raw_cpp: Optional[str] = None,
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

    if (file is None) == (raw_cpp is None):
        raise HTTPException(
            status_code=400,
            detail="Specify exactly one of 'file' or 'raw_cpp'."
        )
    
    if file is not None:
        # Experiment file path submission via IQUIP
        submission_file_path = posixpath.join(configs["repository_path"], file)
        args_dict = json.loads(args)
        expid = {
            "log_level": logging.WARNING,
            "class_name": cls,
            "arguments": args_dict,
            "file": submission_file_path
        }
        
    else:
        # Raw cpp code submission via the control server
        expid = {
            "log_level": logging.WARNING,
            "raw_code": raw_cpp,
            "class_name": cls,
            "arguments": None
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
        latest, status = ttl_manager.current_status(devices)
        await websocket.send_json(status)
        while True:
            latest, modifications = ttl_manager.modifications_since(devices, latest)
            if not any(modifications.values()):  # no modification
                await ttl_manager.modified.wait()
                continue
            await websocket.send_json(modifications)
            await asyncio.sleep(0.5)
    except websockets.exceptions.ConnectionClosedError:
        logger.info("The connection for sending the modifications of TTL status is closed.")
    except websockets.exceptions.WebSocketException:
        logger.exception("Failed to send the modifications of TTL status.")


class TTLControlInfo(pydantic.BaseModel):
    """TTL control information.
    
    Fields:
        devices, values: List of TTL device name in the device DB and value to be modified,
          repectively. The lengths of these lists should be identical. 
    """
    devices: list[str]
    values: list[bool]


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
        channel = ttl_device_channel_mapping.channel(device)
        ttl_manager.connection.inject(channel, TTLOverride.level.value, value)


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
        channel = ttl_device_channel_mapping.channel(device)
        ttl_manager.connection.inject(channel, TTLOverride.en.value, value)


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



########################################################################################
# APIs for the control server to monitor the experiment status
########################################################################################

def is_experiment_complete(rid: int) -> bool:
    """Checks if an experiment with given RID is complete.
    
    Args:
        rid: The run identifier value of the experiment.
    
    Returns:
        True if the experiment is complete (finished, error, or cancelled), False otherwise.
    """
    remote = get_client("master_schedule")
    status = remote.get_status()
    
    # If RID not in status, it means the experiment is complete
    if rid not in status:
        return True
        
    # Get experiment status
    exp_status = status[rid].get("status", None)
    
    # Status that indicate the experiment is still running
    running_states = [
    # TODO: Check for the status tracking of the experiment
        "pending",      # Waiting to start
        "preparing",    # Setting up
        "prepare_done", # Ready to run
        "running",      # Currently running
        "paused"       # Temporarily paused
    ]
    
    return exp_status not in running_states


@app.websocket("/experiment/watch/{rid}")
async def watch_experiment(websocket: WebSocket, rid: int):
    """Watch experiment until completion.
    
    Maintains WebSocket connection while experiment is running.
    Closes connection when experiment completes.
    Client should then use the existing /dataset/rid/ endpoint
    to retrieve the data.
    
    Args:
        websocket: The WebSocket connection
        rid: Run identifier of the experiment
    """
    await websocket.accept()
    try:
        # Keep connection open while experiment is running
        while not is_experiment_complete(rid):
            await asyncio.sleep(0.1)
        
        # Get the list of available datasets for this RID
        dataset_list = await list_dataset_from_rid(rid)
        
        # Send completion message with available datasets
        await websocket.send_json({
            "status": "complete",
            "datasets": dataset_list
        })
        await websocket.close()
        
    except WebSocketDisconnect:
        logger.info(f"Client disconnected from experiment watcher for RID: {rid}")
    except Exception as e:
        logger.exception(f"Error in experiment watcher for RID: {rid}")
        await websocket.close()


########################################################################################
# APIs for getting and setting system mode between 'user' and 'experiment'
########################################################################################

@app.get("/system_mode/")
async def get_system_mode():
    """Get the current system mode ('user' or 'experiment')."""
    with system_mode_lock:
        return {"system_mode": system_mode}


@app.post("/system_mode/")
async def set_system_mode(mode: str = Body(..., embed=True)):
    """Set the system mode to 'user' or 'experiment'."""
    if mode not in ("user", "experiment"):
        return {"error": "Invalid mode. Must be 'user' or 'experiment'."}
    with system_mode_lock:
        global system_mode
        system_mode = mode
    return {"system_mode": system_mode}