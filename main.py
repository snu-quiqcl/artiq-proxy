"""Proxy server to communicate a client to ARTIQ."""

import json
import os
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, List

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
    """Get the list of elements in the given path and returns it.

    The "master_path" in the configuration file is used for the prefix of the path.

    Args:
        directory: The path of the directory to search for.
    """
    remote = get_client("master_experiment_db")
    return remote.list_directory(os.path.join(configs["master_path"], directory))


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
    """Get information of the given experiment file and returns it.
    
    Args:
        file: The path of the experiment file.

    Returns:
        A dictionary containing only one element of which key is the experiment class name.
        The value is an ExperimentInfo object.
    """
    remote = get_client("master_experiment_db")
    return remote.examine(file)


@app.get("/experiment/submit/")
async def submit_experiment(file: str, args: str = "{}") -> int:
    """Submit the given experiment file."""
    expid = {
        "log_level": logging.WARNING,
        "class_name": None,
        "arguments": json.loads(args),
        "file": os.path.join(configs["repository_path"], file)
    }
    remote = get_client("master_schedule")
    return remote.submit("main", expid, 0, None, False)


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
