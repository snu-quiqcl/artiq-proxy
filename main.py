"""Proxy server to communicate a client to ARTIQ."""

import json
import os
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI
from sipyco import pc_rpc as rpc

configs = {}


def load_config_file():
    """Loads config information from the configuration file.

    The file should have the following JSON structure:

      {
        "master_path": {master_path}
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


@app.get("/ls/", response_model=List[str])
async def list_directory(directory: str = "") -> List[str]:
    """Get the list of elements in the given path and returns it.

    The "master_path" in the configuration file is used for the prefix of the path.

    Args:
        directory: The path of the directory to search for.
    """
    remote = get_client("master_experiment_db")
    return remote.list_directory(os.path.join(configs["master_path"], directory))


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
