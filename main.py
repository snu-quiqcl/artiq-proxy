"""Proxy server to communicate a client to ARTIQ."""

import json
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sipyco.pc_rpc import Client

configs = {}


def load_config_file():
    """Loads configuration information from the config file.

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


@app.get("/ls/")
async def list_directory(directory: str = ""):
    """Get the list of elements in the given path.

    The 'master_path' is used for the prefix of the path.
    """
    remote = get_client("master_experiment_db")
    contents = remote.list_directory(os.path.join(configs["master_path"], directory))
    return contents


def get_client(target_name: str) -> Client:
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
    return Client("::1", 3251, target_name)
