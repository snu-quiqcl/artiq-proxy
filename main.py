from contextlib import asynccontextmanager
import json
import os

from fastapi import FastAPI
from sipyco.pc_rpc import Client


settings = {}


def load_setup_file():
    """Loads set-up information from the setup file.
    
    The file should have the following JSON structure:

      {
        "master_path": {master_path}
      }
    """
    global settings
    with open("setup.json", encoding="utf-8") as setup_file:
        settings = json.load(setup_file)


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_setup_file()
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/ls/")
async def list_directory(directory: str = ""):
    """Get the list of elements in the given path.
    
    The 'master_path' is used for the prefix of the path.
    """
    remote = get_client("master_experiment_db")
    contents = remote.list_directory(os.path.join(settings["master_path"], directory))
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
