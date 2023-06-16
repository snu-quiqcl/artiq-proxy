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
    remote = get_client("master_experiment_db")
    contents = remote.list_directory(os.path.join(settings["master_path"], directory))
    return contents


def get_client(target_name: str):
    return Client("::1", 3251, target_name)
