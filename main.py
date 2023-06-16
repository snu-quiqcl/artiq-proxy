from contextlib import asynccontextmanager
import json

from fastapi import FastAPI


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


@app.get("/")
async def root():
    return {"message": "Hello World"}
