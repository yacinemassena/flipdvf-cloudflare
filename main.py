from fastapi import FastAPI
from fastapi.responses import FileResponse
import os

app = FastAPI()

@app.get("/")
def read_root():
    # Serve index.html from the same directory
    return FileResponse(os.path.join(os.path.dirname(__file__), "index.html"))
