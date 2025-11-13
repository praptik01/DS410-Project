from typing import List

from fastapi import FastAPI
from pydantic import BaseModel

from spotify_playlist_generator import generate_playlist_payload, _initialize_model


class PredictRequest(BaseModel):
    track_ids: List[str]
    limit: int = 10


app = FastAPI()


@app.on_event("startup")
def on_startup():
    _initialize_model()


@app.post("/predict")
def predict(req: PredictRequest):
    payload = generate_playlist_payload(req.track_ids, req.limit)
    return payload
