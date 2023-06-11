"""
Constants and control flow for the project.
"""
from pydantic import BaseModel
from app.models import ModelGetter, get_multidense_model, get_convolution_model, get_lstm_model

from typing import Literal

class Config:
    SAMPLES_PER_DAY = 96
    MINUTES_PER_SAMPLE = 15
    CLIP_CEILING = 15
    CLIP_SOFTNESS = 0.2
    DAYS_PER_CYCLE = 7  # week
    FREQUENCIES = [ # in hours
        1,
        3,
        6,
        12,
        24,
        7 * 24,
    ]

class Models:
    MODELS: dict[str, ModelGetter] = {
        # "repeat": get_repeatbaseline_model,
        "dense": get_multidense_model,
        "cnn": get_convolution_model,
        "lstm": get_lstm_model,
    }

class Action(BaseModel):
    type: Literal["fit", "predict"]
    start: int
    end: int


class EventPaths(BaseModel):
    bucket: str
    metrics: str
    events: str
    models: str
    output: str


class Event(BaseModel):
    model: str
    action: Action
    paths: EventPaths
    steps: int = Config.SAMPLES_PER_DAY