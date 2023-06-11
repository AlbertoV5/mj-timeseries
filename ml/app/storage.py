from datetime import datetime, timedelta
from typing import Generator, TypedDict
from pathlib import Path
import pickle as pkl
import numpy as np
import boto3
import json

from app.models import Model, load_model


class FileData(TypedDict):
    path: str
    data: dict


class Store:
    def __init__(
        self, *, models_path: str, metrics_path: str, events_path: str, s3_bucket: str = None
    ) -> None:
        if s3_bucket is not None:
            self.client = boto3.client("s3")
        self.bucket = s3_bucket
        self.models_path = Path(models_path)
        self.metrics_path = Path(metrics_path)
        self.events_path = Path(events_path)

    def compute_time(self, start: int, end: int) -> tuple[datetime, datetime]:
        """Compute start and end time"""
        t0 = datetime.now() - timedelta(days=start + 1)
        t1 = datetime.now() - timedelta(days=end)
        return [t0, t1]

    def store_local_models(self, model: Model, std: np.ndarray, mean: np.ndarray):
        """Store model locally"""
        if not self.models_path.is_dir():
            self.models_path.mkdir(parents=True)
        # store
        model.save(self.models_path / f"{model.name}.h5")
        # store mean as pickle file
        with open(self.models_path / f"{model.name}_mean.pkl", "wb") as f:
            pkl.dump(mean, f)
        # store std as pickle file
        with open(self.models_path / f"{model.name}_std.pkl", "wb") as f:
            pkl.dump(std, f)

    def load_local_models(self, model_name: str):
        """Load models from local storage"""
        with open(self.models_path / f"{model_name}_mean.pkl", "rb") as f:
            mean = pkl.load(f)
        # load std from pickle file
        with open(self.models_path / f"{model_name}_std.pkl", "rb") as f:
            std = pkl.load(f)
        # load model
        model = load_model(self.models_path / f"{model_name}.h5")
        return model, std, mean

    def load_local_files(self, path: Path, start: int, end: int) -> Generator[FileData, None, None]:
        """Compute date and load local files"""
        if not path.is_dir():
            raise ValueError("Path does not exist")
        # glob and filter by date
        t0, t1 = self.compute_time(start, end)
        for file in path.glob("*.json"):
            date = datetime.strptime(file.stem.split("_")[0], "%Y-%m-%d")
            if date >= t0 and date < t1:
                with open(file, "rb") as f:
                    yield {"path": str(file), "data": json.load(f)}

    def download_s3_files(
        self, start: int, end: int
    ) -> Generator[FileData, None, None]:
        """Locate files in s3 bucket based on date start and end."""
        try:
            response = self.client.list_objects_v2(
                Bucket=self.bucket, Prefix=str(self.metrics_path)
            )
        except BaseException as e:
            raise ValueError(e)
        # go over directory and filter by date
        t0, t1 = self.compute_time(start, end)
        for file in response["Contents"]:
            date = datetime.strptime(Path(file["Key"]).stem.split("_")[0], "%Y-%m-%d")
            if date >= t0 and date < t1:
                data = self.client.get_object(Bucket=self.bucket, Key=file["Key"])
                yield {"path": file["Key"], "data": json.load(data["Body"])}
