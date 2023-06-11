import pandas as pd
import numpy as np

from app.preprocessing import (
    clip_data,
    add_time_features,
    generate_sliding_window,
    normalize_training_data,
)
from app.window import get_predict_window_dataset, WindowGenerator
from app.models import Model
from app.compile import compile_and_fit
from app.schema import Event, Models, Config
from app.storage import Store


class ModelFlow:
    """Handle load, preprocessing, training and predict logic"""

    MODELS = Models.MODELS
    
    def __init__(self, event: dict):
        """Initialize model variables and read data files based on date offset start and end."""
        e = Event(**event)
        store = Store(models_path=e.paths.models, metrics_path=e.paths.metrics)
        # get metrics data
        metrics = store.load_local_files(path=e.paths.metrics, start=e.action.start, end=e.action.end)
        metrics = sorted(metrics, key=lambda x: x["path"])
        # get events data
        events = store.load_local_files(path=e.paths.events, start=e.action.start, end=e.action.end)
        events = sorted(events, key=lambda x: x["path"])
        # steps
        self.steps = e.steps
        self.model_name = e.model
        self.is_predict = e.action.type == "predict"

    def process_metrics(self, data: list[dict]):
        """Compute dataframe from data"""
        df = pd.concat([pd.DataFrame(d) for d in data])
        df = clip_data(df)
        return add_time_features(df)
    
    def process_events(self, data: list[dict]):
        index_df = pd.DataFrame({"index": np.arange(0, Config.SAMPLES_PER_DAY, 1)})
        # concatenate same-length data using index_df to fill empty events
        df = pd.concat([index_df.join(pd.DataFrame(d).set_index('index'), on="index") if len(d.keys()) > 0 else index_df for d in data])
        df = df.fillna(0).drop(columns=['index']).reset_index(drop=True)
        # drop all columns that have only zeroes
        df = df.loc[:, (df != 0).any(axis=0)]
        # convert to 'how long ago' the event happened in days
        df = (df / (60 * 60 * 24))
        # filter
        # success = [c for c in df if c.split("_")[0] == "success"]
        errors = [c for c in df if c.split("_")[0] == "error"]
        warnings = [c for c in df if c.split("_")[0] == "warning"]
        return df[[*errors, *warnings]]

    def combine_data(self, metrics: pd.DataFrame, events: pd.DataFrame):
        """Combine metrics and events data"""
        df = metrics.join(events)
        df_frame = df.iloc[0:0]
        return add_time_features(df)


    def fit(self, df: pd.DataFrame):
        """Fit the model."""
        perf = []
        model = self.MODELS[self.model_name](df.shape[1], self.steps, self.model_name)
        print("Fitting model...")
        for train, val, test in generate_sliding_window(population_size=df.shape[0]):
            train_df, val_df, test_df, std, mean = normalize_training_data(
                df.iloc[train], df.iloc[val], df.iloc[test]
            )
            window = WindowGenerator(
                train_df,
                val_df,
                test_df,
                input_width=self.steps,
                label_width=self.steps,
                shift=self.steps,
            )
            compile_and_fit(model, window, patience=4, max_epocs=40)
            perf.append(
                {
                    "val": model.evaluate(window.val, verbose=0)[1],
                    "test": model.evaluate(window.test, verbose=0)[1],
                }
            )
        return model, std, mean, pd.DataFrame(perf)

    def predict(
        self, df: pd.DataFrame, model: Model, std: np.ndarray, mean: np.ndarray
    ):
        """Predict using the model."""
        y_pred = model.predict(get_predict_window_dataset((df - mean) / std))
        Y = y_pred[0, :, 0:6] * std[0:6].values + mean[0:6].values
        Y = clip_data(Y)
        return pd.DataFrame(Y, columns=df.columns[0:6])
