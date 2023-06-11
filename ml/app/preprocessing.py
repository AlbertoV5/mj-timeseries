import tensorflow_probability as tfp
import pandas as pd
import numpy as np

from app.schema import Config


def add_time_features(df: pd.DataFrame, frequencies: list = None):
    """Add time features to dataframe"""
    time_cycle = df.reset_index().index // (
        Config.SAMPLES_PER_DAY * Config.DAYS_PER_CYCLE
    )
    time_minutes = df.reset_index().index * Config.MINUTES_PER_SAMPLE
    time_minutes_cycle = time_minutes - (time_cycle * Config.DAYS_PER_CYCLE * 24 * 60)
    time_seconds = time_minutes_cycle * 60
    # add
    frequencies = Config.FREQUENCIES if frequencies is None else frequencies
    for hours in frequencies:
        seconds = hours * 3600
        df[f"{hours}_hours_sin"] = np.sin(time_seconds * (2 * np.pi / seconds))
        df[f"{hours}_hours_cos"] = np.cos(time_seconds * (2 * np.pi / seconds))
    return df


def clip_data(
    data: np.ndarray | pd.DataFrame, threshold: int = Config.CLIP_CEILING, softness: float = Config.CLIP_SOFTNESS
):
    """Soft clipping of data"""
    clipped = (
        tfp.bijectors.SoftClip(low=None, high=threshold, hinge_softness=softness)
        .forward(np.clip(data, 0, None))
        .numpy()
    )
    return (
        clipped
        if isinstance(data, np.ndarray)
        else pd.DataFrame(clipped, columns=data.columns)
    )


def split_data(df: pd.DataFrame):
    """Split data into train, validation and test sets"""
    n = len(df)
    train_df = df[0:-384]
    val_df = df[-384:-192]
    test_df = df[-192:]
    return train_df, val_df, test_df


def normalize_training_data(train_df, val_df, test_df):
    """Normalize data by subtracting mean and dividing by standard deviation"""
    mean = train_df.mean()
    std = train_df.std().replace(0, 1)
    train_df = (train_df - mean) / std
    val_df = (val_df - mean) / std
    test_df = (test_df - mean) / std

    return train_df, val_df, test_df, std, mean


def generate_sliding_window(
    population_size: int, sample_size=96, cycles=3, step_size=2
):
    X = np.tile(np.arange(0, population_size, 1), 2)  # circular?
    test_size = val_size = sample_size
    train_size = population_size - ((test_size + val_size) * cycles)
    for step in range(0, cycles):
        train_left = sample_size * step
        train_right = train_left + (train_size)
        val_right = train_right + (val_size * step_size)
        test_right = val_right + (test_size * step_size)
        yield X[train_left:train_right], X[train_right:val_right], X[
            val_right:test_right
        ]
