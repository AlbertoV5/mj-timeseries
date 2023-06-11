import tensorflow as tf
from typing import TypeAlias, Callable


Model: TypeAlias = tf.keras.Sequential
ModelGetter: TypeAlias = Callable[[int, int, str], Model]


def load_model(filepath: str):
    return tf.keras.models.load_model(filepath)


def get_repeatbaseline_model(*args):
    class RepeatBaseline(tf.keras.Model):
        def call(self, inputs):
            return inputs
    
    repeat_baseline = RepeatBaseline(name="repeat")
    repeat_baseline.compile(
        loss=tf.keras.losses.MeanSquaredError(),
        metrics=[tf.keras.metrics.MeanAbsoluteError()],
    )
    return repeat_baseline



def get_multidense_model(num_features: int, out_steps=24, name: str = "dense"):
    multi_dense_model = tf.keras.Sequential(
        [
            tf.keras.layers.Lambda(lambda x: x[:, -1:, :]),
            tf.keras.layers.Dense(512, activation="swish"),
            tf.keras.layers.Dense(
                out_steps * num_features, kernel_initializer=tf.initializers.zeros()
            ),
            tf.keras.layers.Reshape([out_steps, num_features]),
        ],
        name=name,
    )
    return multi_dense_model


def get_convolution_model(num_features: int, outsteps=24, name: str = "cnn"):
    conv_width = 3
    multi_conv_model = tf.keras.Sequential(
        [
            tf.keras.layers.Lambda(lambda x: x[:, -conv_width:, :]),
            tf.keras.layers.Conv1D(256, activation="relu", kernel_size=(conv_width)),
            tf.keras.layers.Dense(
                outsteps * num_features, kernel_initializer=tf.initializers.zeros()
            ),
            tf.keras.layers.Reshape([outsteps, num_features]),
        ],
        name=name,
    )
    return multi_conv_model


def get_lstm_model(num_features: int, outsteps=24, name: str = "lstm"):
    lstm_model = tf.keras.Sequential(
        [
            tf.keras.layers.LSTM(32, return_sequences=False),
            tf.keras.layers.Dense(
                outsteps * num_features, kernel_initializer=tf.initializers.zeros()
            ),
            tf.keras.layers.Reshape([outsteps, num_features]),
        ],
        name=name,
    )
    return lstm_model


def get_feedback_model(num_features: int, outsteps=24, name: str = "feedback"):
    feedback_model = FeedBack(
        units=32, out_steps=outsteps, num_features=num_features, name=name
    )
    return feedback_model


class FeedBack(tf.keras.Model):
    """
    ### Advanced: Autoregressive model

    In some cases it may be helpful for the model to decompose this prediction into individual time steps. Then, each model's output can be fed back into itself at each step and predictions can be made conditioned on the previous one, like in the classic <a href="https://arxiv.org/abs/1308.0850" class="external">Generating Sequences With Recurrent Neural Networks</a>.
    One clear advantage to this style of model is that it can be set up to produce output with a varying length.
    You could take any of the single-step multi-output models trained in the first half of this tutorial and run in an autoregressive feedback loop, but here you'll focus on building a model that's been explicitly trained to do that.
    """

    def __init__(self, units, out_steps, num_features, name):
        super().__init__(name=name)
        self.out_steps = out_steps
        self.units = units
        self.lstm_cell = tf.keras.layers.LSTMCell(units)
        # Also wrap the LSTMCell in an RNN to simplify the `warmup` method.
        self.lstm_rnn = tf.keras.layers.RNN(self.lstm_cell, return_state=True)
        self.dense = tf.keras.layers.Dense(num_features)

    def warmup(self, inputs):
        # inputs.shape => (batch, time, features)
        # x.shape => (batch, lstm_units)
        x, *state = self.lstm_rnn(inputs)

        # predictions.shape => (batch, features)
        prediction = self.dense(x)
        return prediction, state

    def call(self, inputs, training=None):
        # Use a TensorArray to capture dynamically unrolled outputs.
        predictions = []
        # Initialize the LSTM state.
        prediction, state = self.warmup(inputs)

        # Insert the first prediction.
        predictions.append(prediction)

        # Run the rest of the prediction steps.
        for n in range(1, self.out_steps):
            # Use the last prediction as input.
            x = prediction
            # Execute one lstm step.
            x, state = self.lstm_cell(x, states=state, training=training)
            # Convert the lstm output to a prediction.
            prediction = self.dense(x)
            # Add the prediction to the output.
            predictions.append(prediction)

        # predictions.shape => (time, batch, features)
        predictions = tf.stack(predictions)
        # predictions.shape => (batch, time, features)
        predictions = tf.transpose(predictions, [1, 0, 2])
        return predictions
