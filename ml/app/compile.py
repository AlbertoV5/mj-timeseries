import tensorflow as tf


def compile_and_fit(model, window, patience=2, max_epocs=20):
    early_stopping = tf.keras.callbacks.EarlyStopping(
        monitor="val_loss", patience=patience, mode="min"
    )
    model.compile(
        loss=tf.keras.losses.MeanSquaredError(),
        optimizer=tf.keras.optimizers.Adam(),
        metrics=[tf.keras.metrics.MeanAbsoluteError()],
    )
    history = model.fit(
        window.train,
        epochs=max_epocs,
        validation_data=window.val,
        callbacks=[early_stopping],
        verbose=0,
    )
    return history
