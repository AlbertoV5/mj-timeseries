from datetime import datetime
import app
import os

bucketname = os.environ.get("MJ_STATUS_BUCKET")


def lambda_handler(event, context):
    """"""
    event = app.Event(**event)
    # storage
    store = app.Store(
        s3_bucket=bucketname,
        models_path=event.paths.models,
        metrics_path=event.paths.metrics,
    )
    data = store.download_s3_files(start=event.action.start, end=event.action.end)
    # load model
    flow = app.ModelFlow(event)
    df = flow.process_metrics([d["data"] for d in sorted(data, key=lambda x: x["path"])])
    # main(event)


def main(event: app.Event):
    # setup
    event = app.Event(**event)
    store = app.Store(models_path=event.paths.models, metrics_path=event.paths.metrics)
    data = store.load_local_files(start=event.action.start, end=event.action.end)
    data = sorted(data, key=lambda x: x["path"])
    # load model
    flow = app.ModelFlow(event)
    df = flow.process_metrics([d["data"] for d in data])
    # fit
    if not flow.is_predict:
        model, std, mean, perf = flow.fit(df)
        print(perf)
        perf.mean().to_csv(f"{event.paths.models}/{event.model}.csv")
        store.store_local_models(model, std, mean)
        return
    # predict
    model, std, mean = store.load_local_models(event.model)
    prediction = flow.predict(df, model, std, mean)
    date = datetime.now().strftime("%Y-%m-%d")
    prediction.to_csv(f"{event.paths.output}/{event.model}_{date}.csv", index=False)
    #
    # local testing storage
    #
    import matplotlib.pyplot as plt

    plt.plot(prediction)
    plt.legend(prediction.columns)
    plt.savefig(
        f"predictions/{event.model}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.png"
    )
    plt.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(prog="Time Series Forecasting")
    parser.add_argument("--model", default="dense", type=str, help="Model name: dense, cnn, lstm")
    parser.add_argument("--predict", action="store_true")
    argv = parser.parse_args()
    paths = {
        "bucket": bucketname,
        "models": "models",
        "metrics": "metrics/relax",
        "events": "metrics/events",
        "output": "predictions",
    }
    fit = {
        "model": argv.model,
        "action": {"type": "fit", "start": 28, "end": 0},
        "paths": paths,
    }
    predict = {
        "model": argv.model,
        "action": {"type": "predict", "start": 1, "end": 0},
        "paths": paths,
    }
    main(predict if argv.predict else fit)
