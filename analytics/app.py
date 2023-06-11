from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import warnings
import boto3
import json
import os

# ignore warnings
warnings.filterwarnings("ignore")
bucketname = os.environ.get("MJ_STATUS_BUCKET")
# aws
s3 = boto3.client("s3")
# db
MJ_ETL_DB = os.environ.get("MJ_ETL_DB")
engine = create_engine(MJ_ETL_DB)
metrics_query = "SELECT * FROM metrics WHERE date_id >= '{before}' AND date_id < '{after}'"
events_query = "SELECT * FROM events WHERE date_id >= '{before}' AND date_id < '{after}'"


def query_metrics(before, after):
    try:
        df_metrics = pd.read_sql(metrics_query.format(before=before, after=after), con=engine)
    except BaseException as e:
        print(e)
        df_metrics = None
    return df_metrics

def query_events(before, after):
    try:
        df_events = pd.read_sql(events_query.format(before=before, after=after), con=engine)
    except BaseException as e:
        print(e)
        df_events = None
    return df_events


def extract_events(df: pd.DataFrame):
    # zero events in a batch / day
    if df.shape[0] == 0:
        return {}
    # encode to binary flags
    for title in df['short_title'].unique():
        for type in df['type'].unique():
            df[f"{type}_{title}"] = df[['short_title', 'type']].apply(lambda x: 1 if x[0] == title and x[1] == type else 0, axis=1)
    # keep only time columns
    df = df.drop(columns=['alert_id', 'short_title', 'type', 'label'])
    # compute time difference in seconds
    delta_seconds = (df['timestamp_id'] - df['date']).dt.seconds
    features = df.columns[3:]
    # multiply binary column with time difference so we have timedelta of seconds instead of binary flag
    for feature in features:
        df[feature] = df[feature] * delta_seconds
    # compute relative index, based on time of day, to deal with no events / no samples
    origin = pd.Timestamp("1970-01-01")
    day = (pd.to_datetime(df['date_id']) - origin) // pd.Timedelta('1s')
    time = (df['timestamp_id'] - origin) // pd.Timedelta('1s')
    df['index'] = (time - day) // (60 * 15) # sample resolution
    # compress multiple events into one sample
    features_df = df[['index', 'timestamp_id', *features]].groupby('timestamp_id').max().reset_index().drop(columns=['timestamp_id'])
    # rename into underscore and lower case, first word is the event type, then the short title
    features_df = features_df.rename(columns={c: c.replace(' ', '_').lower() for c in features_df.columns})
    # format for json output
    return features_df.to_dict(orient='list')
    

def extract_metrics(df: pd.DataFrame, kind="relax"):
    # convert to hour:minutes
    df["time"] = pd.to_datetime(df["timestamp_id"]).dt.strftime("%H:%M")
    # parse metric kind (relax or fast) from original name
    df["kind"] = df["name"].str.split(".").map(lambda x: x[2])
    # get only specified kind
    df = df.loc[df["kind"] == kind]
    # create less verbose label from name
    df["label"] = (
        df["name"]
        .str.split(".")
        .map(lambda x: "".join(x[4:]).replace("job_type_", ""))
    )
    # get only relevant data
    data_df = df[["label", "time", "value"]]
    # convert to custom json format (timestamp is assumed from position, 96 samples per file)
    data = {}
    for label in data_df["label"].unique():
        y = data_df.loc[data_df["label"] == label]
        data[label] = list(y["value"].values)
    return data


def lambda_handler(event: dict, context):
    kind = event.get("kind", "relax")
    time = event.get("date", "yesterday")
    bucket = event.get("bucket", bucketname)
    today = datetime.now()
    # date
    if time == "yesterday":
        before = (today - timedelta(days=1)).strftime("%Y-%m-%d")
        after = today.strftime("%Y-%m-%d")
    elif time == "today":
        before = today.strftime("%Y-%m-%d")
        after = (today + timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        before, after = time.split("_")
    if event.get("action") == "test":
        response = {
            "statusCode": 200,
            "body": {
                "date": f"{before}_{after}",
                "event": event,
            }
        }
        print(response)
        return response
    # Metrics
    metrics_df = query_metrics(before=before, after=after)
    if metrics_df is None:
        return {"statusCode": 404, "body": json.dumps("No data if metrics")}
    metrics = extract_metrics(metrics_df, kind=kind)
    s3_response = s3.put_object(
        Body=json.dumps(metrics),
        Bucket=bucket,
        Key=f"metrics/{kind}/{before}_{after}.json",
    )
    print("metrics", s3_response['ResponseMetadata']['HTTPStatusCode'])
    # Events
    events_df = query_events(before=before, after=after)
    if events_df is None:
        return {"statusCode": 404, "body": json.dumps("No data if events")}
    events = extract_events(events_df)
    s3_response = s3.put_object(
        Body=json.dumps(events),
        Bucket=bucket,
        Key=f"metrics/events/{before}_{after}.json",
    )
    print("events", s3_response['ResponseMetadata']['HTTPStatusCode'])
    return {
        "statusCode": 200,
        "body": s3_response,
    }

# 2023-05-14_2023-05-15 has no events, 2023-05-12_2023-05-13 has all kinds of events
if __name__ == "__main__":
    lambda_handler(
        {"action":"test", "bucket": bucketname, "date": "2023-05-12_2023-05-13", "kind": "relax"}, None
    )
    # before = "2023-05-24"
    # after = "2023-05-25"
    # events_df = query_events(before=before, after=after)
    # events = extract_events(events_df)
    # s3_response = s3.put_object(
    #     Body=json.dumps(events),
    #     Bucket=bucketname,
    #     Key=f"metrics/events/{before}_{after}.json",
    # )
    # print(s3_response['ResponseMetadata']['HTTPStatusCode'])