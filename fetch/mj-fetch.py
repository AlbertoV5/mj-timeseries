from urllib.request import urlopen
from datetime import datetime
from typing import TypedDict
import boto3
import json
import os


class EventBridgeData(TypedDict):
    url: str
    action: str
    tablename: str


class StatusData(TypedDict):
    status: str
    events: list[dict[str, str]]
    metrics: list[dict[str, str]]


dynamodb = boto3.resource("dynamodb")
tablename = os.environ.get("MJ_STATUS_DYNAMODB_TABLE")
client = boto3.client("dynamodb")
TIMESTAMP_FMT = "%Y-%m-%dT%H:%M:%SZ"


def table_exists(tablename):
    try:
        _ = client.describe_table(TableName=tablename)
        return True
    except client.exceptions.ResourceNotFoundException:
        return False


def create_table_if_not_exists(tablename: str):
    """Create table with timestamp and json data."""
    if table_exists(tablename):
        return tablename
    print("Creating table")
    table = dynamodb.create_table(
        TableName=tablename,
        KeySchema=[
            {"AttributeName": "date", "KeyType": "HASH"},
            {"AttributeName": "timestamp", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "date", "AttributeType": "S"},
            {"AttributeName": "timestamp", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    print(table)
    return tablename


def fetch_data(url: str):
    """Fetch data from url and store in dynamodb."""
    try:
        with urlopen(url) as response:
            return json.loads(response.read())
    except BaseException as e:
        print(e)
        return {"status": "failure", "events": [], "metrics": []}


def parse_data(data: StatusData):
    """Verify data contents and format."""
    try:
        events = data["events"]
        metrics = data["metrics"]
        alert_ids = []

        def parse_event(event: dict[str, str]):
            event.pop("day")
            for key, value in event.items():
                if len(value) > 200:
                    event[key] = event[key][:200]
                if key == "date":
                    event[key] = datetime.strptime(
                        value, "%Y-%m-%dT%H:%M:%S.%fZ"
                    ).strftime(TIMESTAMP_FMT)
            alert_ids.append(event["alert_id"])
            return event

        def parse_metric(metric: dict[str, str]):
            for key, value in metric.items():
                if key == "value":
                    metric[key] = f"{float(value):.4f}"
                elif key == "date":
                    metric[key] = datetime.strptime(
                        value, "%Y-%m-%dT%H:%M:%S.%fZ"
                    ).strftime(TIMESTAMP_FMT)
            return metric
        
        return {
            "status": data["status"],
            "events": [parse_event(event) for event in events if event["alert_id"] not in alert_ids],
            "metrics": [parse_metric(metric) for metric in metrics],
        }
    except BaseException as e:
        print(e)
        return {"status": "failure", "events": [], "metrics": []}


def fetch_test_data():
    """Load test json"""
    with open("data.json") as f:
        return json.load(f)


def put_item(tablename, data: StatusData):
    """Add item to table. Store timestamp with year, month, date, hour, minute."""
    table = dynamodb.Table(tablename)
    timestamp = str(datetime.now().strftime(TIMESTAMP_FMT))
    return timestamp, table.put_item(
        Item={
            "date": timestamp[:10],
            "timestamp": timestamp,
            "status": data["status"],
            "metrics": data["metrics"],
            "events": data["events"],
        }
    )


def delete_item(tablename, timestamp):
    """Delete item from table using timestamp."""
    table = dynamodb.Table(tablename)
    return table.delete_item(Key={"date": timestamp[:10], "timestamp": timestamp})


def lambda_handler(event: EventBridgeData, context):
    tablename = create_table_if_not_exists(tablename=tablename)
    if event["action"] == "test":
        data = parse_data(fetch_test_data())
        print(data)
        timestamp, db_response = put_item(tablename, data)
        db_response = delete_item(tablename, timestamp)
        return {
            "statusCode": 200,
            "body": {"status": data["status"], "db": db_response, "event": event},
        }
    elif event["action"] == "fetch":
        data = parse_data(fetch_data(url=event["url"]))
        timestamp, db_response = put_item(tablename, data)
        response = {
            "statusCode": 200,
            "body": {"status": data["status"], "db": db_response, "event": event},
        }
        print(response)
        return response


lambda_handler(
    {
        "action": "test",
        "url": "https://status-feed-streedkusq-ue.a.run.app/",
        "tablename": tablename,
    },
    None,
)
