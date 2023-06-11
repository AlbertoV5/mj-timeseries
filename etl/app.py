"""
This module is responsible for parsing the data from the DynamoDb stream and storing in Postgres SQL database.
"""
from datetime import datetime, date, timedelta
from pydantic import BaseModel, parse_obj_as
from typing import TypedDict
import boto3
import os

from sqlalchemy.orm import mapped_column, Mapped, DeclarativeBase
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import create_engine, MetaData
from typing_extensions import Annotated


DB_URL = os.environ.get("MJ_ETL_DB")
tablename = os.environ.get("MJ_STATUS_DYNAMODB_TABLE")

strpk = Annotated[str, mapped_column(primary_key=True)]
datpk = Annotated[datetime, mapped_column(primary_key=True)]


class Base(DeclarativeBase):
    """Declarative Base. Include metadata with client schema."""

    metadata = MetaData(schema="public")


class Event(Base):
    __tablename__ = "events"

    date_id: Mapped[strpk]
    timestamp_id: Mapped[datpk]
    date: Mapped[datpk]
    short_title: Mapped[str]
    label: Mapped[str]
    type: Mapped[str]
    alert_id: Mapped[int]


class Metric(Base):
    __tablename__ = "metrics"

    date_id: Mapped[strpk]
    timestamp_id: Mapped[datpk]
    name: Mapped[strpk]
    date: Mapped[datetime]
    value: Mapped[float]


class NewImage(TypedDict):
    date: str
    timestamp: str
    metrics: list[dict]
    events: list[dict]


class Dynamodb(TypedDict):
    ApproximateCreationDateTime: int
    Keys: dict
    NewImage: NewImage
    OldImage: dict
    SequenceNumber: str
    SizeBytes: int
    StreamViewType: str


class Record(TypedDict):
    eventID: str
    eventName: str
    eventVersion: str
    eventSource: str
    awsRegion: str
    dynamodb: Dynamodb


class DynamoEvent(TypedDict):
    Records: list[Record]


class EventSchema(BaseModel):
    date_id: date
    timestamp_id: datetime
    date: datetime
    short_title: str
    label: str
    type: str
    alert_id: int

    class Config:
        orm_mode = True


class MetricSchema(BaseModel):
    date_id: date
    timestamp_id: datetime
    date: datetime
    name: str
    value: float

    class Config:
        orm_mode = True


def parse_dynamodb_items(items: list[dict[str, dict]]) -> list[dict[str, dict]]:
    # parse client response
    for item in items:
        for key in item.keys():
            k = next(iter(item[key].keys()))
            v: str | list[dict[str, dict]] = item[key][k]
            item[key] = [{a: b['S'] for a, b in i['M'].items()} for i in v] if isinstance(v, list) else v
    return items

engine = create_engine(DB_URL)
client = boto3.client("dynamodb")


def lambda_handler(event, context):
    #  create tables if not exist
    if event.get('action') == 'test':
        return {"Status": "Success"}
    try:
        Base.metadata.create_all(engine)
    except BaseException as e:
        print(e)
    # date to query
    today = datetime.today()
    if event.get("date") == "today":
        date = today.strftime("%Y-%m-%d")
    else:
        date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    # from dynamodb
    lastEvaluatedKey = None
    items = []
    # https://www.beabetterdev.com/2021/10/20/dynamodb-scan-query-not-returning-data/
    while True:
        if lastEvaluatedKey == None:
            response = client.query(
                TableName=tablename,
                KeyConditionExpression='#date = :dt',
                ExpressionAttributeValues={":dt": {"S": date}},
                ExpressionAttributeNames={"#date": "date"},
            )
        else:
            response = client.query(
                TableName=tablename,
                KeyConditionExpression='#date = :dt',
                ExpressionAttributeValues={":dt": {"S": date}},
                ExpressionAttributeNames={"#date": "date"},
                ExclusiveStartKey=lastEvaluatedKey # In subsequent calls, provide the ExclusiveStartKey
            )
        items.extend(response['Items'])
        if 'LastEvaluatedKey' in response:
            lastEvaluatedKey = response['LastEvaluatedKey']
        else:
            break
    items = parse_dynamodb_items(items)
    print("Items received from DynamoDB:", len(items))
    if len(items) == 0:
        return {"Status": "No data"}
    # parse
    all_metrics = [
        {"date_id": item["date"], "timestamp_id": item["timestamp"], **m}
        for item in items
        for m in item["metrics"]
    ]
    all_events = [
        {"date_id": item["date"], "timestamp_id": item["timestamp"], **e}
        for item in items
        for e in item["events"]
    ]
    metrics_py = parse_obj_as(list[MetricSchema], all_metrics)
    events_py = parse_obj_as(list[EventSchema], all_events)
    print(len(metrics_py), len(events_py))
    # to sql
    with engine.connect() as conn:
        if len(metrics_py) > 0:
            conn.execute(
                pg_insert(Metric).on_conflict_do_nothing(), [m.dict() for m in metrics_py]
            )
        if len(events_py) > 0:
            conn.execute(
                pg_insert(Event).on_conflict_do_nothing(), [e.dict() for e in events_py]
            )
        conn.commit()
        print("Records inserted")
    return {"Status": "Success"}


lambda_handler({"action": "fix", "date": "today"}, None)