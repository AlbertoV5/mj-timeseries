import boto3
import json
import os


client = boto3.client("dynamodb")
tablename = os.environ["MJ_STATUS_DYNAMODB_TABLE"]

def parse_dynamodb_items(items: list[dict[str, dict]]) -> list[dict[str, dict]]:
    # parse client response
    for item in items:
        for key in item.keys():
            k = next(iter(item[key].keys()))
            v: str | list[dict[str, dict]] = item[key][k]
            item[key] = [{a: b['S'] for a, b in i['M'].items()} for i in v] if isinstance(v, list) else v
    return items


if __name__ == "__main__":
    dates = [f"2023-05-2{i}" for i in range(0, 5)]
    date = dates[2]
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
    print(date, len(items))
    responses = {"delete": [], "put": []}
    for i, item in enumerate(items):
        len0 = len(item['events']['L'])
        if len0 == 0:
            print("EMPTY", len0)
            continue
        alert_ids = []
        events = []
        # append the first event with a given alert_id to events
        for event in item['events']["L"]:
            alert_id = event["M"]['alert_id']["S"]
            assert int(alert_id) == int(alert_id)
            # check if alert_id is already in alert_ids
            if alert_id in alert_ids:
                continue
            # append the event and alert_id
            events.append(event)
            alert_ids.append(alert_id)
        len1 = len(events)
        # update item's events
        item['events'] = {"L": events}
        delete_response = client.delete_item(TableName=tablename, Key={'date': item['date'], 'timestamp': item['timestamp']})
        put_response = client.put_item(TableName=tablename, Item=item)
        print("OK", len0, len1)
    
    # batch_items = [{'DeleteRequest': {'Key': {'date': item['date'], 'timestamp': item['timestamp']}}, 'PutRequest': {'Item': item}} for item in items]
    # client.batch_write_item(RequestItems={tablename: batch_items})
    