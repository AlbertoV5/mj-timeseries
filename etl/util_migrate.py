# %%

import boto3
import os


tablename = os.environ["MJ_STATUS_DYNAMODB_TABLE"]
dynamodb = boto3.resource("dynamodb")
table1 = dynamodb.Table() # removed
table2 = dynamodb.Table(tablename)
response = table1.scan()

items = response["Items"]
len(items)
# %%
items.sort(key=lambda x: x["timestamp"], reverse=False)
# %%
items2 = [
    {
        "date": i["timestamp"][:10],
        "timestamp": i["timestamp"],
        "status": i["status"],
        "events": i["events"],
        "metrics": i["metrics"],
    }
    for i in items
]
print(items2)
# %%
with table2.batch_writer() as batch:
    for item in items2:
        batch.put_item(Item=item)
