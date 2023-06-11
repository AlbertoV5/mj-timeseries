# %%
import boto3 
import os

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["MJ_STATUS_DYNAMODB_TABLE"])
response = table.scan()
items = response["Items"]

print(items)
# %%

bad = [i for i in items if "data" in i.keys()]
print(len(bad))

for b in bad:
    b["metrics"] = b["data"]["metrics"]
    b["events"] = b["data"]["events"]
    b["status"] = b["data"]["status"]
    del b["data"]

# %%
print(bad[0]["timestamp"])

# %%
for good in bad:
    r = table.delete_item(Key={"timestamp": good["timestamp"]})
    print(r)
    r = table.put_item(Item=good)
    print(r)

# %%
