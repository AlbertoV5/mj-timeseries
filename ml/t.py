import app
import boto3

import os

bucketname = os.environ.get("MJ_STATUS_BUCKET")

if __name__ == "__main__":
    
    client = boto3.client('s3')
    
    path = 'metrics/relax'
    # get all days from given month
    year = '2023'
    month = '05'
    response = client.list_objects_v2(Bucket=bucketname, Prefix=path, StartAfter=f"{path}/{year}-{month}", MaxKeys=30)
    
    for content in response['Contents']:
        print(content['Key'])
    