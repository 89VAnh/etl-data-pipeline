import boto3

s3 = boto3.client('s3')
paginator = s3.get_paginator("list_objects_v2")

response = s3.list_objects_v2(Bucket='vietanh21-ecom-silver-zone', Prefix="target/dev/data")

print(response)
