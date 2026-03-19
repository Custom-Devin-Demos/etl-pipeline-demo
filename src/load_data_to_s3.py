import boto3
import botocore.exceptions
from io import StringIO


def connect_to_s3(aws_access_key_id, aws_secret_access_key, region_name='us-west-2'):
    return boto3.client(
        "s3",
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )


def df_to_s3(df, key, s3_bucket, aws_access_key_id, aws_secret_access_key, region_name='us-west-2'):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    try:
        s3_client = connect_to_s3(aws_access_key_id, aws_secret_access_key, region_name)
        s3_client.put_object(Bucket=s3_bucket, Key=key, Body=csv_buffer.getvalue())
        print(f"✅ Uploaded {len(df)} rows to s3://{s3_bucket}/{key}")
    except botocore.exceptions.ClientError as e:
        print(f"❌ Failed to upload to S3: {e}")
