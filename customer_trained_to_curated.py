
import sys
import json
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3 = boto3.resource('s3')
bucket_name = 'stedi-kashish-project'
bucket = s3.Bucket(bucket_name)

# 1. Collect emails that successfully sent accelerometer data
active_emails = set()
for obj in bucket.objects.filter(Prefix='accelerometer_trusted/'):
    if obj.key.endswith('/') or not obj.key.endswith('.json'):
        continue
    file_content = obj.get()['Body'].read().decode('utf-8')
    for line in file_content.strip().split('\n'):
        if line:
            data = json.loads(line)
            user_email = data.get('user')
            if user_email:
                active_emails.add(user_email.lower())

# 2. Keep trusted customers only if they are active in that list
for obj in bucket.objects.filter(Prefix='customer_trusted/'):
    if obj.key.endswith('/') or not obj.key.endswith('.json'):
        continue
    try:
        file_content = obj.get()['Body'].read().decode('utf-8')
        filtered_lines = []
        for line in file_content.strip().split('\n'):
            if not line:
                continue
            data = json.loads(line)
            email = data.get('email')
            if email and email.lower() in active_emails:
                filtered_lines.append(json.dumps(data))
        
        if filtered_lines:
            new_key = obj.key.replace('customer_trusted/', 'customer_curated/')
            s3.Object(bucket_name, new_key).put(Body='\n'.join(filtered_lines))
    except Exception as e:
        print(f"Skipping file {obj.key}: {str(e)}")

print("Customer Trusted to Curated Complete!")
job.commit()