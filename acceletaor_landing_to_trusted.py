#acc to trusteed
import sys
import json
import boto3
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3 = boto3.resource('s3')

bucket_name = 'stedi-kashish-project'
bucket = s3.Bucket(bucket_name)

print("Processing accelerometer_landing...")

for obj in bucket.objects.filter(Prefix='accelerometer_landing/'):

    if obj.key.endswith('/') or not obj.key.endswith('.json'):
        continue

    try:
        file_content = obj.get()['Body'].read().decode('utf-8')

        filtered_lines = []

        for line in file_content.strip().split('\n'):
            if not line:
                continue

            data = json.loads(line)

            # accelerometer has no filtering rule → pass-through clean copy
            filtered_lines.append(json.dumps({
                "timestamp": data.get("timestamp"),
                "user": data.get("user"),
                "x": data.get("x"),
                "y": data.get("y"),
                "z": data.get("z")
            }))

        if filtered_lines:
            new_key = obj.key.replace(
                'accelerometer_landing/',
                'accelerometer_trusted/'
            )

            s3.Object(bucket_name, new_key).put(
                Body='\n'.join(filtered_lines)
            )

    except Exception as e:
        print(f"Skipping {obj.key}: {str(e)}")

print("Accelerometer trusted completed")
job.commit()


















