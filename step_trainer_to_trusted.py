#step to trusted
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

# 1. Map trusted customer serial numbers
trusted_serials = set()
for obj in bucket.objects.filter(Prefix='customer_trusted/'):
    if obj.key.endswith('/') or not obj.key.endswith('.json'):
        continue
    file_content = obj.get()['Body'].read().decode('utf-8')
    for line in file_content.strip().split('\n'):
        if line:
            data = json.loads(line)
            # Check standard casing or lowercase for serial number
            serial = data.get('serialNumber') or data.get('serialnumber')
            if serial:
                trusted_serials.add(str(serial).strip())

print(f"Loaded {len(trusted_serials)} trusted serial numbers.")

# 2. Filter Step Trainer records matching the serial numbers
for obj in bucket.objects.filter(Prefix='step_trainer_landing/'):
    if obj.key.endswith('/') or not obj.key.endswith('.json'):
        continue
    try:
        file_content = obj.get()['Body'].read().decode('utf-8')
        filtered_lines = []
        for line in file_content.strip().split('\n'):
            if not line:
                continue
            data = json.loads(line)
            device_serial = data.get('serialNumber') or data.get('serialnumber')
            if device_serial and str(device_serial).strip() in trusted_serials:
                filtered_lines.append(json.dumps(data))
        
        if filtered_lines:
            new_key = obj.key.replace('step_trainer_landing/', 'step_trainer_trusted/')
            s3.Object(bucket_name, new_key).put(Body='\n'.join(filtered_lines))
    except Exception as e:
        print(f"Skipping file {obj.key}: {str(e)}")

print("Step Trainer Landing to Trusted Complete!")
job.commit()