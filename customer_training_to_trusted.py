import sys
import json
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# Required Glue Initialization Boilerplate
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Connect directly to your S3 bucket
s3 = boto3.resource('s3')
bucket_name = 'stedi-kashish-project'
bucket = s3.Bucket(bucket_name)

print("Starting native file-by-file copy and filter process...")

# Look inside your customer_landing directory
for obj in bucket.objects.filter(Prefix='customer_landing/'):
    # Skip the folder marker itself and process only files
    if obj.key.endswith('/') or not obj.key.endswith('.json'):
        continue
        
    try:
        # Read the file contents directly
        file_content = obj.get()['Body'].read().decode('utf-8')
        
        # Handle files that contain multiple JSON records separated by newlines
        filtered_lines = []
        for line in file_content.strip().split('\n'):
            if not line:
                continue
            data = json.loads(line)
            
            # Check for the field using any mix of upper/lowercase variants
            has_research_date = any(
                k.lower() == 'sharewithresearchasofdate' and v is not None and v != ""
                for k, v in data.items()
            )
            
            if has_research_date:
                filtered_lines.append(json.dumps(data))
        
        # If any rows matched the filter, save them to the trusted folder
        if filtered_lines:
            new_key = obj.key.replace('customer_landing/', 'customer_trusted/')
            output_content = '\n'.join(filtered_lines)
            s3.Object(bucket_name, new_key).put(Body=output_content)
            
    except Exception as e:
        print(f"Skipping problematic file {obj.key}: {str(e)}")

print("Process finished successfully!")
job.commit()