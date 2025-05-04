import sys
from awsglue import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import *

# Initialize Glue context
print("Initializing AWS Glue job...")
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Define source and target locations in S3
source_path = "s3://vietanh21-ecom-silver-zone/ecom/results"

input_df = spark.read.parquet(source_path)

# Show sample data and schema
print("Sample JSON data:")
input_df.show(5, truncate=False)
print("JSON Schema:")
input_df.printSchema()

# # Get the count of records
# record_count = input_df.count()
# print(f"Total records in JSON file: {record_count}")

# # Example data exploration
# print("Basic data exploration:")
# for column in input_df.columns:
#     print(f"Column: {column}")
#     input_df.select(column).distinct().show(5, truncate=False)

# # Example transformations (modify based on your actual JSON structure)
# print("Performing transformations on JSON data...")

# # Assuming a customer JSON with fields like id, name, email, etc.
# if "email" in input_df.columns:
#     # Filter records with valid emails (contains @)
#     transformed_df = input_df.filter(F.col("email").contains("@"))

#     # Extract domain from email
#     transformed_df = transformed_df.withColumn("email_domain",
#                                               F.split(F.col("email"), "@")[1])
# else:
#     transformed_df = input_df

# # Add processing metadata
# transformed_df = transformed_df.withColumn("processed_date", F.current_date())
# transformed_df = transformed_df.withColumn("source_file", F.lit(source_path.split("/")[-1]))

# print("Transformed data sample:")
# transformed_df.show(5)

print("Job completed successfully!")
job.commit()
