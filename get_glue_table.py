from pyspark.sql.functions import col
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    TimestampType,
)

spark: SparkSession = (
    SparkSession.builder.config(
        f"spark.sql.catalog.glue_catalog",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    .config(
        f"spark.sql.catalog.glue_catalog.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog",
    )
    .config(
        f"spark.sql.catalog.glue_catalog.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO",
    )
    .config(f"spark.sql.catalog.handle-timestamp-without-timezone", "true")
    .config(
        f"spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .getOrCreate()
)

sc = spark.sparkContext
sql_context = SQLContext(sc)
glue_context = GlueContext(sc)
job = Job(glue_context)

# # Sample DataFrame
# df = spark.createDataFrame(
#     [(1, "abc", "electronics", "2025-04-29")], ["id", "name", "category", "created_at"]
# )

# # Define Iceberg table location
# iceberg_table_location = "s3://vietanh21-ecom-silver-zone/default/demo"

# # Write DataFrame to Iceberg table with multiple partition columns
# df.writeTo("glue_catalog.default.demo").tableProperty(
#     "location", iceberg_table_location
# ).using("iceberg").partitionedBy("category").partitionedBy(
#     "created_at"
# ).createOrReplace()

# # List all tables in the default schema
spark.sql("SELECT * FROM glue_catalog.ecom.results").show()
print(spark.catalog.tableExists("glue_catalog.ecom.results"))

# spark.table("glue_catalog.ecom.results").printSchema()
# existing_df = spark.table("glue_catalog.ecom.results")
# existing_df.show()
# existing_df.printSchema()
