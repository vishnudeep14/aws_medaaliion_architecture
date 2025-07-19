import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_timestamp, expr

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'])

# Read Bronze Layer
bronze_df = glueContext.create_dynamic_frame.from_catalog(
    database="retail_db",
    table_name="bronze_bronze"
).toDF()

# Transform to Silver Layer
silver_df = bronze_df.select(
    col("Store_ID"),
    col("Product_ID"),
    col("Category"),
    to_timestamp(col("Date"), "yyyy-MM-dd").alias("Date"),
    col("Quantity_Sold").cast("int"),
    col("Price").cast("double")
).dropna()

silver_df = silver_df.withColumn("Total_sales_amount", expr("Quantity_Sold * Price"))

# Write to Silver Layer (Parquet format)
silver_output_path = "s3://myretaildatawarehouse-123/silver/sales/"
silver_df.write.mode("overwrite").format("parquet").save(silver_output_path)

job.commit()
