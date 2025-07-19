import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, sum, avg
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

silver_df = spark.read.parquet("s3://myretaildatawarehouse-123/silver/sales/")
silver_df.createOrReplaceTempView("silver_sales")

gold_df=silver_df.groupBy("Category").agg(sum(col("Quantity_Sold") * col("Price")).alias("Total_Sales_Amount"),sum("Quantity_Sold").alias("Total_Units_Sold"),avg("Price").alias("Average_price")
)


gold_df.write.mode("overwrite").parquet("s3://myretaildatawarehouse-123/gold/sales_summary/")








job.commit()