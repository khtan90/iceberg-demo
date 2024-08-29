import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, max

from pyspark.conf import SparkConf

# Parse the required arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME", "iceberg_job_catalog_warehouse"])
conf = SparkConf()

# Print the parsed argument for debugging
print("Iceberg Job Catalog Warehouse:", args['iceberg_job_catalog_warehouse'])

# Set Spark configurations for Iceberg
conf.set("spark.sql.catalog.job_catalog.warehouse", args['iceberg_job_catalog_warehouse'])
conf.set("spark.sql.catalog.job_catalog", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.job_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
conf.set("spark.sql.catalog.job_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
conf.set("spark.sql.iceberg.handle-timestamp-without-timezone","true")

sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


## Read Input Table
## glueContext.create_data_frame.from_catalog can be more 
## performant and can be replaced in place of 
## create_dynamic_frame.from_catalog.

IncrementalInputDyF = glueContext.create_dynamic_frame.from_catalog(database = "iceberg_lf_db", table_name = "csv_input", transformation_ctx = "IncrementalInputDyF")
IncrementalInputDF = IncrementalInputDyF.toDF()

if not IncrementalInputDF.rdd.isEmpty():
    ## Apply De-duplication logic on input data, to pickup latest record based on timestamp and operation
    IDWindowDF = Window.partitionBy(IncrementalInputDF.product_id).orderBy(IncrementalInputDF.last_update_time).rangeBetween(-sys.maxsize, sys.maxsize)

# Add new columns to capture OP value and what is the latest timestamp
inputDFWithTS= IncrementalInputDF.withColumn("max_op_date",max(IncrementalInputDF.last_update_time).over(IDWindowDF))

# Filter out new records that are inserted, then select latest record from existing records and merge both to get deduplicated output
NewInsertsDF = inputDFWithTS.filter("last_update_time=max_op_date").filter("op='I'")
UpdateDeleteDf = inputDFWithTS.filter("last_update_time=max_op_date").filter("op IN ('U','D')")
finalInputDF = NewInsertsDF.unionAll(UpdateDeleteDf)

# Register the deduplicated input as temporary table to use in Iceberg Spark SQL statements
finalInputDF.createOrReplaceTempView("incremental_input_data")
finalInputDF.show()

## Perform merge operation on incremental input data with MERGE INTO. This section of the code uses Spark SQL to showcase the expressive SQL approach of Iceberg to perform a Merge operation
IcebergMergeOutputDF = spark.sql("""
MERGE INTO job_catalog.iceberg_lf_db.iceberg_table_lf t
USING (SELECT op, product_id, category, product_name, quantity_available, to_timestamp(last_update_time) as last_update_time FROM incremental_input_data) s
ON t.product_id = s.product_id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED THEN UPDATE SET t.quantity_available = s.quantity_available, t.last_update_time = s.last_update_time
WHEN NOT MATCHED THEN INSERT (product_id, category, product_name, quantity_available, last_update_time) VALUES (s.product_id, s.category, s.product_name, s.quantity_available, s.last_update_time)
""")

job.commit()