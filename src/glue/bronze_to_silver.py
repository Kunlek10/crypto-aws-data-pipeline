import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "raw_bucket",
        "raw_prefix",
        "silver_bucket",
        "silver_prefix",
    ],
)

spark_context = SparkContext()
glue_context = GlueContext(spark_context)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

raw_path = f"s3://{args['raw_bucket']}/{args['raw_prefix']}"
silver_path = f"s3://{args['silver_bucket']}/{args['silver_prefix']}"

raw_df = (
    spark.read.option("multiLine", True)
    .option("recursiveFileLookup", True)
    .json(raw_path)
)

if raw_df.rdd.isEmpty():
    job.commit()
    raise SystemExit(0)

silver_df = (
    raw_df
    .withColumn("asset", F.explode("data"))
    .select(
        F.col("run_id").alias("pipeline_run_id"),
        F.col("source"),
        F.col("currency"),
        F.col("ingested_at"),
        F.col("asset.id").alias("asset_id"),
        F.upper(F.col("asset.symbol")).alias("asset_symbol"),
        F.col("asset.name").alias("asset_name"),
        F.col("asset.current_price").cast("double").alias("price_usd"),
        F.col("asset.market_cap").cast("double").alias("market_cap"),
        F.col("asset.total_volume").cast("double").alias("total_volume"),
        F.col("asset.last_updated").alias("as_of_timestamp"),
    )
    .withColumn("as_of_timestamp", F.to_timestamp("as_of_timestamp"))
    .withColumn("as_of_date", F.to_date("as_of_timestamp"))
    .withColumn("processed_at", F.current_timestamp())
    .dropDuplicates(["asset_symbol", "as_of_timestamp", "pipeline_run_id"])
)

(
    silver_df.write.mode("overwrite")
    .partitionBy("as_of_date")
    .parquet(silver_path)
)

job.commit()
