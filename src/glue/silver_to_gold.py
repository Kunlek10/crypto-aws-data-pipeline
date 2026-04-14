import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "silver_bucket",
        "silver_prefix",
        "gold_bucket",
        "gold_prefix",
    ],
)

spark_context = SparkContext()
glue_context = GlueContext(spark_context)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

silver_path = f"s3://{args['silver_bucket']}/{args['silver_prefix']}"
gold_path = f"s3://{args['gold_bucket']}/{args['gold_prefix']}"

silver_df = spark.read.parquet(silver_path)

if silver_df.rdd.isEmpty():
    job.commit()
    raise SystemExit(0)

daily_snapshot = (
    silver_df.groupBy("asset_symbol", "asset_name", "as_of_date")
    .agg(
        F.max("as_of_timestamp").alias("latest_timestamp"),
        F.max("price_usd").alias("closing_price_usd"),
        F.avg("price_usd").alias("avg_price_usd"),
        F.max("market_cap").alias("market_cap"),
        F.max("total_volume").alias("total_volume"),
        F.count("*").alias("records_in_day"),
    )
)

asset_window = Window.partitionBy("asset_symbol").orderBy("as_of_date")

gold_df = (
    daily_snapshot
    .withColumn("previous_closing_price_usd", F.lag("closing_price_usd").over(asset_window))
    .withColumn(
        "daily_change_abs",
        F.when(
            F.col("previous_closing_price_usd").isNull(),
            F.lit(None),
        ).otherwise(F.col("closing_price_usd") - F.col("previous_closing_price_usd"))
    )
    .withColumn(
        "daily_change_pct",
        F.when(
            F.col("previous_closing_price_usd").isNull() | (F.col("previous_closing_price_usd") == 0),
            F.lit(None),
        ).otherwise(
            ((F.col("closing_price_usd") - F.col("previous_closing_price_usd")) / F.col("previous_closing_price_usd")) * 100.0
        )
    )
    .withColumn("gold_processed_at", F.current_timestamp())
)

(
    gold_df.write.mode("overwrite")
    .partitionBy("as_of_date")
    .parquet(gold_path)
)

job.commit()
