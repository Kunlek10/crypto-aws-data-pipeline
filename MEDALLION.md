# Medallion Lake Architecture

This project now supports a medallion-style lake design on AWS:

- `bronze`: raw API payloads stored exactly as ingested in the raw S3 bucket
- `silver`: cleaned and standardized crypto price records written by an AWS Glue ETL job to the curated S3 bucket
- `gold`: business-ready daily summary data written by a second AWS Glue ETL job to the gold S3 bucket

## Layer Mapping

- Bronze bucket: `crypto-batch-pipeline-<env>-raw-<account>`
- Silver bucket: `crypto-batch-pipeline-<env>-curated-<account>`
- Gold bucket: `crypto-batch-pipeline-<env>-gold-<account>`

## Glue Components

- Glue databases:
  - `${ProjectName}_${Environment}_bronze`
  - `${ProjectName}_${Environment}_silver`
  - `${ProjectName}_${Environment}_gold`
- Glue jobs:
  - `bronze_to_silver.py`
  - `silver_to_gold.py`
- Glue crawlers:
  - bronze crawler
  - silver crawler
  - gold crawler

## Workflow

1. `ingest_prices` writes raw JSON to the bronze prefix in the raw bucket.
2. `transform_prices` updates DynamoDB and writes operational NDJSON output.
3. Step Functions runs the `bronze-to-silver` Glue job.
4. Step Functions runs the `silver-to-gold` Glue job.
5. `send_alerts` publishes SNS notifications based on the operational price-change logic.

## Script Uploads

`deploy.sh` uploads the Glue scripts to the artifact bucket before packaging and deploying the CloudFormation stack.
