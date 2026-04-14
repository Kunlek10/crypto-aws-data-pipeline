# AWS Crypto Batch Pipeline

This project deploys a daily batch data pipeline on AWS that:

- fetches Bitcoin and Ethereum prices from an external API
- stores the raw payload in Amazon S3
- calculates daily price deltas
- writes curated records to Amazon S3 and DynamoDB
- sends notifications when price changes cross a threshold

## Architecture

EventBridge Scheduler triggers a Step Functions state machine once per day.

The workflow runs these Lambda functions:

1. `ingest_prices` fetches the latest BTC and ETH prices from CoinGecko and stores the raw response in S3.
2. `transform_prices` normalizes the dataset, compares it to the previous day, stores curated output in S3, and updates the latest-price table in DynamoDB.
3. `send_alerts` publishes an SNS message when any asset exceeds the configured daily threshold.
4. `notify_failure` publishes an SNS message if the workflow fails.

## Repository Layout

```text
.
├── infra
│   └── template.yaml
└── src
    └── lambdas
        ├── ingest_prices
        │   └── app.py
        ├── notify_failure
        │   └── app.py
        ├── send_alerts
        │   └── app.py
        └── transform_prices
            └── app.py
```

## Prerequisites

- AWS account with permissions for CloudFormation, Lambda, Step Functions, S3, DynamoDB, SNS, IAM, and EventBridge Scheduler
- AWS CLI configured locally
- An S3 bucket for CloudFormation packaging artifacts

## Deploy

1. Create or choose a deployment artifact bucket in your AWS account.
2. Package the Lambda source directories and upload them to that bucket.
3. Deploy the stack.

```bash
cd /Users/kunlektamang/Desktop/codex_ai_project/infra

aws cloudformation package \
  --template-file template.yaml \
  --s3-bucket YOUR_ARTIFACT_BUCKET \
  --output-template-file packaged.yaml

aws cloudformation deploy \
  --template-file packaged.yaml \
  --stack-name crypto-batch-pipeline-dev \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides AlertEmail=you@example.com
```

## Useful Parameters

- `ProjectName`: base name for AWS resources
- `Environment`: environment suffix such as `dev` or `prod`
- `AlertEmail`: email subscribed to alert and failure topics
- `PriceScheduleExpression`: daily UTC cron schedule
- `DailyAlertThresholdPct`: absolute percentage threshold for alerts
- `DefaultCurrency`: fiat currency, defaults to `usd`
- `Assets`: comma-delimited list of assets, defaults to `bitcoin,ethereum`

## After Deploy

- confirm both SNS email subscriptions
- inspect the Step Functions execution history after the first run
- review CloudWatch logs for each Lambda function
- optionally trigger the state machine manually for validation

## Notes

- The ingestion Lambda uses CoinGecko's public markets endpoint, so no API key is required for the MVP.
- The implementation is config-driven for assets and threshold values, so adding more coins is straightforward.
- Curated data is written as newline-delimited JSON to avoid Python dependency packaging in the first version.
