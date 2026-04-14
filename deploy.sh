#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$PROJECT_ROOT/infra"
TEMPLATE_FILE="$INFRA_DIR/template.yaml"
PACKAGED_TEMPLATE_FILE="$INFRA_DIR/packaged.yaml"

: "${ARTIFACT_BUCKET:?Set ARTIFACT_BUCKET to the S3 bucket used for CloudFormation packaging.}"
: "${ALERT_EMAIL:?Set ALERT_EMAIL to the email that should receive SNS alerts.}"

STACK_NAME="${STACK_NAME:-crypto-batch-pipeline-dev}"
AWS_REGION="${AWS_REGION:-us-east-1}"
PROJECT_NAME="${PROJECT_NAME:-crypto-batch-pipeline}"
ENVIRONMENT="${ENVIRONMENT:-dev}"
PRICE_SCHEDULE_EXPRESSION="${PRICE_SCHEDULE_EXPRESSION:-cron(0 6 * * ? *)}"
DAILY_ALERT_THRESHOLD_PCT="${DAILY_ALERT_THRESHOLD_PCT:-5}"
DEFAULT_CURRENCY="${DEFAULT_CURRENCY:-usd}"
ASSETS="${ASSETS:-bitcoin,ethereum}"

echo "Using AWS region: $AWS_REGION"
echo "Packaging CloudFormation template..."

aws cloudformation package \
  --region "$AWS_REGION" \
  --template-file "$TEMPLATE_FILE" \
  --s3-bucket "$ARTIFACT_BUCKET" \
  --output-template-file "$PACKAGED_TEMPLATE_FILE"

echo "Deploying stack: $STACK_NAME"

aws cloudformation deploy \
  --region "$AWS_REGION" \
  --template-file "$PACKAGED_TEMPLATE_FILE" \
  --stack-name "$STACK_NAME" \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    ProjectName="$PROJECT_NAME" \
    Environment="$ENVIRONMENT" \
    AlertEmail="$ALERT_EMAIL" \
    PriceScheduleExpression="$PRICE_SCHEDULE_EXPRESSION" \
    DailyAlertThresholdPct="$DAILY_ALERT_THRESHOLD_PCT" \
    DefaultCurrency="$DEFAULT_CURRENCY" \
    Assets="$ASSETS"

echo "Deployment complete."
echo "Next steps:"
echo "1. Confirm the SNS email subscription sent to $ALERT_EMAIL"
echo "2. Check the CloudFormation stack in region $AWS_REGION"
echo "3. Trigger the Step Functions state machine manually for a smoke test"
