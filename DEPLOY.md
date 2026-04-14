# Deploying In One Command

Run the deployment script from the project root:

```bash
cd /Users/kunlektamang/Desktop/codex_ai_project
chmod +x deploy.sh

ARTIFACT_BUCKET=your-artifact-bucket \
ALERT_EMAIL=you@example.com \
./deploy.sh
```

Optional environment variables:

- `AWS_REGION` defaults to `us-east-1`
- `STACK_NAME` defaults to `crypto-batch-pipeline-dev`
- `PROJECT_NAME` defaults to `crypto-batch-pipeline`
- `ENVIRONMENT` defaults to `dev`
- `PRICE_SCHEDULE_EXPRESSION` defaults to `cron(0 6 * * ? *)`
- `DAILY_ALERT_THRESHOLD_PCT` defaults to `5`
- `DEFAULT_CURRENCY` defaults to `usd`
- `ASSETS` defaults to `bitcoin,ethereum`

Example with custom values:

```bash
ARTIFACT_BUCKET=your-artifact-bucket \
ALERT_EMAIL=you@example.com \
AWS_REGION=us-east-1 \
STACK_NAME=crypto-batch-pipeline-dev \
ENVIRONMENT=dev \
DAILY_ALERT_THRESHOLD_PCT=3 \
./deploy.sh
```
