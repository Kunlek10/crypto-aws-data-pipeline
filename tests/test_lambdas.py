import importlib.util
import io
import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


PROJECT_ROOT = Path("/Users/kunlektamang/Desktop/codex_ai_project")


def load_module(module_name, relative_path, env_vars, boto3_client=None, boto3_resource=None):
    module_path = PROJECT_ROOT / relative_path
    if module_name in sys.modules:
        del sys.modules[module_name]

    with patch.dict(os.environ, env_vars, clear=False):
        with patch("boto3.client", return_value=boto3_client), patch("boto3.resource", return_value=boto3_resource):
            spec = importlib.util.spec_from_file_location(module_name, module_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module


class IngestPricesTests(unittest.TestCase):
    def test_ingest_fetches_api_and_persists_raw_payload(self):
        s3_client = MagicMock()
        runs_table = MagicMock()
        dynamodb_resource = MagicMock()
        dynamodb_resource.Table.return_value = runs_table

        payload = [
            {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "current_price": 65000},
            {"id": "ethereum", "symbol": "eth", "name": "Ethereum", "current_price": 3200}
        ]

        response = MagicMock()
        response.read.return_value = json.dumps(payload).encode("utf-8")
        response.__enter__.return_value = response
        response.__exit__.return_value = False

        module = load_module(
            "test_ingest_prices",
            "src/lambdas/ingest_prices/app.py",
            {
                "RAW_BUCKET": "raw-bucket",
                "PIPELINE_RUNS": "pipeline-runs",
                "ASSETS": "bitcoin,ethereum",
                "DEFAULT_CURRENCY": "usd"
            },
            boto3_client=s3_client,
            boto3_resource=dynamodb_resource
        )

        with patch.object(module.request, "urlopen", return_value=response) as mock_urlopen:
            result = module.lambda_handler({"triggered_by": "manual-test"}, None)

        self.assertEqual(result["record_count"], 2)
        self.assertEqual(result["currency"], "usd")
        self.assertEqual(result["assets"], ["bitcoin", "ethereum"])
        mock_urlopen.assert_called_once()
        s3_client.put_object.assert_called_once()
        runs_table.put_item.assert_called_once()

        s3_kwargs = s3_client.put_object.call_args.kwargs
        raw_record = json.loads(s3_kwargs["Body"].decode("utf-8"))
        self.assertEqual(raw_record["source"], "coingecko")
        self.assertEqual(len(raw_record["data"]), 2)


class TransformPricesTests(unittest.TestCase):
    def test_transform_calculates_deltas_updates_state_and_emits_alerts(self):
        s3_client = MagicMock()
        latest_prices_table = MagicMock()
        pipeline_runs_table = MagicMock()
        dynamodb_resource = MagicMock()
        dynamodb_resource.Table.side_effect = [latest_prices_table, pipeline_runs_table]

        raw_record = {
            "source": "coingecko",
            "currency": "usd",
            "data": [
                {
                    "id": "bitcoin",
                    "symbol": "btc",
                    "name": "Bitcoin",
                    "current_price": 105,
                    "last_updated": "2026-04-14T06:00:00Z"
                },
                {
                    "id": "ethereum",
                    "symbol": "eth",
                    "name": "Ethereum",
                    "current_price": 100,
                    "last_updated": "2026-04-14T06:00:00Z"
                }
            ]
        }
        s3_client.get_object.return_value = {"Body": io.BytesIO(json.dumps(raw_record).encode("utf-8"))}
        latest_prices_table.get_item.side_effect = [
            {"Item": {"asset_symbol": "BTC", "price_usd": "100"}},
            {}
        ]

        module = load_module(
            "test_transform_prices",
            "src/lambdas/transform_prices/app.py",
            {
                "CURATED_BUCKET": "curated-bucket",
                "LATEST_PRICES_TABLE": "latest-prices",
                "PIPELINE_RUNS_TABLE": "pipeline-runs",
                "DAILY_ALERT_THRESHOLD_PCT": "4"
            },
            boto3_client=s3_client,
            boto3_resource=dynamodb_resource
        )

        result = module.lambda_handler(
            {"raw_bucket": "raw-bucket", "raw_key": "raw/key.json", "run_id": "run-123"},
            None
        )

        self.assertEqual(result["processed_count"], 2)
        self.assertEqual(len(result["alerts"]), 1)
        self.assertEqual(result["alerts"][0]["asset_symbol"], "BTC")
        s3_client.put_object.assert_called_once()
        self.assertEqual(latest_prices_table.put_item.call_count, 2)
        pipeline_runs_table.update_item.assert_called_once()

        curated_payload = s3_client.put_object.call_args.kwargs["Body"].decode("utf-8").splitlines()
        first_record = json.loads(curated_payload[0])
        second_record = json.loads(curated_payload[1])
        self.assertEqual(first_record["asset_symbol"], "BTC")
        self.assertEqual(first_record["daily_change_pct"], 5.0)
        self.assertEqual(second_record["asset_symbol"], "ETH")
        self.assertIsNone(second_record["daily_change_pct"])


class SendAlertsTests(unittest.TestCase):
    def test_send_alerts_publishes_when_alerts_exist(self):
        sns_client = MagicMock()
        module = load_module(
            "test_send_alerts",
            "src/lambdas/send_alerts/app.py",
            {"ALERTS_TOPIC_ARN": "arn:aws:sns:alerts"},
            boto3_client=sns_client,
            boto3_resource=MagicMock()
        )

        result = module.lambda_handler(
            {
                "run_id": "run-123",
                "alerts": [
                    {
                        "asset_name": "Bitcoin",
                        "asset_symbol": "BTC",
                        "price_usd": 105.0,
                        "daily_change_pct": 5.0,
                        "daily_change_abs": 5.0
                    }
                ]
            },
            None
        )

        self.assertTrue(result["notification_sent"])
        sns_client.publish.assert_called_once()

    def test_send_alerts_skips_publish_when_no_alerts_exist(self):
        sns_client = MagicMock()
        module = load_module(
            "test_send_alerts_empty",
            "src/lambdas/send_alerts/app.py",
            {"ALERTS_TOPIC_ARN": "arn:aws:sns:alerts"},
            boto3_client=sns_client,
            boto3_resource=MagicMock()
        )

        result = module.lambda_handler({"run_id": "run-123", "alerts": []}, None)

        self.assertFalse(result["notification_sent"])
        sns_client.publish.assert_not_called()


class NotifyFailureTests(unittest.TestCase):
    def test_notify_failure_publishes_error_payload(self):
        sns_client = MagicMock()
        module = load_module(
            "test_notify_failure",
            "src/lambdas/notify_failure/app.py",
            {"FAILURES_TOPIC_ARN": "arn:aws:sns:failures"},
            boto3_client=sns_client,
            boto3_resource=MagicMock()
        )

        result = module.lambda_handler({"error": {"cause": "boom"}}, None)

        self.assertTrue(result["notification_sent"])
        sns_client.publish.assert_called_once()


if __name__ == "__main__":
    unittest.main()
