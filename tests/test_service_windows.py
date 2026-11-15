import os
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch
import pipeline


class CollectionWindows(unittest.TestCase):
    def run_job(self, service, hour, dates=(), exists=False):
        clock = MagicMock()
        clock.now.return_value = pipeline._ET.localize(datetime(2026, 11, 16, hour))
        env = dict(RAILWAY_SERVICE_NAME=service, R2_ACCOUNT_ID="x", R2_ACCESS_KEY_ID="x",
                   R2_SECRET_ACCESS_KEY="x", R2_BUCKET_NAME="x")
        self.enterContext(patch.dict(os.environ, env))
        self.enterContext(patch.object(pipeline, "datetime", clock))
        for name in ("_load_calendar", "make_r2", "fetch_intraday", "fetch_via_dxlink", "tasty_auth", "upload_df", "save_manifest"):
            setattr(self, name, self.enterContext(patch.object(pipeline, name)))
        self.enterContext(patch.object(pipeline, "is_trading_day", return_value=True))
        self.enterContext(patch.object(pipeline, "load_manifest", return_value={"dates": dates}))
        self.enterContext(patch.object(pipeline, "_r2_key_exists", return_value=exists))
        pipeline.main()

    def test_evening_cannot_replace_missing_morning(self):
        with self.assertRaisesRegex(RuntimeError, "require the morning chain"):
            self.run_job("qqq-prices-pipeline", 17)
        self.fetch_via_dxlink.assert_not_called()
        self.upload_df.assert_not_called()

    def test_deployment_outside_window(self):
        self.run_job("qqq-data-pipeline", 17)
        self.make_r2.assert_not_called()

    def test_existing_morning_archive(self):
        self.run_job("qqq-data-pipeline", 7, ["2026-11-16"], True)
        self.fetch_intraday.assert_not_called()
        self.upload_df.assert_not_called()
