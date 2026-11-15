from datetime import date
from unittest import TestCase
from unittest.mock import patch

import pandas as pd
import pipeline as p


class MorningQuality(TestCase):
    def test_previous_close_requires_matching_session(self):
        session = date(2026, 11, 13)
        bars = pd.DataFrame({"Close": [100.0]}, index=pd.to_datetime(["2026-11-12"]))
        with patch.object(p.yf, "download", return_value=bars):
            with self.assertRaisesRegex(RuntimeError, "daily close"):
                p.previous_close(session)
            bars.index = pd.to_datetime([session])
            self.assertEqual(p.previous_close(session), 100.0)

    def test_timeout_and_missing_values(self):
        today = date(2026, 11, 16)
        chain = [dict(call_sym="C", put_sym="P", call_occ="call", put_occ="put",
                      exp_date=today.isoformat(), strike=100)]
        self.enterContext(patch.object(p, "target_expirations", return_value=[]))
        self.enterContext(patch.object(p, "prior_trading_day", return_value=date(2026, 11, 13)))
        self.enterContext(patch.object(p, "previous_close", return_value=100))
        self.enterContext(patch.object(p, "load_chain_for_pipeline", return_value=chain))
        feed = self.enterContext(patch.object(p, "DXLinkSnapshot")).return_value
        auth = dict(access_token="fixture", streamer_url="fixture", streamer_token="fixture")
        feed.wait_for_oi.return_value = False
        with self.assertRaisesRegex(RuntimeError, "coverage"):
            p.fetch_via_dxlink(today, auth)
        feed.stop.assert_called_once()
        feed.get_state.assert_not_called()
        feed.wait_for_oi.return_value = True
        feed.get_state.return_value = {"C": {"oi": 0}}
        rows = p.fetch_via_dxlink(today, auth)
        self.assertEqual([r["OpenInterest"] for r in rows], [0, None])
