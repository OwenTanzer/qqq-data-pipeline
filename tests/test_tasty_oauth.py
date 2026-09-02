import inspect
import os
import sys
import threading
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

try:
    import requests  # noqa: F401
except ModuleNotFoundError:
    requests = types.ModuleType("requests")
    requests.Response = type("Response", (), {})
    requests.post = Mock()
    requests.get = Mock()
    sys.modules["requests"] = requests

import tasty_oauth


class FakeResponse:
    def __init__(self, status_code, payload=None, headers=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.headers = headers or {}

    def json(self):
        return self._payload


def oauth_response(access_token="access-secret", expires_in=900, token_type="Bearer"):
    return FakeResponse(200, {
        "access_token": access_token,
        "expires_in": expires_in,
        "token_type": token_type,
    })


def quote_response():
    return FakeResponse(200, {
        "data": {
            "token": "stream-secret",
            "dxlink-url": "wss://example.invalid/realtime",
        }
    })


class TastyOAuthTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {
            "TASTY_OAUTH_CLIENT_SECRET": "client-secret-value",
            "TASTY_OAUTH_REFRESH_TOKEN": "refresh-token-value",
            "TASTY_OAUTH_SCOPES": "read",
        }, clear=False)
        self.env.start()
        self.addCleanup(self.env.stop)

    def manager(self, *, clock=lambda: 0.0):
        return tasty_oauth.OAuthTokenManager(
            "client-secret-value", "refresh-token-value", "read", clock=clock
        )

    @patch.object(tasty_oauth.requests, "get")
    @patch.object(tasty_oauth.requests, "post")
    def test_refresh_and_quote_token_use_bearer_auth(self, post, get):
        post.return_value = oauth_response()
        get.return_value = quote_response()

        auth = tasty_oauth.tasty_auth(self.manager())

        self.assertEqual(auth["access_token"], "access-secret")
        self.assertEqual(auth["streamer_token"], "stream-secret")
        self.assertEqual(get.call_args.kwargs["headers"]["Authorization"], "Bearer access-secret")

    @patch.object(tasty_oauth.requests, "get")
    @patch.object(tasty_oauth.requests, "post")
    def test_quote_token_401_forces_exactly_one_refresh(self, post, get):
        post.side_effect = [oauth_response("access-one"), oauth_response("access-two")]
        get.side_effect = [FakeResponse(401, {"error": {"code": "unauthorized"}}), quote_response()]

        auth = tasty_oauth.tasty_auth(self.manager())

        self.assertEqual(auth["access_token"], "access-two")
        self.assertEqual(post.call_count, 2)
        self.assertEqual(get.call_count, 2)

    def test_scope_must_be_exactly_read(self):
        for scopes in ("", "trade", "read trade", "read,openid"):
            with self.subTest(scopes=scopes), self.assertRaises(tasty_oauth.TastyAuthError):
                tasty_oauth.OAuthTokenManager("client", "refresh", scopes)

    @patch.object(tasty_oauth.requests, "get")
    @patch.object(tasty_oauth.requests, "post")
    def test_rejected_refresh_fails_closed(self, post, get):
        post.return_value = FakeResponse(401, {"error": {"code": "invalid-grant"}})

        with self.assertRaises(tasty_oauth.TastyAuthError) as raised:
            tasty_oauth.tasty_auth(self.manager())

        self.assertEqual(raised.exception.phase, "oauth-token")
        get.assert_not_called()

    @patch.object(tasty_oauth.requests, "post")
    def test_malformed_oauth_success_fails_closed(self, post):
        for response in (
            FakeResponse(200, {}),
            oauth_response(access_token=""),
            oauth_response(expires_in=30),
            oauth_response(token_type="MAC"),
        ):
            with self.subTest(payload=response._payload):
                post.reset_mock()
                post.return_value = response
                with self.assertRaises(tasty_oauth.TastyAuthError):
                    self.manager().get_access_token()

    @patch.object(tasty_oauth.requests, "post")
    def test_access_token_refresh_is_single_flight(self, post):
        post.return_value = oauth_response("shared-access")
        manager = self.manager()
        barrier = threading.Barrier(8)
        results = []

        def worker():
            barrier.wait()
            results.append(manager.get_access_token())

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=2)

        self.assertEqual(results, ["shared-access"] * 8)
        self.assertEqual(post.call_count, 1)

    def test_diagnostics_redact_secrets(self):
        response = FakeResponse(400, {
            "message": "secret client-secret-value token refresh-token-value",
            "token": "must-not-be-logged",
            "error": {"reason": "refresh-token-value expired"},
        })

        diagnostic = tasty_oauth._safe_broker_error(response)

        self.assertNotIn("client-secret-value", diagnostic)
        self.assertNotIn("refresh-token-value", diagnostic)
        self.assertNotIn("must-not-be-logged", diagnostic)

    def test_production_code_contains_no_legacy_session_fallback(self):
        source = inspect.getsource(tasty_oauth)
        pipeline_source = (Path(__file__).parents[1] / "pipeline.py").read_text()
        for legacy in (
            '/sessions',
            "TASTY_LOGIN",
            "TASTY_PASSWORD",
            "TASTY_TOTP_SECRET",
            "TASTY_REMEMBER_TOKEN",
        ):
            self.assertNotIn(legacy, source)
            self.assertNotIn(legacy, pipeline_source)


if __name__ == "__main__":
    unittest.main()
