import json
import socket
import threading
import unittest
from datetime import datetime
from urllib.error import HTTPError
from urllib.request import urlopen

from api.main import build_app_config, create_server, main


class ApiHealthTests(unittest.TestCase):
    def setUp(self) -> None:
        self.server = create_server(host="127.0.0.1", port=0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def test_health_endpoint_returns_expected_payload(self) -> None:
        port = self.server.server_address[1]
        url = f"http://127.0.0.1:{port}/health"
        with urlopen(url, timeout=2) as response:
            self.assertEqual(response.status, 200)
            payload = json.loads(response.read().decode("utf-8"))

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["service"], "zemledar-api")
        self.assertEqual(payload["environment"], "dev")
        parsed_timestamp = datetime.fromisoformat(payload["timestamp"])
        self.assertIsNotNone(parsed_timestamp.tzinfo)

    def test_unknown_route_returns_not_found(self) -> None:
        port = self.server.server_address[1]
        url = f"http://127.0.0.1:{port}/missing"
        with self.assertRaises(HTTPError) as error:
            urlopen(url, timeout=2)

        self.assertEqual(error.exception.code, 404)
        payload = json.loads(error.exception.read().decode("utf-8"))
        self.assertEqual(payload, {"error": "Маршрут не найден", "status": 404})

    def test_empty_request_line_is_ignored(self) -> None:
        port = self.server.server_address[1]
        sock = socket.create_connection(("127.0.0.1", port), timeout=2)
        sock.shutdown(socket.SHUT_WR)
        sock.close()

        url = f"http://127.0.0.1:{port}/health"
        with urlopen(url, timeout=2) as response:
            self.assertEqual(response.status, 200)


class ApiMainTests(unittest.TestCase):
    def test_build_app_config_defaults(self) -> None:
        config = build_app_config()

        self.assertEqual(config.app_name, "zemledar-api")
        self.assertEqual(config.app_env, "dev")

    def test_main_starts_server(self) -> None:
        class FakeServer:
            def __init__(self) -> None:
                self.started = False

            def serve_forever(self) -> None:
                self.started = True

        fake_server = FakeServer()

        original_create_server = create_server
        try:
            import api.main as main_module

            main_module.create_server = lambda: fake_server
            main()
        finally:
            import api.main as main_module

            main_module.create_server = original_create_server

        self.assertTrue(fake_server.started)


if __name__ == "__main__":
    unittest.main()
