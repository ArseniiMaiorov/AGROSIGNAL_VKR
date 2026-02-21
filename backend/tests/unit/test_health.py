import unittest
from datetime import datetime

from internal.app.config import AppConfig
from internal.app.health import build_health_payload


class HealthTests(unittest.TestCase):
    def test_build_health_payload_structure_and_values(self) -> None:
        payload = build_health_payload(AppConfig(app_name="zemledar", app_env="test"))

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["service"], "zemledar")
        self.assertEqual(payload["environment"], "test")

        parsed_timestamp = datetime.fromisoformat(payload["timestamp"])
        self.assertIsNotNone(parsed_timestamp.tzinfo)


if __name__ == "__main__":
    unittest.main()
