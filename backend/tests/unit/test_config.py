import unittest

from internal.app.config import AppConfig, load_config


class ConfigTests(unittest.TestCase):
    def test_load_config_uses_defaults(self) -> None:
        config = load_config({})

        self.assertEqual(config, AppConfig(app_name="zemledar-api", app_env="dev", log_level="INFO"))

    def test_load_config_reads_values_from_mapping(self) -> None:
        config = load_config({"APP_NAME": "api-test", "APP_ENV": "prod", "LOG_LEVEL": "DEBUG"})

        self.assertEqual(config.app_name, "api-test")
        self.assertEqual(config.app_env, "prod")
        self.assertEqual(config.log_level, "DEBUG")

    def test_is_production_property(self) -> None:
        self.assertTrue(AppConfig(app_env="prod").is_production)
        self.assertFalse(AppConfig(app_env="dev").is_production)


if __name__ == "__main__":
    unittest.main()
