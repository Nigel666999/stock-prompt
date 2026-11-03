import importlib.util
import unittest
from pathlib import Path


from unittest.mock import patch


MODULE = Path(__file__).resolve().parents[1] / "scripts" / "doctor.py"
SPEC = importlib.util.spec_from_file_location("doctor", MODULE)
DOCTOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DOCTOR)


class DoctorTest(unittest.TestCase):
    def test_collect_status_reports_core_components(self):
        status = DOCTOR.collect_status()
        self.assertTrue(status["version_parity"]["ok"])
        self.assertEqual(status["skills"]["ready"], status["skills"]["total"])
        self.assertEqual(status["mcp"]["tool_count"], 21)
        self.assertIn("last_trading_day", status["calendar"])
        self.assertNotIn("network", status)

    def test_collect_status_with_ping_mocked(self):
        class MockResp:
            def read(self, n=-1):
                return b"test"
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass

        with patch("urllib.request.urlopen", return_value=MockResp()):
            status = DOCTOR.collect_status(check_ping=True)
            self.assertIn("network", status)
            net = status["network"]
            self.assertEqual(net["overall"], "ok")
            self.assertEqual(net["available_count"], 3)
            self.assertEqual(len(net["targets"]), 3)
            for t in net["targets"]:
                self.assertEqual(t["status"], "ok")
                self.assertIsNotNone(t["latency_ms"])
