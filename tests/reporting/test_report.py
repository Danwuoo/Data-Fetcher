import json
import os
import unittest
from reporting.report import ReportModule


class TestReportModule(unittest.TestCase):
    def setUp(self):
        self.run_id = "test_run"
        self.results = {
            "metrics": {
                "total_return": 0.1,
                "sharpe": 1.5,
                "sortino": 2.0,
                "max_drawdown": -0.05,
                "var_95": 0.02,
            },
            "slices": [
                {"slice_id": 0, "metrics": {"sharpe": 1.2, "max_drawdown": -0.03}},
                {"slice_id": 1, "metrics": {"sharpe": 1.8, "max_drawdown": -0.02}},
            ],
            "nav_series": [100, 101, 102, 101, 103],
            "drawdowns": [0, -0.01, -0.02, -0.01, 0],
        }
        self.strategy_name = "TestStrategy"
        self.hyperparams = {"param1": "value1", "param2": "value2"}
        self.report_module = ReportModule(
            self.run_id, self.results, self.strategy_name, self.hyperparams
        )

    def test_generate_json(self):
        json_output = self.report_module.generate_json()
        data = json.loads(json_output)
        self.assertEqual(data["run_id"], self.run_id)
        self.assertEqual(data["strategy"], self.strategy_name)
        self.assertEqual(data["hyperparams"], self.hyperparams)
        self.assertEqual(data["metrics"]["sharpe"], 1.5)
        self.assertEqual(len(data["cpcv_slices"]), 2)

    def test_generate_pdf(self):
        output_file = f"/tmp/{self.run_id}_test_report.pdf"
        self.report_module.generate_pdf(output_file)
        self.assertTrue(os.path.exists(output_file))
        os.remove(output_file)


if __name__ == "__main__":
    unittest.main()
