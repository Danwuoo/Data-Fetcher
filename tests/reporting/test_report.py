import json
import os
import unittest
from pathlib import Path
import pdfplumber
import matplotlib.pyplot as plt

from backtest_data_module.reporting.report import ReportGen, plot_equity_curve, plot_drawdown, plot_return_histogram


class TestReportGen(unittest.TestCase):
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
            "returns": [0.01, 0.01, -0.01, 0.02],
        }
        self.strategy_name = "TestStrategy"
        self.hyperparams = {"param1": "value1", "param2": "value2"}
        self.report_gen = ReportGen(
            self.run_id, self.results, self.strategy_name, self.hyperparams
        )
        self.output_dir = Path("/tmp/test_reports")
        self.output_dir.mkdir(exist_ok=True)

    def tearDown(self):
        for f in self.output_dir.glob("*"):
            os.remove(f)
        os.rmdir(self.output_dir)

    def test_generate_json(self):
        json_output = self.report_gen.generate_json()
        data = json.loads(json_output)
        self.assertEqual(data["run_id"], self.run_id)
        self.assertEqual(data["strategy"], self.strategy_name)
        self.assertEqual(data["hyperparams"], self.hyperparams)
        self.assertEqual(data["metrics"]["sharpe"], 1.5)
        self.assertEqual(len(data["cpcv_slices"]), 2)

    def test_generate_pdf(self):
        output_file = self.output_dir / f"{self.run_id}_test_report.pdf"
        self.report_gen.generate_pdf(output_file)
        self.assertTrue(output_file.exists())

        with pdfplumber.open(output_file) as pdf:
            self.assertGreater(len(pdf.pages), 0)
            page = pdf.pages[0]
            text = page.extract_text()
            self.assertIn(self.run_id, text)
            self.assertIn(self.strategy_name, text)
            self.assertIn("Sharpe", text)

    def test_generate_pdf_from_figures(self):
        output_file = self.output_dir / f"{self.run_id}_test_report_from_figures.pdf"

        # Create some dummy figures
        fig1, ax1 = plt.subplots()
        ax1.plot([1,2,3])

        fig2, ax2 = plt.subplots()
        ax2.plot([4,5,6])

        chart_figures = [fig1, fig2]

        self.report_gen.generate_pdf_from_figures(output_file, chart_figures, self.results["metrics"])
        self.assertTrue(output_file.exists())

        with pdfplumber.open(output_file) as pdf:
            # Cover page + 2 chart pages
            self.assertEqual(len(pdf.pages), 3)
            cover_page = pdf.pages[0]
            text = cover_page.extract_text()
            self.assertIn(self.run_id, text)
            self.assertIn("Sharpe", text)

    def test_plot_functions(self):
        nav_path = self.output_dir / "nav.png"
        drawdown_path = self.output_dir / "drawdown.png"
        hist_path = self.output_dir / "hist.png"

        plot_equity_curve(self.results["nav_series"], nav_path)
        self.assertTrue(nav_path.exists())

        plot_drawdown(self.results["drawdowns"], drawdown_path)
        self.assertTrue(drawdown_path.exists())

        plot_return_histogram(self.results["returns"], hist_path)
        self.assertTrue(hist_path.exists())

    def test_generate_arrow(self):
        import pyarrow as pa
        import io

        # Create some dummy figures
        fig1, ax1 = plt.subplots()
        ax1.plot([1, 2, 3])

        fig2, ax2 = plt.subplots()
        ax2.plot([4, 5, 6])

        chart_figures = [fig1, fig2]

        arrow_data = self.report_gen.generate_arrow(chart_figures, self.results["metrics"])
        self.assertIsInstance(arrow_data, bytes)

        reader = pa.ipc.open_stream(io.BytesIO(arrow_data))
        batch = reader.read_next_batch()

        self.assertEqual(batch.num_rows, 1)
        data = batch.to_pydict()

        self.assertEqual(data['run_id'][0], self.run_id)
        self.assertEqual(data['strategy_name'][0], self.strategy_name)
        self.assertEqual(json.loads(data['hyperparams'][0]), self.hyperparams)
        self.assertEqual(json.loads(data['metrics'][0]), self.results['metrics'])
        self.assertEqual(len(data['charts'][0]), 2)


if __name__ == "__main__":
    unittest.main()
