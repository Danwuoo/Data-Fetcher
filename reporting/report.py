import json
import matplotlib.pyplot as plt
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Image, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet


class ReportModule:
    def __init__(self, run_id, results, strategy_name="Strategy", hyperparams=None):
        if hyperparams is None:
            hyperparams = {}
        self.run_id = run_id
        self.results = results
        self.strategy_name = strategy_name
        self.hyperparams = hyperparams

    def generate_json(self):
        metrics = self.results.get('metrics', {})
        cpcv_slices = self.results.get('slices', [])

        report_data = {
            "run_id": self.run_id,
            "strategy": self.strategy_name,
            "hyperparams": self.hyperparams,
            "metrics": {
                "total_return": metrics.get('total_return', 0),
                "sharpe": metrics.get('sharpe', 0),
                "sortino": metrics.get('sortino', 0),
                "max_drawdown": metrics.get('max_drawdown', 0),
                "var_95": metrics.get('var_95', 0),
            },
            "cpcv_slices": [
                {
                    "slice": s['slice_id'],
                    "sharpe": s['metrics']['sharpe'],
                    "max_dd": s['metrics']['max_drawdown']
                } for s in cpcv_slices
            ]
        }
        return json.dumps(report_data, indent=2)

    def generate_pdf(self, output_file):
        doc = SimpleDocTemplate(output_file, pagesize=letter)
        styles = getSampleStyleSheet()
        story = []

        story.append(Paragraph(f"Report for Run ID: {self.run_id}", styles['h1']))
        story.append(Paragraph(f"Strategy: {self.strategy_name}", styles['h2']))
        story.append(Paragraph(f"Hyperparameters: {json.dumps(self.hyperparams)}", styles['BodyText']))

        # Add metrics
        metrics = self.results.get('metrics', {})
        for key, value in metrics.items():
            story.append(Paragraph(f"<b>{key.replace('_', ' ').title()}:</b> {value:.4f}", styles['BodyText']))

        # Add equity curve plot
        if 'nav_series' in self.results:
            plt.figure()
            plt.plot(self.results['nav_series'])
            plt.title("Equity Curve")
            plt.xlabel("Time")
            plt.ylabel("NAV")
            img_path = f"/tmp/{self.run_id}_equity_curve.png"
            plt.savefig(img_path)
            plt.close()
            story.append(Image(img_path, width=400, height=200))

        # Add drawdown chart
        if 'drawdowns' in self.results:
            plt.figure()
            plt.plot(self.results['drawdowns'])
            plt.title("Drawdown")
            plt.xlabel("Time")
            plt.ylabel("Drawdown")
            img_path = f"/tmp/{self.run_id}_drawdown.png"
            plt.savefig(img_path)
            plt.close()
            story.append(Image(img_path, width=400, height=200))

        doc.build(story)
