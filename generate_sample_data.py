from pathlib import Path
import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

OUTPUT_FILE = DATA_DIR / "sample_risk_data.csv"

# 3 months of daily data
start_date = "2025-01-01"
end_date = "2025-03-31"
portfolios = ["PORT_A", "PORT_B", "PORT_C"]
metrics = ["P50", "P95", "SPREAD", "DELTA", "VOL"]

rng = pd.date_range(start_date, end_date, freq="D")
rows = []

np.random.seed(42)

for date in rng:
    for port in portfolios:
        base_level = {
            "PORT_A": 40,
            "PORT_B": 35,
            "PORT_C": 45,
        }[port]

        p50 = base_level + np.random.normal(0, 3)
        p95 = p50 + np.random.normal(10, 2)
        spread = abs(np.random.normal(6, 1))
        delta = np.random.normal(0, 2)
        vol = abs(np.random.normal(12, 3))

        metric_values = {
            "P50": p50,
            "P95": p95,
            "SPREAD": spread,
            "DELTA": delta,
            "VOL": vol,
        }

        for m in metrics:
            value = metric_values[m]
            if np.random.rand() < 0.03:  # ~3% missing values
                value = np.nan

            rows.append(
                {
                    "date": date.strftime("%Y-%m-%d"),
                    "portfolio": port,
                    "metric": m,
                    "value": value,
                }
            )

df = pd.DataFrame(rows)
df = df.sample(frac=1.0, random_state=42).reset_index(drop=True)

df.to_csv(OUTPUT_FILE, index=False)
print(f"Generated {len(df)} rows in {OUTPUT_FILE}")
