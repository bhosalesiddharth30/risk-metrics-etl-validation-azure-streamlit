"""
Validation engine:
- Loads cleaned data from data/processed
- Applies rules from config/validation_rules.yaml
- Computes null fractions & range checks
- Produces an overall quality flag
- Writes a validation report as CSV + prints summary
"""

from pathlib import Path
import pandas as pd
import yaml
from utils.logger import logger
from etl.risk_etl_cleaner import run_etl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = PROJECT_ROOT / "config" / "validation_rules.yaml"
REPORTS_DIR = PROJECT_ROOT / "data" / "reports"
REPORTS_DIR.mkdir(exist_ok=True)


def load_rules():
    with open(CONFIG_PATH, "r") as f:
        rules = yaml.safe_load(f)
    return rules


def validate(df: pd.DataFrame, rules: dict) -> pd.DataFrame:
    required_cols = rules["columns"]["required"]
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    results = []

    total_rows = len(df)
    null_fraction_total = df["value"].isna().mean()

    for metric, mrules in rules["metrics"].items():
        m_df = df[df["metric"] == metric]
        if m_df.empty:
            continue

        null_fraction = m_df["value"].isna().mean()
        min_val = mrules.get("min_value", None)

        within_min = True
        if min_val is not None:
            within_min = (m_df["value"].dropna() >= min_val).all()

        allowed_null = mrules.get("max_null_fraction", 1.0)
        ok_nulls = null_fraction <= allowed_null

        results.append(
            {
                "metric": metric,
                "null_fraction": round(null_fraction, 4),
                "nulls_ok": ok_nulls,
                "min_value_ok": within_min,
                "row_count": len(m_df),
            }
        )

    results_df = pd.DataFrame(results)

    # overall flag based on total null fraction
    g = rules["flags"]["green_max_null_fraction"]
    y = rules["flags"]["yellow_max_null_fraction"]
    if null_fraction_total <= g:
        overall_flag = "GREEN"
    elif null_fraction_total <= y:
        overall_flag = "YELLOW"
    else:
        overall_flag = "RED"

    logger.info(
        f"Validation finished: overall_flag={overall_flag}, "
        f"null_fraction_total={null_fraction_total:.4f}"
    )

    results_df["overall_null_fraction"] = round(null_fraction_total, 4)
    results_df["overall_flag"] = overall_flag

    return results_df


def main():
    rules = load_rules()
    cleaned_file = run_etl()
    df = pd.read_csv(cleaned_file, parse_dates=["date"])

    report_df = validate(df, rules)
    report_path = REPORTS_DIR / "validation_report.csv"
    report_df.to_csv(report_path, index=False)

    print("Validation report:")
    print(report_df)
    print(f"\nReport saved to: {report_path}")


if __name__ == "__main__":
    main()
