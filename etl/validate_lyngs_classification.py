# etl/validate_lyngs_classification.py
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def normalise_bool(value):
    """
    Convert different true/false formats into 1 or 0.
    """
    if isinstance(value, bool):
        return int(value)

    value = str(value).strip().lower()

    if value in {"true", "1", "yes", "y", "present"}:
        return 1

    return 0


def load_ground_truth(path: str) -> pd.DataFrame:
    """
    Load the original Lyngs ground-truth validation file.

    Required columns:
    - app_key
    - feature
    - expected_flag
    """
    gt_path = Path(path)

    if not gt_path.exists():
        raise SystemExit(f"Ground-truth file not found: {path}")

    gt = pd.read_csv(gt_path)

    required = {"app_key", "feature", "expected_flag"}
    missing = required - set(gt.columns)

    if missing:
        raise SystemExit(
            f"{path} missing required columns: {sorted(missing)}\n"
            "Required columns: app_key, feature, expected_flag"
        )

    gt = gt.copy()
    gt["app_key"] = gt["app_key"].astype(str).str.strip()
    gt["feature"] = gt["feature"].astype(str).str.strip()
    gt["expected_flag"] = gt["expected_flag"].apply(normalise_bool)

    return gt


def load_predictions(path: str, min_confidence: float) -> pd.DataFrame:
    """
    Load LLM predictions from features_llm.csv.

    Required columns:
    - app_key
    - feature
    - llm_flag
    - llm_confidence
    """
    pred_path = Path(path)

    if not pred_path.exists():
        raise SystemExit(f"Prediction file not found: {path}")

    pred = pd.read_csv(pred_path)

    required = {"app_key", "feature", "llm_flag", "llm_confidence"}
    missing = required - set(pred.columns)

    if missing:
        raise SystemExit(
            f"{path} missing required columns: {sorted(missing)}\n"
            "Required columns: app_key, feature, llm_flag, llm_confidence"
        )

    pred = pred.copy()
    pred["app_key"] = pred["app_key"].astype(str).str.strip()
    pred["feature"] = pred["feature"].astype(str).str.strip()
    pred["llm_flag"] = pred["llm_flag"].apply(normalise_bool)
    pred["llm_confidence"] = pd.to_numeric(
        pred["llm_confidence"],
        errors="coerce",
    ).fillna(0.0)

    # If confidence is lower than the threshold, treat prediction as not present.
    pred.loc[pred["llm_confidence"] < min_confidence, "llm_flag"] = 0

    if "llm_evidence" not in pred.columns:
        pred["llm_evidence"] = ""

    pred = (
        pred.groupby(["app_key", "feature"], as_index=False)
        .agg(
            predicted_flag=("llm_flag", "max"),
            max_confidence=("llm_confidence", "max"),
            evidence=(
                "llm_evidence",
                lambda x: " | ".join(
                    str(v) for v in x.dropna().astype(str).unique()
                )[:500],
            ),
        )
    )

    return pred


def validate(
    ground_truth_csv: str,
    predictions_csv: str,
    out_dir: str,
    min_confidence: float,
) -> None:
    """
    Compare original Lyngs labels with LLM-generated labels.
    """
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    gt = load_ground_truth(ground_truth_csv)
    pred = load_predictions(predictions_csv, min_confidence)

    results = gt.merge(
        pred,
        on=["app_key", "feature"],
        how="left",
    )

    results["predicted_flag"] = results["predicted_flag"].fillna(0).astype(int)
    results["max_confidence"] = results["max_confidence"].fillna(0.0)
    results["evidence"] = results["evidence"].fillna("No LLM evidence found.")

    results["match"] = results["expected_flag"] == results["predicted_flag"]

    total = len(results)
    matches = int(results["match"].sum())
    mismatches = results[results["match"] == False].copy()
    match_rate = matches / total if total else 0.0

    summary = pd.DataFrame(
        [
            {
                "total_validation_rows": total,
                "matches": matches,
                "mismatches": int(len(mismatches)),
                "match_rate": round(match_rate, 4),
                "min_confidence": min_confidence,
            }
        ]
    )

    feature_summary = (
        results.groupby("feature", as_index=False)
        .agg(
            total=("app_key", "count"),
            matches=("match", "sum"),
        )
    )

    feature_summary["match_rate"] = (
        feature_summary["matches"] / feature_summary["total"]
    ).round(4)

    results.to_csv(out_path / "validation_results.csv", index=False)
    mismatches.to_csv(out_path / "validation_mismatches.csv", index=False)
    summary.to_csv(out_path / "validation_summary.csv", index=False)
    feature_summary.to_csv(out_path / "validation_by_feature.csv", index=False)

    print("[validation] complete")
    print(f"[validation] total rows: {total}")
    print(f"[validation] matches: {matches}")
    print(f"[validation] mismatches: {len(mismatches)}")
    print(f"[validation] match rate: {match_rate:.2%}")
    print(f"[validation] wrote -> {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Validate LLM feature classification against the original "
            "Lyngs et al. DSCT coding labels."
        )
    )

    parser.add_argument(
        "--ground-truth",
        required=True,
        help="CSV with columns: app_key, feature, expected_flag",
    )

    parser.add_argument(
        "--predictions",
        default="data/curated/features_llm.csv",
        help="LLM output CSV from llm/feature_llm.py",
    )

    parser.add_argument(
        "--out-dir",
        default="data/curated/validation",
        help="Output folder for validation results",
    )

    parser.add_argument(
        "--min-confidence",
        type=float,
        default=0.0,
        help="Minimum LLM confidence required to count a positive prediction",
    )

    args = parser.parse_args()

    validate(
        ground_truth_csv=args.ground_truth,
        predictions_csv=args.predictions,
        out_dir=args.out_dir,
        min_confidence=args.min_confidence,
    )


if __name__ == "__main__":
    main()