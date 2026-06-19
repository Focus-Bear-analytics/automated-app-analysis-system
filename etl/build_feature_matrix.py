# etl/build_feature_matrix.py
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import yaml


# ------------------------------------------------------------
# Taxonomy helpers
# ------------------------------------------------------------
def load_taxonomy_features(taxonomy_path: str) -> pd.DataFrame:
    """
    Loads nested app_taxonomy from taxonomy.yml and returns a flat dataframe:
    app_type, app_type_label, feature, feature_label, source
    """
    path = Path(taxonomy_path)

    if not path.exists():
        raise SystemExit(f"Taxonomy file not found: {taxonomy_path}")

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    app_taxonomy = data.get("app_taxonomy", {})

    if not isinstance(app_taxonomy, dict):
        raise SystemExit("taxonomy.yml must contain 'app_taxonomy:'")

    rows: List[Dict[str, Any]] = []

    for app_type, app_info in app_taxonomy.items():
        if not isinstance(app_info, dict):
            continue

        app_type_label = app_info.get("label", app_type)
        features = app_info.get("features", {})

        if not isinstance(features, dict):
            continue

        for feature_key, feature_info in features.items():
            if not isinstance(feature_info, dict):
                feature_info = {}

            rows.append(
                {
                    "app_type": app_type,
                    "app_type_label": app_type_label,
                    "feature": feature_key,
                    "feature_label": feature_info.get(
                        "label",
                        feature_key.replace("_", " ").title(),
                    ),
                    "source": feature_info.get("source", ""),
                }
            )

    if not rows:
        raise SystemExit("No taxonomy features found in taxonomy.yml.")

    return pd.DataFrame(rows)


# ------------------------------------------------------------
# General helpers
# ------------------------------------------------------------
def normalise_bool_series(series: pd.Series) -> pd.Series:
    """
    Converts mixed boolean-style values into 0/1 integers.
    """
    if series.dtype == bool:
        return series.astype(int)

    series_str = series.astype(str).str.strip().str.lower()

    true_values = {"true", "1", "yes", "y", "present"}
    false_values = {"false", "0", "no", "n", "absent", "nan", "none", ""}

    return series_str.apply(
        lambda x: 1 if x in true_values else 0 if x in false_values else 0
    ).astype(int)


def ensure_column(df: pd.DataFrame, col: str, default: Any = "") -> None:
    if col not in df.columns:
        df[col] = default


def combine_unique_text(values: pd.Series, max_chars: int = 800) -> str:
    items = []
    seen = set()

    for value in values.dropna().astype(str):
        value = value.strip()

        if not value or value.lower() in {"nan", "none", "null"}:
            continue

        if value not in seen:
            items.append(value)
            seen.add(value)

    return " | ".join(items)[:max_chars]


def first_non_empty(values: pd.Series) -> str:
    for value in values.dropna().astype(str):
        value = value.strip()

        if value and value.lower() not in {"nan", "none", "null"}:
            return value

    return ""


# ------------------------------------------------------------
# Load LLM output
# ------------------------------------------------------------
def load_features_from_llm_file(
    llm_csv: Path,
    taxonomy_df: pd.DataFrame,
    min_conf: float,
) -> pd.DataFrame:
    if not llm_csv.exists():
        raise SystemExit(f"LLM feature file not found: {llm_csv}")

    df = pd.read_csv(llm_csv)

    required = {
        "app_key",
        "app_type",
        "app_type_label",
        "feature",
        "feature_label",
        "llm_flag",
        "llm_confidence",
    }

    missing = required - set(df.columns)

    if missing:
        raise SystemExit(f"{llm_csv} missing required columns: {sorted(missing)}")

    approved_features = set(taxonomy_df["feature"].astype(str))

    df = df.copy()
    df["app_key"] = df["app_key"].astype(str)
    df["feature"] = df["feature"].astype(str).str.strip()

    before = len(df)
    df = df[df["feature"].isin(approved_features)].copy()
    after = len(df)

    if before != after:
        print(f"[llm] skipped {before - after} rows with non-approved feature names")

    # Standard fields
    df["confidence"] = pd.to_numeric(
        df["llm_confidence"],
        errors="coerce",
    ).fillna(0.0)

    df["flag"] = normalise_bool_series(df["llm_flag"])
    df.loc[df["confidence"] < min_conf, "flag"] = 0

    if "llm_evidence" in df.columns:
        df["evidence"] = df["llm_evidence"].fillna("").astype(str)
    else:
        df["evidence"] = ""

    if "source" not in df.columns:
        source_map = dict(zip(taxonomy_df["feature"], taxonomy_df["source"]))
        df["source"] = df["feature"].map(source_map).fillna("")

    # Optional fields from updated llm/feature_llm.py
    optional_defaults = {
        "routed_app_types": "",
        "llm_sub_code": "",
        "llm_cognitive_primary": "",
        "llm_cognitive_secondary": "",
        "llm_cognitive_tertiary": "",
        "llm_uncertain": False,
        "llm_uncertainty_note": "",
        "llm_core_cluster": False,
        "customisation_blacklist": False,
        "customisation_whitelist": False,
        "customisation_other_control": "",
        "llm_notes": "",
    }

    for col, default in optional_defaults.items():
        ensure_column(df, col, default)

    df["uncertain"] = normalise_bool_series(df["llm_uncertain"])
    df["core_cluster"] = normalise_bool_series(df["llm_core_cluster"])
    df["customisation_blacklist"] = normalise_bool_series(df["customisation_blacklist"])
    df["customisation_whitelist"] = normalise_bool_series(df["customisation_whitelist"])

    text_cols = [
        "routed_app_types",
        "llm_sub_code",
        "llm_cognitive_primary",
        "llm_cognitive_secondary",
        "llm_cognitive_tertiary",
        "llm_uncertainty_note",
        "customisation_other_control",
        "llm_notes",
    ]

    for col in text_cols:
        df[col] = df[col].fillna("").astype(str)

    out = df[
        [
            "app_key",
            "routed_app_types",
            "app_type",
            "app_type_label",
            "feature",
            "feature_label",
            "source",
            "flag",
            "confidence",
            "evidence",
            "llm_sub_code",
            "llm_cognitive_primary",
            "llm_cognitive_secondary",
            "llm_cognitive_tertiary",
            "uncertain",
            "llm_uncertainty_note",
            "core_cluster",
            "customisation_blacklist",
            "customisation_whitelist",
            "customisation_other_control",
            "llm_notes",
        ]
    ].copy()

    # Deduplicate safely if the LLM returned repeated feature rows.
    out = (
        out.groupby(
            [
                "app_key",
                "app_type",
                "app_type_label",
                "feature",
                "feature_label",
                "source",
            ],
            as_index=False,
        )
        .agg(
            {
                "routed_app_types": first_non_empty,
                "flag": "max",
                "confidence": "max",
                "evidence": lambda x: combine_unique_text(x, 800),
                "llm_sub_code": lambda x: combine_unique_text(x, 500),
                "llm_cognitive_primary": lambda x: combine_unique_text(x, 300),
                "llm_cognitive_secondary": lambda x: combine_unique_text(x, 300),
                "llm_cognitive_tertiary": lambda x: combine_unique_text(x, 300),
                "uncertain": "max",
                "llm_uncertainty_note": lambda x: combine_unique_text(x, 500),
                "core_cluster": "max",
                "customisation_blacklist": "max",
                "customisation_whitelist": "max",
                "customisation_other_control": lambda x: combine_unique_text(x, 500),
                "llm_notes": lambda x: combine_unique_text(x, 700),
            }
        )
    )

    return out


# ------------------------------------------------------------
# Output writers
# ------------------------------------------------------------
def write_outputs(
    long_df: pd.DataFrame,
    taxonomy_df: pd.DataFrame,
    out_dir: Path,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1. Long format with all evidence and metadata.
    long_df.to_csv(out_dir / "features_long.csv", index=False)

    # 2. Binary feature matrix.
    flags = (
        long_df.pivot_table(
            index="app_key",
            columns="feature",
            values="flag",
            aggfunc="max",
            fill_value=0,
        )
        .sort_index(axis=1)
    )

    # 3. Confidence matrix.
    confidence = (
        long_df.pivot_table(
            index="app_key",
            columns="feature",
            values="confidence",
            aggfunc="max",
            fill_value=0.0,
        )
        .sort_index(axis=1)
    )

    # 4. Core-cluster matrix for DSCT core-design analysis.
    core_cluster = (
        long_df.pivot_table(
            index="app_key",
            columns="feature",
            values="core_cluster",
            aggfunc="max",
            fill_value=0,
        )
        .sort_index(axis=1)
    )

    # 5. Uncertainty matrix.
    uncertainty = (
        long_df.pivot_table(
            index="app_key",
            columns="feature",
            values="uncertain",
            aggfunc="max",
            fill_value=0,
        )
        .sort_index(axis=1)
    )

    flags.to_csv(out_dir / "features_matrix_flags.csv")
    confidence.to_csv(out_dir / "features_matrix_confidence.csv")
    core_cluster.to_csv(out_dir / "features_matrix_core_clusters.csv")
    uncertainty.to_csv(out_dir / "features_matrix_uncertainty.csv")

    # 6. Feature summary.
    positive = long_df[long_df["flag"] == 1].copy()

    if not positive.empty:
        summary = (
            positive.groupby(
                ["app_type", "app_type_label", "feature", "feature_label", "source"],
                as_index=False,
            )
            .agg(
                num_apps=("app_key", "nunique"),
                avg_confidence=("confidence", "mean"),
                core_apps=("core_cluster", "sum"),
                uncertain_apps=("uncertain", "sum"),
            )
        )
    else:
        summary = pd.DataFrame(
            columns=[
                "app_type",
                "app_type_label",
                "feature",
                "feature_label",
                "source",
                "num_apps",
                "avg_confidence",
                "core_apps",
                "uncertain_apps",
            ]
        )

    all_features = taxonomy_df[
        ["app_type", "app_type_label", "feature", "feature_label", "source"]
    ].drop_duplicates()

    summary = all_features.merge(
        summary,
        on=["app_type", "app_type_label", "feature", "feature_label", "source"],
        how="left",
    )

    summary["num_apps"] = summary["num_apps"].fillna(0).astype(int)
    summary["avg_confidence"] = summary["avg_confidence"].fillna(0.0).round(3)
    summary["core_apps"] = summary["core_apps"].fillna(0).astype(int)
    summary["uncertain_apps"] = summary["uncertain_apps"].fillna(0).astype(int)

    summary = summary.sort_values(
        ["num_apps", "app_type", "feature"],
        ascending=[False, True, True],
    )

    summary.to_csv(out_dir / "features_summary.csv", index=False)

    # 7. App type summary.
    if not positive.empty:
        app_type_summary = (
            positive.groupby(["app_type", "app_type_label"], as_index=False)
            .agg(
                num_apps=("app_key", "nunique"),
                total_feature_hits=("feature", "count"),
                avg_confidence=("confidence", "mean"),
            )
            .sort_values("num_apps", ascending=False)
        )

        app_type_summary["avg_confidence"] = (
            app_type_summary["avg_confidence"].fillna(0.0).round(3)
        )
    else:
        app_type_summary = pd.DataFrame(
            columns=[
                "app_type",
                "app_type_label",
                "num_apps",
                "total_feature_hits",
                "avg_confidence",
            ]
        )

    app_type_summary.to_csv(out_dir / "features_app_type_summary.csv", index=False)

    # 8. DSCT core-design summary.
    core_positive = long_df[long_df["core_cluster"] == 1].copy()

    if not core_positive.empty:
        core_summary = (
            core_positive.groupby(
                ["app_type", "app_type_label", "feature", "feature_label", "source"],
                as_index=False,
            )["app_key"]
            .nunique()
            .rename(columns={"app_key": "num_core_apps"})
            .sort_values("num_core_apps", ascending=False)
        )
    else:
        core_summary = taxonomy_df[
            ["app_type", "app_type_label", "feature", "feature_label", "source"]
        ].copy()
        core_summary["num_core_apps"] = 0

    core_summary.to_csv(out_dir / "features_core_cluster_summary.csv", index=False)

    # 9. Customisation summary.
    customisation_summary = (
        long_df.groupby("app_key", as_index=False)
        .agg(
            routed_app_types=("routed_app_types", first_non_empty),
            customisation_blacklist=("customisation_blacklist", "max"),
            customisation_whitelist=("customisation_whitelist", "max"),
            customisation_other_control=(
                "customisation_other_control",
                lambda x: combine_unique_text(x, 500),
            ),
        )
        .sort_values("app_key")
    )

    customisation_summary.to_csv(out_dir / "features_customisation_summary.csv", index=False)

    # 10. Evidence review file.
    evidence_cols = [
        "app_key",
        "routed_app_types",
        "app_type",
        "app_type_label",
        "feature",
        "feature_label",
        "source",
        "flag",
        "confidence",
        "core_cluster",
        "uncertain",
        "llm_sub_code",
        "llm_cognitive_primary",
        "llm_cognitive_secondary",
        "llm_cognitive_tertiary",
        "evidence",
        "llm_uncertainty_note",
        "llm_notes",
    ]

    evidence_df = long_df[evidence_cols].copy()
    evidence_df.to_csv(out_dir / "features_evidence.csv", index=False)

    print(
        "[features-matrix] wrote: "
        "features_long.csv, "
        "features_summary.csv, "
        "features_app_type_summary.csv, "
        "features_core_cluster_summary.csv, "
        "features_customisation_summary.csv, "
        "features_evidence.csv, "
        "features_matrix_flags.csv, "
        "features_matrix_confidence.csv, "
        "features_matrix_core_clusters.csv, "
        "features_matrix_uncertainty.csv"
    )


def maybe_bundle(
    out_dir: Path,
    long_df: pd.DataFrame,
    apps_csv: str | None,
) -> None:
    apps_path = Path(apps_csv) if apps_csv else None

    if not apps_path or not apps_path.exists():
        for candidate in (
            out_dir / "apps_all_clean.csv",
            out_dir / "apps_clean.csv",
            out_dir / "apps_all.csv",
        ):
            if candidate.exists():
                apps_path = candidate
                break

    if not apps_path or not apps_path.exists():
        return

    apps = pd.read_csv(apps_path, dtype=str)

    keep_cols = [
        col
        for col in [
            "app_key",
            "store",
            "title",
            "rating_avg",
            "rating_count",
            "installs_or_users",
            "relevance_score",
        ]
        if col in apps.columns
    ]

    if "app_key" not in keep_cols:
        return

    bundle = long_df.merge(
        apps[keep_cols].drop_duplicates("app_key"),
        on="app_key",
        how="left",
    )

    bundle.to_csv(out_dir / "features_bundle.csv", index=False)
    print(f"[features-matrix] wrote bundle -> {out_dir / 'features_bundle.csv'}")


# ------------------------------------------------------------
# Main builder
# ------------------------------------------------------------
def build_matrices(
    in_dir: str,
    out_dir: str,
    taxonomy_path: str,
    llm_csv: str | None,
    min_conf: float,
    apps_csv: str | None,
    bundle_apps: bool,
) -> None:
    in_dir_path = Path(in_dir)
    out_dir_path = Path(out_dir)

    taxonomy_df = load_taxonomy_features(taxonomy_path)

    print("[features-matrix] approved taxonomy features:")
    for _, row in taxonomy_df.iterrows():
        print(f"  - {row['app_type']} / {row['feature']} ({row['feature_label']})")

    llm_path = Path(llm_csv) if llm_csv else in_dir_path / "features_llm.csv"

    long_df = load_features_from_llm_file(
        llm_csv=llm_path,
        taxonomy_df=taxonomy_df,
        min_conf=min_conf,
    )

    write_outputs(
        long_df=long_df,
        taxonomy_df=taxonomy_df,
        out_dir=out_dir_path,
    )

    if bundle_apps:
        maybe_bundle(out_dir_path, long_df, apps_csv)


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build feature matrices from Lyngs-based DSCT, Habit, and Planner "
            "LLM classification outputs."
        )
    )

    parser.add_argument("--in-dir", default="data/curated")
    parser.add_argument("--out-dir", default="data/curated")
    parser.add_argument("--taxonomy", default="llm/taxonomy.yml")
    parser.add_argument("--llm-csv", default=None)
    parser.add_argument("--min-confidence", type=float, default=0.0)

    parser.add_argument("--bundle-apps", action="store_true")
    parser.add_argument("--apps-csv", default=None)

    # Kept so your old command still works.
    parser.add_argument("--prefer-llm", action="store_true")

    args = parser.parse_args()

    build_matrices(
        in_dir=args.in_dir,
        out_dir=args.out_dir,
        taxonomy_path=args.taxonomy,
        llm_csv=args.llm_csv,
        min_conf=args.min_confidence,
        apps_csv=args.apps_csv,
        bundle_apps=args.bundle_apps,
    )


if __name__ == "__main__":
    main()