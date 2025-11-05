# etl/flatten_features.py
import argparse, json, re
from pathlib import Path
import pandas as pd

# ---------- small utils ----------
def coerce_text(x) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x)

def norm_feat(s: str) -> str:
    s = coerce_text(s).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def strip_code_fence(s: str) -> str:
    s = coerce_text(s).strip()
    # remove ```json ... ``` fences if present
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.I)
        s = re.sub(r"\s*```$", "", s)
    return s.strip()

def try_load_json(raw: str):
    s = strip_code_fence(coerce_text(raw))
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return None

# ---------- parsing helpers ----------
def collect_features(obj) -> list[str]:
    """
    Accepts a variety of shapes:
      1) {"features": ["A", "B", ...]}
      2) {"features": [{"name": "A", "support": "strong_support"}, ...]}
      3) Just a bare list: ["A","B"]
      4) {"items":[...]} (fallback)
    Returns a list of feature *strings* (keep original text).
    """
    if obj is None:
        return []

    # bare list
    if isinstance(obj, list):
        out = []
        for it in obj:
            if isinstance(it, str):
                out.append(it.strip())
            elif isinstance(it, dict):
                name = it.get("name") or it.get("feature") or it.get("feature_name")
                if name:
                    out.append(coerce_text(name).strip())
        return [f for f in out if f]

    # dict with "features"
    if isinstance(obj, dict):
        feats = obj.get("features")
        if isinstance(feats, list):
            out = []
            for it in feats:
                if isinstance(it, str):
                    out.append(it.strip())
                elif isinstance(it, dict):
                    name = it.get("name") or it.get("feature") or it.get("feature_name")
                    if name:
                        out.append(coerce_text(name).strip())
            return [f for f in out if f]
        # fallback: sometimes the LLM might put them under another key
        items = obj.get("items")
        if isinstance(items, list):
            out = []
            for it in items:
                if isinstance(it, str):
                    out.append(it.strip())
                elif isinstance(it, dict):
                    name = it.get("name") or it.get("feature") or it.get("feature_name")
                    if name:
                        out.append(coerce_text(name).strip())
            return [f for f in out if f]

    return []

def collect_support_map(obj) -> dict:
    """
    Build a mapping {normalized_feature: support_label} from many shapes:
      A) {"goldilocks_support": {"feature x": "strong_support", ...}}
      B) {"goldilocks_support": [{"feature":"x","label":"..."}, ...]}
      C) features entries inline with support: {"features":[{"name":"x","support":"..."}, ...]}
      D) single global support: {"support":"neutral"}
    If no support is found for a feature, caller will default to "neutral".
    """
    mapping = {}

    if not isinstance(obj, (dict, list)):
        return mapping

    def add_pair(name, label):
        name = coerce_text(name).strip()
        label = coerce_text(label).strip() or "neutral"
        if name:
            mapping[norm_feat(name)] = label

    # A / B
    if isinstance(obj, dict) and "goldilocks_support" in obj:
        gs = obj.get("goldilocks_support")
        if isinstance(gs, dict):
            for k, v in gs.items():
                add_pair(k, v)
        elif isinstance(gs, list):
            for it in gs:
                if isinstance(it, dict):
                    nm = it.get("feature") or it.get("name") or it.get("feature_name")
                    lab = it.get("label") or it.get("support")
                    if nm and lab:
                        add_pair(nm, lab)

    # C: support inline in features list
    feats = obj.get("features") if isinstance(obj, dict) else None
    if isinstance(feats, list):
        for it in feats:
            if isinstance(it, dict):
                nm = it.get("name") or it.get("feature") or it.get("feature_name")
                lab = it.get("support") or it.get("label")
                if nm and lab:
                    add_pair(nm, lab)

    # D: single global support
    if isinstance(obj, dict) and "support" in obj and isinstance(obj.get("support"), str):
        mapping["*global*"] = obj["support"].strip()

    return mapping

def pick_json_cell(row: pd.Series) -> str:
    """Find the column that contains the LLM JSON blob."""
    for c in ("features_json", "llm_json", "response_json", "json", "raw_json"):
        if c in row and coerce_text(row[c]).strip():
            return coerce_text(row[c])
    # some pipelines might store the whole model text in 'answer'
    for c in ("answer", "response", "model_output"):
        if c in row and coerce_text(row[c]).strip():
            return coerce_text(row[c])
    return ""

# ---------- main row parser ----------
def parse_row(row):
    raw = pick_json_cell(row)
    obj = try_load_json(raw)
    if obj is None:
        return []

    features = collect_features(obj)
    support_map = collect_support_map(obj)

    out = []
    for f in features:
        f_norm = norm_feat(f)
        sup = support_map.get(f_norm)
        if not sup:
            sup = support_map.get("*global*", "neutral")
        out.append({
            "app_key": row.get("app_key"),
            "title": row.get("title"),
            "vendor": row.get("vendor"),
            "model": row.get("model"),
            "feature": f,
            "feature_norm": f_norm,
            "support": sup,
        })

    # If nothing came through, keep a placeholder so app appears downstream
    if not out:
        out.append({
            "app_key": row.get("app_key"),
            "title": row.get("title"),
            "vendor": row.get("vendor"),
            "model": row.get("model"),
            "feature": "",
            "feature_norm": "",
            "support": "neutral",
        })
    return out

# ---------- CLI ----------
def main(in_csv: str, out_csv: str):
    df = pd.read_csv(in_csv, dtype=str)
    rows = []
    for _, r in df.iterrows():
        rows.extend(parse_row(r))
    out = pd.DataFrame(rows).drop_duplicates(["app_key","feature_norm"])
    out_path = Path(out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    print(f"[flatten] wrote {len(out)} rows -> {out_path}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", default="data/curated/features.csv")
    ap.add_argument("--out", default="data/curated/features_flat.csv")
    args = ap.parse_args()
    main(args.inp, args.out)
