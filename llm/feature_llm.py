# llm/feature_llm.py
from __future__ import annotations

import argparse
import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import yaml


# ------------------------------------------------------------
# CSV helpers
# ------------------------------------------------------------
def _load_csv(path: str, required_cols: List[str]) -> pd.DataFrame:
    df = pd.read_csv(path)

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise SystemExit(f"{path} missing columns: {missing}")

    return df


def _take(text: str | float | None, max_chars: int) -> str:
    if not isinstance(text, str):
        return ""

    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_chars]


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _normalise_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        return value.strip().lower() in {"true", "yes", "1", "present", "y"}

    return bool(value)


# ------------------------------------------------------------
# Taxonomy helpers
# ------------------------------------------------------------
def load_taxonomy(taxonomy_path: str) -> Dict[str, Any]:
    path = Path(taxonomy_path)

    if not path.exists():
        raise SystemExit(f"Taxonomy file not found: {taxonomy_path}")

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    app_taxonomy = data.get("app_taxonomy", {})

    if not isinstance(app_taxonomy, dict):
        raise SystemExit("taxonomy.yml must contain a dictionary called 'app_taxonomy:'")

    return app_taxonomy


def flatten_taxonomy(app_taxonomy: Dict[str, Any]) -> List[Dict[str, Any]]:
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
                    "definition": feature_info.get("def", ""),
                    "positive": feature_info.get("positive", []),
                    "negative": feature_info.get("negative", []),
                }
            )

    if not rows:
        raise SystemExit(
            "No features found in app_taxonomy. "
            "Check that taxonomy.yml has app_taxonomy -> dsct/habit_apps/planners -> features."
        )

    return rows


def allowed_feature_keys(flat_features: List[Dict[str, Any]]) -> List[str]:
    return [item["feature"] for item in flat_features]


def feature_lookup(flat_features: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {item["feature"]: item for item in flat_features}


def features_for_app_type(
    flat_features: List[Dict[str, Any]],
    app_type: str,
) -> List[Dict[str, Any]]:
    return [item for item in flat_features if item["app_type"] == app_type]


def build_taxonomy_prompt_block(flat_features: List[Dict[str, Any]]) -> str:
    lines: List[str] = []

    for item in flat_features:
        lines.append(f"- Feature key: {item['feature']}")
        lines.append(f"  Feature label: {item['feature_label']}")
        lines.append(f"  Source: {item.get('source', '')}")

        if item.get("definition"):
            lines.append(f"  Definition: {item['definition']}")

        positive = item.get("positive", [])
        if positive:
            lines.append("  Positive examples: " + "; ".join(str(x) for x in positive[:12]))

        negative = item.get("negative", [])
        if negative:
            lines.append("  Negative examples: " + "; ".join(str(x) for x in negative[:10]))

        lines.append("")

    return "\n".join(lines).strip()


# ------------------------------------------------------------
# App context
# ------------------------------------------------------------
def build_app_context(
    app: Dict[str, Any],
    web_map: Dict[str, str],
    reviews_map: Dict[str, List[str]],
) -> Dict[str, Any]:
    app_key = app["app_key"]

    return {
        "app_key": app_key,
        "title": _take(str(app.get("title", "")), 180),
        "description": _take(str(app.get("description", "")), 3000),
        "website_excerpt": _take(web_map.get(app_key, ""), 4000),
        "sample_reviews": [_take(t, 700) for t in reviews_map.get(app_key, [])[:6]],
    }


def context_as_text(ctx: Dict[str, Any]) -> str:
    return f"""
App key:
{ctx["app_key"]}

Title:
{ctx["title"]}

Description:
{ctx["description"]}

Website excerpt:
{ctx["website_excerpt"]}

Sample user reviews:
{json.dumps(ctx["sample_reviews"], ensure_ascii=False)}
""".strip()


# ------------------------------------------------------------
# App type routing
# ------------------------------------------------------------
def route_app_types(ctx: Dict[str, Any], available_app_types: List[str]) -> List[str]:
    """
    Routes each app to the correct prompt area.

    - DSCT apps should use Lyngs et al.'s DSCT rules.
    - Habit trackers should use a separate habit prompt.
    - Planners should use a separate planner prompt.

    An app can be routed to more than one area.
    """
    text = " ".join(
        [
            ctx.get("title", ""),
            ctx.get("description", ""),
            ctx.get("website_excerpt", ""),
            " ".join(ctx.get("sample_reviews", [])),
        ]
    ).lower()

    selected: List[str] = []

    dsct_terms = [
        "block",
        "blocked",
        "blocking",
        "website blocker",
        "app blocker",
        "site blocker",
        "distraction",
        "distracting",
        "screen time",
        "screentime",
        "digital wellbeing",
        "digital well-being",
        "self-control",
        "self control",
        "focus mode",
        "stay focused",
        "focus session",
        "focus timer",
        "pomodoro",
        "limit app",
        "time limit",
        "whitelist",
        "blacklist",
        "prevent access",
        "social media blocker",
        "internet blocker",
        "phone addiction",
        "reduce phone use",
        "digital detox",
    ]

    habit_terms = [
        "habit",
        "habits",
        "routine",
        "routines",
        "streak",
        "streaks",
        "daily goal",
        "habit tracker",
        "habit tracking",
        "consistency",
        "check-in",
        "check in",
        "daily reminder",
        "build habits",
        "goal tracker",
        "habit log",
        "habit analytics",
    ]

    planner_terms = [
        "planner",
        "planning",
        "task",
        "tasks",
        "to-do",
        "todo",
        "calendar",
        "schedule",
        "scheduling",
        "time blocking",
        "time-blocking",
        "project",
        "projects",
        "priority",
        "priorities",
        "deadline",
        "deadlines",
        "kanban",
        "notes",
        "agenda",
        "task manager",
        "project planning",
    ]

    if "dsct" in available_app_types and any(term in text for term in dsct_terms):
        selected.append("dsct")

    if "habit_apps" in available_app_types and any(term in text for term in habit_terms):
        selected.append("habit_apps")

    if "planners" in available_app_types and any(term in text for term in planner_terms):
        selected.append("planners")

    if not selected:
        if "dsct" in available_app_types:
            selected.append("dsct")
        elif available_app_types:
            selected.append(available_app_types[0])

    return selected


# ------------------------------------------------------------
# Prompt builders
# ------------------------------------------------------------
LYNGS_DSCT_CORE_KEYS = {
    "block_removal",
    "self_tracking",
    "goal_advancement",
    "reward_punishment",
}







def build_dsct_prompt(
    dsct_features: List[Dict[str, Any]],
    ctx: Dict[str, Any],
) -> str:
    """
    DSCT prompt based on Lyngs et al.'s taxonomy.

    The four Lyngs core clusters are:
    - block_removal
    - self_tracking
    - goal_advancement
    - reward_punishment

    Extra project-specific DSCT features are included separately as extensions.
    """
    feature_keys = [item["feature"] for item in dsct_features]
    feature_key_text = ", ".join(feature_keys)

    extension_features = [
        item for item in dsct_features if item["feature"] not in LYNGS_DSCT_CORE_KEYS
    ]

    extension_block = ""
    if extension_features:
        extension_block = f"""
PROJECT-SPECIFIC DSCT EXTENSION FEATURES
These are not additional Lyngs headline clusters. Code them only when explicitly evidenced, and do not use them to replace the four Lyngs clusters.

{build_taxonomy_prompt_block(extension_features)}
""".strip()

    return f"""
ROLE
You are a coding assistant for a structured review of Digital Self-Control Tools (DSCTs): apps and browser extensions designed to help people self-regulate their use of digital devices.

Your job is to classify the supplied tool using Lyngs et al.'s DSCT taxonomy. Follow the coding rules carefully. Do not invent categories. If evidence is insufficient, say so rather than guessing.

IMPORTANT
Use this DSCT prompt only for digital self-control tools. Treat the supplied app description, website text, and reviews as the only evidence. Do not assume features that are not described.

APPROVED DSCT FEATURE KEYS
Return one result for every approved DSCT feature key listed below:
{feature_key_text}

LYNGS ET AL. DSCT TAXONOMY: FOUR CORE FEATURE CLUSTERS

1. block_removal
Block / Removal:
- Block access entirely
- Set a time limit before blocking
- Set a launch-count limit before blocking
- Add a time lag before the distraction loads
- Disallow stopping a session once started
- Require an effortful task or password to override
- Use admin permissions to prevent uninstalling
- Add a time lag before override/settings change
- Require payment to override
- Remove elements from specific sites, such as hiding a feed or inbox
- Strip distracting content while browsing
- Replace new-tab content
- Minimal writing modes that remove irrelevant functionality
- Limit functionality available on a device home screen

2. self_tracking
Self-Tracking:
- Record usage history
- Visualise captured usage data
- Display a timer or countdown
- Track time spent not using the device

3. goal_advancement
Goal-Advancement:
- Remind users of a concrete time goal or task
- Show pop-ups, to-do lists, or new-tab task lists
- Remind users of general goals or values
- Ask users to set explicit goals, such as time goals or task goals
- Let users compare actual behaviour against the goals they set

4. reward_punishment
Reward / Punishment:
- Points or streaks
- Leaderboards or social sharing
- Achievement unlocks
- Lifeform rewards/punishments, such as a tree that dies
- Real-world reward or punishment, such as losing money or earning discounts

CROSS-CUTTING CUSTOMISATION DIMENSION
Code separately if mentioned:
- Blacklist: user blocks specified apps or sites and allows everything else
- Whitelist: user allows only specified apps or sites and blocks everything else
- Other control over what is targeted

{extension_block}

CLASSIFICATION RULES
R1. Code at the feature level first. List each distinct feature and assign it to a cluster.
R2. Core-design rule. Tag a Lyngs cluster as core only if roughly 25 percent or more of the tool's described functionality relates to that cluster.
R3. Cognitive-mechanism mapping. For each DSCT feature, map it to the most direct cognitive component:
    - Prevent non-conscious habits
    - Scaffold non-conscious habits
    - Conscious goals and self-monitoring
    - Expected value of control: Reward
    - Expected value of control: Delay
    - Expected value of control: Expectancy
R4. Evidence and uncertainty. Use only the supplied evidence. If ambiguous, mark uncertain true and explain why.
R5. Return one output item for every approved DSCT feature key, even if false.
R6. Do not invent feature names.
R7. Output valid JSON only.

STRICT MATCHING RULES BASED ON VALIDATION AGAINST THE ORIGINAL LYNGS CODING
R8. Be conservative when matching the original Lyngs coding. Do not mark a feature as present just because it is loosely related. Only mark it true when the feature directly matches the Lyngs cluster definition.
R9. Do not classify a focus timer, Pomodoro timer, countdown, or focus session as goal_advancement by itself. Classify it as self_tracking only if it displays, records, or visualises time/session data. Classify it as goal_advancement only if it explicitly reminds the user of a task, goal, value, or lets them compare behaviour against a goal.
R10. Do not classify reward/punishment tools such as virtual trees, plants, coins, streaks, or badges as block_removal unless the tool actually blocks access to apps/websites or removes distracting content. If the user can still access distractions but receives a consequence, classify it as reward_punishment.
R11. Do not classify scheduling, app limits, usage rules, or planned sessions as goal_advancement unless the tool explicitly reminds users of goals or tasks. Scheduling restrictions usually belong under block_removal or the project extension feature scheduling.
R12. Do not classify payment to override as reward_punishment. In Lyngs-style coding, payment to override is an override-friction sub-code under block_removal.
R13. Do not classify general productivity wording, such as "focus", "productive", "work better", or "stay focused", as goal_advancement unless there is clear evidence of goal reminders, task reminders, explicit goal setting, or goal comparison.
R14. Do not classify analytics, reports, or usage dashboards as goal_advancement unless the app compares actual usage against explicit user-set goals. Otherwise, classify these under self_tracking.
R15. For validation consistency, prefer false when evidence is weak or indirect. Use uncertain=true when the description suggests a possible feature but does not clearly prove it.


APP INFORMATION
{context_as_text(ctx)}

OUTPUT FORMAT
Return valid JSON only:

{{
  "app_key": "{ctx["app_key"]}",
  "app_area": "dsct",
  "features": [
    {{
      "name": "block_removal",
      "flag": true,
      "confidence": 0.90,
      "sub_code": "Block access entirely",
      "cognitive_mapping": {{
        "primary": "Prevent non-conscious habits",
        "secondary": null,
        "tertiary": null
      }},
      "evidence": "Short evidence from input",
      "uncertain": false,
      "uncertainty_note": null
    }},
    {{
      "name": "self_tracking",
      "flag": false,
      "confidence": 0.50,
      "sub_code": null,
      "cognitive_mapping": {{
        "primary": null,
        "secondary": null,
        "tertiary": null
      }},
      "evidence": "No clear usage tracking evidence is mentioned.",
      "uncertain": false,
      "uncertainty_note": null
    }}
  ],
  "customisation": {{
    "blacklist": false,
    "whitelist": false,
    "other_control": null
  }},
  "core_clusters": ["block_removal"],
  "notes": "Coder notes or missing-evidence notes"
}}
""".strip()

def build_habit_prompt(
    habit_features: List[Dict[str, Any]],
    ctx: Dict[str, Any],
) -> str:
    taxonomy_block = build_taxonomy_prompt_block(habit_features)
    feature_keys = ", ".join([item["feature"] for item in habit_features])

    return f"""
ROLE
You are a coding assistant for analysing habit tracking and routine-building applications.

Your task is to classify the supplied app using the approved habit-app taxonomy only.

IMPORTANT
Habit tracker apps are not always digital self-control tools. Do not force habit tracker features into Lyngs et al.'s DSCT categories unless DSCT features are explicitly present and handled by the DSCT prompt separately.

APPROVED HABIT FEATURE KEYS
Use only these feature keys:
{feature_keys}

APPROVED HABIT TAXONOMY DETAILS
{taxonomy_block}

CLASSIFICATION RULES
1. Return one result for every approved habit feature key.
2. If a feature is clearly present, set flag true.
3. If a feature is absent or weakly evidenced, set flag false.
4. Do not classify a feature as true just because the app is generally about productivity.
5. Evidence must come only from the supplied app information.
6. If unsure, set flag false, confidence around 0.50, and mark uncertain true.
7. Do not invent feature names.
8. Output valid JSON only.

APP INFORMATION
{context_as_text(ctx)}

OUTPUT FORMAT
Return valid JSON only:

{{
  "app_key": "{ctx["app_key"]}",
  "app_area": "habit_apps",
  "features": [
    {{
      "name": "habit_tracking",
      "flag": true,
      "confidence": 0.90,
      "sub_code": "Daily habit tracking",
      "evidence": "Short evidence from input",
      "uncertain": false,
      "uncertainty_note": null
    }}
  ],
  "notes": "Coder notes or missing-evidence notes"
}}
""".strip()


def build_planner_prompt(
    planner_features: List[Dict[str, Any]],
    ctx: Dict[str, Any],
) -> str:
    taxonomy_block = build_taxonomy_prompt_block(planner_features)
    feature_keys = ", ".join([item["feature"] for item in planner_features])

    return f"""
ROLE
You are a coding assistant for analysing planner, task-management, calendar, and productivity-planning applications.

Your task is to classify the supplied app using the approved planner taxonomy only.

IMPORTANT
Planner apps are not always digital self-control tools. Do not force planner features into Lyngs et al.'s DSCT categories unless DSCT features are explicitly present and handled by the DSCT prompt separately.

APPROVED PLANNER FEATURE KEYS
Use only these feature keys:
{feature_keys}

APPROVED PLANNER TAXONOMY DETAILS
{taxonomy_block}

CLASSIFICATION RULES
1. Return one result for every approved planner feature key.
2. If a feature is clearly present, set flag true.
3. If a feature is absent or weakly evidenced, set flag false.
4. Do not classify a feature as true just because the app is generally about productivity.
5. Evidence must come only from the supplied app information.
6. If unsure, set flag false, confidence around 0.50, and mark uncertain true.
7. Do not invent feature names.
8. Output valid JSON only.

APP INFORMATION
{context_as_text(ctx)}

OUTPUT FORMAT
Return valid JSON only:

{{
  "app_key": "{ctx["app_key"]}",
  "app_area": "planners",
  "features": [
    {{
      "name": "task_management",
      "flag": true,
      "confidence": 0.90,
      "sub_code": "Task list or to-do planning",
      "evidence": "Short evidence from input",
      "uncertain": false,
      "uncertainty_note": null
    }}
  ],
  "notes": "Coder notes or missing-evidence notes"
}}
""".strip()


def build_prompt_for_app_type(
    app_type: str,
    flat_features: List[Dict[str, Any]],
    ctx: Dict[str, Any],
) -> str:
    app_features = features_for_app_type(flat_features, app_type)

    if not app_features:
        raise ValueError(f"No taxonomy features found for app_type={app_type}")

    if app_type == "dsct":
        return build_dsct_prompt(app_features, ctx)

    if app_type == "habit_apps":
        return build_habit_prompt(app_features, ctx)

    if app_type == "planners":
        return build_planner_prompt(app_features, ctx)

    taxonomy_block = build_taxonomy_prompt_block(app_features)
    feature_keys = ", ".join([item["feature"] for item in app_features])

    return f"""
You are a careful taxonomy classification assistant.

Classify the app using only the approved feature keys for app type: {app_type}

APPROVED FEATURE KEYS
{feature_keys}

APPROVED TAXONOMY DETAILS
{taxonomy_block}

RULES
1. Return one result for every approved feature key.
2. Use only supplied evidence.
3. Do not invent categories.
4. Output valid JSON only.

APP INFORMATION
{context_as_text(ctx)}

OUTPUT FORMAT
{{
  "app_key": "{ctx["app_key"]}",
  "app_area": "{app_type}",
  "features": [
    {{
      "name": "feature_key_here",
      "flag": true,
      "confidence": 0.90,
      "sub_code": "string or null",
      "evidence": "Short evidence from input",
      "uncertain": false,
      "uncertainty_note": null
    }}
  ],
  "notes": "Coder notes"
}}
""".strip()


# ------------------------------------------------------------
# OpenAI-compatible API call
# ------------------------------------------------------------
def call_chat_completions(
    base_url: str,
    api_key: str,
    model: str,
    prompt: str,
    temperature: float = 0.0,
    max_tokens: int = 3200,
) -> str:
    url = base_url.rstrip("/") + "/v1/chat/completions"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": (
                 "You are a careful, conservative taxonomy classification assistant. "
                 "Use only approved feature keys. "
                 "Do not invent categories. "
                 "Use direct evidence only. "
                 "Apply the general category-boundary rules carefully. "
                 "Prefer false when evidence is weak, indirect, vague, or only loosely related. "
                 "Return valid JSON only."
                ),
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    response = requests.post(url, headers=headers, json=payload, timeout=120)
    response.raise_for_status()

    return response.json()["choices"][0]["message"]["content"]


def _extract_json(text: str) -> dict:
    text = text.strip()

    try:
        return json.loads(text)
    except Exception:
        pass

    text = re.sub(r"^```(?:json)?", "", text.strip(), flags=re.I)
    text = re.sub(r"```$", "", text.strip())

    try:
        return json.loads(text)
    except Exception:
        pass

    start = text.find("{")
    end = text.rfind("}")

    if start != -1 and end != -1 and end > start:
        return json.loads(text[start : end + 1])

    raise ValueError("LLM did not return valid JSON")


def _normalise_feature_name(item: Dict[str, Any]) -> str:
    name = item.get("name")
    if not name:
        name = item.get("cluster")
    return _safe_str(name)


def _get_cognitive_mapping(item: Dict[str, Any]) -> Dict[str, Optional[str]]:
    mapping = item.get("cognitive_mapping", {})

    if not isinstance(mapping, dict):
        mapping = {}

    return {
        "primary": mapping.get("primary"),
        "secondary": mapping.get("secondary"),
        "tertiary": mapping.get("tertiary"),
    }


def _empty_customisation() -> Dict[str, Any]:
    return {
        "blacklist": False,
        "whitelist": False,
        "other_control": None,
    }


def _normalise_customisation(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return _empty_customisation()

    return {
        "blacklist": _normalise_bool(value.get("blacklist", False)),
        "whitelist": _normalise_bool(value.get("whitelist", False)),
        "other_control": value.get("other_control"),
    }


# ------------------------------------------------------------
# Main LLM labelling
# ------------------------------------------------------------
def run_llm_labeling(
    apps_csv: str,
    web_csv: str,
    reviews_csv: str,
    out_csv: str,
    flat_features: List[Dict[str, Any]],
    base_url: str,
    api_key: str,
    model: str,
    batch: int = 12,
    sleep_s: float = 0.6,
    dry: bool = False,
    app_type_mode: str = "auto",
) -> None:
    apps = _load_csv(apps_csv, ["app_key", "title", "description"])
    web = _load_csv(web_csv, ["app_key", "website_text"])
    reviews = _load_csv(reviews_csv, ["app_key", "body"])

    web_map = (
        web.groupby("app_key")["website_text"]
        .apply(lambda s: " ".join(s.dropna().astype(str)))
        .to_dict()
    )

    reviews_map = (
        reviews.groupby("app_key")["body"]
        .apply(lambda s: s.dropna().astype(str).tolist())
        .to_dict()
    )

    feature_keys = allowed_feature_keys(flat_features)
    lookup = feature_lookup(flat_features)
    available_app_types = sorted(set(item["app_type"] for item in flat_features))

    rows = []

    for i, app_row in enumerate(apps.itertuples(index=False), start=1):
        app_dict = app_row._asdict()
        app_key = app_dict["app_key"]

        ctx = build_app_context(app_dict, web_map, reviews_map)

        if app_type_mode == "auto":
            selected_app_types = route_app_types(ctx, available_app_types)
        elif app_type_mode == "all":
            selected_app_types = available_app_types
        else:
            if app_type_mode not in available_app_types:
                raise SystemExit(
                    f"--app-type must be one of: auto, all, {', '.join(available_app_types)}"
                )
            selected_app_types = [app_type_mode]

        selected_feature_keys = {
            item["feature"]
            for item in flat_features
            if item["app_type"] in selected_app_types
        }

        app_rows = []
        seen = set()

        if dry:
            print("=" * 90)
            print(f"[DRY] App: {app_key}")
            print(f"[DRY] Routed app types: {', '.join(selected_app_types)}")
            print("=" * 90)

        for app_type in selected_app_types:
            try:
                prompt = build_prompt_for_app_type(app_type, flat_features, ctx)

                if dry:
                    print()
                    print("-" * 90)
                    print(f"[DRY] Prompt for app_type={app_type}")
                    print("-" * 90)
                    print(prompt[:7000])
                    print()
                    continue

                text = call_chat_completions(
                    base_url=base_url,
                    api_key=api_key,
                    model=model,
                    prompt=prompt,
                )

                obj = _extract_json(text)
                returned_features = obj.get("features", [])

                if not isinstance(returned_features, list):
                    raise ValueError("JSON field 'features' must be a list")

                customisation = _normalise_customisation(obj.get("customisation"))
                core_clusters = obj.get("core_clusters", [])

                if not isinstance(core_clusters, list):
                    core_clusters = []

                notes = _safe_str(obj.get("notes"))

                for item in returned_features:
                    if not isinstance(item, dict):
                        continue

                    name = _normalise_feature_name(item)

                    if name not in feature_keys:
                        continue

                    if name not in selected_feature_keys:
                        continue

                    meta = lookup[name]
                    cognitive = _get_cognitive_mapping(item)

                    is_core_cluster = name in core_clusters

                    app_rows.append(
                        {
                            "app_key": app_key,
                            "routed_app_types": ";".join(selected_app_types),
                            "app_type": meta["app_type"],
                            "app_type_label": meta["app_type_label"],
                            "feature": name,
                            "feature_label": meta["feature_label"],
                            "source": meta.get("source", ""),
                            "llm_flag": _normalise_bool(item.get("flag")),
                            "llm_confidence": _safe_float(item.get("confidence"), 0.0),
                            "llm_sub_code": _safe_str(item.get("sub_code")),
                            "llm_cognitive_primary": cognitive.get("primary"),
                            "llm_cognitive_secondary": cognitive.get("secondary"),
                            "llm_cognitive_tertiary": cognitive.get("tertiary"),
                            "llm_evidence": _safe_str(item.get("evidence")),
                            "llm_uncertain": _normalise_bool(item.get("uncertain", False)),
                            "llm_uncertainty_note": _safe_str(item.get("uncertainty_note")),
                            "llm_core_cluster": is_core_cluster,
                            "customisation_blacklist": customisation["blacklist"],
                            "customisation_whitelist": customisation["whitelist"],
                            "customisation_other_control": customisation["other_control"],
                            "llm_notes": notes,
                        }
                    )

                    seen.add(name)

            except Exception as e:
                print(f"[llm-features] ERROR for {app_key} / {app_type}: {e}")

                for item in features_for_app_type(flat_features, app_type):
                    feature = item["feature"]

                    app_rows.append(
                        {
                            "app_key": app_key,
                            "routed_app_types": ";".join(selected_app_types),
                            "app_type": item["app_type"],
                            "app_type_label": item["app_type_label"],
                            "feature": feature,
                            "feature_label": item["feature_label"],
                            "source": item.get("source", ""),
                            "llm_flag": False,
                            "llm_confidence": 0.0,
                            "llm_sub_code": "",
                            "llm_cognitive_primary": None,
                            "llm_cognitive_secondary": None,
                            "llm_cognitive_tertiary": None,
                            "llm_evidence": f"Skipped due to error: {e}",
                            "llm_uncertain": True,
                            "llm_uncertainty_note": str(e),
                            "llm_core_cluster": False,
                            "customisation_blacklist": False,
                            "customisation_whitelist": False,
                            "customisation_other_control": None,
                            "llm_notes": f"Error during {app_type} classification.",
                        }
                    )

                    seen.add(feature)

        if dry:
            continue

        for feature in feature_keys:
            if feature in seen:
                continue

            meta = lookup[feature]

            if feature in selected_feature_keys:
                evidence = "Feature not returned by LLM."
                confidence = 0.0
            else:
                evidence = "Feature not classified because this app was not routed to that app area."
                confidence = 0.0

            app_rows.append(
                {
                    "app_key": app_key,
                    "routed_app_types": ";".join(selected_app_types),
                    "app_type": meta["app_type"],
                    "app_type_label": meta["app_type_label"],
                    "feature": feature,
                    "feature_label": meta["feature_label"],
                    "source": meta.get("source", ""),
                    "llm_flag": False,
                    "llm_confidence": confidence,
                    "llm_sub_code": "",
                    "llm_cognitive_primary": None,
                    "llm_cognitive_secondary": None,
                    "llm_cognitive_tertiary": None,
                    "llm_evidence": evidence,
                    "llm_uncertain": False,
                    "llm_uncertainty_note": "",
                    "llm_core_cluster": False,
                    "customisation_blacklist": False,
                    "customisation_whitelist": False,
                    "customisation_other_control": None,
                    "llm_notes": "",
                }
            )
            
        #app_rows = apply_lyngs_validation_overrides(app_key, app_rows)

        rows.extend(app_rows)
        print(
            f"[llm-features] labelled {i}/{len(apps)}: {app_key} "
            f"using {', '.join(selected_app_types)}"
        )

        if i % batch == 0:
            time.sleep(sleep_s)

                

    if dry:
        print("[llm-features] dry run complete. No CSV written.")
        return

    Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out_csv, index=False)

    print(f"[llm-features] wrote -> {out_csv}")


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "LLM feature extraction using separate prompts for "
            "Lyngs-based DSCT apps, habit trackers, and planners."
        )
    )

    parser.add_argument("--apps", required=True, help="Path to apps CSV")
    parser.add_argument("--web", required=True, help="Path to websites CSV")
    parser.add_argument("--reviews", required=True, help="Path to reviews CSV")

    parser.add_argument(
        "--out",
        default="data/curated/features_llm.csv",
        help="Output CSV path",
    )

    parser.add_argument(
        "--taxonomy",
        default="llm/taxonomy.yml",
        help="Path to taxonomy YAML file",
    )

    parser.add_argument("--model", default="gpt-4.1-mini")

    parser.add_argument(
        "--base-url",
        default=os.environ.get("OPENAI_BASE_URL", "https://api.openai.com"),
    )

    parser.add_argument(
        "--api-key",
        default=os.environ.get("OPENAI_API_KEY", ""),
    )

    parser.add_argument("--batch", type=int, default=12)
    parser.add_argument("--sleep", type=float, default=0.6)
    parser.add_argument("--dry", action="store_true")

    parser.add_argument(
        "--app-type",
        default="auto",
        help=(
            "Which prompt to run: auto, all, dsct, habit_apps, planners. "
            "Default is auto."
        ),
    )

    args = parser.parse_args()

    taxonomy = load_taxonomy(args.taxonomy)
    flat_features = flatten_taxonomy(taxonomy)

    if not args.api_key and not args.dry:
        raise SystemExit("No API key. Set OPENAI_API_KEY or use --dry")

    print("[llm-features] using approved taxonomy features:")
    for item in flat_features:
        print(f"  - {item['app_type']} / {item['feature']} ({item['feature_label']})")

    run_llm_labeling(
        apps_csv=args.apps,
        web_csv=args.web,
        reviews_csv=args.reviews,
        out_csv=args.out,
        flat_features=flat_features,
        base_url=args.base_url,
        api_key=args.api_key,
        model=args.model,
        batch=args.batch,
        sleep_s=args.sleep,
        dry=args.dry,
        app_type_mode=args.app_type,
    )


if __name__ == "__main__":
    main()