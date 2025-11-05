import os, time
from typing import Dict, List, Any

class NotConfigured(Exception): ...

def have_openai() -> bool:
    return bool(os.environ.get("OPENAI_API_KEY"))

def have_gemini() -> bool:
    return bool(os.environ.get("GEMINI_API_KEY"))

# Lazy imports so code loads without libs when skipping
def _openai_client():
    import openai  # pip install openai
    openai.api_key = os.environ["OPENAI_API_KEY"]
    return openai

def _gemini_client():
    import google.generativeai as genai  # pip install google-generativeai
    genai.configure(api_key=os.environ["GEMINI_API_KEY"])
    return genai

def classify_with_openai(text: str, taxonomy: Dict[str, Any], model: str="gpt-4o-mini") -> Dict[str, Dict[str, Any]]:
    """
    Return: {feature: {present: bool, confidence: float}}
    """
    openai = _openai_client()
    prompt = f"""You are a precise product analyst.
Taxonomy: {taxonomy}
Given this app text, output JSON with each feature -> {{present: true/false, confidence: 0..1}}.
App text (truncated to 6K chars):
{text[:6000]}"""
    # Chat Completions (minimal, robust)
    resp = openai.chat.completions.create(
        model=model,
        messages=[{"role":"user","content":prompt}],
        temperature=0,
        response_format={"type": "json_object"},
    )
    import json
    try:
        return json.loads(resp.choices[0].message.content)
    except Exception:
        return {}

def classify_with_gemini(text: str, taxonomy: Dict[str, Any], model: str="gemini-1.5-flash") -> Dict[str, Dict[str, Any]]:
    genai = _gemini_client()
    prompt = f"""You are a precise product analyst.
Taxonomy: {taxonomy}
Given this app text, output JSON with each feature -> {{present: true/false, confidence: 0..1}}.
App text (truncated to 6K chars):
{text[:6000]}"""
    mdl = genai.GenerativeModel(model)
    out = mdl.generate_content(prompt)
    import json
    try:
        return json.loads(out.text)
    except Exception:
        return {}
