# scrapers/discovery/filtering.py
def keep_title_or_desc(title: str, desc: str, include_terms: list[str], exclude_terms: list[str]) -> bool:
    text = f"{title or ''} {desc or ''}".lower()
    if any(x in text for x in exclude_terms):
        return False
    return any(x in text for x in include_terms)
