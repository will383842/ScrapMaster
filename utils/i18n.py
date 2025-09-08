# utils/i18n.py
KW = {
    "lawyer": {
        "fr": ["avocat", "cabinet d'avocats", "immigration", "contact", "email"],
        "en": ["lawyer", "law firm", "immigration", "contact", "email"],
        "th": ["ทนายความ", "สำนักงานกฎหมาย", "ตรวจคนเข้าเมือง", "ติดต่อ", "อีเมล"],
    },
    "translator": {
        "fr": ["traducteur", "interprète", "assermenté", "agence de traduction", "contact", "email"],
        "en": ["translator", "interpreter", "sworn translator", "translation agency", "contact", "email"],
        "th": ["นักแปล", "ล่าม", "รับรอง", "บริษัทแปลภาษา", "ติดต่อ", "อีเมล"],
    },
    "expat": {
        "fr": ["expatrié", "expat", "communauté", "groupe", "contact", "email"],
        "en": ["expat", "expatriate", "community", "group", "contact", "email"],
        "th": ["ชาวต่างชาติ", "ชุมชน", "กลุ่ม", "ติดต่อ", "อีเมล"],
    },
}

def keyword_bundle(profession: str, lang: str) -> list:
    p = (profession or "").strip().lower()
    l = (lang or "en").split("-")[0].lower()
    base = KW.get(p, {}).get(l) or KW.get(p, {}).get("en") or []
    # unique + ordre stable
    seen = set(); out = []
    for k in base:
        if k not in seen:
            seen.add(k); out.append(k)
    return out
