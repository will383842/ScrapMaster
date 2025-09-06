
import re, unicodedata, urllib.parse
from typing import List, Dict, Optional

# Optional deps (graceful fallback)
try:
    import phonenumbers  # type: ignore
except Exception:  # pragma: no cover
    phonenumbers = None  # type: ignore

try:
    from langdetect import detect  # type: ignore
except Exception:  # pragma: no cover
    detect = None  # type: ignore

THAI_RE = re.compile(r'[\u0E00-\u0E7F]')
CYRILLIC_RE = re.compile(r'[\u0400-\u04FF]')
ARABIC_RE = re.compile(r'[\u0600-\u06FF]')
HEBREW_RE = re.compile(r'[\u0590-\u05FF]')
CJK_RE = re.compile(r'[\u4E00-\u9FFF]')
HIRAGANA_KATAKANA_RE = re.compile(r'[\u3040-\u30FF]')
HANGUL_RE = re.compile(r'[\uAC00-\uD7AF]')
DEVANAGARI_RE = re.compile(r'[\u0900-\u097F]')

def normalize_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    url = url.strip()
    if not url:
        return None
    # ensure scheme
    if not re.match(r'^[a-z]+://', url, re.I):
        url = 'https://' + url.lstrip('/')
    # strip tracking params common ones
    try:
        parts = urllib.parse.urlsplit(url)
        q = urllib.parse.parse_qsl(parts.query, keep_blank_values=False)
        q = [(k,v) for (k,v) in q if not k.lower().startswith("utm_")]
        new_query = urllib.parse.urlencode(q, doseq=True)
        url = urllib.parse.urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path, new_query, ''))
        # remove trailing slash unless root
        if url.endswith('/') and parts.path not in ('', '/'):
            url = url[:-1]
    except Exception:
        pass
    return url

def _tld_lang_hint(netloc: str) -> Optional[str]:
    netloc = netloc.lower()
    if netloc.endswith(".th"): return "th"
    if netloc.endswith(".de"): return "de"
    if netloc.endswith(".ru"): return "ru"
    if netloc.endswith(".fr"): return "fr"
    if netloc.endswith(".es"): return "es"
    if netloc.endswith(".it"): return "it"
    if netloc.endswith(".pt"): return "pt"
    if netloc.endswith(".jp"): return "ja"
    if netloc.endswith(".kr"): return "ko"
    if netloc.endswith(".cn"): return "zh"
    return None

def detect_language(text: Optional[str], url: Optional[str]=None) -> Optional[str]:
    t = (text or "").strip()
    # Script-based quick wins
    if THAI_RE.search(t): return "th"
    if CYRILLIC_RE.search(t): return "ru"
    if ARABIC_RE.search(t): return "ar"
    if HEBREW_RE.search(t): return "he"
    if HIRAGANA_KATAKANA_RE.search(t): return "ja"
    if HANGUL_RE.search(t): return "ko"
    if CJK_RE.search(t): return "zh"
    if DEVANAGARI_RE.search(t): return "hi"
    # TLD hint
    if url:
        try:
            netloc = urllib.parse.urlsplit(url).netloc
            hint = _tld_lang_hint(netloc)
            if hint: return hint
        except Exception:
            pass
    # Fallback lightweight detector
    if detect:
        try:
            code = detect(t)
            return code
        except Exception:
            return None
    return None

def _clean_obfuscations(s: str) -> str:
    s = s.replace("[at]", "@").replace("(at)", "@").replace(" at ", "@")
    s = s.replace("[dot]", ".").replace("(dot)", ".").replace(" dot ", ".")
    s = s.replace(" ", " ").replace("\u200b","")  # nbsp, zero width
    return s

EMAIL_RE = re.compile(r'[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}', re.I)
PHONE_RE = re.compile(r'(\+?\d[\d\s().-]{6,}\d)')

def extract_emails(text: Optional[str]) -> List[str]:
    if not text: return []
    t = _clean_obfuscations(text)
    found = set(re.findall(EMAIL_RE, t))
    return sorted(found)

def extract_phones(text: Optional[str]) -> List[str]:
    if not text: return []
    return re.findall(PHONE_RE, text)

def normalize_phone_list(values: List[str], default_region: str="TH") -> List[str]:
    out = []
    for raw in values or []:
        v = (raw or "").strip()
        if not v: continue
        if phonenumbers is None:
            # fallback: keep only digits and +, minimal normalisation
            v2 = re.sub(r'[^+\d]', '', v)
            out.append(v2)
            continue
        try:
            num = phonenumbers.parse(v, default_region)
            if phonenumbers.is_valid_number(num):
                out.append(phonenumbers.format_number(num, phonenumbers.PhoneNumberFormat.E164))
        except Exception:
            continue
    # unique, stable order
    seen = set(); uniq = []
    for x in out:
        if x not in seen:
            seen.add(x); uniq.append(x)
    return uniq

SOCIAL_PATTERNS = {
    "facebook": r'(?:https?://)?(?:www\.)?facebook\.com/[A-Za-z0-9_.\-/%?=&#]+',
    "instagram": r'(?:https?://)?(?:www\.)?instagram\.com/[A-Za-z0-9_.\-/%?=&#]+',
    "linkedin": r'(?:https?://)?(?:[a-z]{2,3}\.)?linkedin\.com/[A-Za-z0-9_.\-/%?=&#]+',
    "line": r'(?:https?://)?line\.me/[A-Za-z0-9_.\-/%?=&#]+',
    "telegram": r'(?:https?://)?t\.me/[A-Za-z0-9_.\-/%?=&#]+',
    "wechat": r'(?:https?://)?weixin\.qq\.com/[A-Za-z0-9_.\-/%?=&#]+',
    "youtube": r'(?:https?://)?(?:www\.)?youtube\.com/[A-Za-z0-9_.\-/%?=&#]+'
}
def extract_socials(text: Optional[str]) -> Dict[str, List[str]]:
    out = {k: [] for k in SOCIAL_PATTERNS}
    if not text: return out
    for key, pat in SOCIAL_PATTERNS.items():
        hits = re.findall(pat, text, flags=re.I)
        out[key] = sorted(set(hits))
    return out

def normalize_name(name: Optional[str]) -> Optional[str]:
    if not name: return None
    n = unicodedata.normalize("NFKC", name).strip().lower()
    n = re.sub(r'\s+', ' ', n)
    n = re.sub(r'[^\w\s\u0E00-\u0E7F-]', '', n)  # keep thai script, words, dash
    return n or None

# Minimal location normalization (Thai provinces aliases)
TH_PROVINCES = {
    "bangkok": {"th": "กรุงเทพมหานคร", "en": "Bangkok"},
    "chiang mai": {"th": "เชียงใหม่", "en": "Chiang Mai"},
    "phuket": {"th": "ภูเก็ต", "en": "Phuket"},
    "chonburi": {"th": "ชลบุรี", "en": "Chonburi", "aliases": ["pattaya"]},
    "prachuap khiri khan": {"th": "ประจวบคีรีขันธ์", "en": "Prachuap Khiri Khan", "aliases": ["hua hin"]},
    "surat thani": {"th": "สุราษฎร์ธานี", "en": "Surat Thani", "aliases": ["koh samui", "samui"]},
    "khon kaen": {"th": "ขอนแก่น", "en": "Khon Kaen"},
    "udon thani": {"th": "อุดรธานี", "en": "Udon Thani"}
}

def normalize_location(s: Optional[str]) -> Optional[str]:
    if not s: return None
    t = normalize_name(s) or s.lower()
    for key, data in TH_PROVINCES.items():
        if key in t or any(alias in t for alias in data.get("aliases", [])):
            return data["en"]
    return s
