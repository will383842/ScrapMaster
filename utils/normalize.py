import re, unicodedata, urllib.parse
from typing import List, Dict, Optional, Set, Any
from urllib.parse import urljoin  # pour find_contact_like_links

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

# ---- REGEX STRICTS (remplacement des anciens motifs trop permissifs) ----
# WhatsApp : liens seulement, pas les numéros dans le texte libre
WA_LINK_RE = re.compile(r'(?:https?://)?(?:wa\.me|api\.whatsapp\.com)/(\+?\d{7,15})', re.I)

# Line ID : format officiel (4-20, lettres/chiffres/._-), tolère "line id:"
LINE_ID_RE = re.compile(r'(?:^|[\s,])(?:line\s*id\s*[:\-]?\s*)(@?[a-z0-9_.-]{4,20})(?=[\s,.]|$)', re.I | re.M)

# Telegram : username format officiel (5-32, lettres/chiffres/_), supporte @username ou t.me/username
TG_USERNAME_RE = re.compile(r'(?:^|[\s,])(?:@|t\.me/)([a-z0-9_]{5,32})(?=[\s,.]|$)', re.I | re.M)

# WeChat : format ID officiel (6-20, lettres/chiffres/_-), tolère "wechat id:"
WECHAT_ID_RE = re.compile(r'(?:^|[\s,])(?:wechat\s*id\s*[:\-]?\s*)([a-z0-9_-]{6,20})(?=[\s,.]|$)', re.I | re.M)


# -------------------- EXTRACTEURS CONTACT --------------------
def extract_whatsapp(text: Optional[str]) -> List[str]:
    """Extrait liens WhatsApp uniquement (pas numéros libres) avec validation stricte."""
    if not text:
        return []
    matches = WA_LINK_RE.findall(text)
    valid_numbers: Set[str] = set()
    for m in matches:
        # Exiger format international clair (+) et taille correcte
        digits = m.replace('+', '')
        if m.startswith('+') and 7 <= len(digits) <= 15 and digits.isdigit():
            valid_numbers.add(m)
    return sorted(valid_numbers)


def extract_line_id(text: Optional[str]) -> List[str]:
    """Extrait Line ID avec validation format et nettoyage (@ facultatif)."""
    if not text:
        return []
    matches = LINE_ID_RE.findall(text)
    valid_ids: Set[str] = set()
    for m in matches:
        clean_id = m.strip().lstrip('@').lower()
        if 4 <= len(clean_id) <= 20 and re.fullmatch(r'[a-z0-9_.-]+', clean_id):
            # éviter des URLs déguisées
            if not clean_id.startswith(('http', 'www')):
                valid_ids.add(clean_id)
    return sorted(valid_ids)


def extract_telegram(text: Optional[str]) -> List[str]:
    """Extrait usernames Telegram (@user ou t.me/user) avec validation officielle (5-32)."""
    if not text:
        return []
    matches = TG_USERNAME_RE.findall(text)
    valid: Set[str] = set()
    for m in matches:
        user = m.lower()
        if 5 <= len(user) <= 32 and re.fullmatch(r'[a-z0-9_]+', user):
            valid.add(user)
    return sorted(valid)


def extract_wechat(text: Optional[str]) -> List[str]:
    """Extrait WeChat IDs (6-20, lettres/chiffres/_-), via mentions explicites."""
    if not text:
        return []
    matches = WECHAT_ID_RE.findall(text)
    valid: Set[str] = set()
    for m in matches:
        w = m.lower()
        if 6 <= len(w) <= 20 and re.fullmatch(r'[a-z0-9_-]+', w):
            valid.add(w)
    return sorted(valid)


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
        q = [(k, v) for (k, v) in q if not k.lower().startswith("utm_")]
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


# -------------------- DÉTECTION LANGUE AMÉLIORÉE --------------------
def detect_language(text: Optional[str], url: Optional[str] = None) -> Optional[str]:
    """Détection langue basée sur des seuils de scripts + indice TLD + fallback langdetect."""
    if not text or len(text.strip()) < 10:
        return None

    t = text.strip()
    text_length = len(t)

    # Compter caractères par script
    thai_chars = len(THAI_RE.findall(t))
    cyrillic_chars = len(CYRILLIC_RE.findall(t))
    arabic_chars = len(ARABIC_RE.findall(t))
    cjk_chars = len(CJK_RE.findall(t))
    hiragana_katakana_chars = len(HIRAGANA_KATAKANA_RE.findall(t))

    # 20% du texte ou minimum 5 caractères
    threshold = max(5, int(text_length * 0.2))

    if thai_chars >= threshold:
        return "th"
    if hiragana_katakana_chars >= threshold:
        return "ja"
    if cyrillic_chars >= threshold:
        return "ru"
    if arabic_chars >= threshold:
        return "ar"
    if cjk_chars >= threshold:
        return "zh"

    # Fallback TLD si pas de script dominant
    if url:
        try:
            tld_hint = _tld_lang_hint(urllib.parse.urlsplit(url).netloc)
            if tld_hint:
                return tld_hint
        except Exception:
            pass

    # Détecteur externe en dernier recours (texte suffisamment long pour fiabilité)
    if detect and text_length >= 50:
        try:
            return detect(t)
        except Exception:
            pass

    return None  # Incertain plutôt que faux


def _clean_obfuscations(s: str) -> str:
    s = s.replace("[at]", "@").replace("(at)", "@").replace(" at ", "@")
    s = s.replace("[dot]", ".").replace("(dot)", ".").replace(" dot ", ".")
    s = s.replace(" ", " ").replace("\u200b", "")  # nbsp, zero width
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


def normalize_phone_list(values: List[str], default_region: str = "TH") -> List[str]:
    out = []
    for raw in values or []:
        v = (raw or "").strip()
        if not v:
            continue
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
    seen = set()
    uniq = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


SOCIAL_PATTERNS = {
    "facebook": r'(?:https?://)?(?:www\.)?facebook\.com/[A-Za-z0-9_.\-/%?=&#]+',
    "instagram": r'(?:https?://)?(?:www\.)?instagram\.com/[A-Za-z0-9_.\-/%?=&#]+',
    "linkedin": r'(?:https?://)?(?:[a-z]{2,3}\.)?linkedin\.com/[A-Za-z0-9_.\-/%?=&#]+',
    "line": r'(?:https?://)?line\.me/[A-Za-z0-9_.\-/%?=&#]+',
    "telegram": r'(?:https?://)?t\.me/[A-Za-z0-9_.\-/%?=&#]+',
    "wechat": r'(?:https?://)?weixin\.qq\.com/[A-Za-z0-9_.\-/%?=&#]+',
    "youtube": r'(?:https?://)?(?:www\.)?youtube\.com/[A-Za-z0-9_.\-/%?=&#]+',
    # WhatsApp liens uniquement (cohérent avec extract_whatsapp)
    "whatsapp": r'(?:https?://)?(?:wa\.me|api\.whatsapp\.com)/[A-Za-z0-9_.\-/%?=&#]+',
}


def extract_socials(text: Optional[str]) -> Dict[str, List[str]]:
    out = {k: [] for k in SOCIAL_PATTERNS}
    if not text:
        return out
    for key, pat in SOCIAL_PATTERNS.items():
        hits = re.findall(pat, text, flags=re.I)
        out[key] = sorted(set(hits))
    return out


def normalize_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
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
    "udon thani": {"th": "อุดรธานี", "en": "Udon Thani"},
}


def normalize_location(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    t = normalize_name(s) or s.lower()
    for key, data in TH_PROVINCES.items():
        if key in t or any(alias in t for alias in data.get("aliases", [])):
            return data["en"]
    return s


# ---- repérage des pages “Contact / About / Mentions / Legal” ----
def find_contact_like_links(html: str, base_url: str) -> list:
    """
    Retourne une liste d'URLs (absolues) pointant vers des pages de type Contact/About/Impressum/Mentions/Légal.
    - html: contenu HTML d'une page
    - base_url: URL de base pour résoudre les liens relatifs
    """
    links = set()
    try:
        # capture href + innerHTML texte, puis on nettoie le label
        for href, text_ in re.findall(r'href=["\']([^"\']+)["\'][^>]*>(.*?)<', html or "", flags=re.I | re.S):
            label = re.sub(r'<[^>]+>', ' ', text_ or '', flags=re.I).strip().lower()
            if any(w in label for w in ["contact", "contacts", "about", "impress", "impressum", "mentions", "legal", "legal notice", "privacy", "terms"]):
                try:
                    links.add(urljoin(base_url, href))
                except Exception:
                    # fallback au cas où
                    try:
                        links.add(urllib.parse.urljoin(base_url, href))
                    except Exception:
                        pass
    except Exception:
        pass
    return sorted(links)


# ===== EXTRACTION AVANCÉE DE CONTACTS =====

def extract_all_contact_methods(text: str) -> Dict[str, List[str]]:
    """Extraction complète de tous les moyens de contact"""
    if not text:
        return {}
    
    # Nettoyage préalable
    cleaned_text = _clean_obfuscations(text)
    
    contacts = {
        'emails': extract_emails(cleaned_text),
        'phones': extract_phones(cleaned_text),
        'whatsapp': extract_whatsapp(cleaned_text),
        'line_id': extract_line_id(cleaned_text),
        'telegram': extract_telegram(cleaned_text),
        'wechat': extract_wechat(cleaned_text),
        'social_media': extract_socials(cleaned_text),
        'websites': extract_websites(cleaned_text),
        'contact_forms': extract_contact_forms(cleaned_text),
        'physical_addresses': extract_addresses(cleaned_text),
        'business_hours': extract_business_hours(cleaned_text),
        'contact_persons': extract_contact_persons(cleaned_text)
    }
    
    return {k: v for k, v in contacts.items() if v}

def extract_websites(text: str) -> List[str]:
    """Extraction URLs de sites web"""
    if not text:
        return []
    
    # Pattern URL plus strict
    url_pattern = r'https?://(?:[-\w.])+(?:\.[a-zA-Z]{2,})+(?:/[^\s]*)?'
    urls = re.findall(url_pattern, text, re.I)
    
    # Filtrer les URLs valides
    valid_urls = []
    for url in urls:
        # Exclure images, documents, réseaux sociaux déjà gérés ailleurs
        if not re.search(r'\.(jpg|jpeg|png|gif|pdf|doc|docx)$', url, re.I):
            if not any(social in url.lower() for social in ['facebook.com', 'instagram.com', 'linkedin.com', 'twitter.com']):
                valid_urls.append(url)
    
    return list(set(valid_urls))

def extract_contact_forms(text: str) -> List[str]:
    """Extraction de mentions de formulaires de contact"""
    if not text:
        return []
    
    form_patterns = [
        r'contact\s+form',
        r'formulaire\s+de\s+contact',
        r'get\s+in\s+touch',
        r'send\s+message',
        r'contact\s+us\s+form'
    ]
    
    forms = []
    for pattern in form_patterns:
        matches = re.findall(pattern, text, re.I)
        forms.extend(matches)
    
    return list(set(forms))

def extract_addresses(text: str) -> List[str]:
    """Extraction d'adresses physiques"""
    if not text:
        return []
    
    # Patterns d'adresses (simplifié, peut être amélioré)
    address_patterns = [
        r'\d+\s+[A-Za-z\s]+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Lane|Ln)[\w\s,]*',
        r'[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s*,\s*\d{5}(?:-\d{4})?',  # US format
        r'\d{5}\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*',  # French format
    ]
    
    addresses = []
    for pattern in address_patterns:
        matches = re.findall(pattern, text, re.M)
        addresses.extend(matches)
    
    return list(set(addresses))

def extract_business_hours(text: str) -> List[str]:
    """Extraction horaires d'ouverture"""
    if not text:
        return []
    
    # Patterns horaires
    hours_patterns = [
        r'(?:open|ouvert|hours|horaires)[:\s]*([^.!?]*(?:am|pm|h\d{2}|\d{1,2}:\d{2})[^.!?]*)',
        r'(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday|lundi|mardi|mercredi|jeudi|vendredi|samedi|dimanche)[:\s]*([^.!?]*(?:am|pm|h\d{2}|\d{1,2}:\d{2})[^.!?]*)',
        r'\d{1,2}:\d{2}\s*-\s*\d{1,2}:\d{2}',
        r'\d{1,2}h\d{2}\s*-\s*\d{1,2}h\d{2}'
    ]
    
    hours = []
    for pattern in hours_patterns:
        matches = re.findall(pattern, text, re.I | re.M)
        if not matches:
            continue
        # Protection cas tuple sans match
        if isinstance(matches[0], tuple):
            hours.extend([m for m in matches if m])
        else:
            hours.extend(matches)
    
    return list(set(hours))

def extract_contact_persons(text: str) -> List[str]:
    """Extraction noms de personnes de contact"""
    if not text:
        return []
    
    # Patterns pour identifier des personnes
    person_patterns = [
        r'(?:contact|responsable|manager|director|président|directeur)[:\s]*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)',
        r'(?:mr|mrs|ms|dr|prof|m\.|mme|mlle)\.?\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
        r'([A-Z][a-z]+\s+[A-Z][a-z]+)(?:\s*,\s*(?:manager|director|responsable|président))',
    ]
    
    persons = []
    for pattern in person_patterns:
        matches = re.findall(pattern, text, re.M)
        persons.extend(matches)
    
    # Filtrer les noms trop courts ou génériques
    valid_persons = []
    generic_terms = {'contact', 'information', 'service', 'team', 'support', 'admin'}
    
    for person in persons:
        if len(person.split()) >= 2:  # Au moins prénom + nom
            if not any(term.lower() in person.lower() for term in generic_terms):
                valid_persons.append(person.strip())
    
    return list(set(valid_persons))

# ===== DÉTECTION DE SECTEURS D'ACTIVITÉ =====

def detect_business_sector(text: str, profession: str = "") -> Dict[str, Any]:
    """Détecte le secteur d'activité depuis le texte"""
    if not text:
        return {}
    
    text_lower = text.lower()
    
    # Base de secteurs avec mots-clés
    sectors = {
        'legal': {
            'keywords': ['droit', 'legal', 'law', 'avocat', 'lawyer', 'tribunal', 'justice', 'contentieux'],
            'confidence_boost': 2 if 'avocat' in profession.lower() else 0
        },
        'healthcare': {
            'keywords': ['santé', 'health', 'medical', 'médical', 'docteur', 'doctor', 'clinic', 'hospital'],
            'confidence_boost': 0
        },
        'technology': {
            'keywords': ['tech', 'digital', 'informatique', 'software', 'développement', 'web', 'app'],
            'confidence_boost': 2 if 'digital' in profession.lower() else 0
        },
        'hospitality': {
            'keywords': ['hotel', 'restaurant', 'tourism', 'voyage', 'hébergement', 'food', 'cuisine'],
            'confidence_boost': 2 if any(word in profession.lower() for word in ['restaurant', 'hotel']) else 0
        },
        'education': {
            'keywords': ['education', 'école', 'school', 'formation', 'training', 'université', 'university'],
            'confidence_boost': 0
        },
        'nonprofit': {
            'keywords': ['association', 'ONG', 'NGO', 'charity', 'nonprofit', 'foundation', 'bénévole'],
            'confidence_boost': 2 if 'association' in profession.lower() else 0
        }
    }
    
    detected_sectors = {}
    
    for sector, data in sectors.items():
        score = 0
        matched_keywords = []
        
        for keyword in data['keywords']:
            if keyword in text_lower:
                score += 1
                matched_keywords.append(keyword)
        
        # Boost basé sur la profession
        score += data['confidence_boost']
        
        if score > 0:
            detected_sectors[sector] = {
                'score': score,
                'confidence': min(score * 10, 100),  # Score sur 100
                'matched_keywords': matched_keywords
            }
    
    return detected_sectors

# ===== ENRICHISSEMENT GÉOGRAPHIQUE =====

def enrich_geographic_info(text: str, country: str = "") -> Dict[str, str]:
    """Enrichit les informations géographiques"""
    geographic_info = {}
    
    # Détection de villes par pays
    city_patterns = {
        'thaïlande': ['bangkok', 'phuket', 'chiang mai', 'pattaya', 'krabi', 'samui', 'hua hin'],
        'france': ['paris', 'lyon', 'marseille', 'toulouse', 'nice', 'nantes', 'strasbourg'],
        'états-unis': ['new york', 'los angeles', 'chicago', 'houston', 'phoenix', 'philadelphia'],
        'royaume-uni': ['london', 'manchester', 'birmingham', 'glasgow', 'liverpool', 'bristol']
    }
    
    text_lower = text.lower()
    country_lower = country.lower()
    
    if country_lower in city_patterns:
        for city in city_patterns[country_lower]:
            if city in text_lower:
                geographic_info['detected_city'] = city.title()
                break
    
    # Détection codes postaux
    postal_patterns = {
        'france': r'\b\d{5}\b',
        'états-unis': r'\b\d{5}(?:-\d{4})?\b',
        'royaume-uni': r'\b[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}\b',
        'thaïlande': r'\b\d{5}\b'
    }
    
    if country_lower in postal_patterns:
        pattern = postal_patterns[country_lower]
        postal_matches = re.findall(pattern, text, re.I)
        if postal_matches:
            geographic_info['postal_code'] = postal_matches[0]
    
    return geographic_info
