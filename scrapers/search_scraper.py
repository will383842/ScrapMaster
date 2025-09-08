# scrapers/search_scraper.py
import os, re, time, html, random
from urllib.parse import quote_plus
import requests
from utils.ua import pick_user_agent

# Petits vocabulaires multilingues pour booster la recherche
_LANG_CONTACT_WORDS = {
    "en": ["email", "contact", "directory"],
    "fr": ["email", "contact", "annuaire"],
    "es": ["email", "contacto", "directorio"],
    "de": ["email", "kontakt", "verzeichnis"],
    "it": ["email", "contatto", "elenco"],
    "pt": ["email", "contato", "diretório"],
    "nl": ["email", "contact", "gids"],
    "ru": ["email", "контакты", "каталог"],
    "zh": ["email", "联系", "名录"],
    "ja": ["email", "連絡先", "ディレクトリ"],
    "ko": ["email", "연락처", "디렉토리"],
    "th": ["email", "ติดต่อ", "ไดเรกทอรี"],
}

def _kw_bundle(language: str):
    lang = (language or "en").lower()
    base = _LANG_CONTACT_WORDS.get(lang, _LANG_CONTACT_WORDS["en"])
    # toujours garder quelques génériques
    return list(dict.fromkeys(base + ["official site", "association", "directory", "impressum", "legal"]))

class SearchScraper:
    """
    Recherche robuste via DuckDuckGo HTML + Bing HTML (sans clé).
    Objectif: renvoyer une liste d'URLs candidates (sites/annuaires) pour le GenericScraper.
    Respecte les réglages .env (delay, max_pages, proxy, UA rotation).
    """

    def __init__(self):
        self.headers = {
            "User-Agent": pick_user_agent(),
            "Accept-Language": "en,fr;q=0.9",
            "Cache-Control": "no-cache",
        }
        # Réglages depuis .env
        try:
            self.delay_s = max(0.2, int(os.getenv("SCRAPMASTER_DELAY_MS", "1200")) / 1000.0)
        except Exception:
            self.delay_s = 1.2
        try:
            self.max_pages = max(1, int(os.getenv("SCRAPMASTER_MAX_PAGES", "2")))
        except Exception:
            self.max_pages = 2
        proxy = os.getenv("SCRAPMASTER_PROXY", "").strip()
        self.proxies = {"http": proxy, "https": proxy} if proxy else None

        # Endpoints HTML
        self.ddg_base = "https://duckduckgo.com/html/"
        self.bing_base = "https://www.bing.com/search?q="

    # ---------------- Core API ----------------

    def search(self, profession: str, country: str, language: str, extra_keywords: str = "") -> list:
        """
        Construit plusieurs requêtes (dorks) multilingues et interroge DDG + Bing HTML.
        Retourne jusqu'à ~200 URLs nettoyées/dédupliquées.
        """
        prof = (profession or "").strip()
        ctry = (country or "").strip()
        lang = (language or "en").strip().lower()

        # Bundle mots de contact pour la langue
        contact_words = _kw_bundle(lang)

        # Extra keywords (ex: "immigration; expat; visa")
        extra = []
        if extra_keywords:
            extra = [k.strip() for k in extra_keywords.replace(",", ";").split(";") if k.strip()]

        # Dorks/variantes de recherche
        base_q = " ".join([x for x in [prof, ctry] if x]).strip()
        queries = [
            base_q,
            f"{base_q} " + " ".join(contact_words[:2]),
            f"{base_q} " + " ".join(contact_words[:3]),
            f"{base_q} email contact site:*.org",
            f"{base_q} {contact_words[1]} site:*.gov",
            f"{base_q} directory listing email",
        ]
        # Ajout des extras
        if extra:
            queries += [f"{base_q} {' '.join(extra)}", f"{base_q} {' '.join(extra)} email contact"]

        urls = []

        # Interroger DuckDuckGo HTML
        for q in queries:
            urls.extend(self._ddg_query(q, max_pages=self.max_pages))
            time.sleep(self.delay_s + random.random() * 0.4)

        # Interroger Bing HTML (fallback, sans clé)
        for q in queries:
            urls.extend(self._bing_query(q, max_pages=self.max_pages))
            time.sleep(self.delay_s + random.random() * 0.4)

        # Nettoyage & filtrage
        cleaned = self._clean_urls(urls)
        return cleaned[:200]

    # ---------------- Engines ----------------

    def _ddg_query(self, q: str, max_pages=2):
        """Scraping DuckDuckGo HTML: POST /html avec param q, extraire <a class="result__a">."""
        out = []
        for p in range(max_pages):
            try:
                params = {"q": q, "s": str(p * 50)}
                r = requests.post(self.ddg_base, data=params, headers=self.headers,
                                  timeout=12, proxies=self.proxies)
                r.raise_for_status()
                html_text = r.text

                for m in re.finditer(r'<a[^>]+class="result__a"[^>]+href="([^"]+)"', html_text, flags=re.I):
                    u = html.unescape(m.group(1)).strip()
                    if u and not u.startswith("http://r.duckduckgo.com"):
                        out.append(u)
            except Exception:
                pass
        return out

    def _bing_query(self, q: str, max_pages=2):
        """Scraping Bing HTML: GET /search?q= ...; extraire href="https://..." (en filtrant bing.com)."""
        out = []
        for p in range(max_pages):
            try:
                qp = f"{q} site:*/contact OR site:*/about OR annuaire"
                if p > 0:
                    qp += f" &first={p*10+1}"
                url = self.bing_base + quote_plus(qp)
                r = requests.get(url, headers=self.headers, timeout=12, proxies=self.proxies)
                r.raise_for_status()
                # Liens externes
                hits = re.findall(r'href="(https?://[^"]+)"', r.text, flags=re.I)
                for u in hits:
                    if "bing.com" in u.lower():
                        continue
                    out.append(u)
            except Exception:
                pass
        return out

    # ---------------- Helpers ----------------

    def _clean_urls(self, urls: list) -> list:
        """Déduplique et filtre les URLs bruyantes (login, pdf, tracking)."""
        seen, out = set(), []
        BAD = ("login", "signin", "signup", "account", ".pdf", "webcache.googleusercontent.com", "translate.google")
        for u in urls:
            if not u:
                continue
            ul = u.lower()
            if any(b in ul for b in BAD):
                continue
            # enlever ancres & trivial stuff
            u = u.split("#")[0].strip()
            if u and u not in seen:
                seen.add(u)
                out.append(u)
        return out
