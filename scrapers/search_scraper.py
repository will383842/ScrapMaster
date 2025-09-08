# scrapers/search_scraper.py
import os, time, random, re
import requests
from urllib.parse import quote_plus
from utils.normalize import normalize_url
from utils.ua import pick_user_agent
from utils.i18n import keyword_bundle

class SearchScraper:
    """
    Recherche de sites à visiter via une API (recommandé) ou fallback HTML.
    Par défaut : Bing Web Search (clé via BING_API_KEY).
    Retourne une liste d'URLs que le GenericScraper visitera ensuite.
    """

    def __init__(self):
        self.headers = {
            "User-Agent": pick_user_agent(),
            "Accept-Language": "en,fr;q=0.9",
            "Cache-Control": "no-cache",
        }
        self.delay_s = max(0.2, int(os.getenv("SCRAPMASTER_DELAY_MS", "1200")) / 1000.0)
        self.max_pages = max(1, int(os.getenv("SCRAPMASTER_MAX_PAGES", "3")))
        self.bing_key = os.getenv("BING_API_KEY", "").strip()
        proxy = os.getenv("SCRAPMASTER_PROXY", "").strip()
        self.proxies = {"http": proxy, "https": proxy} if proxy else None

    def search(self, profession: str, country: str, language: str, extra_keywords: str = "") -> list:
        kws = keyword_bundle(profession, language)
        if extra_keywords:
            kws += [k.strip() for k in extra_keywords.replace(",", ";").split(";") if k.strip()]

        queries = [
            f"{profession} {country} " + " ".join(kws[:3]),
            f"{profession} {country} email contact",
            f"{profession} {country} site:*.org email",
        ]

        urls = set()
        for q in queries:
            if self.bing_key:
                urls |= set(self._search_bing_api(q))
            else:
                urls |= set(self._search_bing_html(q))
            time.sleep(self.delay_s + random.random() * 0.4)

        # Nettoyage & normalisation
        out, seen = [], set()
        for u in urls:
            nu = normalize_url(u)
            if not nu: continue
            if any(b in nu for b in ("accounts.google.com", "webcache.googleusercontent.com")):
                continue
            if nu not in seen:
                seen.add(nu); out.append(nu)
        return out[:200]

    # --- Implémentations ---
    def _search_bing_api(self, query: str) -> list:
        try:
            endpoint = "https://api.bing.microsoft.com/v7.0/search"
            params = {"q": query, "count": 20, "mkt": "en-US", "responseFilter": "Webpages"}
            r = requests.get(endpoint,
                             headers={**self.headers, "Ocp-Apim-Subscription-Key": self.bing_key},
                             params=params, timeout=10, proxies=self.proxies)
            r.raise_for_status()
            data = r.json()
            items = (data.get("webPages") or {}).get("value", [])
            return [it.get("url") for it in items if it.get("url")]
        except Exception:
            return []

    def _search_bing_html(self, query: str) -> list:
        try:
            q = quote_plus(query)
            url = f"https://www.bing.com/search?q={q}"
            r = requests.get(url, headers=self.headers, timeout=10, proxies=self.proxies)
            r.raise_for_status()
            # très simple : récupère href="https://..."
            return list(set(m for m in re.findall(r'href="(https?://[^"]+)"', r.text, flags=re.I)
                            if "bing.com" not in m))[:20]
        except Exception:
            return []
