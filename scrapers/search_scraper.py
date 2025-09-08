# scrapers/search_scraper.py
import os, re, time, html as _html, random, logging
from typing import List
import urllib.parse
from urllib.parse import quote_plus
import requests

from utils.ua import pick_user_agent

logger = logging.getLogger(__name__)

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
        """Recherche sécurisée avec validation et échappement"""
        # Validation stricte des entrées
        profession = self._sanitize_search_term(profession)
        country = self._sanitize_search_term(country)
        language = self._sanitize_search_term(language)
        extra_keywords = self._sanitize_search_term(extra_keywords)

        if not profession or not country:
            logger.warning("Paramètres recherche insuffisants",
                           extra={"profession": profession, "country": country})
            return []

        # Construction sécurisée
        contact_words = _kw_bundle(language)
        query_templates = [
            "{profession} {country}",
            "{profession} {country} {contact_word1} {contact_word2}",
            "{profession} {country} email contact",
            "{profession} {country} directory association",
            "{profession} {country} {contact_word1} site:*.org",
            "{profession} {country} {contact_word1} site:*.gov",
        ]

        queries: List[str] = []
        for template in query_templates:
            try:
                q = template.format(
                    profession=profession,
                    country=country,
                    contact_word1=contact_words[0] if contact_words else "contact",
                    contact_word2=contact_words[1] if len(contact_words) > 1 else "email"
                )
                # Ajout mots-clés extra sécurisés
                if extra_keywords:
                    q = f"{q} {extra_keywords}"

                # Validation finale longueur
                if 5 <= len(q) <= 200:
                    queries.append(q)
            except (KeyError, IndexError) as e:
                logger.warning("Erreur template query", extra={"template": template, "error": str(e)})
                continue

        # Exécution avec rate limiting & retry
        urls = self._execute_searches(queries)
        return urls[:200]

    # ---------------- Validation & orchestration ----------------

    def _sanitize_search_term(self, term: str) -> str:
        """Nettoie et valide un terme de recherche"""
        if not term:
            return ""
        term = term.strip()

        # Enlever caractères dangereux
        for ch in ['<', '>', '"', "'", '&', ';', '|', '`', '$']:
            term = term.replace(ch, ' ')

        # Normaliser espaces
        term = re.sub(r'\s+', ' ', term).strip()
        # Limite longueur
        return term[:100]

    def _execute_searches(self, queries: List[str]) -> List[str]:
        """Exécute recherches avec rate limiting et retry"""
        all_urls: List[str] = []

        for i, query in enumerate(queries):
            try:
                # DuckDuckGo
                ddg_urls = self._ddg_query_safe(query, max_pages=self.max_pages)
                all_urls.extend(ddg_urls)

                # Rate limiting intelligent entre moteurs
                if i < len(queries) - 1:
                    delay = self.delay_s + random.uniform(0.5, 1.5)
                    time.sleep(delay)
                else:
                    delay = self.delay_s

                # Bing
                bing_urls = self._bing_query_safe(query, max_pages=self.max_pages)
                all_urls.extend(bing_urls)

                if i < len(queries) - 1:
                    time.sleep(delay)

            except Exception as e:
                logger.warning("Erreur recherche query", extra={"query": query, "error": str(e)})
                continue

        return self._clean_and_validate_urls(all_urls)

    # ---------------- Engines (robustes) ----------------

    def _ddg_query_safe(self, query: str, max_pages: int = 2) -> List[str]:
        """DuckDuckGo avec gestion d'erreur robuste"""
        urls: List[str] = []

        for page in range(max_pages):
            try:
                headers = self.headers.copy()
                if os.getenv("SCRAPMASTER_UA_ROTATION", "true").lower() == "true":
                    headers['User-Agent'] = pick_user_agent()

                params = {
                    "q": query[:500],  # Limite longueur
                    "s": str(page * 50)
                }

                response = requests.post(
                    self.ddg_base,
                    data=params,
                    headers=headers,
                    timeout=15,
                    proxies=self.proxies
                )

                if response.status_code == 429:  # Rate limited
                    logger.warning("DDG rate limit atteint", extra={"query": query})
                    time.sleep(30)
                    continue
                elif response.status_code == 403:
                    logger.warning("DDG access bloqué", extra={"query": query})
                    break

                response.raise_for_status()

                page_urls = self._extract_ddg_results(response.text, query)

                if not page_urls and page == 0:
                    logger.info("DDG: aucun résultat", extra={"query": query})
                    break
                elif not page_urls:
                    logger.debug("DDG: page vide", extra={"page": page + 1, "query": query})
                    break

                urls.extend(page_urls)

                if page < max_pages - 1:
                    time.sleep(random.uniform(2, 4))

            except requests.exceptions.Timeout:
                logger.warning("DDG timeout", extra={"page": page + 1, "query": query})
                break
            except requests.exceptions.RequestException as e:
                logger.warning("DDG erreur", extra={"page": page + 1, "query": query, "error": str(e)})
                break
            except Exception as e:
                logger.error("DDG erreur inattendue", extra={"query": query, "error": str(e)})
                break

        return urls

    def _extract_ddg_results(self, html: str, query: str) -> List[str]:
        """Extraction robuste résultats DDG"""
        urls: List[str] = []

        patterns = [
            r'<a[^>]+class="result__a"[^>]+href="([^"]+)"',            # Standard
            r'<a[^>]+href="([^"]+)"[^>]*class="result__a"',            # Ordre inversé
            r'data-testid="result-title-a"[^>]+href="([^"]+)"',        # Nouveau format
        ]

        for pattern in patterns:
            matches = re.findall(pattern, html, flags=re.I)
            for match in matches:
                try:
                    url = _html.unescape(match).strip()
                    # Filtrer redirections DDG
                    if "duckduckgo.com" in url:
                        continue
                    if self._is_valid_search_result(url, query):
                        urls.append(url)
                except Exception as e:
                    logger.debug("Erreur parsing URL DDG", extra={"url": match, "error": str(e)})
                    continue

        # Dédup en préservant l'ordre
        deduped = list(dict.fromkeys(urls))
        return deduped

    def _bing_query_safe(self, query: str, max_pages: int = 2) -> List[str]:
        """Scraping Bing HTML avec durcissement (codes d'erreur, rotation UA, pagination)"""
        out: List[str] = []

        for page in range(max_pages):
            try:
                headers = self.headers.copy()
                if os.getenv("SCRAPMASTER_UA_ROTATION", "true").lower() == "true":
                    headers['User-Agent'] = pick_user_agent()

                qp = f"{query} site:*/contact OR site:*/about OR annuaire"
                url = self.bing_base + quote_plus(qp)
                if page > 0:
                    # Pagination Bing: &first=11, 21, ...
                    url += f"&first={page*10+1}"

                r = requests.get(url, headers=headers, timeout=15, proxies=self.proxies, allow_redirects=True)

                if r.status_code == 429:
                    logger.warning("Bing rate limit atteint", extra={"query": query})
                    time.sleep(20)
                    continue
                elif r.status_code == 403:
                    logger.warning("Bing access bloqué", extra={"query": query})
                    break

                r.raise_for_status()

                hits = re.findall(r'href="(https?://[^"]+)"', r.text, flags=re.I)
                for u in hits:
                    if "bing.com" in u.lower():
                        continue
                    if self._is_valid_search_result(u, query):
                        out.append(u)

                if page < max_pages - 1:
                    time.sleep(random.uniform(2, 4))

            except requests.exceptions.Timeout:
                logger.warning("Bing timeout", extra={"page": page + 1, "query": query})
                break
            except requests.exceptions.RequestException as e:
                logger.warning("Bing erreur", extra={"page": page + 1, "query": query, "error": str(e)})
                break
            except Exception as e:
                logger.error("Bing erreur inattendue", extra={"query": query, "error": str(e)})
                break

        return out

    # ---------------- Helpers ----------------

    def _is_valid_search_result(self, url: str, query: str) -> bool:
        """Valide qu'une URL est un bon résultat de recherche"""
        bad_domains = {
            'google.com', 'bing.com', 'yahoo.com', 'duckduckgo.com',
            'facebook.com', 'twitter.com', 'instagram.com', 'linkedin.com',
            'youtube.com', 'wikipedia.org'
        }
        try:
            parsed = urllib.parse.urlparse(url)
            domain = (parsed.netloc or "").lower()
            if domain.startswith('www.'):
                domain = domain[4:]

            if not domain or domain in bad_domains:
                return False

            # Filtrer documents lourds / non-HTML
            if parsed.path.lower().endswith(('.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx')):
                return False

            # Éviter d'autres pages de résultats
            u_lower = url.lower()
            if any(word in u_lower for word in ['search?', 'results?', 'query=', '/search/']):
                return False

            # Longueur raisonnable
            if len(url) > 1000:
                return False

            return True
        except Exception:
            return False

    def _clean_and_validate_urls(self, urls: List[str]) -> List[str]:
        """Déduplique + filtre les URLs bruyantes, puis valide."""
        seen, out = set(), []
        BAD_SUBSTR = (
            "login", "signin", "signup", "account",
            "webcache.googleusercontent.com", "translate.google",
        )
        for u in urls:
            if not u:
                continue
            u = u.split("#")[0].strip()
            if any(b in u.lower() for b in BAD_SUBSTR):
                continue
            if not self._is_valid_search_result(u, ""):
                continue
            if u not in seen:
                seen.add(u)
                out.append(u)
        return out
