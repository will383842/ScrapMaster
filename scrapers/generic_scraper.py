# ENRICHISSEUR MULTI-SOURCES - SCRAPMASTER
# Nom du fichier : enrichers/multi_source_enricher.py

import os
import time
import logging
from typing import Dict, Any, Optional, List

import requests

from utils.normalize import (
    normalize_url, extract_all_contact_methods, extract_emails, extract_phones,
    normalize_phone_list, extract_socials, find_contact_like_links
)

logger = logging.getLogger(__name__)


class MultiSourceEnricher:
    """
    Enrichissement d'une entrée via multiples sources :
    - Page principale du site
    - Pages 'contact/about/imprint'
    - (optionnel) endpoints API publics simples
    """

    def __init__(self):
        self.headers = {
            "User-Agent": os.getenv("SCRAPMASTER_UA", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
            "Accept-Language": "en,fr;q=0.9",
            "Cache-Control": "no-cache",
        }
        proxy = os.getenv("SCRAPMASTER_PROXY", "").strip()
        self.proxies = {"http": proxy, "https": proxy} if proxy else None

        try:
            self.backoff_ms = max(200, int(os.getenv("SCRAPMASTER_BACKOFF_MS", "1500")))
        except Exception:
            self.backoff_ms = 1500

        try:
            self.max_retries = max(1, int(os.getenv("SCRAPMASTER_MAX_RETRIES", "3")))
        except Exception:
            self.max_retries = 3

        self.session = requests.Session()
        self.session.headers.update(self.headers)

    # ------------------ Public API ------------------

    def enrich_entry_complete(self, entry: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrichissement 'complet' d'une seule entrée.
        - 1) récupère la page principale (si site)
        - 2) extrait emails/phones/socials
        - 3) seconde passe sur pages 'contact/about' si nécessaire (rectification demandée)
        - 4) normalise et merge
        """
        out = dict(entry)

        url = normalize_url(entry.get("website"))
        page_html = ""
        if url:
            page_html = self._http_get_with_retry(url) or ""
            if page_html:
                out = self._merge_contact_data(out, page_html, cfg)

        # ---------------- rectification : seconde passe “contact/about” ----------------
        if not out.get("email") and not out.get("phone") and page_html:
            try:
                candidate_links = find_contact_like_links(page_html, base_url=url)
            except Exception:
                candidate_links = []
            for u in (candidate_links or [])[:3]:
                u_norm = normalize_url(u)
                if not u_norm:
                    continue
                try:
                    r = self.session.get(u_norm, timeout=10, proxies=self.proxies, allow_redirects=True)
                    if r.status_code == 200 and r.text:
                        emails, phones = extract_emails(r.text), extract_phones(r.text)
                        if emails and not out.get("email"):
                            out["email"] = "; ".join(sorted(set(emails))[:3])
                        if phones and not out.get("phone"):
                            region = self._country_to_region(out.get("country"))
                            out["phone"] = "; ".join(normalize_phone_list(phones, default_region=region)[:3])
                        if out.get("email") or out.get("phone"):
                            break
                except Exception:
                    # on continue silencieusement
                    pass
                time.sleep(0.2)

        # Normalisation finale
        out = self._normalize(out, cfg)
        out["enrichment_quality"] = self._score_enrichment(out)

        return out

    # ------------------ Internals ------------------

    def _http_get_with_retry(self, url: str) -> Optional[str]:
        last_err = None
        for attempt in range(1, self.max_retries + 1):
            try:
                r = self.session.get(url, timeout=(10, 20), allow_redirects=True, proxies=self.proxies)
                if r.status_code == 200:
                    return r.text
                if r.status_code in (429, 503, 502, 500):
                    time.sleep(self.backoff_ms / 1000.0)
                else:
                    break
            except Exception as e:
                last_err = e
                time.sleep(self.backoff_ms / 1000.0)
        if last_err:
            logger.debug("http_get_with_retry failed", extra={"url": url, "error": str(last_err)})
        return None

    def _merge_contact_data(self, out: Dict[str, Any], html: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
        # Méthode compacte : emails / phones
        emails = set(extract_emails(html))
        if out.get("email"):
            for e in str(out["email"]).replace(",", ";").split(";"):
                e = e.strip()
                if e:
                    emails.add(e)
        if emails:
            out["email"] = "; ".join(sorted(emails)[:3])

        phones = extract_phones(html)
        if phones:
            region = self._country_to_region(out.get("country"))
            norm = normalize_phone_list(phones, default_region=region)
            existing = set((out.get("phone") or "").replace(",", ";").split(";"))
            for p in norm:
                existing.add(p)
            merged = [x.strip() for x in existing if x and x.strip()]
            out["phone"] = "; ".join(sorted(set(merged))[:3]) if merged else out.get("phone")

        # Réseaux sociaux
        socials = extract_socials(html)
        if isinstance(socials, dict):
            for k, v in socials.items():
                if v and not out.get(k):
                    link = v[0] if isinstance(v, (list, tuple)) and v else v
                    out[k] = normalize_url(link)

        return out

    def _normalize(self, out: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
        # Nettoyage emails
        if out.get("email"):
            emails = extract_emails(out["email"])
            out["email"] = "; ".join(sorted(set(emails))) if emails else None

        # Téléphones E.164
        if out.get("phone"):
            region = self._country_to_region(out.get("country"))
            phones = normalize_phone_list([p for p in out["phone"].replace(",", ";").split(";") if p], default_region=region)
            out["phone"] = "; ".join(phones) if phones else None

        # URLs
        for f in ("website", "facebook", "instagram", "linkedin"):
            if out.get(f):
                out[f] = normalize_url(out[f])

        return out

    def _score_enrichment(self, out: Dict[str, Any]) -> int:
        score = 0
        if out.get("email"):
            score += 4
        if out.get("phone"):
            score += 3
        if out.get("facebook") or out.get("linkedin") or out.get("instagram"):
            score += 1
        return min(10, score)

    def _country_to_region(self, country: Optional[str]) -> str:
        mapping = {
            'Thaïlande': 'TH', 'Thailand': 'TH',
            'France': 'FR',
            'États-Unis': 'US', 'United States': 'US', 'USA': 'US',
            'Royaume-Uni': 'GB', 'United Kingdom': 'GB', 'UK': 'GB',
            'Allemagne': 'DE', 'Germany': 'DE',
            'Espagne': 'ES', 'Spain': 'ES',
            'Italie': 'IT', 'Italy': 'IT',
            'Russie': 'RU', 'Russia': 'RU',
            'Chine': 'CN', 'China': 'CN',
            'Japon': 'JP', 'Japan': 'JP'
        }
        return mapping.get((country or "").strip(), "TH")


# Instance simple utilisable par le moteur
multi_enricher = MultiSourceEnricher()
