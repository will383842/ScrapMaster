# SCRAPER MÉTIER — TRADUCTEURS / INTERPRÈTES
# Fichier : scrapers/translator_scraper.py

from typing import List, Dict
from copy import deepcopy

from scrapers.generic_scraper import GenericScraper


# --- Seeds de qualité (annuaire & requêtes) ---
SEED_SITES: Dict[str, List[str]] = {
    'Thaïlande': [
        "https://www.yellowpages.co.th/en/category/translation-services",
        "https://www.proz.com/translator-directory?country=thailand",
        "https://www.expat.com/en/business/asia/thailand/translation/"
    ],
    'France': [
        "https://www.sft.fr/trouver-un-traducteur",
        "https://www.proz.com/translator-directory?country=france"
    ],
}

SEED_QUERIES: Dict[str, List[str]] = {
    'Thaïlande': [
        "translator thailand email",
        "interpreter bangkok contact",
        "ล่าม กรุงเทพ อีเมล"
    ],
    'France': [
        "traducteur assermenté email",
        "agence de traduction contact",
        "interprète paris email"
    ],
}


def _country_key(country: str) -> str:
    c = (country or "").strip()
    if "Thail" in c or "Thaï" in c or "Thailand" in c:
        return "Thaïlande"
    if "France" in c:
        return "France"
    return c


def _to_sources(urls: List[str], label: str) -> List[dict]:
    out = []
    for u in urls or []:
        if not u:
            continue
        url = u if u.endswith("/") else u + "/"
        out.append({"name": label, "url": url})
    if out:
        return [{"name": f"Seeds:{label}", "categories": out}]
    return []


class TranslatorScraper:
    """Scraper métier Traducteurs/Interprètes : injecte des seeds puis délègue au GenericScraper."""

    def __init__(self):
        self.generic = GenericScraper()

    def scrape(self, config: dict):
        cfg = deepcopy(config)
        country_key = _country_key(cfg.get("country", ""))

        # 1) Sources annuaires
        seed_site_urls = SEED_SITES.get(country_key, [])
        seed_site_sources = _to_sources(seed_site_urls, "translators")

        # 2) Sources du projet
        project_sources = cfg.get("sources") or []

        # 3) Fusion
        merged_sources = []
        if isinstance(project_sources, list):
            merged_sources.extend(project_sources)
        elif isinstance(project_sources, dict):
            merged_sources.extend(project_sources.get("seed_sources", []))

        merged_sources.extend(seed_site_sources)

        # 4) Keywords boost (utiles pour l’orchestrateur SearchScraper)
        qs = SEED_QUERIES.get(country_key, [])
        extra_kw = "; ".join(qs)
        base_kw = (cfg.get("keywords") or "").strip()
        cfg["keywords"] = f"{base_kw}; {extra_kw}".strip("; ").strip()

        # 5) Exécution
        cfg["sources"] = merged_sources
        cfg["profession"] = cfg.get("profession") or "Traducteurs"
        cfg["keep_incomplete"] = True

        return self.generic.scrape(cfg)
