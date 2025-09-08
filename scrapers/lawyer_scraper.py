# SCRAPER MÉTIER — AVOCATS
# Fichier : scrapers/lawyer_scraper.py

from typing import List, Dict
from copy import deepcopy

from scrapers.generic_scraper import GenericScraper


# --- Seeds de qualité (annuaire & requêtes) ---
SEED_SITES: Dict[str, List[str]] = {
    'Thaïlande': [
        "https://www.thailawonline.com/",
        "https://lawyers.findlaw.com/international/thailand",
        "https://www.hg.org/lawyers/thailand",
        "https://www.expat.com/en/business/asia/thailand/lawyers/"
    ],
    'France': [
        "https://www.cnb.avocat.fr/annuaire",
        "https://www.barreau-paris.fr/annuaire",
        "https://www.justifit.fr/trouver/avocats/",
        "https://www.avocat.fr/annuaire"
    ],
}

SEED_QUERIES: Dict[str, List[str]] = {
    'Thaïlande': [
        "immigration lawyer thailand email",
        "law firm bangkok contact",
        "avocat francophone thailande email"
    ],
    'France': [
        "avocat immigration email site:fr",
        "cabinet d'avocats contact",
        "avocat assermenté email"
    ],
}


def _country_key(country: str) -> str:
    c = (country or "").strip()
    if "Thail" in c or "Thaï" in c or "Thailand" in c:
        return "Thaïlande"
    if "France" in c:
        return "France"
    return c  # essaie clé brute


def _to_sources(urls: List[str], label: str) -> List[dict]:
    out = []
    for u in urls or []:
        if not u:
            continue
        # Le GenericScraper attend des "categories" avec des URLs finissant souvent par /
        url = u if u.endswith("/") else u + "/"
        out.append({"name": label, "url": url})
    if out:
        return [{"name": f"Seeds:{label}", "categories": out}]
    return []


class LawyerScraper:
    """Scraper métier Avocats : injecte des seeds riches puis délègue au GenericScraper."""

    def __init__(self):
        self.generic = GenericScraper()

    def scrape(self, config: dict):
        cfg = deepcopy(config)
        country_key = _country_key(cfg.get("country", ""))

        # 1) Construit les sources à partir des annuaires seeds
        seed_site_urls = SEED_SITES.get(country_key, [])
        seed_site_sources = _to_sources(seed_site_urls, "lawyers")

        # 2) Sources déjà présentes dans le projet (GUI/JSON)
        project_sources = cfg.get("sources") or []

        # 3) Fusion : Project seeds + nos seeds annuaires
        merged_sources = []
        if isinstance(project_sources, list):
            merged_sources.extend(project_sources)
        elif isinstance(project_sources, dict):
            # Autorise projet à fournir {"seed_sources":[...]}
            merged_sources.extend(project_sources.get("seed_sources", []))

        merged_sources.extend(seed_site_sources)

        # 4) Pousse les SEED_QUERIES dans cfg["keywords"] (optionnel, utile au SearchScraper)
        qs = SEED_QUERIES.get(country_key, [])
        extra_kw = "; ".join(qs)
        base_kw = (cfg.get("keywords") or "").strip()
        cfg["keywords"] = f"{base_kw}; {extra_kw}".strip("; ").strip()

        # 5) Remise des sources fusionnées et exécution generic
        cfg["sources"] = merged_sources
        cfg["profession"] = cfg.get("profession") or "Avocats"
        cfg["keep_incomplete"] = True

        return self.generic.scrape(cfg)
