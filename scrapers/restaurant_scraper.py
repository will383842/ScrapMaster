# Scraper spécialisé "Restaurant" s'appuyant sur GenericScraper
from .generic_scraper import GenericScraper

class RestaurantScraper(GenericScraper):
    """Scraper pour Restaurateurs (alias GenericScraper pour l’instant)"""

    # Country- and language-aware seeds (override/extend in VS Code as you like)
    SEED_SITES = {
        "_global": [
            "https://www.google.com/search?q=restaurant+associations+{country}"
        ]
    }

    SEED_QUERIES = {
        "en": [
            "https://www.google.com/search?q=expat+restaurant+owners+{country}+{keywords}"
        ],
        "_any": [
            "https://www.google.com/search?q=restaurant+{country}+{keywords}"
        ]
    }

    def get_seeds(self, country: str, language: str, keywords: str = ""):
        # 1) static seeds per country/language
        sites = []
        lang = (language or "en").lower()
        ctry = (country or "").lower()

        # collect static sites by country
        if ctry in self.SEED_SITES:
            sites += self.SEED_SITES[ctry]
        if "_global" in self.SEED_SITES:
            sites += self.SEED_SITES["_global"]

        # 2) query templates (search engine URLs)
        queries = []
        for tmpl in self.SEED_QUERIES.get(lang, self.SEED_QUERIES.get("_any", [])):
            q = tmpl.format(country=country, language=language, keywords=keywords or "")
            queries.append(q)

        # return combined
        # GenericScraper should be able to handle both direct sites and search URLs
        return list(dict.fromkeys(sites + queries))  # unique, keep order


# Compatibilité avec le moteur (fallback sur 'Scraper')
Scraper = RestaurantScraper
