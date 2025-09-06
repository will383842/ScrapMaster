# Auto-stub: specialized scraper that leverages GenericScraper but provides
# seed queries/sites tailored to the profession (Digital Nomads).

from .generic_scraper import GenericScraper

class NomadScraper(GenericScraper):
    """Scraper pour Digital Nomads (alias GenericScraper pour l’instant).
    Fournit des seeds par langue/pays et peut être étendu dans VS Code.
    """

    # Country- and language-aware seeds (override/extend as you like)
    SEED_SITES = {
        "_global": [
            "https://nomadlist.com/places/thailand",
            "https://www.meetup.com/topics/digital-nomad/",
        ]
        # Tu peux ajouter un bloc spécifique pays si besoin, ex.:
        # "thaïlande": ["https://exemple.org/ressources-nomads-thailande"]
    }

    SEED_QUERIES = {
        "en": [
            "https://www.google.com/search?q=digital+nomad+group+{country}+{keywords}"
        ],
        "_any": [
            "https://www.google.com/search?q=expat+remote+workers+{country}+{keywords}"
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


# Compatibilité avec ton moteur (qui peut chercher une classe 'Scraper')
Scraper = NomadScraper
