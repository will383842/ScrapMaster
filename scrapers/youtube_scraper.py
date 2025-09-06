# Scraper spécialisé "YouTube" — s'appuie sur GenericScraper mais
# fournit des seeds (sites & requêtes) adaptés aux YouTubeurs/expats.
from .generic_scraper import GenericScraper


class YouTubeChannelScraper(GenericScraper):
    """Scraper pour YouTubeurs (alias GenericScraper pour l’instant).
    Fournit des seeds spécifiques et une méthode get_seeds(country, language, keywords).
    """

    # Seeds par pays/langue. Tu peux enrichir librement ces listes.
    # Astuce : des placeholders {country}, {language}, {keywords} peuvent être utilisés,
    # ils seront formatés dans get_seeds().
    SEED_SITES = {
        "_global": [
            "https://www.youtube.com/results?search_query=expat+in+{country}+{keywords}"
        ]
    }

    SEED_QUERIES = {
        "en": [
            "https://www.youtube.com/results?search_query=expat+in+{country}+{keywords}"
        ],
        "fr": [
            "https://www.youtube.com/results?search_query=expatri%C3%A9s+{country}+{keywords}"
        ],
        "_any": [
            "https://www.youtube.com/results?search_query=expat+{country}+{keywords}"
        ]
    }

    def get_seeds(self, country: str, language: str, keywords: str = ""):
        """Construit la liste des URLs seeds à partir des sites et modèles de requêtes.
        - Formatte aussi les placeholders présents dans SEED_SITES et SEED_QUERIES.
        - Déduplique en conservant l'ordre.
        """
        lang = (language or "en").lower()
        ctry = (country or "").lower()
        kw = keywords or ""

        sites = []
        # 1) sites statiques par pays + globaux
        if ctry in self.SEED_SITES:
            sites += self.SEED_SITES[ctry]
        if "_global" in self.SEED_SITES:
            sites += self.SEED_SITES["_global"]

        # formattage des placeholders éventuels des sites
        sites = [s.format(country=country, language=language, keywords=kw) for s in sites]

        # 2) modèles de requêtes (moteur de recherche / YouTube)
        queries = []
        for tmpl in self.SEED_QUERIES.get(lang, self.SEED_QUERIES.get("_any", [])):
            q = tmpl.format(country=country, language=language, keywords=kw)
            queries.append(q)

        # 3) retour fusionné et dédupliqué (ordre préservé)
        return list(dict.fromkeys(sites + queries))


# Compatibilité avec ton moteur : il peut chercher "Scraper" par défaut
Scraper = YouTubeChannelScraper
