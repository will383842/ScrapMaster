# Scraper spécialisé "Hôteliers" — s'appuie sur GenericScraper mais fournit des seeds
from .generic_scraper import GenericScraper

class HotelScraper(GenericScraper):
    """Scraper pour Hôteliers (alias GenericScraper pour l’instant)
    
    Fournit des seeds (sites et requêtes) par pays/langue et une méthode get_seeds()
    que GenericScraper peut appeler si elle est présente.
    """

    # Seeds statiques (sites) par pays ; "_global" = valable partout
    SEED_SITES = {
        "_global": [
            "https://www.google.com/search?q=hotel+associations+{country}"
        ]
        # Exemple pour spécialiser la Thaïlande :
        # "thaïlande": [
        #     "https://www.google.com/search?q=hotel+association+Thailand",
        #     "https://www.google.com/search?q=hoteliers+club+Thailand"
        # ]
    }

    # Modèles de requêtes par langue ; "_any" = fallback si la langue n'est pas listée
    SEED_QUERIES = {
        "en": [
            "https://www.google.com/search?q=expat+hoteliers+{country}+{keywords}"
        ],
        "_any": [
            "https://www.google.com/search?q=hotelier+{country}+{keywords}"
        ]
    }

    def get_seeds(self, country: str, language: str, keywords: str = ""):
        """Retourne une liste d'URLs seeds (sites directs + requêtes moteur) pour lancer le crawl."""
        sites = []
        lang = (language or "en").lower()
        ctry = (country or "").lower()

        # 1) Sites statiques par pays
        if ctry in self.SEED_SITES:
            sites += self.SEED_SITES[ctry]
        if "_global" in self.SEED_SITES:
            sites += self.SEED_SITES["_global"]

        # 2) Requêtes de recherche par langue (fallback sur _any)
        queries = []
        for tmpl in self.SEED_QUERIES.get(lang, self.SEED_QUERIES.get("_any", [])):
            q = tmpl.format(country=country, language=language, keywords=keywords or "")
            queries.append(q)

        # 3) Déduplique tout en conservant l'ordre
        return list(dict.fromkeys(sites + queries))

# Compatibilité attendue par le moteur
Scraper = HotelScraper
