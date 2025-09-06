# Scraper spécialisé "Traducteurs" basé sur GenericScraper
# - Définit la classe attendue par le moteur : TranslatorScraper
# - Fournit des SEED_SITES / SEED_QUERIES par langue/pays
# - Alias Scraper = TranslatorScraper pour compat

from .generic_scraper import GenericScraper


class TranslatorScraper(GenericScraper):
    """Scraper pour Traducteurs : hérite de GenericScraper et fournit des seeds par pays/langue.
    Compatible avec le moteur qui attend la classe TranslatorScraper.
    """

    # Sites de départ (peuvent être étendus/édités)
    SEED_SITES = {
        "_global": [
            "https://www.proz.com/translator-directory"
        ]
        # Exemple pour personnaliser par pays :
        # "thailand": [...],
        # "thaïlande": [...],
    }

    # Requêtes (URLs de recherche) par langue
    SEED_QUERIES = {
        "en": [
            "https://www.google.com/search?q=translator+{country}+{keywords}"
        ],
        "fr": [
            "https://www.google.com/search?q=traducteur+{country}+{keywords}"
        ],
        "_any": [
            "https://www.google.com/search?q=interpreter+{country}+{keywords}"
        ],
    }

    def get_seeds(self, country: str, language: str, keywords: str = ""):
        """Retourne la liste d'URLs seed (sites + recherches) en fonction du pays/langue."""
        sites = []
        lang = (language or "en").lower()
        ctry = (country or "").lower()

        # 1) Sites statiques par pays + globaux
        if ctry in self.SEED_SITES:
            sites += self.SEED_SITES[ctry]
        if "_global" in self.SEED_SITES:
            sites += self.SEED_SITES["_global"]

        # 2) Requêtes de recherche selon la langue (fallback sur _any)
        queries = []
        for tmpl in self.SEED_QUERIES.get(lang, self.SEED_QUERIES.get("_any", [])):
            queries.append(tmpl.format(country=country, language=language, keywords=keywords or ""))

        # 3) Fusion unique en conservant l'ordre
        seen = set()
        out = []
        for url in sites + queries:
            if url not in seen:
                seen.add(url)
                out.append(url)
        return out


# Alias de compatibilité pour les chargeurs qui s'attendent à "Scraper"
Scraper = TranslatorScraper
