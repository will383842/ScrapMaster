# -*- coding: utf-8 -*-
# Scraper "Avocats" — spécialisé mais basé sur GenericScraper.
# Fournit des sites/requêtes de départ adaptés, tout en restant compatible avec ton moteur.

from .generic_scraper import GenericScraper


class LawyerScraper(GenericScraper):
    """Scraper pour Avocats (alias GenericScraper pour l’instant)"""

    # Country- and language-aware seeds (modifiables facilement)
    SEED_SITES = {
        "_global": [
            "https://www.thailawonline.com/",
            "https://www.lawyerscouncil.or.th/"
        ]
    }

    SEED_QUERIES = {
        "en": [
            "https://www.google.com/search?q=lawyer+for+expats+{country}+{keywords}"
        ],
        "fr": [
            "https://www.google.com/search?q=avocat+%C3%A9trangers+{country}+{keywords}"
        ],
        "_any": [
            "https://www.google.com/search?q=lawyer+{country}+{keywords}"
        ]
    }

    def get_seeds(self, country: str, language: str, keywords: str = ""):
        """
        Retourne une liste d’URLs “seed” (sites directs + requêtes moteurs),
        en fonction du pays, de la langue et de mots-clés optionnels.
        """
        sites = []
        lang = (language or "en").lower()
        ctry = (country or "").lower()

        # 1) Sites statiques par pays
        if ctry in self.SEED_SITES:
            sites += self.SEED_SITES[ctry]
        if "_global" in self.SEED_SITES:
            sites += self.SEED_SITES["_global"]

        # 2) Requêtes (templates) selon la langue
        queries = []
        for tmpl in self.SEED_QUERIES.get(lang, self.SEED_QUERIES.get("_any", [])):
            q = tmpl.format(country=country, language=language, keywords=keywords or "")
            queries.append(q)

        # Fusion unique en conservant l’ordre
        return list(dict.fromkeys(sites + queries))


# Compatibilité avec le moteur (si celui-ci attend "Scraper")
Scraper = LawyerScraper
