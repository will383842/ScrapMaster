# Scraper spécialisé "Associations" — hérite de GenericScraper
# et fournit des seeds (sites/queries) adaptés par pays/langue.

from .generic_scraper import GenericScraper


class AssociationScraper(GenericScraper):
    """Scraper pour Associations (alias GenericScraper pour l’instant) avec seeds dédiés."""

    # Sites de départ (par pays + global)
    SEED_SITES = {
        "_global": [
            "https://www.facebook.com/search/groups/?q=expat%20thailand",
            "https://www.meetup.com/find/?source=EVENTS&location=th--Bangkok",
            "https://www.britishcouncil.or.th/en",
            "https://www.alliancefr.org/en",
            "https://www.goethe.de/ins/th/en/index.html",
        ],
        # variantes d'écriture pour robustesse
        "thaïlande": [
            "https://www.mfa.go.th/en/content/associations-1",
            "https://www.mots.go.th/",
            "https://www.thailand.go.th/",
        ],
        "thailande": [
            "https://www.mfa.go.th/en/content/associations-1",
            "https://www.mots.go.th/",
            "https://www.thailand.go.th/",
        ],
        "thailand": [
            "https://www.mfa.go.th/en/content/associations-1",
            "https://www.mots.go.th/",
            "https://www.thailand.go.th/",
        ],
    }

    # Modèles de requêtes (choisis selon la langue)
    SEED_QUERIES = {
        "en": [
            "https://www.google.com/search?q=expat+association+{country}",
            "https://www.google.com/search?q=foreigner+community+{country}+facebook",
            "https://www.google.com/search?q=ngo+helping+expats+{country}",
        ],
        "fr": [
            "https://www.google.com/search?q=association+expatri%C3%A9s+{country}",
            "https://www.google.com/search?q=entraide+expatri%C3%A9s+{country}+facebook",
        ],
        "th": [
            "https://www.google.com/search?q=%E0%B8%AA%E0%B8%A1%E0%B8%B2%E0%B8%84%E0%B8%A1+%E0%B8%8A%E0%B8%B2%E0%B8%A7%E0%B8%95%E0%B9%88%E0%B8%B2%E0%B8%87%E0%B8%8A%E0%B8%B2%E0%B8%95%E0%B8%B4+{country}",
        ],
        "_any": [
            "https://www.google.com/search?q=expat+community+{country}+{keywords}",
        ],
    }

    def get_seeds(self, country: str, language: str, keywords: str = ""):
        """Retourne une liste d’URLs seeds (sites & recherches) dédupliquée, ordonnée."""
        sites = []
        lang = (language or "en").lower()
        ctry = (country or "").lower()

        # Sites statiques par pays (gère quelques variantes d’orthographe)
        for key in (ctry, ctry.replace("ï", "i"), ctry.replace("é", "e")):
            if key in self.SEED_SITES:
                sites += self.SEED_SITES[key]
        if "_global" in self.SEED_SITES:
            sites += self.SEED_SITES["_global"]

        # Requêtes par langue
        templates = self.SEED_QUERIES.get(lang) or self.SEED_QUERIES.get("_any", [])
        queries = [t.format(country=country, language=language, keywords=keywords or "") for t in templates]

        # Déduplication en conservant l’ordre
        seen, merged = set(), []
        for url in (sites + queries):
            if url not in seen:
                seen.add(url)
                merged.append(url)
        return merged


# Alias attendu par le moteur
Scraper = AssociationScraper
