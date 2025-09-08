# config/semantic_database.py
import json
import re
from typing import List, Dict, Set
from pathlib import Path


class SemanticDatabase:
    """Base de données sémantique pour expansion de mots-clés"""

    def __init__(self):
        self.config_dir = Path(__file__).parent
        self.synonyms_db = self._load_synonyms()
        self.sector_db = self._load_sectors()
        self.location_variants = self._load_location_variants()

    def _load_synonyms(self) -> Dict[str, List[str]]:
        """Base de synonymes par profession"""
        return {
            "avocat": [
                "avocat", "avocate", "cabinet d'avocats", "juriste", "conseiller juridique",
                "lawyer", "attorney", "legal counsel", "law firm", "legal advisor",
                "barrister", "solicitor", "legal practitioner", "counsel",
                "cabinet juridique", "étude d'avocat", "société d'avocats"
            ],
            "association": [
                "association", "ONG", "organisation", "fondation", "collectif",
                "NGO", "non-profit", "nonprofit", "charity", "organization",
                "federation", "union", "society", "club", "groupe",
                "organisme", "institution", "mouvement", "réseau"
            ],
            "traducteur": [
                "traducteur", "traductrice", "interprète", "translator", "interpreter",
                "agence de traduction", "translation agency", "bureau de traduction",
                "services linguistiques", "linguistic services", "sworn translator",
                "traducteur assermenté", "expert traducteur"
            ],
            "youtubeur": [
                "youtubeur", "youtubeuse", "créateur de contenu", "content creator",
                "influenceur", "influencer", "vidéaste", "vlogger", "blogueur vidéo",
                "chaîne youtube", "youtube channel", "digital creator"
            ],
            "digital_nomad": [
                "digital nomad", "nomade numérique", "remote worker", "freelance",
                "travailleur nomade", "location independent", "remote professional",
                "digital entrepreneur", "online worker", "télétravailleur"
            ],
            "restaurateur": [
                "restaurateur", "restaurant", "chef", "traiteur", "food business",
                "établissement alimentaire", "food service", "catering", "bistrot",
                "brasserie", "café", "bar restaurant", "food truck"
            ],
            "hôtelier": [
                "hôtelier", "hôtel", "hébergement", "hotel", "accommodation",
                "hospitality", "lodging", "bed and breakfast", "guesthouse",
                "resort", "auberge", "pension", "chambres d'hôtes"
            ]
        }

    def _load_sectors(self) -> Dict[str, Dict]:
        """Base de secteurs d'activité avec contexte"""
        return {
            "juridique": {
                "terms": ["droit", "legal", "justice", "tribunal", "contentieux", "litigation"],
                "related_professions": ["avocat", "juriste", "notaire", "huissier"],
                "keywords": ["conseil", "expertise", "défense", "représentation", "assistance"]
            },
            "humanitaire": {
                "terms": ["humanitaire", "social", "solidarity", "aide", "help", "charity"],
                "related_professions": ["association", "ONG", "bénévole", "volunteer"],
                "keywords": ["mission", "projet", "développement", "communauté", "entraide"]
            },
            "numérique": {
                "terms": ["digital", "numérique", "tech", "technology", "online", "web"],
                "related_professions": ["digital nomad", "développeur", "designer", "consultant"],
                "keywords": ["innovation", "solution", "plateforme", "service", "application"]
            },
            "restauration": {
                "terms": ["food", "cuisine", "gastronomie", "culinary", "alimentaire"],
                "related_professions": ["chef", "restaurateur", "traiteur", "pâtissier"],
                "keywords": ["saveur", "menu", "spécialité", "plat", "service"]
            },
            "tourisme": {
                "terms": ["tourism", "travel", "voyage", "hospitality", "hébergement"],
                "related_professions": ["hôtelier", "guide", "agent de voyage", "tour operator"],
                "keywords": ["séjour", "visite", "découverte", "expérience", "destination"]
            }
        }

    def _load_location_variants(self) -> Dict[str, List[str]]:
        """Variantes géographiques"""
        return {
            "thaïlande": [
                "thailand", "thai", "thaïlande", "kingdom of thailand", "siam",
                "bangkok", "phuket", "chiang mai", "pattaya", "krabi"
            ],
            "france": [
                "france", "french", "français", "république française", "hexagone",
                "paris", "lyon", "marseille", "toulouse", "nice", "bordeaux"
            ],
            "états-unis": [
                "usa", "united states", "america", "us", "états-unis", "amerique",
                "new york", "california", "texas", "florida", "chicago"
            ]
        }

    def expand_profession_keywords(self, profession: str) -> List[str]:
        """Expanse une profession en termes connexes"""
        prof_lower = profession.lower()
        expanded: Set[str] = set([profession])

        # Recherche directe
        if prof_lower in self.synonyms_db:
            expanded.update(self.synonyms_db[prof_lower])

        # Recherche partielle
        for key, synonyms in self.synonyms_db.items():
            if prof_lower in key or any(prof_lower in syn.lower() for syn in synonyms):
                expanded.update(synonyms)

        # Recherche par secteur
        for sector, data in self.sector_db.items():
            if prof_lower in " ".join(data["related_professions"]).lower():
                expanded.update(data["terms"])
                expanded.update(data["keywords"])

        return list(expanded)

    def expand_location_keywords(self, location: str) -> List[str]:
        """Expanse une localisation"""
        loc_lower = location.lower()
        expanded: Set[str] = set([location])

        for key, variants in self.location_variants.items():
            if loc_lower in key or any(loc_lower in var.lower() for var in variants):
                expanded.update(variants)

        return list(expanded)

    def detect_sector(self, keywords: str) -> List[str]:
        """Détecte le secteur d'activité depuis des mots-clés"""
        keywords_lower = keywords.lower()
        detected_sectors: List[str] = []

        for sector, data in self.sector_db.items():
            score = 0
            for term in data["terms"]:
                if term.lower() in keywords_lower:
                    score += 2
            for keyword in data["keywords"]:
                if keyword.lower() in keywords_lower:
                    score += 1

            if score >= 2:
                detected_sectors.append(sector)

        return detected_sectors

    def generate_search_variations(self, profession: str, location: str, keywords: str = "") -> List[str]:
        """Génère des variations de recherche OPTIMISÉES"""
        prof_variants = self.expand_profession_keywords(profession)[:3]  # Réduire à 3
        loc_variants = self.expand_location_keywords(location)[:2]       # Réduire à 2

        variations: List[str] = []

        # 1. Combinaisons de base (plus efficaces)
        for prof in prof_variants:
            for loc in loc_variants:
                variations.append(f"{prof} {loc}")
                variations.append(f"{prof} {loc} contact")

        # 2. Requêtes avec sites spécifiques (plus de résultats)
        base_terms = prof_variants[:2]
        for term in base_terms:
            variations.append(f"{term} {location} site:facebook.com")
            variations.append(f"{term} {location} site:linkedin.com")
            variations.append(f"{term} {location} directory")
            variations.append(f"{term} {location} annuaire")

        # 3. Mots-clés spécifiques
        if keywords:
            keyword_list = [k.strip() for k in keywords.split(',') if k.strip()]
            for kw in keyword_list[:2]:  # Max 2 mots-clés
                variations.append(f"{kw} {location}")
                variations.append(f"{profession} {kw} {location}")

        # Déduplication et limitation à 12 (plus efficace)
        unique_variations = list(dict.fromkeys(variations))
        return unique_variations[:12]


# Instance globale
semantic_db = SemanticDatabase()
