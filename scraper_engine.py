# MOTEUR DE SCRAPING - SCRAPMASTER ENGINE
# Nom du fichier : scraper_engine.py

import requests
from bs4 import BeautifulSoup
import time
import re
import json
import importlib
import os
from datetime import datetime
from typing import List, Dict, Any, Optional
import logging
from functools import lru_cache

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# jsonschema (validation sources)
# ---------------------------------------------------------------------
try:
    import jsonschema  # type: ignore
except ImportError:  # fallback doux si jsonschema absent
    class _DummySchema:
        @staticmethod
        def validate(*args, **kwargs):
            return None
    jsonschema = _DummySchema()  # type: ignore

SOURCES_SCHEMA: Dict[str, Any] = {
    "type": "array",
    "items": {
        "type": "object",
        "required": ["name", "categories"],
        "properties": {
            "name": {"type": "string", "minLength": 1},
            "base_url": {"type": "string"},
            "categories": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["name", "url"],
                    "properties": {
                        "name": {"type": "string"},
                        "url": {"type": "string", "format": "uri"}
                    }
                }
            }
        }
    }
}

# --- Scrapers principaux ---
# GenericScraper (fallback solide)
try:
    from scrapers.generic_scraper import GenericScraper
except ImportError:
    class GenericScraper:
        def scrape(self, config):
            print("⚠️ GenericScraper fallback utilisé")
            return []

# SearchScraper (orchestrateur "search-first")
try:
    from scrapers.search_scraper import SearchScraper
except ImportError:
    class SearchScraper:
        def search(self, profession, country, language, extra_keywords=""):
            print("⚠️ SearchScraper non disponible, aucune URL trouvée via recherche")
            return []

# ---------------------------------------------------------------------
# Helper cache pour charger/valider les sources par pays
# ---------------------------------------------------------------------
@lru_cache(maxsize=32)
def _load_sources_cached(country: str) -> List[dict]:
    """Charge et valide les sources pour un pays, avec cache LRU (module-level)."""
    sources_files = {
        'Thaïlande': 'thailand_sources.json',
        'France': 'france_sources.json',
        'Expatriés Thaïlande': 'expat_thailand_sources.json',
        'Digital Nomads Asie': 'nomads_asia_sources.json',
        'Voyageurs Asie du Sud-Est': 'travelers_sea_sources.json',
        'Royaume-Uni': 'uk_sources.json',
        'États-Unis': 'usa_sources.json',
        'Allemagne': 'germany_sources.json'
    }

    filename = sources_files.get(country, 'generic_sources.json')

    base_dir = os.path.dirname(__file__)
    possible_paths = [
        os.path.join('sources', filename),
        os.path.join('..', 'sources', filename),
        os.path.join('.', 'sources', filename),
        os.path.join(base_dir, 'sources', filename),
    ]

    for filepath in possible_paths:
        try:
            if os.path.exists(filepath):
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                # Validation stricte (si jsonschema dispo)
                try:
                    jsonschema.validate(data, SOURCES_SCHEMA)  # type: ignore
                except Exception as e:
                    print(f"❌ Sources invalides {filepath}: {e}")
                    continue

                print(f"📂 Sources validées: {filepath}")
                return data  # type: ignore[list-item]
        except json.JSONDecodeError as e:
            print(f"❌ JSON invalide {filepath}: {e}")
            continue
        except Exception as e:
            print(f"❌ Erreur lecture {filepath}: {e}")
            continue

    # Si aucune source valide trouvée -> liste vide (laissera place aux defaults)
    return []


class ScrapingEngine:
    """Moteur principal de scraping modulaire"""

    def __init__(self):
        # Scrapers "métier" dynamiques
        self.scrapers: Dict[str, Any] = {}
        self.load_scrapers()

        # Orchestrateurs clés
        self.generic = GenericScraper()
        self.searcher = SearchScraper()

    # -----------------------------------------------------------------
    # Normalisation robuste du projet
    # -----------------------------------------------------------------
    def _normalize_project_config(self, project) -> dict:
        """Convertit project (tuple/Row/dict) en dict standardisé"""
        if isinstance(project, dict):
            return project.copy()

        if hasattr(project, '_fields'):  # namedtuple
            return project._asdict()

        if isinstance(project, (tuple, list)):
            # Mapping explicite avec defaults
            fields = [
                'id', 'name', 'profession', 'country', 'language',
                'sources', 'status', 'created_at', 'last_run', 'total_results'
            ]
            result = {}
            for i, field in enumerate(fields):
                result[field] = project[i] if i < len(project) else None
            return result

        # sqlite3.Row (et assimilés)
        if hasattr(project, 'keys'):
            return dict(project)

        raise TypeError(f"Type project non supporté: {type(project)}")

    def load_scrapers(self):
        """Charge tous les scrapers modulaires avec gestion d'erreur robuste"""
        scrapers_config = {
            'youtube_scraper': 'YouTubeChannelScraper',
            'lawyer_scraper': 'LawyerScraper',
            'association_scraper': 'AssociationScraper',
            'translator_scraper': 'TranslatorScraper',
            'nomad_scraper': 'NomadScraper',
            'restaurant_scraper': 'RestaurantScraper',
            'hotel_scraper': 'HotelScraper',
            'generic_scraper': 'GenericScraper'
        }

        for scraper_name, class_name in scrapers_config.items():
            try:
                module = importlib.import_module(f'scrapers.{scraper_name}')
                scraper_class = getattr(module, class_name, None) or getattr(module, 'Scraper', None)
                if scraper_class:
                    self.scrapers[scraper_name] = scraper_class()
                    print(f"✅ Scraper chargé: {scraper_name} -> {class_name}")
                else:
                    print(f"⚠️ Classe {class_name} non trouvée dans {scraper_name}")
                    self.scrapers[scraper_name] = GenericScraper()
            except ImportError as e:
                print(f"⚠️ Module {scraper_name} non trouvé: {e}")
                self.scrapers[scraper_name] = GenericScraper()
            except Exception as e:
                print(f"❌ Erreur chargement {scraper_name}: {e}")
                self.scrapers[scraper_name] = GenericScraper()

        if 'generic_scraper' not in self.scrapers:
            self.scrapers['generic_scraper'] = GenericScraper()

        print(f"🔧 {len(self.scrapers)} scrapers chargés")

    # ---------------------------------------------------------------------
    # ORCHESTRATION "SEARCH → GENERIC"
    # ---------------------------------------------------------------------
    def run_scraping(self, project):
        """
        Orchestration principale.
        1) Normalise le projet (tuple/dict/Row) -> dict project_cfg
        2) Lance une recherche auto (SearchScraper) pour collecter des URLs cibles
        3) Construit des sources "consommables" par le GenericScraper
        4) Fusionne avec d'éventuels seeds existants + sources pays
        5) Exécute le GenericScraper
        6) Valide et renvoie les résultats
        """
        # --- Normalisation du format project ---
        project_cfg = self._normalize_project_config(project)

        # Si sources au format str -> JSON
        if isinstance(project_cfg.get("sources"), str):
            try:
                project_cfg["sources"] = json.loads(project_cfg["sources"])
            except Exception:
                project_cfg["sources"] = []

        # --- Lecture des paramètres ---
        profession = (project_cfg.get("profession") or "").strip()
        country    = (project_cfg.get("country") or "").strip()
        language   = (project_cfg.get("language") or "en").strip()

        # Récupération des keywords éventuels (depuis sources si dict)
        keywords = ""
        srcs = project_cfg.get("sources")
        if isinstance(srcs, dict):
            keywords = (srcs.get("keywords") or "").strip()

        print(f"🚀 Début scraping: {project_cfg.get('name')}")
        print(f"📊 Métier: {profession} | Pays: {country} | Langue: {language}")

        # --- 1) Recherche automatique → URLs candidates ---
        try:
            found_urls = self.searcher.search(profession, country, language, extra_keywords=keywords) or []
        except Exception as e:
            print(f"⚠️ SearchScraper erreur: {e}")
            found_urls = []

        # --- 2) Construire des sources "consommables" par le GenericScraper ---
        search_sources = [{
            "name": f"Search:{profession}-{country}-{language}",
            "categories": [
                {"name": "search", "url": u if u.endswith("/") else u + "/"}
                for u in found_urls[:50]
            ]
        }]

        # --- 3) Fusionner avec d'éventuelles sources existantes (seeds) + sources pays ---
        seeds: List[dict] = []
        if isinstance(srcs, list):
            seeds = srcs
        elif isinstance(srcs, dict) and srcs.get("seed_sources"):
            seeds = srcs["seed_sources"]

        # pays -> sources validées (cache)
        validated_country_sources = _load_sources_cached(country) or []

        # Priorité : seeds + sources pays validées + search_sources
        sources = (seeds or []) + (validated_country_sources or [])
        sources += search_sources

        # --- 4) Lancer le GenericScraper sur ces sources ---
        cfg = dict(project_cfg)
        cfg["sources"] = sources
        cfg["keep_incomplete"] = True

        try:
            raw_results = self.generic.scrape(cfg) or []
            print(f"✅ Scraping terminé (generic): {len(raw_results)} résultats")
        except Exception as e:
            print(f"❌ Erreur GenericScraper: {e}")
            raw_results = []

        # --- 5) Validation / nettoyage stricts ---
        validated_results = self.validate_results(raw_results, cfg)
        print(f"🎯 Résultats validés: {len(validated_results)} résultats")

        return validated_results

    # ---------------------------------------------------------------------
    # OUTILS EXISTANTS / COMPAT
    # ---------------------------------------------------------------------
    def extract_keywords_from_sources(self, project_sources):
        """Extrait les keywords des sources du projet (compat ancien)."""
        keywords = ""
        try:
            if isinstance(project_sources, list) and project_sources:
                for source in project_sources:
                    if isinstance(source, dict) and 'keywords' in source:
                        keywords = source['keywords']
                        break
            elif isinstance(project_sources, dict) and 'keywords' in project_sources:
                keywords = project_sources['keywords']
        except Exception:
            pass
        return keywords

    def get_scraper_template(self, profession):
        """Détermine le template de scraper selon le métier (si jamais utilisé ailleurs)."""
        profession_mapping = {
            'YouTubeurs': 'youtube_scraper',
            'Avocats': 'lawyer_scraper',
            'Associations': 'association_scraper',
            'Traducteurs': 'translator_scraper',
            'Interprètes': 'translator_scraper',
            'Digital Nomads': 'nomad_scraper',
            'Restaurateurs': 'restaurant_scraper',
            'Hôteliers': 'hotel_scraper'
        }
        return profession_mapping.get(profession, 'generic_scraper')

    # ---------------------------------------------------------------------
    # Chargement sources (wrap vers cache)
    # ---------------------------------------------------------------------
    def load_sources(self, country: str) -> List[dict]:
        """Charge sources avec cache et validation (via helper LRU)."""
        data = _load_sources_cached(country)
        if data:
            return data
        print(f"📂 Utilisation sources par défaut pour: {country}")
        return self.get_default_sources(country)

    def get_default_sources(self, country):
        """Sources par défaut minimales selon le pays (compat ancien)."""
        if 'Thaïlande' in country or 'Thailand' in country:
            return [
                {
                    'name': 'Annuaire Thailand Guide',
                    'base_url': 'https://annuaire.thailande-guide.com',
                    'categories': [
                        {'name': 'associations', 'url': 'https://annuaire.thailande-guide.com/fr/cat-61/'},
                        {'name': 'services', 'url': 'https://annuaire.thailande-guide.com/fr/cat-28/'},
                        {'name': 'clubs', 'url': 'https://annuaire.thailande-guide.com/fr/cat-120/'},
                        {'name': 'universites', 'url': 'https://annuaire.thailande-guide.com/fr/cat-58/'},
                        {'name': 'sante', 'url': 'https://annuaire.thailande-guide.com/fr/cat-71/'}
                    ]
                }
            ]
        elif country == 'France':
            return [
                {
                    'name': 'Pages Jaunes France',
                    'base_url': 'https://www.pagesjaunes.fr',
                    'categories': [
                        {'name': 'associations', 'url': 'https://www.pagesjaunes.fr/annuaire/chercherlespros?quoiqui=association'},
                        {'name': 'services', 'url': 'https://www.pagesjaunes.fr/annuaire/chercherlespros?quoiqui=service'}
                    ]
                }
            ]
        elif 'Expatriés' in country:
            return [
                {
                    'name': "Communautés d'expatriés",
                    'base_url': 'https://www.expat.com',
                    'categories': [
                        {'name': 'communautes', 'url': 'https://www.expat.com/forum/'},
                        {'name': 'services_expat', 'url': 'https://www.expat.com/services/'}
                    ]
                }
            ]
        else:
            return [
                {
                    'name': 'Sources génériques',
                    'base_url': 'https://example.com',
                    'categories': [
                        {'name': 'general', 'url': 'https://example.com/directory/'}
                    ]
                }
            ]

    # ---------------------------------------------------------------------
    # Validation / nettoyage des résultats (STRICT + LOGGING)
    # ---------------------------------------------------------------------
    def validate_results(self, results: List[dict], config: dict) -> List[dict]:
        """Valide et nettoie les résultats avec reporting détaillé"""
        validated: List[dict] = []
        stats = {
            'total_input': len(results),
            'skipped_not_dict': 0,
            'skipped_no_name': 0,
            'skipped_no_contact': 0,
            'validated': 0
        }

        for i, result in enumerate(results):
            if not isinstance(result, dict):
                stats['skipped_not_dict'] += 1
                logger.warning(f"Résultat #{i} ignoré: pas un dict", extra={"result_type": str(type(result))})
                continue

            name = (result.get('name') or '').strip()
            if not name or len(name) < 2:
                stats['skipped_no_name'] += 1
                logger.warning(f"Résultat #{i} ignoré: nom invalide", extra={"name": name})
                continue

            # Validation contact obligatoire
            has_contact = any([
                result.get('website'),
                result.get('email'),
                result.get('phone'),
                result.get('facebook'),
                result.get('whatsapp'),
                (result.get('description') or '') and len(result.get('description', '')) > 50
            ])

            if not has_contact:
                stats['skipped_no_contact'] += 1
                logger.warning(f"Résultat #{i} ignoré: aucun contact", extra={"name": name})
                continue

            # Nettoyage et normalisation
            cleaned = self._clean_result(result, config)
            validated.append(cleaned)
            stats['validated'] += 1

        logger.info("Validation résultats terminée", extra=stats)
        return validated

    # --- Nettoyage unifié (utilise les helpers existants) ---
    def _clean_result(self, r: dict, config: dict) -> dict:
        name = self.clean_text((r.get('name') or ''))[:500]
        description = self.clean_text((r.get('description') or ''))[:2000]
        category = self.clean_text((r.get('category') or ''))[:200]

        website = self.clean_url(r.get('website', ''))
        email = self.clean_email(r.get('email', ''))
        phone = self.clean_phone(r.get('phone', ''))

        city = self.clean_text((r.get('city') or ''))[:200]
        country = r.get('country') or config.get('country', '')
        language = r.get('language') or config.get('language', '')

        source_url = self.clean_url(r.get('source_url', ''))

        # Socials / contacts additionnels (nettoyage léger)
        facebook = self.clean_url(r.get('facebook', ''))
        instagram = self.clean_url(r.get('instagram', ''))
        linkedin = self.clean_url(r.get('linkedin', ''))
        line_id = self.clean_text(r.get('line_id', ''))[:200]
        whatsapp = self.clean_text(r.get('whatsapp', ''))[:200]
        telegram = self.clean_url(r.get('telegram', ''))  # souvent URL
        wechat = self.clean_text(r.get('wechat', ''))[:200]
        other_contact = self.clean_text(r.get('other_contact', ''))[:200]
        contact_name = self.clean_text(r.get('contact_name', ''))[:200]
        province = self.clean_text(r.get('province', ''))[:200]
        address = self.clean_text(r.get('address', ''))[:500]
        latitude = self.clean_text(r.get('latitude', ''))[:100]
        longitude = self.clean_text(r.get('longitude', ''))[:100]

        cleaned = {
            'name': name,
            'category': category,
            'description': description,
            'website': website,
            'email': email,
            'phone': phone,
            'city': city,
            'country': country,
            'language': language,
            'source_url': source_url,
            'profession': config.get('profession', ''),
            'scraped_at': r.get('scraped_at', datetime.now().isoformat()),
            'quality_score': r.get('quality_score', 5),

            # Champs étendus (compat DB)
            'facebook': facebook,
            'instagram': instagram,
            'linkedin': linkedin,
            'line_id': line_id,
            'whatsapp': whatsapp,
            'telegram': telegram,
            'wechat': wechat,
            'other_contact': other_contact,
            'contact_name': contact_name,
            'province': province,
            'address': address,
            'latitude': latitude,
            'longitude': longitude,
        }
        return cleaned

    # ---------------------------------------------------------------------
    # Helpers de nettoyage existants
    # ---------------------------------------------------------------------
    def clean_text(self, text):
        """Nettoie un texte"""
        if not text:
            return ''
        cleaned = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x84\x86-\x9f]', '', str(text))
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        return cleaned

    def clean_url(self, url):
        """Nettoie une URL"""
        if not url:
            return ''
        url = str(url).strip()
        if not url:
            return ''
        if not url.startswith(('http://', 'https://')):
            if url.startswith('www.'):
                url = 'https://' + url
            elif '.' in url and not url.startswith('//'):
                url = 'https://' + url
        if not re.match(r'https?://[^\s]+\.[^\s]+', url):
            return ''
        return url[:500]

    def clean_email(self, email):
        """Nettoie un email"""
        if not email:
            return ''
        email = str(email).strip().lower()
        if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', email):
            return email[:200]
        return ''

    def clean_phone(self, phone):
        """Nettoie un numéro de téléphone"""
        if not phone:
            return ''
        phone = str(phone).strip()
        phone = re.sub(r'[^\d\s+()-]', '', phone)
        digits_only = re.sub(r'[^\d]', '', phone)
        if len(digits_only) >= 7:
            return phone[:50]
        return ''

    def is_valid_result(self, result):
        """Vérifie si un résultat est valide"""
        if not result.get('name'):
            return False
        has_contact = (
            result.get('website') or
            result.get('email') or
            result.get('phone') or
            (result.get('description') and len(result.get('description')) > 50)
        )
        return has_contact


# ---------------------------------------------------------------------
# Scraper générique de fallback intégré (pour compat ancienne)
# ---------------------------------------------------------------------
class FallbackGenericScraper:
    """Scraper générique de fallback intégré"""

    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/91.0.4472.124 Safari/537.36'
        }

    def scrape(self, config):
        """Lance le scraping générique"""
        print("🔧 Utilisation du scraper générique de fallback")

        all_results = []
        sources = config.get('sources', [])

        if not sources:
            print("⚠️ Aucune source configurée")
            return []

        for source in sources[:3]:
            print(f"📂 Source: {source.get('name', 'Sans nom')}")
            categories = source.get('categories', [])
            if not categories:
                continue

            for category in categories[:2]:
                print(f"  📄 Catégorie: {category.get('name', 'general')}")
                try:
                    results = self.scrape_category(
                        category.get('url', ''),
                        category.get('name', 'general'),
                        config
                    )
                    all_results.extend(results)
                    print(f"    ✅ {len(results)} résultats")
                except Exception as e:
                    print(f"    ❌ Erreur: {e}")

                time.sleep(1)

        print(f"🎯 Total scraper générique: {len(all_results)} résultats")
        return all_results

    def scrape_category(self, base_url, category_name, config):
        """Scrape une catégorie spécifique"""
        if not base_url:
            return []

        results = []
        for page in range(1, 4):
            try:
                if '/cat-' in base_url and 'thailande-guide.com' in base_url:
                    url = f"{base_url}{page}/index.html"
                else:
                    url = base_url
                    if page > 1:
                        url += f"?page={page}"

                response = requests.get(url, headers=self.headers, timeout=10)

                if response.status_code == 404:
                    break

                if response.status_code != 200:
                    print(f"    ⚠️ Status {response.status_code} pour {url}")
                    continue

                soup = BeautifulSoup(response.content, 'html.parser')
                page_results = self.extract_from_page(soup, url, category_name, config)

                if not page_results:
                    if page == 1:
                        print(f"    ⚠️ Aucune donnée extraite de {url}")
                    break

                results.extend(page_results)

                if len(results) >= 50:
                    break

                time.sleep(0.5)

            except Exception as e:
                print(f"    ❌ Erreur page {page}: {e}")
                break

        return results

    def extract_from_page(self, soup, source_url, category, config):
        """Extraction basique de données depuis une page"""
        results = []
        links = soup.find_all('a', href=True)

        for link in links[:20]:
            href = link.get('href', '').strip()
            title = link.get_text(strip=True)

            if not title or len(title) < 5 or len(title) > 200:
                continue

            if any(word in title.lower() for word in ['accueil', 'contact', 'mentions', 'top clics', 'nouveautés']):
                continue

            if href.startswith('http'):
                website = href
            elif href.startswith('/'):
                from urllib.parse import urljoin
                website = urljoin(source_url, href)
            else:
                continue

            result = {
                'name': title,
                'category': category,
                'description': f"Trouvé dans la catégorie {category}",
                'website': website,
                'email': '',
                'phone': '',
                'city': '',
                'country': config.get('country', ''),
                'language': config.get('language', ''),
                'source_url': source_url,
                'profession': config.get('profession', ''),
                'scraped_at': datetime.now().isoformat(),
                'quality_score': 3
            }

            results.append(result)

        return results
