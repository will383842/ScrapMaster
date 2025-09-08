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

# --- Imports utilitaires nécessaires aux enrichissements ---
try:
    from utils.normalize import normalize_phone_list, normalize_url
except Exception:
    # Fallbacks minimaux pour éviter une casse, si utils.normalize indisponible
    def normalize_phone_list(values: List[str], default_region: str = "TH") -> List[str]:
        cleaned = []
        for v in values or []:
            v2 = re.sub(r'[^+\d]', '', v or '')
            if v2:
                cleaned.append(v2)
        # unicité en conservant l'ordre
        seen, out = set(), []
        for p in cleaned:
            if p not in seen:
                seen.add(p); out.append(p)
        return out

    def normalize_url(url: Optional[str]) -> Optional[str]:
        if not url:
            return None
        u = str(url).strip()
        if not u:
            return None
        if not u.startswith(("http://", "https://")):
            if u.startswith("www.") or "." in u:
                u = "https://" + u.lstrip("/")
        return u

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
    # ORCHESTRATION AVANCÉE
    # ---------------------------------------------------------------------
    def run_scraping(self, project):
        """
        Orchestration principale AVANCÉE avec recherche sémantique et enrichissement multi-sources
        """
        start_time = time.time()
        
        # Normalisation du projet (code existant)
        project_cfg = self._normalize_project_config(project)
        
        # Si sources au format str -> JSON
        if isinstance(project_cfg.get("sources"), str):
            try:
                project_cfg["sources"] = json.loads(project_cfg["sources"])
            except Exception:
                project_cfg["sources"] = []
        
        # Paramètres
        profession = (project_cfg.get("profession") or "").strip()
        country = (project_cfg.get("country") or "").strip()
        language = (project_cfg.get("language") or "en").strip()
        
        # Récupération keywords et expansion sémantique
        keywords = ""
        srcs = project_cfg.get("sources")
        if isinstance(srcs, dict):
            keywords = (srcs.get("keywords") or "").strip()
        
        print(f"🚀 Début scraping AVANCÉ: {project_cfg.get('name')}")
        print(f"📊 Métier: {profession} | Pays: {country} | Langue: {language}")
        print(f"🔍 Mots-clés: {keywords}")
        
        # --- 1) RECHERCHE SÉMANTIQUE AVANCÉE ---
        try:
            from scrapers.search_scraper import SearchScraper
            searcher = SearchScraper()
            found_urls = searcher.search(profession, country, language, extra_keywords=keywords) or []
            print(f"🔍 Recherche sémantique: {len(found_urls)} URLs trouvées")
        except Exception as e:
            print(f"⚠️ Erreur recherche sémantique: {e}")
            found_urls = []
        
        # --- 2) CONSTRUCTION SOURCES MULTI-NIVEAUX ---
        search_sources = [{
            "name": f"SemanticSearch:{profession}-{country}-{language}",
            "categories": [
                {"name": "semantic_search", "url": u if u.endswith("/") else u + "/"}
                for u in found_urls[:30]  # Augmenté à 30 pour plus de données
            ]
        }] if found_urls else []
        
        # Sources existantes (seeds + pays)
        seeds: List[dict] = []
        if isinstance(srcs, list):
            seeds = srcs
        elif isinstance(srcs, dict) and srcs.get("seed_sources"):
            seeds = srcs["seed_sources"]
        
        # Sources pays validées
        try:
            validated_country_sources = _load_sources_cached(country, profession) or []  # type: ignore[arg-type]
        except Exception:
            validated_country_sources = _load_sources_cached(country) or []
        
        # Fusion sources avec priorité
        all_sources = []
        all_sources.extend(search_sources)      # Priorité 1: Recherche sémantique
        all_sources.extend(seeds or [])         # Priorité 2: Seeds utilisateur
        all_sources.extend(validated_country_sources or [])  # Priorité 3: Sources pays
        
        print(f"📂 Sources totales: {len(all_sources)} ({len(search_sources)} sémantiques + {len(seeds)} seeds + {len(validated_country_sources)} pays)")
        
        # --- 3) SCRAPING AVEC EXTRACTION STRUCTURÉE ---
        cfg = dict(project_cfg)
        cfg["sources"] = all_sources
        cfg["keep_incomplete"] = True
        cfg["enable_structured_extraction"] = True  # Flag pour extraction avancée
        
        try:
            raw_results = self.generic.scrape(cfg) or []
            print(f"✅ Scraping brut terminé: {len(raw_results)} résultats")
        except Exception as e:
            print(f"❌ Erreur scraping: {e}")
            raw_results = []
        
        # --- 4) ENRICHISSEMENT MULTI-SOURCES ---
        enriched_results = []
        if raw_results:
            print(f"🔄 Début enrichissement multi-sources...")
            try:
                from enrichers.multi_source_enricher import multi_enricher
                for i, result in enumerate(raw_results):
                    try:
                        enriched_result = multi_enricher.enrich_entry_complete(result, cfg)
                        enriched_results.append(enriched_result)
                        if (i + 1) % 10 == 0:
                            print(f"🔄 Enrichissement: {i + 1}/{len(raw_results)} traités")
                        if i % 5 == 0:
                            time.sleep(0.5)
                    except Exception as e:
                        print(f"⚠️ Erreur enrichissement résultat {i}: {e}")
                        enriched_results.append(result)  # Garder non enrichi
                print(f"✅ Enrichissement terminé: {len(enriched_results)} résultats enrichis")
            except ImportError:
                print("⚠️ Enrichisseur non disponible, utilisation données brutes")
                enriched_results = raw_results
            except Exception as e:
                print(f"⚠️ Erreur enrichissement global: {e}")
                enriched_results = raw_results
        else:
            enriched_results = raw_results

        # --- 4.b) BOOST QUALITÉ SI LANGUE OK (rectification demandée) ---
        # Si la langue de la page matche la langue demandée, on garantit un quality_score >= 7
        for r in enriched_results or []:
            try:
                if r.get("language_match"):
                    r["quality_score"] = max(r.get("quality_score", 5), 7)
            except Exception:
                # On ne casse jamais le pipeline pour un score
                pass
        
        # --- 5) VALIDATION ET NETTOYAGE FINAL ---
        validated_results = self.validate_results(enriched_results, cfg)
        
        # --- 6) MÉTRIQUES FINALES ---
        duration = time.time() - start_time
        
        # Calcul métriques qualité
        high_quality_results = [r for r in validated_results if r.get('quality_score', 0) >= 7]
        enriched_count = len([r for r in validated_results if r.get('enrichment_quality', 0) > 0])
        
        print(f"⏱️ Scraping total terminé en {duration:.2f}s")
        print(f"📊 Résultats: {len(raw_results)} bruts → {len(enriched_results)} enrichis → {len(validated_results)} validés")
        print(f"🌟 Qualité: {len(high_quality_results)} résultats haute qualité")
        print(f"🔍 Enrichissement: {enriched_count} résultats enrichis")
        
        if validated_results:
            avg_quality = sum(r.get('quality_score', 0) for r in validated_results) / len(validated_results)
            print(f"📈 Score qualité moyen: {avg_quality:.1f}/10")
        
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
    # Validation / nettoyage des résultats — VERSION PLUS PERMISSIVE
    # ---------------------------------------------------------------------
    def validate_results(self, results: List[dict], config: dict) -> List[dict]:
        """
        Validation plus permissive :
        - Accepte si AU MOINS un contact (site/email/tel) OU une petite description (>=10 chars)
        - Si keep_incomplete=True (par défaut), garde aussi les fiches incomplètes pour enrichissement
        """
        validated: List[dict] = []
        stats = {"skipped_no_contact": 0}

        keep_incomplete = bool(config.get("keep_incomplete", True))

        for result in results or []:
            if not isinstance(result, dict):
                continue

            # Toujours exiger un nom minimal
            name = (result.get('name') or '').strip()
            if not name or len(name) < 2:
                continue

            contact_fields = [
                "website", "email", "phone",
                "email_enriched", "phone_enriched", "emails_from_sources", "phones_from_sources"
            ]
            has_contact = any(result.get(f) for f in contact_fields)
            description = (result.get("description") or "").strip()
            has_description = len(description) >= 10

            if not has_contact and not has_description and not keep_incomplete:
                stats["skipped_no_contact"] += 1
                continue

            # Nettoyage final
            cleaned = self._clean_result_advanced(result, config)
            validated.append(cleaned)

        self._last_validation_stats = stats
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

    # --- Nettoyage avancé avec gestion des enrichissements ---
    def _clean_result_advanced(self, r: dict, config: dict) -> dict:
        """Nettoyage avancé avec gestion des enrichissements"""
        cleaned = self._clean_result(r, config)

        # Fusion enrichissements emails
        all_emails = set()
        for email_field in ['email', 'email_enriched', 'emails_from_sources']:
            email_value = r.get(email_field)
            if email_value:
                emails = [e.strip() for e in str(email_value).split(';') if e.strip()]
                all_emails.update(self.extract_emails_from_text('; '.join(emails)))

        if all_emails:
            cleaned['email'] = '; '.join(sorted(all_emails)[:3])  # Max 3 emails

        # Fusion enrichissements téléphones
        all_phones = set()
        for phone_field in ['phone', 'phone_enriched', 'phones_from_sources']:
            phone_value = r.get(phone_field)
            if phone_value:
                phones = [p.strip() for p in str(phone_value).split(';') if p.strip()]
                region = self.country_to_region(config.get('country'))
                normalized_phones = normalize_phone_list(phones, default_region=region)
                all_phones.update(normalized_phones)

        if all_phones:
            cleaned['phone'] = '; '.join(sorted(all_phones)[:3])  # Max 3 téléphones

        # Réseaux sociaux enrichis
        social_platforms = ['facebook', 'linkedin', 'instagram', 'twitter']
        for platform in social_platforms:
            enriched_field = f'{platform}_enriched'
            sources_field = f'{platform}_from_sources'
            value = r.get(enriched_field) or r.get(sources_field) or r.get(platform)
            if value:
                cleaned[platform] = normalize_url(value)

        # Métadonnées enrichissement
        cleaned['enrichment_quality'] = r.get('enrichment_quality', 0)
        cleaned['detected_sectors'] = r.get('detected_sectors', {})
        cleaned['extraction_method'] = r.get('extraction_method', 'standard')

        # Informations géographiques enrichies
        if r.get('detected_city'):
            cleaned['city'] = r['detected_city']
        if r.get('postal_code'):
            cleaned['postal_code'] = r['postal_code']
        if r.get('address_enriched'):
            cleaned['address'] = r['address_enriched']

        # Données business
        if r.get('business_hours'):
            cleaned['business_hours'] = r['business_hours']
        if r.get('contact_person'):
            cleaned['contact_person'] = r['contact_person']

        return cleaned

    def extract_emails_from_text(self, text: str) -> List[str]:
        """Helper pour extraction emails depuis texte"""
        from utils.normalize import extract_emails
        return extract_emails(text)

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

    # --- Mapping léger pays -> région pour la normalisation des téléphones (utile au moteur) ---
    def country_to_region(self, country: Optional[str]) -> str:
        """Convertit un nom de pays en code région (E.164)"""
        mapping = {
            'Thaïlande': 'TH', 'Thailand': 'TH',
            'France': 'FR',
            'États-Unis': 'US', 'United States': 'US', 'USA': 'US',
            'Royaume-Uni': 'GB', 'United Kingdom': 'GB', 'UK': 'GB',
            'Allemagne': 'DE', 'Germany': 'DE',
            'Espagne': 'ES', 'Spain': 'ES',
            'Italie': 'IT', 'Italy': 'IT',
            'Russie': 'RU', 'Russia': 'RU',
            'Chine': 'CN', 'China': 'CN',
            'Japon': 'JP', 'Japan': 'JP'
        }
        c = (country or '').strip()
        return mapping.get(c, 'TH')


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
