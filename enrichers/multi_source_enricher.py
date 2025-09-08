# enrichers/multi_source_enricher.py
import time
import requests
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote_plus
import logging

from utils.normalize import (
    extract_all_contact_methods,
    detect_business_sector,
    enrich_geographic_info,
)
from utils.ua import pick_user_agent

logger = logging.getLogger(__name__)


class MultiSourceEnricher:
    """Enrichisseur multi-sources pour données de contact"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': pick_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en,fr;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        })
        self.timeout = 10
        self.max_workers = 3

    def enrich_entry_complete(self, entry: Dict, config: Dict) -> Dict:
        """Enrichissement complet d'une entrée"""
        enriched_entry = entry.copy()

        try:
            # 1. Enrichissement depuis le site web principal
            if entry.get('website'):
                web_enrichment = self._enrich_from_website(entry['website'])
                enriched_entry.update(web_enrichment)

            # 2. Recherche de sources alternatives
            alternative_sources = self._find_alternative_sources(entry, config)

            # 3. Enrichissement depuis sources alternatives (parallèle)
            if alternative_sources:
                alt_enrichment = self._enrich_from_multiple_sources(alternative_sources)
                enriched_entry = self._merge_enrichments(enriched_entry, alt_enrichment)

            # 4. Analyse sémantique et géographique
            text_content = f"{entry.get('name', '')} {entry.get('description', '')}"

            # Détection secteur
            sector_info = detect_business_sector(text_content, config.get('profession', ''))
            if sector_info:
                enriched_entry['detected_sectors'] = sector_info

            # Enrichissement géographique
            geo_info = enrich_geographic_info(text_content, config.get('country', ''))
            if geo_info:
                enriched_entry.update(geo_info)

            # 5. Score de qualité final
            enriched_entry['enrichment_quality'] = self._calculate_enrichment_quality(enriched_entry)

            return enriched_entry

        except Exception as e:
            logger.warning(f"Erreur enrichissement: {e}")
            return enriched_entry

    def _enrich_from_website(self, website_url: str) -> Dict:
        """Enrichissement depuis site web principal"""
        try:
            response = self.session.get(website_url, timeout=self.timeout)
            response.raise_for_status()

            # Extraction complète des contacts
            contacts = extract_all_contact_methods(response.text)

            # Simplification pour intégration
            enrichment: Dict[str, Any] = {}

            if contacts.get('emails'):
                enrichment['email_enriched'] = '; '.join(contacts['emails'][:3])

            if contacts.get('phones'):
                enrichment['phone_enriched'] = '; '.join(contacts['phones'][:3])

            # Réseaux sociaux
            social_media = contacts.get('social_media', {})
            if isinstance(social_media, dict):
                for platform, links in social_media.items():
                    if isinstance(links, list) and links:
                        enrichment[f'{platform}_enriched'] = links[0]
                    elif links:
                        enrichment[f'{platform}_enriched'] = links

            # Autres infos
            if contacts.get('physical_addresses'):
                enrichment['address_enriched'] = contacts['physical_addresses'][0]

            if contacts.get('business_hours'):
                enrichment['business_hours'] = '; '.join(contacts['business_hours'][:2])

            if contacts.get('contact_persons'):
                enrichment['contact_person'] = contacts['contact_persons'][0]

            return enrichment

        except Exception as e:
            logger.debug(f"Erreur enrichissement web {website_url}: {e}")
            return {}

    def _find_alternative_sources(self, entry: Dict, config: Dict) -> List[str]:
        """Trouve des sources alternatives pour enrichissement"""
        alternative_urls: List[str] = []

        name = (entry.get('name') or '').strip()
        country = (config.get('country') or '').strip()

        if not name:
            return []

        q_name_plus = quote_plus(name)
        q_name_pct = quote_plus(name).replace('+', '%20')  # selon besoins d'URL

        # 1. Recherche sur réseaux sociaux
        social_searches = [
            f"https://www.facebook.com/search/pages/?q={q_name_pct}",
            f"https://www.linkedin.com/search/results/companies/?keywords={q_name_pct}",
        ]
        alternative_urls.extend(social_searches)

        # 2. Recherche sur annuaires locaux
        cl = country.lower()
        if 'thaïlande' in cl or 'thailand' in cl:
            thai_searches = [
                f"https://www.yellowpages.co.th/en/search?q={q_name_plus}",
                f"https://foursquare.com/explore?near=Thailand&q={q_name_pct}",
            ]
            alternative_urls.extend(thai_searches)
        elif 'france' in cl:
            french_searches = [
                f"https://www.pagesjaunes.fr/annuaire/chercherlespros?quoiqui={q_name_plus}",
                f"https://www.yelp.fr/search?find_desc={q_name_plus}",
            ]
            alternative_urls.extend(french_searches)

        # 3. Recherches génériques
        generic_searches = [
            f"https://www.google.com/search?q=%22{q_name_pct}%22+{quote_plus(country)}+contact",
            f"https://duckduckgo.com/?q=%22{q_name_pct}%22+{quote_plus(country)}+email",
        ]
        alternative_urls.extend(generic_searches)

        # Limiter à 5 pour rester léger
        return alternative_urls[:5]

    def _enrich_from_multiple_sources(self, urls: List[str]) -> Dict:
        """Enrichissement parallèle depuis plusieurs sources"""
        all_enrichments: List[Dict[str, Any]] = []

        def fetch_and_extract(url: str) -> Optional[Dict[str, Any]]:
            try:
                resp = self.session.get(url, timeout=self.timeout)
                if resp.status_code == 200 and resp.text:
                    contacts = extract_all_contact_methods(resp.text)
                    return {'url': url, 'contacts': contacts}
            except Exception as e:
                logger.debug(f"Erreur enrichissement {url}: {e}")
            return None

        # Exécution parallèle avec limite de workers
        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_url = {executor.submit(fetch_and_extract, url): url for url in urls}
                for future in as_completed(future_to_url):
                    try:
                        result = future.result(timeout=self.timeout + 2)
                        if result:
                            all_enrichments.append(result)
                    except Exception as e:
                        logger.debug(f"Erreur future: {e}")
        except Exception as e:
            logger.debug(f"Erreur pool threads: {e}")

        # Fusion des enrichissements
        return self._merge_multiple_enrichments(all_enrichments)

    def _merge_multiple_enrichments(self, enrichments: List[Dict[str, Any]]) -> Dict:
        """Fusionne plusieurs enrichissements"""
        merged: Dict[str, Any] = {}

        all_emails: set = set()
        all_phones: set = set()
        all_social: Dict[str, set] = {}

        for enrichment in enrichments:
            contacts = enrichment.get('contacts', {}) or {}

            # Emails
            if contacts.get('emails'):
                all_emails.update(contacts['emails'])

            # Téléphones
            if contacts.get('phones'):
                all_phones.update(contacts['phones'])

            # Réseaux sociaux
            social_media = contacts.get('social_media', {})
            if isinstance(social_media, dict):
                for platform, links in social_media.items():
                    if not links:
                        continue
                    if platform not in all_social:
                        all_social[platform] = set()
                    if isinstance(links, list):
                        all_social[platform].update(links)
                    else:
                        all_social[platform].add(links)

        # Compilation finale
        if all_emails:
            merged['emails_from_sources'] = '; '.join(sorted(all_emails)[:5])

        if all_phones:
            merged['phones_from_sources'] = '; '.join(sorted(all_phones)[:5])

        for platform, links in all_social.items():
            if links:
                merged[f'{platform}_from_sources'] = sorted(links)[0]  # Prendre le premier

        return merged

    def _merge_enrichments(self, base_entry: Dict, new_enrichment: Dict) -> Dict:
        """Fusionne enrichissement avec entrée de base"""
        merged = base_entry.copy()

        # Stratégie de fusion intelligente
        for key, value in new_enrichment.items():
            if value is None or value == '':
                continue

            existing_value = merged.get(key)

            if not existing_value:
                # Pas de valeur existante, ajouter directement
                merged[key] = value
            elif key.endswith('_enriched') or key.endswith('_from_sources'):
                # Données d'enrichissement: on écrase/priorise l'enrichi
                merged[key] = value
            elif key in ['email', 'phone']:
                # Fusionner contacts (email/téléphone)
                merged[key] = self._merge_contact_field(str(existing_value), str(value))
            else:
                # Pour autres champs, garder existant sauf si nouvelle valeur plus informative (plus longue)
                if len(str(value)) > len(str(existing_value)):
                    merged[key] = value

        return merged

    def _merge_contact_field(self, existing: str, new: str) -> str:
        """Fusionne deux champs de contact (email/téléphone)"""
        if not existing:
            return new
        if not new:
            return existing

        # Séparer et dédupliquer
        existing_items = set(item.strip() for item in existing.split(';') if item.strip())
        new_items = set(item.strip() for item in new.split(';') if item.strip())

        # Fusionner et limiter
        all_items = existing_items.union(new_items)
        return '; '.join(sorted(all_items)[:5])  # Max 5 items

    def _calculate_enrichment_quality(self, entry: Dict) -> int:
        """Calcule la qualité de l'enrichissement (1-10)"""
        score = 0

        # Contacts de base
        if entry.get('email'):
            score += 2
        if entry.get('phone'):
            score += 2
        if entry.get('website'):
            score += 1

        # Enrichissements
        if entry.get('email_enriched'):
            score += 1
        if entry.get('phone_enriched'):
            score += 1

        # Réseaux sociaux (présence)
        social_platforms = ['facebook', 'linkedin', 'instagram', 'twitter']
        social_count = sum(1 for platform in social_platforms if entry.get(platform) or entry.get(f'{platform}_enriched') or entry.get(f'{platform}_from_sources'))
        score += min(social_count, 2)  # Max 2 points pour réseaux sociaux

        # Données géographiques
        if entry.get('address') or entry.get('address_enriched') or entry.get('detected_city'):
            score += 1

        # Données métier
        if entry.get('detected_sectors'):
            score += 1

        return min(score, 10)


# Instance globale
multi_enricher = MultiSourceEnricher()
