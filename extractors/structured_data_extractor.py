# extractors/structured_data_extractor.py
import json
import re
from typing import Dict, List, Optional, Any
from bs4 import BeautifulSoup
import logging

logger = logging.getLogger(__name__)


class StructuredDataExtractor:
    """Extracteur de données structurées (JSON-LD, Microdata, etc.)"""

    def __init__(self):
        self.contact_schemas = self._load_contact_schemas()

    def _load_contact_schemas(self) -> Dict:
        """Schémas de données de contact reconnus"""
        return {
            "organization": ["Organization", "LocalBusiness", "Corporation", "NGO"],
            "person": ["Person", "ContactPage"],
            "legal": ["LegalService", "Attorney", "Lawyer"],
            "place": ["Place", "PostalAddress", "ContactPoint"],
        }

    def extract_all_structured_data(self, html: str, source_url: str) -> List[Dict]:
        """Point d'entrée principal - extrait toutes les données structurées"""
        if not html:
            return []

        try:
            soup = BeautifulSoup(html, "html.parser")
            extracted_data: List[Dict] = []

            # 1) JSON-LD
            jsonld_data = self._extract_jsonld(soup)
            extracted_data.extend(jsonld_data)

            # 2) Microdata
            microdata = self._extract_microdata(soup)
            extracted_data.extend(microdata)

            # 3) Méta tags spécialisés
            meta_data = self._extract_meta_tags(soup)
            if meta_data:
                extracted_data.append(meta_data)

            # 4) Motifs HTML (sections contact/footer/vCard)
            html_structured = self._extract_html_patterns(soup, source_url)
            extracted_data.extend(html_structured)

            # Déduplication + nettoyage
            return self._deduplicate_and_clean(extracted_data)

        except Exception as e:
            logger.warning(f"Erreur extraction données structurées: {e}")
            return []

    def _extract_jsonld(self, soup: BeautifulSoup) -> List[Dict]:
        """Extraction JSON-LD (Schema.org)"""
        results: List[Dict] = []

        for script in soup.find_all("script", type="application/ld+json"):
            try:
                raw = script.string
                if not raw:
                    # parfois le contenu est dans .text
                    raw = script.text
                if not raw:
                    continue

                data = json.loads(raw)

                # Tableau ou objet unique
                if isinstance(data, list):
                    for item in data:
                        parsed = self._parse_jsonld_item(item)
                        if parsed:
                            results.append(parsed)
                elif isinstance(data, dict):
                    parsed = self._parse_jsonld_item(data)
                    if parsed:
                        results.append(parsed)
            except json.JSONDecodeError:
                continue
            except Exception as e:
                logger.debug(f"Erreur parsing JSON-LD: {e}")
                continue

        return results

    def _parse_jsonld_item(self, data: Dict) -> Optional[Dict]:
        """Parse un item JSON-LD individuel"""
        if not isinstance(data, dict):
            return None

        schema_type = data.get("@type", "")
        if not schema_type:
            return None

        # @type peut être une liste
        schema_types = schema_type if isinstance(schema_type, list) else [schema_type]

        # Pertinence
        relevant = any(
            t in schema_types
            for types in self.contact_schemas.values()
            for t in types
        )
        if not relevant:
            return None

        # Extraction
        contact_data: Dict[str, Any] = {
            "source": "jsonld",
            "schema_type": schema_types[0] if schema_types else None,
            "name": self._extract_name(data),
            "description": data.get("description"),
            "email": self._extract_email_jsonld(data),
            "phone": self._extract_phone_jsonld(data),
            "website": data.get("url"),
            "address": self._extract_address_jsonld(data),
            "social_media": self._extract_social_jsonld(data),
            "business_type": data.get("additionalType"),
            "opening_hours": data.get("openingHours"),
            "rating": (data.get("aggregateRating") or {}).get("ratingValue"),
        }

        if contact_data.get("name") or contact_data.get("email") or contact_data.get("phone"):
            return {k: v for k, v in contact_data.items() if v}
        return None

    def _extract_name(self, data: Dict) -> Optional[str]:
        """Extraction nom avec fallbacks"""
        name_fields = ["name", "legalName", "alternateName", "brand"]
        for field in name_fields:
            if field in data:
                name = data[field]
                if isinstance(name, str):
                    return name.strip()
                if isinstance(name, dict) and "name" in name:
                    return str(name["name"]).strip()
        return None

    def _extract_email_jsonld(self, data: Dict) -> Optional[str]:
        """Extraction email depuis JSON-LD"""
        if "email" in data:
            return data["email"]

        contacts = data.get("contactPoint", [])
        if not isinstance(contacts, list):
            contacts = [contacts]

        for contact in contacts:
            if isinstance(contact, dict):
                email = contact.get("email")
                if email:
                    return email
        return None

    def _extract_phone_jsonld(self, data: Dict) -> Optional[str]:
        """Extraction téléphone depuis JSON-LD"""
        for field in ["telephone", "phone", "phoneNumber"]:
            if field in data:
                return data[field]

        contacts = data.get("contactPoint", [])
        if not isinstance(contacts, list):
            contacts = [contacts]

        for contact in contacts:
            if isinstance(contact, dict):
                phone = contact.get("telephone") or contact.get("phone")
                if phone:
                    return phone
        return None

    def _extract_address_jsonld(self, data: Dict) -> Optional[str]:
        """Extraction adresse depuis JSON-LD"""
        address = data.get("address")
        if not address:
            return None

        if isinstance(address, str):
            return address

        if isinstance(address, dict):
            parts = [
                address.get("streetAddress"),
                address.get("addressLocality"),
                address.get("addressRegion"),
                address.get("postalCode"),
                address.get("addressCountry"),
            ]
            return ", ".join([p for p in parts if p])

        return None

    def _extract_social_jsonld(self, data: Dict) -> Dict[str, str]:
        """Extraction réseaux sociaux depuis JSON-LD"""
        social: Dict[str, str] = {}
        same_as = data.get("sameAs", [])
        if isinstance(same_as, str):
            same_as = [same_as]

        for url in same_as:
            u = str(url).lower()
            if "facebook.com" in u:
                social["facebook"] = url
            elif "linkedin.com" in u:
                social["linkedin"] = url
            elif "instagram.com" in u:
                social["instagram"] = url
            elif "twitter.com" in u or "x.com" in u:
                social["twitter"] = url
        return social

    def _extract_microdata(self, soup: BeautifulSoup) -> List[Dict]:
        """Extraction Microdata (Schema.org dans HTML)"""
        results: List[Dict] = []

        for element in soup.find_all(attrs={"itemscope": True}):
            itemtype = element.get("itemtype", "") or ""
            # Pertinence
            if not any(
                schema in itemtype
                for schemas in self.contact_schemas.values()
                for schema in schemas
            ):
                continue

            microdata: Dict[str, Any] = {
                "source": "microdata",
                "schema_type": itemtype.split("/")[-1] if "/" in itemtype else itemtype,
            }

            # Propriétés
            for prop_elem in element.find_all(attrs={"itemprop": True}):
                prop_name = prop_elem.get("itemprop")
                prop_value = self._get_microdata_value(prop_elem)
                if prop_name and prop_value:
                    microdata[prop_name] = prop_value

            if len(microdata) > 2:  # plus que source et schema_type
                results.append(microdata)

        return results

    def _get_microdata_value(self, element) -> Optional[str]:
        """Extrait la valeur d'une propriété microdata"""
        for attr in ["content", "datetime", "href", "src"]:
            if element.get(attr):
                return element[attr]
        text = element.get_text(strip=True)
        return text or None

    def _extract_meta_tags(self, soup: BeautifulSoup) -> Optional[Dict]:
        """Extraction depuis méta tags spécialisés"""
        meta_data: Dict[str, Any] = {"source": "meta_tags"}

        # Open Graph
        og_title = soup.find("meta", property="og:title")
        if og_title and og_title.get("content"):
            meta_data["og_title"] = og_title.get("content")

        og_description = soup.find("meta", property="og:description")
        if og_description and og_description.get("content"):
            meta_data["og_description"] = og_description.get("content")

        # Business-specific meta
        business_meta = {
            "business:contact_data:phone_number": "phone",
            "business:contact_data:email": "email",
            "business:contact_data:website": "website",
        }

        for meta_name, field_name in business_meta.items():
            meta_tag = soup.find("meta", {"name": meta_name}) or soup.find(
                "meta", {"property": meta_name}
            )
            if meta_tag and meta_tag.get("content"):
                meta_data[field_name] = meta_tag["content"]

        return meta_data if len(meta_data) > 1 else None

    def _extract_html_patterns(self, soup: BeautifulSoup, source_url: str) -> List[Dict]:
        """Extraction via patterns HTML reconnus"""
        results: List[Dict] = []

        # 1) Sections contact dédiées
        contact_sections = soup.find_all(
            ["div", "section"], class_=re.compile(r"contact|about|info", re.I)
        )
        for section in contact_sections:
            contact_data = self._extract_from_contact_section(section, source_url)
            if contact_data:
                results.append(contact_data)

        # 2) Footer
        footer = soup.find("footer")
        if footer:
            footer_data = self._extract_from_footer(footer, source_url)
            if footer_data:
                results.append(footer_data)

        # 3) vCard / hCard
        vcard_data = self._extract_vcard_patterns(soup)
        results.extend(vcard_data)

        return results

    def _extract_from_contact_section(self, section, source_url: str) -> Optional[Dict]:
        """Extraction depuis section contact"""
        from utils.normalize import extract_emails, extract_phones

        text = section.get_text() or ""

        contact_data: Dict[str, Any] = {
            "source": "html_contact_section",
            "source_url": source_url,
            "emails": extract_emails(text),
            "phones": extract_phones(text),
        }

        title = section.find(["h1", "h2", "h3", "h4"])
        if title:
            contact_data["section_title"] = title.get_text(strip=True)

        return contact_data if (contact_data["emails"] or contact_data["phones"]) else None

    def _extract_from_footer(self, footer, source_url: str) -> Optional[Dict]:
        """Extraction depuis footer"""
        from utils.normalize import extract_emails, extract_phones, extract_socials

        text = footer.get_text() or ""

        footer_data: Dict[str, Any] = {
            "source": "html_footer",
            "source_url": source_url,
            "emails": extract_emails(text),
            "phones": extract_phones(text),
            "social_links": extract_socials(text),
        }

        return (
            footer_data
            if any(footer_data[k] for k in ["emails", "phones", "social_links"])
            else None
        )

    def _extract_vcard_patterns(self, soup: BeautifulSoup) -> List[Dict]:
        """Extraction patterns vCard/hCard"""
        results: List[Dict] = []

        vcard_elements = soup.find_all(class_=re.compile(r"vcard|hcard|contact-card", re.I))
        for element in vcard_elements:
            vcard_data: Dict[str, Any] = {"source": "vcard_pattern"}

            vcard_props = {
                "fn": "name",
                "org": "organization",
                "email": "email",
                "tel": "phone",
                "adr": "address",
                "url": "website",
            }

            for vcard_class, field_name in vcard_props.items():
                prop_elem = element.find(class_=re.compile(vcard_class, re.I))
                if prop_elem:
                    value = prop_elem.get_text(strip=True)
                    if value:
                        vcard_data[field_name] = value

            if len(vcard_data) > 1:
                results.append(vcard_data)

        return results

    def _deduplicate_and_clean(self, data_list: List[Dict]) -> List[Dict]:
        """Déduplication et nettoyage final"""
        if not data_list:
            return []

        merged_data: Dict[str, Dict[str, Any]] = {}

        for item in data_list:
            key = item.get("name") or item.get("email") or item.get("source_url") or str(
                hash(str(item))
            )
            if key not in merged_data:
                merged_data[key] = dict(item)
            else:
                # Fusion champ par champ (garder existant, compléter manquants)
                for field, value in item.items():
                    if value and not merged_data[key].get(field):
                        merged_data[key][field] = value

        cleaned_results: List[Dict] = []
        for item in merged_data.values():
            cleaned_item = self._clean_structured_item(item)
            if cleaned_item:
                cleaned_results.append(cleaned_item)

        return cleaned_results

    def _clean_structured_item(self, item: Dict) -> Optional[Dict]:
        """Nettoyage d'un item structuré"""
        has_contact = any(
            [
                item.get("email"),
                item.get("phone"),
                item.get("emails"),
                item.get("phones"),
                item.get("website"),
            ]
        )
        if not has_contact:
            return None

        cleaned: Dict[str, Any] = {}

        # Nom
        name = item.get("name") or item.get("section_title") or item.get("og_title")
        if name:
            cleaned["name"] = str(name).strip()[:200]

        # Description
        desc = item.get("description") or item.get("og_description")
        if desc:
            cleaned["description"] = str(desc).strip()[:500]

        # Emails
        emails: List[str] = []
        if item.get("email"):
            emails.append(str(item["email"]))
        if item.get("emails"):
            emails.extend(item["emails"] if isinstance(item["emails"], list) else [item["emails"]])
        if emails:
            cleaned["email"] = "; ".join(sorted(set(emails)))

        # Téléphones
        phones: List[str] = []
        if item.get("phone"):
            phones.append(str(item["phone"]))
        if item.get("phones"):
            phones.extend(item["phones"] if isinstance(item["phones"], list) else [item["phones"]])
        if phones:
            cleaned["phone"] = "; ".join(sorted(set(phones)))

        # Autres champs
        for field in ["website", "address", "business_type", "opening_hours", "rating"]:
            if item.get(field):
                cleaned[field] = str(item[field]).strip()

        # Réseaux sociaux
        social_fields = ["facebook", "linkedin", "instagram", "twitter"]
        for social in social_fields:
            if item.get("social_media", {}).get(social):
                cleaned[social] = item["social_media"][social]
            elif item.get(social):
                cleaned[social] = item[social]

        # Métadonnées
        cleaned["extraction_source"] = item.get("source", "unknown")
        cleaned["schema_type"] = item.get("schema_type")
        cleaned["source_url"] = item.get("source_url")
        cleaned["quality_score"] = self._calculate_quality_score(cleaned)

        return cleaned

    def _calculate_quality_score(self, item: Dict) -> int:
        """Score de qualité des données structurées (1-10)"""
        score = 0

        if item.get("name"):
            score += 2
        if item.get("email"):
            score += 3
        if item.get("phone"):
            score += 2
        if item.get("website"):
            score += 2

        if item.get("address"):
            score += 1
        if any(item.get(s) for s in ["facebook", "linkedin", "instagram"]):
            score += 1

        if item.get("extraction_source") == "jsonld":
            score += 1
        elif item.get("schema_type"):
            score += 1

        return min(score, 10)


# Instance globale
structured_extractor = StructuredDataExtractor()
