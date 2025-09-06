
from typing import Dict, List, Optional, Tuple
import re, urllib.parse

try:
    from rapidfuzz import fuzz  # type: ignore
except Exception:  # pragma: no cover
    fuzz = None  # type: ignore

def domain(url: Optional[str]) -> Optional[str]:
    if not url: return None
    try:
        netloc = urllib.parse.urlsplit(url).netloc.lower()
        if netloc.startswith("www."): netloc = netloc[4:]
        return netloc or None
    except Exception:
        return None

def signature_key(rec: Dict) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Signature basée sur: domaine(site_web), telephone, nom normalisé (si dispo)
    """
    site = rec.get("website") or rec.get("site_web") or rec.get("source_url") or rec.get("source_url_principale")
    tel = rec.get("phone") or rec.get("telephone")
    name = (rec.get("normalized_name") or rec.get("name") or rec.get("nom_organisation"))
    return (domain(site), tel, (name or "").lower().strip() or None)

def fuzzy_duplicate(candidate: Dict, existing: List[Dict], threshold: int = 90) -> Optional[Dict]:
    """
    Retourne un enregistrement existant considéré comme doublon si score >= threshold,
    sinon None. Utilise rapidfuzz si dispo, sinon compare la signature exacte.
    """
    c_sig = signature_key(candidate)
    # exact match on domain+phone
    for e in existing:
        if signature_key(e)[:2] == c_sig[:2] and c_sig[0] is not None and c_sig[1] is not None:
            return e

    # fuzzy on name
    if fuzz:
        cname = (candidate.get("normalized_name") or candidate.get("name") or "").lower().strip()
        for e in existing:
            ename = (e.get("normalized_name") or e.get("name") or "").lower().strip()
            if not cname or not ename: 
                continue
            score = fuzz.token_set_ratio(cname, ename)  # robust on word order
            if score >= threshold:
                return e
    else:
        # fallback simplistic: exact lowercase containment
        cname = (candidate.get("name") or "").lower().strip()
        for e in existing:
            ename = (e.get("name") or "").lower().strip()
            if cname and ename and (cname in ename or ename in cname):
                return e
    return None
