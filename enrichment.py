import time, os, httpx
from utils.normalize import extract_emails, extract_phones, extract_whatsapp, extract_line_id, extract_telegram, extract_wechat, extract_social_links, find_contact_like_links
from utils.ua import pick_user_agent

DELAY_MS = int(os.getenv("SCRAPMASTER_DELAY_MS", "800"))

def fetch(url, proxy=None, timeout=20):
    headers = {"User-Agent": pick_user_agent(), "Accept-Language": "en,fr;q=0.9"}
    proxies = proxy or os.getenv("SCRAPMASTER_PROXY") or None
    with httpx.Client(timeout=timeout, proxies=proxies, follow_redirects=True, headers=headers) as c:
        r = c.get(url)
        r.raise_for_status()
        return r.text

def enrich_from_site(base_url: str, html: str, proxy=None) -> dict:
    out = {}
    # 1) extraction directe
    text = html
    emails = extract_emails(text)
    phones = extract_phones(text)
    wa = extract_whatsapp(text)
    line = extract_line_id(text)
    tg = extract_telegram(text)
    wc = extract_wechat(text)
    socials = extract_social_links(text)
    if emails: out["email"] = emails[0]
    if phones: out["phone"] = phones[0]
    out["whatsapp"] = "; ".join(wa) if wa else ""
    out["line_id"] = "; ".join(line) if line else ""
    out["telegram"] = "; ".join(tg) if tg else ""
    out["wechat"] = "; ".join(wc) if wc else ""
    for k,v in socials.items():
        out[k] = "; ".join(v)

    # 2) suivre pages "contact/about"
    links = find_contact_like_links(html, base_url)
    for u in links[:5]:
        try:
            time.sleep(DELAY_MS/1000.0)
            sub = fetch(u, proxy=proxy)
            emails2 = extract_emails(sub)
            phones2 = extract_phones(sub)
            wa2 = extract_whatsapp(sub); line2 = extract_line_id(sub)
            tg2 = extract_telegram(sub); wc2 = extract_wechat(sub)
            socials2 = extract_social_links(sub)
            if (not out.get("email")) and emails2: out["email"] = emails2[0]
            if (not out.get("phone")) and phones2: out["phone"] = phones2[0]
            out["whatsapp"] = out.get("whatsapp") or ("; ".join(wa2) if wa2 else "")
            out["line_id"] = out.get("line_id") or ("; ".join(line2) if line2 else "")
            out["telegram"] = out.get("telegram") or ("; ".join(tg2) if tg2 else "")
            out["wechat"] = out.get("wechat") or ("; ".join(wc2) if wc2 else "")
            for k,v in socials2.items():
                if not out.get(k):
                    out[k] = "; ".join(v)
            if out.get("email") and out.get("phone"):
                break
        except Exception:
            continue
    return out
