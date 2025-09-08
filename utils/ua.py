import os, random
from fake_useragent import UserAgent

def pick_user_agent() -> str:
    if os.getenv("SCRAPMASTER_UA_ROTATION", "true").lower() != "true":
        return "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    try:
        return UserAgent().random
    except Exception:
        pool = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 Version/16.5 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
        ]
        return random.choice(pool)
