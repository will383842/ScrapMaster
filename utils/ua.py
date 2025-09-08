import os
import random

# Pool minimal d'user-agents "réalistes"
_UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_4 like Mac OS X) AppleWebKit/605.1.15 Version/16.4 Mobile/15E148 Safari/604.1",
]

def pick_user_agent() -> str:
    """
    Retourne un user-agent :
    - si SCRAPMASTER_UA_ROTATION != "true" → premier UA du pool (stable)
    - sinon → UA aléatoire du pool
    """
    if os.getenv("SCRAPMASTER_UA_ROTATION", "true").lower() != "true":
        return _UA_POOL[0]
    return random.choice(_UA_POOL)
