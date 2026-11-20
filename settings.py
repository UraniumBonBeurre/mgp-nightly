"""
settings.py — Accès du relevé et des scripts de suivi (charge .env.local en local ; secrets GitHub en CI)
"""

import os
from pathlib import Path

ROOT = Path(__file__).parent
for _env in (ROOT / ".env.local", ROOT / ".env"):
    if _env.exists():
        for _line in _env.read_text(encoding="utf-8").splitlines():
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _, _v = _line.partition("=")
                os.environ.setdefault(_k.strip(), _v.strip())

# Supabase
SUPABASE_URL: str = os.environ.get("NEXT_PUBLIC_SUPABASE_URL", "")
SUPABASE_KEY: str = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

# Awin
AWIN_API_TOKEN: str = os.environ.get("AWIN_API_TOKEN", "")
AWIN_PUBLISHER_ID: str = os.environ.get("AWIN_PUBLISHER_ID", "")
AWIN_PRODUCTDATA_KEY: str = os.environ.get("AWIN_PRODUCTDATA_KEY", "") or AWIN_API_TOKEN

# Google AI (classement des produits en rayons)
GOOGLE_AI_API_KEY: str = os.environ.get("GOOGLE_AI_API_KEY", "")

# Cloudflare R2
R2_ACCOUNT_ID: str = os.environ.get("R2_ACCOUNT_ID", "")
R2_ACCESS_KEY_ID: str = os.environ.get("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY: str = os.environ.get("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET_NAME: str = os.environ.get("R2_BUCKET_NAME", "")
