#!/usr/bin/env python3
"""
import_asins.py

Seeds the affiliate_products table in videos.db from verified_asins.json.
Safe to re-run — uses upsert (existing rows are updated, not duplicated).

Usage:
    python3 import_asins.py                          # reads ../verified_asins.json
    python3 import_asins.py /path/to/custom.json     # custom path
"""

import json
import sys
from pathlib import Path

# Allow running from repo root or content-engine/
BASE_DIR = Path(__file__).parent
DEFAULT_JSON = BASE_DIR.parent / "verified_asins.json"

sys.path.insert(0, str(BASE_DIR))
from database.queries import upsert_affiliate_product

def main():
    json_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_JSON
    if not json_path.exists():
        print(f"ERROR: {json_path} not found")
        sys.exit(1)

    data = json.loads(json_path.read_text())
    imported = 0
    for subject, info in data.items():
        upsert_affiliate_product(
            subject=subject,
            asin=info["asin"],
            product_name=info.get("product_name", ""),
            price=info.get("price", ""),
        )
        print(f"  ✓ {subject:<20} → {info['asin']}")
        imported += 1

    print(f"\nImported {imported} products into affiliate_products table.")

if __name__ == "__main__":
    main()
