# src/collect.py
"""
Usage:
    python -m src.collect --max-rows 15000 --outfile data/raw/2025-05-27.ndjson
"""
import argparse, json, logging, time, pathlib, sys, requests
from datetime import datetime
from src.utils import get_app_token

ROOT = pathlib.Path(__file__).resolve().parents[1]

def load_categories(fp=ROOT / "categories.json"):
    return [c["id"] for c in json.loads(fp.read_text())]

def fetch_page(category_id, offset=0, limit=200):
    token = get_app_token()
    url = (
        "https://api.ebay.com/buy/browse/v1/item_summary/search"
        f"?category_ids={category_id}&limit={limit}&offset={offset}&filter=sold_items:true"
    )
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
    r.raise_for_status()
    return r.json().get("itemSummaries", [])

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-rows", type=int, default=12000)
    parser.add_argument("--outfile", type=pathlib.Path, required=True)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    out = args.outfile
    out.parent.mkdir(parents=True, exist_ok=True)

    total = 0
    with out.open("w", encoding="utf-8") as fh:
        for cat in load_categories():
            offset = 0
            while total < args.max_rows:
                try:
                    items = fetch_page(cat, offset)
                except Exception as e:
                    logging.warning("API error %s", e)
                    break
                if not items:
                    break
                for itm in items:
                    itm["snapshot_ts"] = datetime.utcnow().isoformat()
                    fh.write(json.dumps(itm) + "\n")
                total += len(items)
                logging.info("cat %s offset %s → rows=%s", cat, offset, total)
                offset += len(items)
                if total >= args.max_rows:
                    break
                time.sleep(0.25)        # stay well below rate limits
        logging.info("Finished with %s rows", total)

if __name__ == "__main__":
    sys.exit(main())
