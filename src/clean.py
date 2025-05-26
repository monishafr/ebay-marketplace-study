# src/clean.py
"""
Flatten raw NDJSON → clean CSV.
Usage:
    python -m src.clean --infile data/raw/2025-05-26.ndjson \
                        --outfile data/clean/listings_2025-05-26.parquet
"""
import argparse, json, pathlib, pandas as pd, tqdm, datetime, zoneinfo

KEEP = {
    "itemId"              : "item_id",
    "title"               : "title",
    "condition"           : "condition",
    "categoryId"          : "category_id",
    "soldDate"            : "sold_dt",
    "soldPrice.value"     : "sold_price",
    "soldPrice.currency"  : "currency",
    "seller.username"     : "seller",
    "seller.feedbackScore": "feedback",
    "sellerInfo.topRatedSeller": "top_rated",
    "shippingOptions[0].shippingCost.value": "ship_cost",
    "quantity"            : "qty_listed",
    "quantitySold"        : "qty_sold",
    "listingFormat"       : "format",
    "listingStartTime"    : "list_start",
    "listingEndTime"      : "list_end",
    "snapshot_ts"         : "snapshot_ts",
}

def flatten_one(rec):
    out = {}
    for k, new_k in KEEP.items():
        cur = rec
        for part in k.replace("[0]", ".0").split("."):
            if isinstance(cur, list):
                part = int(part)
            cur = cur.get(part) if isinstance(cur, dict) else cur[part]
            if cur is None:
                break
        out[new_k] = cur
    # derived columns
    out["title_len"] = len(out["title"]) if out["title"] else None
    return out

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--infile",  type=pathlib.Path, required=True)
    p.add_argument("--outfile", type=pathlib.Path, required=True)
    args = p.parse_args()

    rows = []
    with args.infile.open() as fh:
        for line in tqdm.tqdm(fh, desc="flatten"):
            rows.append(flatten_one(json.loads(line)))

    df = pd.DataFrame(rows)
    # normalise datetimes (timezone-aware)
    tz = zoneinfo.ZoneInfo("UTC")
    for col in ["sold_dt", "list_start", "list_end", "snapshot_ts"]:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.tz_localize(tz)

    args.outfile.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.outfile, index=False)
    print("wrote", len(df), "rows →", args.outfile)

if __name__ == "__main__":
    main()
