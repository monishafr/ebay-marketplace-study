# src/clean.py
"""
Flatten raw NDJSON → clean (wide) table.

Usage
-----
Parquet :
    python -m src.clean --infile data/raw/2025-05-26.ndjson \
                        --outfile data/clean/listings_wide_2025-05-26.parquet
CSV :
    python -m src.clean --infile data/raw/2025-05-26.ndjson \
                        --outfile data/clean/listings_wide_2025-05-26.csv
"""
import argparse, json, pathlib, pandas as pd, tqdm, zoneinfo

# ─────────────────────────────────────────────────────────────────────────
# Fields to keep  ➜  new column names
# ─────────────────────────────────────────────────────────────────────────
KEEP = {
    "itemId"                        : "item_id",
    "title"                         : "title",
    "condition"                     : "condition",
    "categoryId"                    : "category_id",
    "soldDate"                      : "sold_dt",
    "soldPrice.value"               : "sold_price",
    "soldPrice.currency"            : "currency",
    "seller.username"               : "seller",
    "seller.feedbackScore"          : "feedback",
    "sellerInfo.topRatedSeller"     : "top_rated",
    "shippingOptions[0].shippingCost.value": "ship_cost",
    "quantity"                      : "qty_listed",
    "quantitySold"                  : "qty_sold",
    "listingFormat"                 : "format",
    "listingStartTime"              : "list_start",
    "listingEndTime"                : "list_end",
    "snapshot_ts"                   : "snapshot_ts",
    "itemGroupType"                 : "group_type",
    "leafCategoryIds[0]"            : "leaf_cat",
    "categories[0].categoryName"    : "cat_name",
    "image.imageUrl"                : "image_url",
    "thumbnailImages[0].imageUrl"   : "thumb_url",
    "itemLocation.postalCode"       : "seller_zip",
    "itemLocation.country"          : "seller_country",
    "buyingOptions[0]"              : "buy_opt",
    "itemOriginDate"                : "origin_dt",
    "itemCreationDate"              : "item_dt",
    "priorityListing"               : "priority_listing",
    "availableCoupons"              : "coupon_flag",
    "epid"                          : "product_epid",
}

# ─────────────────────────────────────────────────────────────────────────
def flatten_one(rec: dict) -> dict:
    """
    Drill into the raw JSON record and pull out every path in KEEP.
    Works even when a path segment is missing.
    """
    out = {}
    for raw_key, new_key in KEEP.items():
        parts = raw_key.replace("[0]", ".0").split(".")
        cur = rec
        for p in parts:
            if cur is None:                               # stopped early
                break
            if isinstance(cur, list):                     # jump into list
                idx = int(p)
                cur = cur[idx] if len(cur) > idx else None
            elif isinstance(cur, dict):                   # dict lookup
                cur = cur.get(p)
            else:                                         # primitive -> dead end
                cur = None
                break
        out[new_key] = cur

    # derived column
    out["title_len"] = len(out["title"]) if out["title"] else None
    return out

# ─────────────────────────────────────────────────────────────────────────
def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--infile",  type=pathlib.Path, required=True)
    p.add_argument("--outfile", type=pathlib.Path, required=True)
    args = p.parse_args()

    # ── flatten NDJSON → rows -------------------------------------------------
    rows = []
    with args.infile.open() as fh:
        for line in tqdm.tqdm(fh, desc="flatten"):
            rows.append(flatten_one(json.loads(line)))

    df = pd.DataFrame(rows)

    # ── normalise datetime columns to UTC ------------------------------------
    utc = zoneinfo.ZoneInfo("UTC")
    for col in ["sold_dt", "list_start", "list_end",
                "origin_dt", "item_dt", "snapshot_ts"]:
        ser = pd.to_datetime(df[col], errors="coerce")
        # only localise tz-naïve series
        if ser.dt.tz is None:
            ser = ser.dt.tz_localize(utc)
        df[col] = ser

    # ── write out ------------------------------------------------------------
    args.outfile.parent.mkdir(parents=True, exist_ok=True)
    if args.outfile.suffix.lower() == ".csv":
        df.to_csv(args.outfile, index=False)
    else:
        df.to_parquet(args.outfile, index=False)
    print("✅ wrote", len(df), "rows →", args.outfile)

# ─────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
