# eBay Marketplace Study

> Work-in-progress dataset & analysis of >10 k eBay listings and seller records.

## Directory layout
data/
  raw/       # unprocessed API dumps
  interim/   # intermediate merges (e.g., unique seller list)
  clean/     # final, analysis-ready CSVs or Parquet
src/         # Python collection & cleaning scripts
notebooks/   # ad-hoc exploration & EDA
tests/       # integrity checks
env/         # .env.example lives here


## TODO (high-level)

- [ ] Register eBay developer keys (Phase 1)
- [ ] Build `src/collect.py` to hit Browse API
- [ ] Clean & merge data (Phase 3)
- [ ] Draft README sections on data fields & challenges (Phase 6)
