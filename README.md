# eBay Marketplace Study

Collecting and analysing **>10 000** eBay sold-listing records + seller data.

## Platform & Key Fields

| Field | Example | Reason we keep it |
|-------|---------|-------------------|
| `item_id` | `v1|254123456789` | unique key for joins |
| `title` | `"Apple iPhone 15 Pro Max 256GB"` | used for title-length vs price |
| `soldPrice.value` | `1099.00` | dependent variable in Q1, Q2, Q5 |
| `soldDate` | `2025-05-26T14:11:33Z` | temporal trends, conversion speed |
| `shippingOptions.cost` | `0.0` | free vs paid shipping (Q3) |
| `listingFormat` | `"FIXED_PRICE"` / `"AUCTION"` | Q4 |
| `quantity`, `quantitySold` | `1`, `1` | sell-through calculation |
| `condition` | `"MANUFACTURER_REFURBISHED"` | refurbished price trend (Q5) |
| `seller.username` | `"top-seller123"` | join to TRS flag |
| `sellerInfo.topRatedSeller` | `true` | Q2 |

*(Full field list will be finalised after the first bulk pull.)*

## Folder Layout
data/ raw/ interim/ clean/
src/ notebooks/
tests/
