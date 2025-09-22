# Kaavin

Scraper for data at <https://avoindata.eduskunta.fi>

First install [`uv`](https://docs.astral.sh/uv/getting-started/installation/), and then either run the script with `uv run scraper.py $TABLE_NAME` for some `$TABLE_NAME` listed in `tables.txt`, or run the script for all tables by copying and pasting the following to your terminal:

```bash
while read -r LINE; do
  uv run scraper.py "$LINE"
done < tables.txt
```

