# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "aiohttp==3.11.18",
#     "tqdm==4.67.1",
# ]
# ///

import csv
import argparse
import asyncio

import aiohttp
from tqdm import tqdm

ROWS = 100

def get_api_url(table_name):
    return f"https://avoindata.eduskunta.fi/api/v1/tables/{table_name}/rows?perPage={ROWS}&page={{}}"

async def fetch_page(session, url, page):
    async with session.get(url.format(page)) as response:
        return await response.json()

async def fetch_all_data(table_name, parallel_requests):
    api_url = get_api_url(table_name)
    all_data = []
    page = 0
    has_data = True

    async with aiohttp.ClientSession() as session:
        with tqdm(desc=f"Scraping {table_name}", unit="page") as pbar:
            while has_data:
                tasks = [fetch_page(session, api_url, p) for p in range(page, page + parallel_requests)]
                responses = await asyncio.gather(*tasks)
                
                all_data.extend(responses)
                for data in responses:
                    if not data["hasMore"]:
                        has_data = False
                    else:
                        pbar.update(1)
                
                page += parallel_requests

    with open(f"{table_name}.tsv", "w", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(responses[0]["columnNames"])
        for resp in tqdm(all_data, desc="Writing", unit="row"):
            writer.writerows(resp["rowData"])
    
    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch all Eduskunta Avoin Data from specified table")
    parser.add_argument("table_name", type=str, help="Name of the table to fetch data from.")
    parser.add_argument("--parallel", type=int, default=10, help="Number of parallel requests (default: 10).")
    args = parser.parse_args()

    asyncio.run(fetch_all_data(args.table_name, args.parallel))

