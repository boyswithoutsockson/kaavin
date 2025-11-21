import csv
import asyncio
import requests
import polars as pl
from tqdm import tqdm

def get_activity_term_url(term):
    return f"https://public.api.avoimuusrekisteri.fi/open-data-activity-notification/term/{term}"

def get_lobby_activity_url():
    return f"https://public.api.avoimuusrekisteri.fi/open-data-activity-notification/company/{{}}"

def get_term_url():
    return f"https://public.api.avoimuusrekisteri.fi/open-data-term/all"

def get_targets_url():
    return f"https://public.api.avoimuusrekisteri.fi/open-data-target/targets"

def write_tsv(filename, data):
    with open(f"{filename}.tsv", "w", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(data[0])
        for row in data:
            writer.writerow(row.values())

async def fetch_page(session, url, Y_id):
    async with session.get(url.format(Y_id)) as response:
        return await response.json()

async def fetch_all_data():


    # Terms
    print("Scraping terms...")
    terms_url = get_term_url()
    response = requests.get(terms_url)
    terms = response.json()

    df = pl.DataFrame(terms, infer_schema_length=10000)  
    df.write_json("lobby_terms.json")
    print("Terms done!")


    # Targets
    print("Scraping targets...")

    targets_url = get_targets_url()
    response = requests.get(targets_url)
    targets = response.json()

    df = pl.DataFrame(targets, infer_schema_length=10000)  
    df.write_json("lobby_targets.json")
    print("Targets done!")


    # Actions
    print("Scraping actions...")

    term = 1
    activity_term_url = get_activity_term_url(term)
    response = requests.get(activity_term_url)
    actions_json = response.json()
    actions = []

    while actions_json:
        actions.extend(actions_json)
        term += 1
        activity_term_url = get_activity_term_url(term)
        response = requests.get(activity_term_url)
        actions_json = response.json()

    df = pl.DataFrame(actions, infer_schema_length=10000)  
    df.write_json("lobby_actions.json")
    print("Actions done!")

if __name__ == "__main__":
    asyncio.run(fetch_all_data())
