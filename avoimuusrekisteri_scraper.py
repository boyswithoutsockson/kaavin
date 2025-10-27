import csv
import asyncio
import requests
import polars as pl
import aiohttp
from tqdm import tqdm

def get_activity_term_url(term):
    return f"https://public.api.avoimuusrekisteri.fi/open-data-activity-notification/term/{term}"

def get_lobby_activity_url():
    return f"https://public.api.avoimuusrekisteri.fi/open-data-activity-notification/company/{{}}"

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

    # Targets
    print("Scraping targets...")

    targets_url = get_targets_url()
    response = requests.get(targets_url)
    targets = response.json()

    write_tsv("lobby_targets", targets)
    print("Targets done!")


    # Lobbies
    print("Scraping lobbies...")

    term = 1
    activity_term_url = get_activity_term_url(term)
    response = requests.get(activity_term_url)
    lobbies_json = response.json()
    lobbies = []

    while lobbies_json:
        lobbies.extend(lobbies_json)
        term += 1
        activity_term_url = get_activity_term_url(term)
        response = requests.get(activity_term_url)
        lobbies_json = response.json()

    write_tsv("lobbies", lobbies)
    print("Lobbies done!")


    # Activity
    print("Scraping activity...")

    lobbies_df = pl.DataFrame(lobbies, infer_schema_length=10000)
    Y_ids = lobbies_df["companyId"].unique().to_list()

    url = get_lobby_activity_url()
    activity = []

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_page(session, url, Y_id) for Y_id in Y_ids]
        with tqdm(total=len(tasks), desc="Scraping activity", unit="lobby") as pbar:
            for task in asyncio.as_completed(tasks):
                data = await task
                activity.extend(data)
                pbar.update(1)
            
    write_tsv("lobby_activity", activity)
    print("Activity done!")

if __name__ == "__main__":
    asyncio.run(fetch_all_data())
