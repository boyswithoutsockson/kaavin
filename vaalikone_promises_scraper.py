import requests
import polars as pl
from tqdm import tqdm
from bs4 import BeautifulSoup


def fetch_candidate_data():

    candidates = pl.DataFrame()

    for i in range(1,14):
        response = requests.get(f"https://vaalit.yle.fi/content/ev2023/58/electorates/{i}/partyAndCandidateResults.json")

        df = pl.DataFrame(response.json()["candidateResults"])

        df = df.select(pl.col("caid"), 
                       pl.col("firstName"), 
                       pl.col("lastName"))
        
        df = df.with_columns(pl.lit(i).alias("district"))

        candidates = candidates.vstack(df)

    return candidates


def fetch_promises(candidates):

    promises = pl.DataFrame()

    for candidate in tqdm(candidates.iter_rows(named=True)):
        response = requests.get(f"https://vaalit.yle.fi/ev2023/tulospalvelu/fi/electoral-districts/{candidate["district"]}/candidates/{candidate["caid"]}/")

        soup = BeautifulSoup(response.content, "html.parser")

        # Promises in this div
        divs = soup.find_all("div", class_="sc-6a4c8255-9 gzMiRN")

        for div in divs:
            promise = div.get_text(strip=True)

            if promise:
                candidate["promise"] = promise
                promises = promises.vstack(pl.DataFrame(candidate))

    promises = promises.drop(["caid", "district"])
    promises.write_json("vaalilupaukset_2023.json")


if __name__ == "__main__":
    candidates = fetch_candidate_data()
    fetch_promises(candidates)
