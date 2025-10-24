import csv
import requests

def fetch_election_seasons():
    response = requests.get("https://api.julkinen.beta.eduskunta.fi/api/v1/reference-data/vaalikaudet")
    election_seasons = response.json()
    print(election_seasons)

    with open(f"election_seasons.tsv", "w", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(election_seasons[0])
        for row in election_seasons:
            writer.writerow(row.values())
        
        print("Done.")

if __name__ == "__main__":
    fetch_election_seasons()
