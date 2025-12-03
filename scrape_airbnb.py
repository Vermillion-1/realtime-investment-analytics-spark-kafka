import os
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

BASE_URL = "https://insideairbnb.com/get-the-data/"

def fetch_data():
    print("Fetching InsideAirbnb data page...")
    resp = requests.get(BASE_URL)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    all_data = []

    for h3 in soup.find_all("h3"):
        city_name = h3.get_text(strip=True)
        # filter for North American cities
        if "United States" not in city_name and "Canada" not in city_name:
            continue

        # find the next table sibling
        table = h3.find_next_sibling("table")
        if not table:
            continue

        links = []
        for a in table.find_all("a", href=True):
            href = a["href"]
            if href.endswith((".csv", ".csv.gz")):
                links.append(href)

        if links:
            all_data.append((city_name, links))

    print(f"Found {len(all_data)} North American cities with data.")
    return all_data


def download_files(city_links):
    os.makedirs("insideairbnb_data", exist_ok=True)
    for city, links in tqdm(city_links, desc="Downloading"):
        city_dir = os.path.join("insideairbnb_data", city.replace(",", "").replace(" ", "_"))
        os.makedirs(city_dir, exist_ok=True)

        for url in links:
            filename = os.path.join(city_dir, os.path.basename(url))
            if not os.path.exists(filename):
                r = requests.get(url)
                with open(filename, "wb") as f:
                    f.write(r.content)

    print("✅ Done! All data saved under: insideairbnb_data/")


if __name__ == "__main__":
    city_links = fetch_data()
    download_files(city_links)
