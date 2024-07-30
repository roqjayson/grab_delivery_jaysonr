import requests
import pandas as pd
from datetime import datetime

def fetch_data(url: str, headers: dict, params: dict) -> dict:
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()

def flatten_json(json_obj: dict, parent_key: str = '', sep: str = '_') -> dict:
    items = []
    for k, v in json_obj.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_json(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            for i, item in enumerate(v):
                items.extend(flatten_json(item, f"{new_key}{sep}{i}", sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def run_ingestion():
    url = "https://cost-of-living-and-prices.p.rapidapi.com/cities"
    headers = {
        "x-rapidapi-host": "cost-of-living-and-prices.p.rapidapi.com",
        "x-rapidapi-key": "04b4dd4655msh61602c297ac8517p196fd8jsn4ce04f636dc9"
    }
    batch_size = 100
    start_page = 1
    max_pages = 10
    all_data = []

    for page in range(start_page, start_page + max_pages):
        params = {
            'page': page,
            'limit': batch_size
        }
        try:
            data = fetch_data(url, headers, params)
            current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            data['sysingest_datetime'] = current_datetime
            if 'cities' in data:
                for city in data['cities']:
                    flattened_city = flatten_json(city)
                    all_data.append(flattened_city)
        except Exception as e:
            print(f"An error occurred: {e}")
            break

    df = pd.DataFrame(all_data)
    df.to_csv('/cities_batch.csv', index=False)