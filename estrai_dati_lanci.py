import requests

#URL = (
#    "https://ll.thespacedevs.com/2.3.0/launches/"
#    "?lsp__name=Rocket%20Lab"
#)

from datetime import datetime, timedelta, timezone

# Calcola le date usando datetime aware in UTC
two_days_ago = datetime.now(timezone.utc) - timedelta(days=2)
tomorrow = datetime.now(timezone.utc) + timedelta(days=1)

# Formatta le date in ISO 8601
start_date = two_days_ago.strftime("%Y-%m-%dT00:00:00Z")
end_date = tomorrow.strftime("%Y-%m-%dT23:59:59Z")

# Costruisci l'URL
URL = (
    "https://ll.thespacedevs.com/2.3.0/launches/"
    f"?window_start__gte={start_date}"
    f"&window_end__lte={end_date}"
)


OUTPUT_FILE = "last_launches.txt"

HEADERS = {
    "User-Agent": "LL2-Pagination-Test/1.0"
}

url = URL
launches = []

while url:
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    data = response.json()

    for launch in data.get("results", []):
        launches.append({
            "url": launch.get("url"),
            "name": launch.get("name"),
            "net": launch.get("net")
        })

    url = data.get("next")  # se next è None, il loop termina

print(f"Trovati {len(launches)} lanci.")

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    for l in launches:
        f.write("{} | {} | {}\n".format(l["net"], l["name"], l["url"]))
        
        