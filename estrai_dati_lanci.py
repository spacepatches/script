import requests

#URL = (
#    "https://ll.thespacedevs.com/2.3.0/launches/"
#    "?lsp__name=Rocket%20Lab"
#)

URL = (
    "https://ll.thespacedevs.com/2.3.0/launches/"
    "?window_start__gte=2025-12-28T00:00:00Z"
    "&window_end__lte=2025-12-31T23:59:59Z"
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
        
        