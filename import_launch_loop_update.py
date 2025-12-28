import os
import time
import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# --------------------------------------------------
# ENV & DB SETUP
# --------------------------------------------------

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("SUPABASE_DB_HOST"),
    dbname=os.getenv("SUPABASE_DB_NAME"),
    user=os.getenv("SUPABASE_DB_USER"),
    password=os.getenv("SUPABASE_DB_PASSWORD"),
    port=os.getenv("SUPABASE_DB_PORT"),
    connect_timeout=10
)

cur = conn.cursor()

# --------------------------------------------------
# INPUT
# --------------------------------------------------

def load_launch_ids(path: str) -> list[str]:
    launch_ids = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            url = line.split("|")[-1].strip()
            launch_ids.append(url.rstrip("/").split("/")[-1])
    return launch_ids


# --------------------------------------------------
# CORE IMPORT FUNCTION
# --------------------------------------------------

def import_launch(launch_id: str) -> None:
    LL2_URL = f"https://ll.thespacedevs.com/2.3.0/launches/{launch_id}/"

    response = requests.get(LL2_URL, timeout=30)
    response.raise_for_status()
    launch = response.json()

    info_url = next(
        (u["url"] for u in launch.get("info_urls", []) if u.get("priority") == 10),
        None
    )
    vid_url = next(
        (u["url"] for u in launch.get("vid_urls", []) if u.get("priority") == 10),
        None
    )

    # --------------------------------------------------
    # launch_ref (UPSERT MIRATO)
    # --------------------------------------------------

    sql = """
    insert into launch_ref (
        id, name, net,
        status_abbrev, status_description,
        lsp_name, lsp_abbrev,
        rocket_full_name,
        mission_name, mission_type, mission_description,
        orbit_name, orbit_abbrev,
        pad_name, location_name,
        info_url, vid_url,
        orbital_launch_attempt_count,
        agency_launch_attempt_count,
        orbital_launch_attempt_count_year,
        agency_launch_attempt_count_year,
        last_synced_at
    )
    values (
        %(id)s, %(name)s, %(net)s,
        %(status_abbrev)s, %(status_description)s,
        %(lsp_name)s, %(lsp_abbrev)s,
        %(rocket_full_name)s,
        %(mission_name)s, %(mission_type)s, %(mission_description)s,
        %(orbit_name)s, %(orbit_abbrev)s,
        %(pad_name)s, %(location_name)s,
        %(info_url)s, %(vid_url)s,
        %(orbital_total)s,
        %(agency_total)s,
        %(orbital_year)s,
        %(agency_year)s,
        now()
    )
    on conflict (id) do update set
        net = excluded.net,
        status_abbrev = excluded.status_abbrev,
        status_description = excluded.status_description,
        vid_url = excluded.vid_url,
        orbital_launch_attempt_count = excluded.orbital_launch_attempt_count,
        agency_launch_attempt_count = excluded.agency_launch_attempt_count,
        orbital_launch_attempt_count_year = excluded.orbital_launch_attempt_count_year,
        agency_launch_attempt_count_year = excluded.agency_launch_attempt_count_year,
        last_synced_at = now();
    """

    data = {
        "id": launch["id"],
        "name": launch["name"],
        "net": launch["net"],
        "status_abbrev": launch.get("status", {}).get("abbrev"),
        "status_description": launch.get("status", {}).get("description"),
        "lsp_name": launch.get("launch_service_provider", {}).get("name"),
        "lsp_abbrev": launch.get("launch_service_provider", {}).get("abbrev"),
        "rocket_full_name": launch.get("rocket", {}).get("configuration", {}).get("full_name"),
        "mission_name": launch.get("mission", {}).get("name"),
        "mission_type": launch.get("mission", {}).get("type"),
        "mission_description": launch.get("mission", {}).get("description"),
        "orbit_name": launch.get("mission", {}).get("orbit", {}).get("name"),
        "orbit_abbrev": launch.get("mission", {}).get("orbit", {}).get("abbrev"),
        "pad_name": launch.get("pad", {}).get("name"),
        "location_name": launch.get("pad", {}).get("location", {}).get("name"),
        "info_url": info_url,
        "vid_url": vid_url,
        "orbital_total": launch.get("orbital_launch_attempt_count"),
        "agency_total": launch.get("agency_launch_attempt_count"),
        "orbital_year": launch.get("orbital_launch_attempt_count_year"),
        "agency_year": launch.get("agency_launch_attempt_count_year"),
    }

    cur.execute(sql, data)

    # --------------------------------------------------
    # mission_agency (immutabile)
    # --------------------------------------------------

    agency_rows = [
        (launch["id"], a.get("name"), a.get("abbrev"))
        for a in launch.get("mission", {}).get("agencies", [])
    ]

    if agency_rows:
        execute_values(
            cur,
            """
            insert into mission_agency (launch_id, agency_name, agency_abbrev)
            values %s
            on conflict do nothing;
            """,
            agency_rows
        )

    # --------------------------------------------------
    # launcher_stage (immutabile)
    # --------------------------------------------------

    launcher_rows = []
    for stage in launch.get("rocket", {}).get("launcher_stage", []):
        landing = stage.get("landing", {})
        launcher = stage.get("launcher", {})
        launcher_rows.append((
            launch["id"],
            stage.get("type"),
            launcher.get("serial_number"),
            launcher.get("flights"),
            landing.get("success"),
            landing.get("landing_location", {}).get("name"),
            landing.get("landing_location", {}).get("abbrev"),
        ))

    if launcher_rows:
        execute_values(
            cur,
            """
            insert into launcher_stage (
                launch_id, stage_type, serial_number, flights,
                landing_success, landing_location_name, landing_location_abbrev
            ) values %s
            on conflict do nothing;
            """,
            launcher_rows
        )

    # --------------------------------------------------
    # spacecraft + crew (immutabile)
    # --------------------------------------------------

    spacecraft_rows = []
    crew_rows = []
    nationality_rows = []

    for stage in launch.get("rocket", {}).get("spacecraft_stage", []):
        spacecraft = stage.get("spacecraft", {})
        spacecraft_rows.append((
            launch["id"],
            stage.get("destination"),
            stage.get("duration"),
            spacecraft.get("name"),
            spacecraft.get("serial_number"),
            spacecraft.get("flights_count"),
        ))

        for crew in stage.get("launch_crew", []):
            astronaut = crew.get("astronaut", {})
            crew_rows.append((
                launch["id"],
                crew.get("role", {}).get("role"),
                astronaut.get("name"),
                astronaut.get("agency", {}).get("abbrev"),
                astronaut.get("age"),
            ))

            for nat in astronaut.get("nationality", []):
                nationality_rows.append((
                    launch["id"],
                    astronaut.get("name"),
                    nat.get("alpha_3_code"),
                ))

    if spacecraft_rows:
        execute_values(
            cur,
            """
            insert into spacecraft_stage (
                launch_id, destination, duration,
                spacecraft_name, serial_number, flights_count
            ) values %s
            on conflict do nothing;
            """,
            spacecraft_rows
        )

    if crew_rows:
        execute_values(
            cur,
            """
            insert into launch_crew (
                launch_id, astronaut_role, astronaut_name,
                astronaut_agency, astronaut_age
            ) values %s
            on conflict do nothing;
            """,
            crew_rows
        )

    if nationality_rows:
        execute_values(
            cur,
            """
            insert into crew_nationality (
                launch_id, astronaut_name, astronaut_nationality
            ) values %s
            on conflict do nothing;
            """,
            nationality_rows
        )


# --------------------------------------------------
# MAIN LOOP
# --------------------------------------------------

launch_ids = load_launch_ids("last_launches.txt")
print(f"Trovati {len(launch_ids)} launch_id.")

for launch_id in launch_ids:
    try:
        import_launch(launch_id)
        conn.commit()
        print(f"Import / update completato: {launch_id}")
        time.sleep(1.2)
    except requests.HTTPError as e:
        conn.rollback()
        print(f"Errore HTTP su {launch_id}: {e}")
    except Exception as e:
        conn.rollback()
        print(f"Errore generico su {launch_id}: {e}")

cur.close()
conn.close()
