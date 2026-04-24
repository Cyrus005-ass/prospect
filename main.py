from __future__ import annotations

import csv
import hashlib
import os
import re
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, urljoin

import pandas as pd
import requests
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import HTMLResponse

app = FastAPI(title="ProspectHunter", version="5.1.0")

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_FILE = DATA_DIR / "prospect.db"

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
GOOGLE_TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
GOOGLE_PLACE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"
UA = {"User-Agent": "ProspectHunter/5.1 (contact: admin@example.com)"}
DEFAULT_API_KEY = os.getenv("PROSPECT_API_KEY", "dev-key-change-me")
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "").strip()
VALID_CRM_STATUSES = {"new", "contacted", "replied", "closed", "ignored"}
RUN_LOCK = threading.Lock()

DM_TEMPLATES = {
    "restaurant": {
        "no_website": "Boss, j'ai vu {name}. Vous n'avez pas de site vitrine. Je peux vous livrer une page simple qui ramene des clients en local.",
        "no_hours": "Boss, vos horaires ne sont pas remplis sur votre fiche. On peut corriger ca et eviter de perdre des clients qui viennent ferme.",
        "weak_profile": "Boss, votre fiche a plusieurs manques ({tags}). Je peux la remettre au propre rapidement.",
    },
    "default": {
        "no_website": "Bonjour, j'ai vu {name}. Votre fiche n'a pas de site web. Je peux vous en mettre un simple et efficace.",
        "no_phone": "Bonjour, votre fiche manque un numero exploitable. On peut corriger ca rapidement.",
        "no_hours": "Bonjour, vos horaires ne sont pas renseignes. Cela peut faire perdre des clients.",
        "weak_profile": "Bonjour, votre fiche est incomplete ({tags}). Je peux vous aider a l'optimiser.",
    },
}


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with db_conn() as conn:
        conn.executescript(
            """
            PRAGMA journal_mode = WAL;

            CREATE TABLE IF NOT EXISTS users (
              api_key TEXT PRIMARY KEY,
              created_utc TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS jobs (
              job_id TEXT PRIMARY KEY,
              owner_api_key TEXT NOT NULL,
              status TEXT NOT NULL,
              query TEXT NOT NULL,
              city TEXT NOT NULL,
              country TEXT,
              weakness TEXT NOT NULL,
              limit_n INTEGER NOT NULL,
              vertical TEXT NOT NULL,
              source TEXT NOT NULL DEFAULT 'overpass',
              created_utc TEXT NOT NULL,
              started_utc TEXT,
              finished_utc TEXT,
              total_raw INTEGER,
              total_filtered INTEGER,
              csv_path TEXT,
              xlsx_path TEXT,
              error TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_jobs_owner_created ON jobs(owner_api_key, created_utc DESC);

            CREATE TABLE IF NOT EXISTS leads (
              job_id TEXT NOT NULL,
              lead_id TEXT NOT NULL,
              owner_api_key TEXT NOT NULL,
              name TEXT NOT NULL,
              phone TEXT,
              email TEXT,
              whatsapp_link TEXT,
              website TEXT,
              address TEXT,
              country TEXT,
              note TEXT,
              reviews TEXT,
              tags TEXT,
              score INTEGER,
              priority TEXT,
              maps_link TEXT,
              source TEXT,
              Script_DM TEXT,
              status TEXT,
              last_update_utc TEXT,
              crm_note TEXT,
              PRIMARY KEY (job_id, lead_id)
            );
            CREATE INDEX IF NOT EXISTS idx_leads_job_filters ON leads(job_id, priority, status);
            CREATE INDEX IF NOT EXISTS idx_leads_owner_lead ON leads(owner_api_key, lead_id);

            CREATE TABLE IF NOT EXISTS crm (
              owner_api_key TEXT NOT NULL,
              lead_id TEXT NOT NULL,
              status TEXT NOT NULL,
              note TEXT,
              updated_utc TEXT NOT NULL,
              PRIMARY KEY (owner_api_key, lead_id)
            );
            CREATE INDEX IF NOT EXISTS idx_crm_owner_status ON crm(owner_api_key, status);
            """
                )
        _ensure_column(conn, "jobs", "country", "TEXT")
        _ensure_column(conn, "jobs", "source", "TEXT NOT NULL DEFAULT 'overpass'")
        _ensure_column(conn, "leads", "email", "TEXT")
        _ensure_column(conn, "leads", "country", "TEXT")
        _ensure_column(conn, "leads", "source", "TEXT")


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(r[1]) for r in rows}


def _ensure_column(conn: sqlite3.Connection, table: str, col: str, definition: str) -> None:
    if col in _table_columns(conn, table):
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {definition}")


def ensure_user(api_key: str) -> None:
    with db_conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO users(api_key, created_utc) VALUES (?, ?)",
            (api_key, now_utc()),
        )


def auth_api_key(api_key: str | None, x_api_key: str | None) -> str:
    key = (api_key or x_api_key or DEFAULT_API_KEY).strip()
    if not key:
        raise HTTPException(status_code=401, detail="api_key requis")
    ensure_user(key)
    return key


def normalize_phone(phone: str) -> str:
    digits = re.sub(r"[^0-9+]", "", phone or "")
    if digits.startswith("00"):
        digits = "+" + digits[2:]
    return digits


def wa_link(phone: str) -> str:
    p = normalize_phone(phone).replace("+", "")
    return f"https://wa.me/{p}" if p else ""


def weakness_tags(tags: dict[str, str]) -> list[str]:
    out: list[str] = []
    if not (tags.get("website") or tags.get("contact:website")):
        out.append("no_website")
    if not (tags.get("phone") or tags.get("contact:phone")):
        out.append("no_phone")
    if not tags.get("opening_hours"):
        out.append("no_hours")
    if len(out) >= 2:
        out.append("weak_profile")
    return out


def weakness_from_presence(has_website: bool, has_phone: bool, has_hours: bool) -> list[str]:
    out: list[str] = []
    if not has_website:
        out.append("no_website")
    if not has_phone:
        out.append("no_phone")
    if not has_hours:
        out.append("no_hours")
    if len(out) >= 2:
        out.append("weak_profile")
    return out

def score_lead(tags: list[str], has_phone: bool) -> tuple[int, str]:
    score = 0
    if "no_website" in tags:
        score += 45
    if "no_phone" in tags:
        score += 15
    if "no_hours" in tags:
        score += 15
    if "weak_profile" in tags:
        score += 10
    if has_phone:
        score += 15
        if "no_website" in tags:
            score += 10
    score = max(0, min(100, score))
    if score >= 70:
        return score, "HOT"
    if score >= 45:
        return score, "WARM"
    return score, "COLD"


def infer_vertical(query: str, vertical: str | None) -> str:
    if vertical:
        return vertical.strip().lower()
    return "restaurant" if "restaurant" in (query or "").lower() else "default"


def dm_script(name: str, tags: list[str], vertical: str) -> str:
    bucket = DM_TEMPLATES.get(vertical, DM_TEMPLATES["default"])
    chosen = "weak_profile"
    for k in ["no_website", "no_phone", "no_hours", "weak_profile"]:
        if k in tags and k in bucket:
            chosen = k
            break
    return bucket[chosen].format(name=name, tags=", ".join(tags))


def dm_ab_variant(name: str, tags: list[str], vertical: str) -> dict[str, str]:
    base = dm_script(name, tags, vertical)
    return {
        "A": base + " Si tu veux, je te fais un mini audit gratuit.",
        "B": f"Salut, j'ai verifie {name}. Point faible detecte: {', '.join(tags) or 'profil incomplet'}. Je peux corriger ca rapidement.",
    }


def lead_id_for(name: str, phone: str, address: str) -> str:
    raw = f"{name}|{phone}|{address}".strip().lower().encode("utf-8")
    return hashlib.sha1(raw).hexdigest()[:12]


def overpass_fetch(query: str, city: str, limit: int) -> list[dict[str, Any]]:
    esc_query = re.sub(r'"', r'\\"', query)
    esc_city = re.sub(r'"', r'\\"', city)
    q = f"""
[out:json][timeout:60];
area["name"="{esc_city}"]["boundary"="administrative"]->.searchArea;
(
  nwr["name"~"{esc_query}",i](area.searchArea);
  nwr["amenity"~"{esc_query}",i](area.searchArea);
  nwr["shop"~"{esc_query}",i](area.searchArea);
);
out tags center {limit};
"""
    r = requests.post(OVERPASS_URL, data=q.encode("utf-8"), headers=UA, timeout=90)
    r.raise_for_status()
    return r.json().get("elements", [])



def _google_api_key() -> str:
    if not GOOGLE_MAPS_API_KEY:
        raise RuntimeError("GOOGLE_MAPS_API_KEY manquante. Ajoute la variable d'environnement pour utiliser Google Maps.")
    return GOOGLE_MAPS_API_KEY


def _extract_emails(text: str) -> list[str]:
    if not text:
        return []
    raw = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
    bad = {"example.com", "email.com", "domain.com"}
    out: list[str] = []
    seen: set[str] = set()
    for e in raw:
        v = e.strip().strip(".,;:)").lower()
        if "@" not in v:
            continue
        dom = v.split("@", 1)[1]
        if dom in bad:
            continue
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _emails_from_website(website: str, timeout_sec: int = 12) -> list[str]:
    if not website:
        return []
    base = website.strip()
    if not base:
        return []
    if not base.startswith("http://") and not base.startswith("https://"):
        base = "https://" + base

    candidates = [base]
    for p in ["/contact", "/contact-us", "/about", "/a-propos", "/nous-contacter"]:
        candidates.append(urljoin(base if base.endswith("/") else base + "/", p.lstrip("/")))

    seen_url: set[str] = set()
    found: list[str] = []
    for url in candidates:
        if url in seen_url:
            continue
        seen_url.add(url)
        try:
            resp = requests.get(url, headers=UA, timeout=timeout_sec, allow_redirects=True)
            if resp.status_code >= 400:
                continue
            found.extend(_extract_emails(resp.text or ""))
        except Exception:
            continue

    dedup: list[str] = []
    seen: set[str] = set()
    for e in found:
        if e not in seen:
            seen.add(e)
            dedup.append(e)
    return dedup[:3]


def google_places_fetch(query: str, city: str, country: str, limit: int) -> list[dict[str, Any]]:
    key = _google_api_key()
    full_query = " ".join([x for x in [query.strip(), city.strip(), country.strip()] if x])
    if not full_query:
        return []

    all_results: list[dict[str, Any]] = []
    page_token = ""
    while len(all_results) < limit:
        params: dict[str, str] = {"key": key}
        if page_token:
            params["pagetoken"] = page_token
            time.sleep(2)
        else:
            params["query"] = full_query

        r = requests.get(GOOGLE_TEXT_SEARCH_URL, params=params, headers=UA, timeout=40)
        r.raise_for_status()
        payload = r.json()
        status = payload.get("status", "")
        if status not in {"OK", "ZERO_RESULTS"}:
            raise RuntimeError(f"Google Text Search error: {status}")

        batch = payload.get("results", [])
        all_results.extend(batch)
        page_token = payload.get("next_page_token", "")
        if not page_token or status == "ZERO_RESULTS":
            break
    return all_results[:limit]


def google_place_details(place_id: str) -> dict[str, Any]:
    key = _google_api_key()
    fields = [
        "name",
        "formatted_address",
        "formatted_phone_number",
        "international_phone_number",
        "website",
        "url",
        "rating",
        "user_ratings_total",
        "opening_hours",
        "types",
    ]
    r = requests.get(
        GOOGLE_PLACE_DETAILS_URL,
        params={"key": key, "place_id": place_id, "fields": ",".join(fields)},
        headers=UA,
        timeout=40,
    )
    r.raise_for_status()
    payload = r.json()
    status = payload.get("status", "")
    if status not in {"OK", "ZERO_RESULTS"}:
        raise RuntimeError(f"Google Place Details error: {status}")
    return payload.get("result", {})


def to_lead(el: dict[str, Any], city: str, country: str, vertical: str) -> dict[str, Any]:
    tags = el.get("tags", {})
    name = tags.get("name", "N/A")
    phone = normalize_phone(tags.get("phone") or tags.get("contact:phone") or "")
    website = tags.get("website") or tags.get("contact:website") or ""
    addr_parts = [tags.get("addr:housenumber", ""), tags.get("addr:street", ""), tags.get("addr:suburb", ""), tags.get("addr:city", city)]
    address = " ".join([x for x in addr_parts if x]).strip()
    lat = el.get("lat") or el.get("center", {}).get("lat")
    lon = el.get("lon") or el.get("center", {}).get("lon")
    maps_link = f"https://www.google.com/maps/search/?api=1&query={lat},{lon}" if lat and lon else ""
    tags_w = weakness_tags(tags)
    score, priority = score_lead(tags_w, has_phone=bool(phone))
    return {
        "lead_id": lead_id_for(name, phone, address),
        "name": name,
        "phone": phone,
        "email": "",
        "whatsapp_link": wa_link(phone),
        "website": website,
        "address": address,
        "country": country,
        "note": "N/A",
        "reviews": "N/A",
        "tags": ",".join(tags_w),
        "score": score,
        "priority": priority,
        "maps_link": maps_link,
        "source": "overpass",
        "Script_DM": dm_script(name, tags_w, vertical),
        "status": "new",
        "last_update_utc": now_utc(),
        "crm_note": "",
    }


def google_to_lead(item: dict[str, Any], city: str, country: str, vertical: str) -> dict[str, Any]:
    place_id = item.get("place_id", "")
    details = google_place_details(place_id) if place_id else {}
    name = details.get("name") or item.get("name") or "N/A"
    phone_raw = details.get("international_phone_number") or details.get("formatted_phone_number") or ""
    phone = normalize_phone(phone_raw)
    website = details.get("website") or ""
    address = details.get("formatted_address") or item.get("formatted_address") or ""
    maps_link = details.get("url") or f"https://www.google.com/maps/search/?api=1&query={quote_plus(f'{name} {city} {country}'.strip())}"

    rating = details.get("rating")
    ratings_total = details.get("user_ratings_total")
    reviews = "N/A"
    if rating is not None and ratings_total is not None:
        reviews = f"rating={rating} ({ratings_total} avis)"

    has_hours = bool(details.get("opening_hours", {}).get("weekday_text"))
    tags_w = weakness_from_presence(has_website=bool(website), has_phone=bool(phone), has_hours=has_hours)
    score, priority = score_lead(tags_w, has_phone=bool(phone))

    emails = _emails_from_website(website)
    email = emails[0] if emails else ""

    return {
        "lead_id": lead_id_for(name, phone, address),
        "name": name,
        "phone": phone,
        "email": email,
        "whatsapp_link": wa_link(phone),
        "website": website,
        "address": address,
        "country": country,
        "note": "N/A",
        "reviews": reviews,
        "tags": ",".join(tags_w),
        "score": score,
        "priority": priority,
        "maps_link": maps_link,
        "source": "google_maps",
        "Script_DM": dm_script(name, tags_w, vertical),
        "status": "new",
        "last_update_utc": now_utc(),
        "crm_note": "",
    }


def sort_leads(leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    pmap = {"HOT": 3, "WARM": 2, "COLD": 1}
    return sorted(leads, key=lambda x: (pmap.get(x.get("priority", "COLD"), 0), x.get("score", 0)), reverse=True)


def filter_leads(leads: list[dict[str, Any]], weakness: str) -> list[dict[str, Any]]:
    weakness = (weakness or "all").strip().lower()
    out = [x for x in leads if len([t for t in x["tags"].split(",") if t]) >= 2] if weakness == "all" else [x for x in leads if weakness in x["tags"].split(",")]
    return sort_leads(out)


def dedup_leads(leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    for l in leads:
        old = by_id.get(l["lead_id"])
        if old is None or int(l["score"]) > int(old["score"]):
            by_id[l["lead_id"]] = l
    return list(by_id.values())


def crm_map_for(owner_api_key: str, lead_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not lead_ids:
        return {}
    ph = ",".join(["?"] * len(lead_ids))
    with db_conn() as conn:
        rows = conn.execute(f"SELECT lead_id,status,note,updated_utc FROM crm WHERE owner_api_key=? AND lead_id IN ({ph})", [owner_api_key, *lead_ids]).fetchall()
    return {str(r["lead_id"]): {"status": r["status"], "note": r["note"] or "", "updated_utc": r["updated_utc"]} for r in rows}


def merge_crm(owner_api_key: str, leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    mapping = crm_map_for(owner_api_key, [x["lead_id"] for x in leads])
    for l in leads:
        c = mapping.get(l["lead_id"])
        if c:
            l["status"] = c.get("status", l["status"])
            l["crm_note"] = c.get("note", l["crm_note"])
            l["last_update_utc"] = c.get("updated_utc", l["last_update_utc"])
    return leads


def save_csv(job_id: str, query: str, city: str, leads: list[dict[str, Any]]) -> Path:
    safe_q = re.sub(r"[^a-zA-Z0-9]+", "_", query).strip("_").lower() or "query"
    safe_c = re.sub(r"[^a-zA-Z0-9]+", "_", city).strip("_").lower() or "city"
    out = DATA_DIR / f"leads_{safe_q}_{safe_c}_{job_id[:8]}.csv"
    fields = ["lead_id", "name", "phone", "email", "whatsapp_link", "website", "address", "country", "note", "reviews", "tags", "score", "priority", "maps_link", "source", "Script_DM", "status", "last_update_utc", "crm_note"]
    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(leads)
    return out


def save_xlsx(job_id: str, query: str, city: str, leads: list[dict[str, Any]]) -> Path:
    safe_q = re.sub(r"[^a-zA-Z0-9]+", "_", query).strip("_").lower() or "query"
    safe_c = re.sub(r"[^a-zA-Z0-9]+", "_", city).strip("_").lower() or "city"
    out = DATA_DIR / f"leads_{safe_q}_{safe_c}_{job_id[:8]}.xlsx"
    pd.DataFrame(leads).to_excel(out, index=False)
    return out


def update_job(job_id: str, **kwargs: Any) -> None:
    if not kwargs:
        return
    cols = ", ".join([f"{k}=?" for k in kwargs.keys()])
    vals = list(kwargs.values()) + [job_id]
    with db_conn() as conn:
        conn.execute(f"UPDATE jobs SET {cols} WHERE job_id=?", vals)


def persist_leads(job_id: str, owner_api_key: str, leads: list[dict[str, Any]]) -> None:
    with db_conn() as conn:
        conn.execute("DELETE FROM leads WHERE job_id=?", (job_id,))
        conn.executemany(
            """
            INSERT INTO leads(job_id, lead_id, owner_api_key, name, phone, email, whatsapp_link, website, address, country, note, reviews, tags, score, priority, maps_link, source, Script_DM, status, last_update_utc, crm_note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [(job_id, l["lead_id"], owner_api_key, l["name"], l.get("phone", ""), l.get("email", ""), l.get("whatsapp_link", ""), l.get("website", ""), l.get("address", ""), l.get("country", ""), l.get("note", ""), l.get("reviews", ""), l.get("tags", ""), int(l.get("score", 0)), l.get("priority", "COLD"), l.get("maps_link", ""), l.get("source", "overpass"), l.get("Script_DM", ""), l.get("status", "new"), l.get("last_update_utc", now_utc()), l.get("crm_note", "")) for l in leads],
        )


def run_job(job_id: str, owner_api_key: str, query: str, city: str, country: str, weakness: str, limit: int, vertical: str, source: str) -> None:
    with RUN_LOCK:
        update_job(job_id, status="running", started_utc=now_utc())
    try:
        src = (source or "overpass").strip().lower()
        if src == "google_maps" and not GOOGLE_MAPS_API_KEY:
            # Keep the app usable on free deployments when no Google key is configured.
            src = "overpass"
            update_job(job_id, source=src)
        if src == "google_maps":
            raw = google_places_fetch(query, city, country, limit)
            leads = [google_to_lead(el, city, country, vertical) for el in raw]
        else:
            raw = overpass_fetch(query, city, limit)
            leads = [to_lead(el, city, country, vertical) for el in raw]
        leads = [x for x in leads if x["name"] != "N/A"]
        leads = dedup_leads(leads)
        leads = filter_leads(leads, weakness)
        leads = merge_crm(owner_api_key, leads)
        out_csv = save_csv(job_id, query, city, leads)
        out_xlsx = save_xlsx(job_id, query, city, leads)
        persist_leads(job_id, owner_api_key, leads)
        update_job(job_id, status="done", finished_utc=now_utc(), total_raw=len(raw), total_filtered=len(leads), csv_path=str(out_csv.resolve()), xlsx_path=str(out_xlsx.resolve()), error=None)
    except Exception as exc:
        update_job(job_id, status="failed", finished_utc=now_utc(), error=str(exc))

@app.on_event("startup")
def startup() -> None:
    init_db()
    ensure_user(DEFAULT_API_KEY)


@app.get("/health")
def health() -> dict[str, str]:
    init_db()
    return {"status": "ok", "db": str(DB_FILE.resolve())}


@app.get("/jobs")
def jobs(api_key: str | None = Query(None), x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> list[dict[str, Any]]:
    owner = auth_api_key(api_key, x_api_key)
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT job_id,status,query,city,country,weakness,limit_n,vertical,source,created_utc,started_utc,finished_utc,total_raw,total_filtered,csv_path,xlsx_path,error FROM jobs WHERE owner_api_key=? ORDER BY created_utc DESC LIMIT 30",
            (owner,),
        ).fetchall()
    return [dict(r) for r in rows]


@app.get("/dm/templates")
def dm_templates(vertical: str = Query("default")) -> dict[str, Any]:
    v = vertical.strip().lower()
    return {"vertical": v, "templates": DM_TEMPLATES.get(v, DM_TEMPLATES["default"])}


@app.get("/dm/ab")
def dm_ab(name: str = Query("Business"), tags: str = Query("weak_profile"), vertical: str = Query("default")) -> dict[str, Any]:
    tag_list = [x.strip() for x in tags.split(",") if x.strip()]
    return {"vertical": vertical, "name": name, "variants": dm_ab_variant(name, tag_list, vertical)}


@app.get("/search")
def search(
    query: str = Query(..., min_length=2),
    city: str = Query(..., min_length=2),
    country: str = Query(""),
    weakness: str = Query("all"),
    limit: int = Query(100, ge=10, le=500),
    vertical: str | None = Query(None),
    source: str = Query("overpass"),
    api_key: str | None = Query(None),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    owner = auth_api_key(api_key, x_api_key)
    job_id = str(uuid.uuid4())
    v = infer_vertical(query, vertical)
    src = source.strip().lower()
    if src not in {"overpass", "google_maps"}:
        raise HTTPException(status_code=400, detail="source doit etre overpass ou google_maps")
    if src == "google_maps" and not GOOGLE_MAPS_API_KEY:
        src = "overpass"
    with db_conn() as conn:
        conn.execute(
            "INSERT INTO jobs(job_id,owner_api_key,status,query,city,country,weakness,limit_n,vertical,source,created_utc) VALUES (?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?)",
            (job_id, owner, query, city, country, weakness, limit, v, src, now_utc()),
        )
    threading.Thread(target=run_job, args=(job_id, owner, query, city, country, weakness, limit, v, src), daemon=True).start()
    return {"ok": True, "job_id": job_id, "status": "queued", "vertical": v, "source": src}


@app.get("/jobs/{job_id}")
def job_status(job_id: str, api_key: str | None = Query(None), x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> dict[str, Any]:
    owner = auth_api_key(api_key, x_api_key)
    with db_conn() as conn:
        row = conn.execute(
            "SELECT job_id,status,query,city,country,weakness,limit_n,vertical,source,created_utc,started_utc,finished_utc,total_raw,total_filtered,csv_path,xlsx_path,error FROM jobs WHERE job_id=? AND owner_api_key=?",
            (job_id, owner),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="job_id introuvable")
    return dict(row)


@app.get("/jobs/{job_id}/leads")
def job_leads(
    job_id: str,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    priority: str | None = Query(None),
    status: str | None = Query(None),
    tag: str | None = Query(None),
    api_key: str | None = Query(None),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    owner = auth_api_key(api_key, x_api_key)
    with db_conn() as conn:
        job = conn.execute("SELECT status,csv_path,xlsx_path FROM jobs WHERE job_id=? AND owner_api_key=?", (job_id, owner)).fetchone()
        if not job:
            raise HTTPException(status_code=404, detail="job_id introuvable")
        if job["status"] != "done":
            return {"status": job["status"], "total": 0, "leads": []}

        clauses = ["job_id=?", "owner_api_key=?"]
        args: list[Any] = [job_id, owner]
        if priority:
            clauses.append("UPPER(priority)=?")
            args.append(priority.strip().upper())
        if status:
            clauses.append("LOWER(status)=?")
            args.append(status.strip().lower())
        if tag:
            clauses.append("LOWER(tags) LIKE ?")
            args.append(f"%{tag.strip().lower()}%")
        where = " AND ".join(clauses)

        total = int(conn.execute(f"SELECT COUNT(*) AS c FROM leads WHERE {where}", args).fetchone()["c"])
        rows = conn.execute(
            f"SELECT lead_id,name,phone,email,whatsapp_link,website,address,country,note,reviews,tags,score,priority,maps_link,source,Script_DM,status,last_update_utc,crm_note FROM leads WHERE {where} ORDER BY CASE priority WHEN 'HOT' THEN 3 WHEN 'WARM' THEN 2 ELSE 1 END DESC, score DESC LIMIT ? OFFSET ?",
            [*args, limit, offset],
        ).fetchall()

    return {"status": "done", "total": total, "offset": offset, "limit": limit, "leads": [dict(r) for r in rows], "csv_path": job["csv_path"], "xlsx_path": job["xlsx_path"]}


@app.get("/crm/update")
def crm_update(
    lead_id: str = Query(...),
    status: str = Query(..., pattern="^(new|contacted|replied|closed|ignored)$"),
    note: str = Query(""),
    api_key: str | None = Query(None),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    owner = auth_api_key(api_key, x_api_key)
    s = status.strip().lower()
    if s not in VALID_CRM_STATUSES:
        raise HTTPException(status_code=400, detail="status CRM invalide")
    updated = now_utc()
    with db_conn() as conn:
        conn.execute(
            "INSERT INTO crm(owner_api_key,lead_id,status,note,updated_utc) VALUES (?, ?, ?, ?, ?) ON CONFLICT(owner_api_key,lead_id) DO UPDATE SET status=excluded.status,note=excluded.note,updated_utc=excluded.updated_utc",
            (owner, lead_id, s, note, updated),
        )
        conn.execute("UPDATE leads SET status=?,crm_note=?,last_update_utc=? WHERE owner_api_key=? AND lead_id=?", (s, note, updated, owner, lead_id))
    return {"ok": True, "lead_id": lead_id, "status": s}


@app.get("/crm/summary")
def crm_summary(api_key: str | None = Query(None), x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> dict[str, Any]:
    owner = auth_api_key(api_key, x_api_key)
    counts = {"new": 0, "contacted": 0, "replied": 0, "closed": 0, "ignored": 0}
    total = 0
    with db_conn() as conn:
        rows = conn.execute("SELECT status,COUNT(*) AS c FROM crm WHERE owner_api_key=? GROUP BY status", (owner,)).fetchall()
    for r in rows:
        counts[str(r["status"])] = int(r["c"])
        total += int(r["c"])
    return {"total": total, "counts": counts}


@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    return """
<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>
<title>ProspectHunter V5.1 - C-Y ASS</title>
<style>body{font-family:Segoe UI,Arial,sans-serif;padding:16px;background:#0b1220;color:#e5e7eb}input,select,button{padding:8px;border-radius:8px;border:1px solid #334155;background:#0f172a;color:#e5e7eb}.row{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px}.mono{font-family:Consolas,monospace}.muted{color:#94a3b8}table{width:100%;border-collapse:collapse}th,td{padding:8px;border-bottom:1px solid #1f2937}</style>
</head><body>
<h2>ProspectHunter V5.1</h2><div class='muted'>Signature: C-Y ASS</div>
<div class='row'><input id='apikey' value='dev-key-change-me' placeholder='api_key' style='min-width:220px'><span class='muted'>Isolation multi-utilisateur via api_key</span></div>
<div class='row'><input id='query' value='restaurant'><input id='city' value='Cotonou'><input id='country' value='Benin'><select id='source'><option value='overpass'>overpass</option><option value='google_maps'>google_maps</option></select><select id='weakness'><option value='all'>all</option><option value='no_website'>no_website</option><option value='no_phone'>no_phone</option><option value='no_hours'>no_hours</option><option value='weak_profile'>weak_profile</option></select><input id='limit' type='number' value='120'><button onclick='launch()'>Lancer Job</button></div>
<div class='row'><span class='muted'>Job:</span><span id='jobid' class='mono'>-</span><span id='jobstate' class='muted'>Aucun</span></div>
<div class='row'><button onclick='refreshLeads()'>Rafraichir</button><select id='fPriority'><option value=''>Priority all</option><option>HOT</option><option>WARM</option><option>COLD</option></select><select id='fStatus'><option value=''>Status all</option><option>new</option><option>contacted</option><option>replied</option><option>closed</option><option>ignored</option></select><select id='fTag'><option value=''>Tag all</option><option>no_website</option><option>no_phone</option><option>no_hours</option><option>weak_profile</option></select></div>
<table><thead><tr><th>Name</th><th>Priority</th><th>Score</th><th>Status</th><th>Tags</th><th>Phone</th><th>Email</th><th>CRM</th></tr></thead><tbody id='tbody'><tr><td colspan='8' class='muted'>Aucun lead</td></tr></tbody></table>
<script>
let currentJob=null,timer=null; const esc=v=>(v||'').toString().replace(/[&<>\"]/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;'}[m]));
const auth=path=>`${path}${path.includes('?')?'&':'?'}api_key=${encodeURIComponent(document.getElementById('apikey').value||'')}`;
async function launch(){const q=query.value,cityv=city.value,countryv=country.value,src=source.value,w=weakness.value,l=limit.value;const r=await fetch(auth(`/search?query=${encodeURIComponent(q)}&city=${encodeURIComponent(cityv)}&country=${encodeURIComponent(countryv)}&source=${encodeURIComponent(src)}&weakness=${encodeURIComponent(w)}&limit=${encodeURIComponent(l)}`));const j=await r.json();if(!r.ok){alert(j.detail||'Erreur');return;}currentJob=j.job_id;jobid.textContent=currentJob;jobstate.textContent=`${j.status} (${j.source})`;poll();}
async function poll(){if(!currentJob)return;const r=await fetch(auth(`/jobs/${currentJob}`));const j=await r.json();if(!r.ok){jobstate.textContent=j.detail||'Erreur';return;}jobstate.textContent=`Etat: ${j.status}`;if(j.status==='done'){await refreshLeads();return;}if(j.status==='failed'){jobstate.textContent=`Erreur: ${j.error||'unknown'}`;return;}timer=setTimeout(poll,1500);}
async function refreshLeads(){if(!currentJob)return;const qs=new URLSearchParams({limit:'500',offset:'0'});if(fPriority.value)qs.set('priority',fPriority.value);if(fStatus.value)qs.set('status',fStatus.value);if(fTag.value)qs.set('tag',fTag.value);const r=await fetch(auth(`/jobs/${currentJob}/leads?${qs.toString()}`));const j=await r.json();if(!r.ok){alert(j.detail||'Erreur');return;}tbody.innerHTML=(j.leads||[]).map(l=>`<tr><td><b>${esc(l.name)}</b><div class='mono muted'>${esc(l.lead_id)}</div></td><td>${esc(l.priority)}</td><td>${esc(l.score)}</td><td>${esc(l.status)}</td><td class='mono'>${esc(l.tags)}</td><td class='mono'>${esc(l.phone||'-')}</td><td class='mono'>${esc(l.email||'-')}</td><td><button onclick="setStatus('${esc(l.lead_id)}','contacted')">contacted</button> <button onclick="setStatus('${esc(l.lead_id)}','closed')">closed</button></td></tr>`).join('')||"<tr><td colspan='8' class='muted'>Aucun lead</td></tr>";}
async function setStatus(leadId,status){const note=prompt('Note CRM (optionnel):','')||'';const qs=new URLSearchParams({lead_id:leadId,status:status,note:note});const r=await fetch(auth(`/crm/update?${qs.toString()}`));const j=await r.json();if(!j.ok){alert('Erreur CRM');return;}refreshLeads();}
</script></body></html>
"""
