import os
import re
import gzip
import json
import sqlite3
import tempfile
import requests
import asyncio
import ipaddress
import time as std_time
import threading
import logging
import secrets
import hashlib
from datetime import datetime, time, timedelta
from fastapi import FastAPI, HTTPException, Header, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.concurrency import run_in_threadpool
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager
from functools import lru_cache
import master_scraper
import scryfall_search

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

_thread_local = threading.local()

def get_scryfall_session() -> requests.Session:
    session = getattr(_thread_local, "scryfall_session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({
            "User-Agent": "CentMeta/1.0 (privacy@centmeta.it; https://centmeta.it)",
            "Accept": "application/json;q=0.9,*/*;q=0.8"
        })
        _thread_local.scryfall_session = session
    return session

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn

def get_health_db():
    conn = sqlite3.connect(HEALTH_DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn

def sanitize_commanders_in_db():
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("SELECT id_moxfield, comandante FROM decks WHERE comandante LIKE '% / %' AND comandante NOT LIKE '%//%'")
        rows = c.fetchall()
        updated_count = 0
        
        for row in rows:
            deck_id = row['id_moxfield']
            commander_name = row['comandante']
            new_name = master_scraper.normalize_partner_commander(commander_name)
            
            if new_name != commander_name:
                c.execute("UPDATE decks SET comandante = ? WHERE id_moxfield = ?", (new_name, deck_id))
                updated_count += 1
                
        if updated_count > 0:
            conn.commit()
            print(f"[DB] Pulizia Partner completata: uniformati {updated_count} mazzi.")
        else:
            print("[DB] Controllo Partner superato: nessun comandante da riordinare.")
    except Exception as e:
        print(f"[DB ERROR] Errore durante la pulizia dei comandanti: {e}")
    finally:
        conn.close()

def setup_database_indices():
    conn = get_db()
    c = conn.cursor()
    try:
        needs_analyze = master_scraper.ensure_covering_indices(c)

        has_stat1 = c.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='sqlite_stat1'"
        ).fetchone()
        if needs_analyze or not has_stat1:
            c.execute("ANALYZE")
            conn.commit()
            print("[DB] Indici covering e statistiche ANALYZE ottimizzati con successo.")
        else:
            conn.commit()
            print("[DB] Indici ottimizzati con successo.")
    except Exception as e:
        print(f"[DB ERROR] Creazione indici fallita: {e}")
    finally:
        conn.close()

_scryfall_lock = threading.RLock()
_scryfall_file_lock = threading.Lock()
_expansions_lock = threading.RLock()
_expansions_fetch_lock = threading.Lock()
_daily_cache_lock = threading.Lock()
_LAST_CACHE_DATE = datetime.now().strftime("%Y-%m-%d")
_scryfall_rate_lock = threading.Lock()
_scryfall_last_request_ts = 0.0
_SCRYFALL_MISSING_CACHE = set()

def _wait_scryfall_rate_limit(min_interval: float = 0.55):
    """Garantisce almeno min_interval secondi tra richieste consecutive verso Scryfall (limite 2 req/s su /cards/*)."""
    global _scryfall_last_request_ts
    with _scryfall_rate_lock:
        now = std_time.monotonic()
        elapsed = now - _scryfall_last_request_ts
        if elapsed < min_interval:
            std_time.sleep(min_interval - elapsed)
        _scryfall_last_request_ts = std_time.monotonic()

def clear_all_caches():
    global _SCRYFALL_CACHE_MEMORY
    global _EXPANSIONS_CACHE_MEMORY, _EXPANSIONS_CACHE_DATE
    global _LAST_CACHE_DATE
    with _scryfall_lock:
        _SCRYFALL_CACHE_MEMORY = None
        _SCRYFALL_MISSING_CACHE.clear()
    with _expansions_lock:
        _EXPANSIONS_CACHE_MEMORY = None
        _EXPANSIONS_CACHE_DATE = ""
    get_global_commanders_validity.cache_clear()
    get_global_cards_stats.cache_clear()
    get_valid_commanders_set.cache_clear()
    get_decks_stats_cached.cache_clear()
    get_card_details_cached.cache_clear()
    get_commander_decks_cached.cache_clear()
    get_dashboard_data_cached.cache_clear()
    get_metagame_history_cached.cache_clear()
    _LAST_CACHE_DATE = datetime.now().strftime("%Y-%m-%d")
    print("[CACHE] Tutte le cache in memoria sono state svuotate.")

def ensure_daily_caches_fresh():
    """Invalida le cache @lru_cache al cambio giorno anche se lo scraper notturno non è stato eseguito."""
    global _LAST_CACHE_DATE
    today_str = datetime.now().strftime("%Y-%m-%d")
    if _LAST_CACHE_DATE == today_str:
        return
    with _daily_cache_lock:
        if _LAST_CACHE_DATE == today_str:
            return
        clear_all_caches()
        _LAST_CACHE_DATE = today_str

def setup_health_db(init_main_blacklist: bool = True):
    if init_main_blacklist:
        conn = get_db()
        try:
            c = conn.cursor()
            c.execute('''
                CREATE TABLE IF NOT EXISTS blacklist (
                    id_moxfield TEXT PRIMARY KEY,
                    data_rimozione TEXT
                )
            ''')
            conn.commit()
        finally:
            conn.close()

    h_conn = get_health_db()
    try:
        h_conn.execute("PRAGMA journal_mode=WAL;")
        h_c = h_conn.cursor()
        h_c.execute('''
            CREATE TABLE IF NOT EXISTS health_logs (
                timestamp TEXT,
                service TEXT,
                status TEXT
            )
        ''')
        h_conn.commit()
    finally:
        h_conn.close()

def _record_service_ping(c, now: str, service: str, request_fn, redact_secret: Optional[str] = None):
    try:
        res = request_fn()
        if res.status_code == 200:
            status = "operational"
        elif res.status_code == 429:
            status = "rate_limited"
        else:
            status = "error"
    except Exception as req_err:
        err_msg = str(req_err)
        if redact_secret and redact_secret in err_msg:
            err_msg = err_msg.replace(redact_secret, "[REDACTED_UA]")
        logging.warning(f"[HEALTH WARNING] Rilevata anomalia connessione {service}: {err_msg}")
        status = "down"

    c.execute("INSERT INTO health_logs (timestamp, service, status) VALUES (?, ?, ?)", (now, service, status))
    c.execute("""
        DELETE FROM health_logs 
        WHERE service=? AND rowid NOT IN (
            SELECT rowid FROM health_logs 
            WHERE service=? ORDER BY timestamp DESC LIMIT 48
        )
    """, (service, service))

def _get_last_ping_seconds_ago(cursor, service: str) -> Optional[float]:
    row = cursor.execute(
        "SELECT timestamp FROM health_logs WHERE service=? ORDER BY timestamp DESC LIMIT 1",
        (service,)
    ).fetchone()
    if not row or not row["timestamp"]:
        return None
    try:
        last_dt = datetime.fromisoformat(row["timestamp"])
        return (datetime.now() - last_dt).total_seconds()
    except Exception:
        return None

def _perform_health_pings():
    setup_health_db(init_main_blacklist=False)
    now = datetime.now().isoformat()
    conn = get_health_db()
    try:
        c = conn.cursor()
        
        moxfield_ua = os.getenv("MOXFIELD_UA")
        if moxfield_ua:
            moxfield_ua = moxfield_ua.strip()
        
        # 1. Ping Scryfall (salta se già eseguito negli ultimi 45 minuti da un altro worker)
        scry_sec = _get_last_ping_seconds_ago(c, "scryfall")
        if scry_sec is None or scry_sec >= 2700:
            def _ping_scryfall():
                _wait_scryfall_rate_limit()
                return get_scryfall_session().get("https://api.scryfall.com/cards/named?exact=Sol Ring", timeout=10)

            _record_service_ping(
                c,
                now,
                "scryfall",
                _ping_scryfall
            )
        
        # 2. Ping Moxfield (rispetta rigidamente il rate limiter di 1.2s condiviso con lo scraper)
        if not moxfield_ua:
            logging.warning("[HEALTH WARNING] Variabile MOXFIELD_UA non impostata. Salto il ping a Moxfield.")
        else:
            mox_sec = _get_last_ping_seconds_ago(c, "moxfield")
            if mox_sec is None or mox_sec >= 2700:
                _record_service_ping(
                    c,
                    now,
                    "moxfield",
                    lambda: master_scraper.rate_limited_get(
                        "https://api.moxfield.com/v2/decks/search?pageNumber=1&pageSize=1&fmt=centurion",
                        headers={"User-Agent": moxfield_ua},
                        timeout=10
                    ),
                    redact_secret=moxfield_ua
                )
        
        conn.commit()
    finally:
        conn.close()
    print(f"[HEALTH] Controllo stato completato alle {now}")

async def background_health_pinger():
    while True:
        try:
            await run_in_threadpool(_perform_health_pings)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logging.error(f"[HEALTH ERROR] Errore critico nel loop di ping: {e}")
        await asyncio.sleep(3600)

async def warmup_caches(periodi: Optional[List[str]] = None):
    """Esegue il pre-warming della cache. Se non specificato, scalda tutti i periodi."""
    if periodi is None:
        periodi = [
            "Ultimi 7 giorni",
            "Ultimi 30 giorni",
            "Ultimi 3 mesi",
            "Ultimi 6 mesi",
            "Ultimo anno",
            "Tutti i tempi"
        ]
    print(f"[WARMUP] Avvio pre-warming per {len(periodi)} periodo/i: {', '.join(periodi)}...")
    await run_in_threadpool(get_global_commanders_validity)
    await run_in_threadpool(get_global_cards_stats)
    for periodo in periodi:
        print(f"[WARMUP] Costruzione cache per: {periodo}...")
        await run_in_threadpool(get_decks_stats_cached, periodo, None)
        await run_in_threadpool(get_dashboard_data_cached, periodo)
    print("[WARMUP] Pre-warming completato. Sistema pronto.")

async def background_db_updater():
    while True:
        try:
            now = datetime.now()
            target = datetime.combine(now.date(), time(23, 0))
            if now >= target:
                target = target + timedelta(days=1)
            
            seconds_to_wait = (target - now).total_seconds()
            print(f"[CRON] Prossimo aggiornamento alle 23:00 (tra {int(seconds_to_wait / 3600)} ore).")
            
            await asyncio.sleep(seconds_to_wait)
            print("[CRON] Avvio aggiornamento giornaliero del database...")

            scraped = await run_in_threadpool(master_scraper.run_scraper)
            if scraped:
                clear_all_caches()
                # Alle 23:00 scalda tutti i periodi
                await warmup_caches()
            else:
                print("[CRON] Scraper saltato o già in corso su un altro processo: cache conservate.")
            
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[CRON ERROR] Errore aggiornamento: {e}")
            await asyncio.sleep(3600)

CARTELLA_SCRIPT = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(CARTELLA_SCRIPT, "centurion.db")
HEALTH_DB_PATH = os.path.join(CARTELLA_SCRIPT, "health.db")
CACHE_FILE = os.path.join(CARTELLA_SCRIPT, "scryfall_cache.json")
SETS_CACHE_FILE = os.path.join(CARTELLA_SCRIPT, "scryfall_sets_cache.json")
_EXPANSIONS_CACHE_MEMORY = None
_EXPANSIONS_CACHE_DATE = ""

_SCRYFALL_CACHE_MEMORY = None

def get_db_mtime():
    mtimes = []
    for path in (DB_PATH, f"{DB_PATH}-wal"):
        try:
            if os.path.exists(path) and (path == DB_PATH or os.path.getsize(path) > 0):
                mtimes.append(os.path.getmtime(path))
        except OSError:
            pass
    return max(mtimes) if mtimes else None

@asynccontextmanager
async def lifespan(app: FastAPI):
    orig_mtime = get_db_mtime()

    # Abilita il WAL mode prima di qualsiasi altra operazione
    conn = get_db()
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
    finally:
        conn.close()

    setup_health_db(init_main_blacklist=True)
    sanitize_commanders_in_db()
    setup_database_indices()

    if orig_mtime is not None:
        try:
            conn = get_db()
            try:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            finally:
                conn.close()
            os.utime(DB_PATH, (orig_mtime, orig_mtime))
        except Exception:
            pass
    
    updater_task = asyncio.create_task(background_db_updater())
    health_task = asyncio.create_task(background_health_pinger())
    
    # Pre-caching in background all'avvio solo per il periodo di default (30 giorni)
    async def deferred_warmup():
        await asyncio.sleep(2)  # Permette a Uvicorn di aprire la porta e accettare connessioni subito
        await warmup_caches(["Ultimi 30 giorni"])

    warmup_task = asyncio.create_task(deferred_warmup())
    artist_backfill_task = asyncio.create_task(background_scryfall_artist_backfill())

    yield  # <-- Il server apre la porta ORA ed è subito accessibile

    if warmup_task:
        warmup_task.cancel()
    artist_backfill_task.cancel()
    updater_task.cancel()
    health_task.cancel()

app = FastAPI(title="CentMeta API", version="4.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://centmeta.it",
        "https://www.centmeta.it",
        "http://localhost:3000"
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_time_conditions_both(filtro_tempo: str):
    if filtro_tempo == "Ultimi 7 giorni":
        return "data_aggiornamento >= strftime('%Y-%m-%dT%H:%M:%S', 'now', '-7 days')", "data_aggiornamento >= strftime('%Y-%m-%dT%H:%M:%S', 'now', '-14 days') AND data_aggiornamento < strftime('%Y-%m-%dT%H:%M:%S', 'now', '-7 days')"
    elif filtro_tempo == "Ultimi 30 giorni":
        return "data_aggiornamento >= strftime('%Y-%m-%dT%H:%M:%S', 'now', '-30 days')", "data_aggiornamento >= strftime('%Y-%m-%dT%H:%M:%S', 'now', '-60 days') AND data_aggiornamento < strftime('%Y-%m-%dT%H:%M:%S', 'now', '-30 days')"
    elif filtro_tempo == "Ultimi 3 mesi":
        return "data_aggiornamento >= strftime('%Y-%m-%dT%H:%M:%S', 'now', '-3 months')", "data_aggiornamento >= strftime('%Y-%m-%dT%H:%M:%S', 'now', '-6 months') AND data_aggiornamento < strftime('%Y-%m-%dT%H:%M:%S', 'now', '-3 months')"
    elif filtro_tempo == "Ultimi 6 mesi":
        return "data_aggiornamento >= strftime('%Y-%m-%dT%H:%M:%S', 'now', '-6 months')", "data_aggiornamento >= strftime('%Y-%m-%dT%H:%M:%S', 'now', '-12 months') AND data_aggiornamento < strftime('%Y-%m-%dT%H:%M:%S', 'now', '-6 months')"
    elif filtro_tempo == "Ultimo anno":
        return "data_aggiornamento >= strftime('%Y-%m-%dT%H:%M:%S', 'now', '-1 year')", "data_aggiornamento >= strftime('%Y-%m-%dT%H:%M:%S', 'now', '-2 years') AND data_aggiornamento < strftime('%Y-%m-%dT%H:%M:%S', 'now', '-1 year')"
    return None, None

def get_time_condition(filtro_tempo: str):
    curr, _ = get_time_conditions_both(filtro_tempo)
    return curr

def load_scryfall_cache():
    global _SCRYFALL_CACHE_MEMORY
    with _scryfall_lock:
        if _SCRYFALL_CACHE_MEMORY is not None:
            return _SCRYFALL_CACHE_MEMORY.copy()
        try:
            if os.path.exists(CACHE_FILE):
                with open(CACHE_FILE, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                    _SCRYFALL_CACHE_MEMORY = loaded if isinstance(loaded, dict) else {}
            else:
                _SCRYFALL_CACHE_MEMORY = {}
        except Exception as e:
            ts = int(std_time.time())
            corrupt_name = f"{CACHE_FILE}.corrupt-{ts}"
            logging.warning(f"Cache Scryfall corrotta ({e}). Rinomino in {corrupt_name} e riparto pulito.")
            try:
                os.replace(CACHE_FILE, corrupt_name)
            except Exception:
                pass
            _SCRYFALL_CACHE_MEMORY = {}
        return _SCRYFALL_CACHE_MEMORY.copy()

def _write_scryfall_cache_to_disk():
    with _scryfall_file_lock:
        with _scryfall_lock:
            snapshot = (_SCRYFALL_CACHE_MEMORY or {}).copy()
        tmp_file = f"{CACHE_FILE}.{os.getpid()}.{threading.get_ident()}.tmp"
        try:
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(snapshot, f)
            os.replace(tmp_file, CACHE_FILE)
        except Exception as e:
            logging.warning(f"Errore durante il salvataggio della cache Scryfall: {e}")
            try:
                os.remove(tmp_file)
            except OSError:
                pass

def save_scryfall_cache(cache):
    global _SCRYFALL_CACHE_MEMORY
    with _scryfall_lock:
        _SCRYFALL_CACHE_MEMORY = cache.copy()
    _write_scryfall_cache_to_disk()

def _commit_scryfall_cache_updates(new_entries: dict, delete_keys: Optional[List[str]] = None) -> dict:
    global _SCRYFALL_CACHE_MEMORY
    with _scryfall_lock:
        if _SCRYFALL_CACHE_MEMORY is None:
            _SCRYFALL_CACHE_MEMORY = {}
        updated = _SCRYFALL_CACHE_MEMORY.copy()
        if delete_keys:
            for k in delete_keys:
                updated.pop(k, None)
        if new_entries:
            updated.update(new_entries)
        _SCRYFALL_CACHE_MEMORY = updated
        snapshot = updated.copy()
    _write_scryfall_cache_to_disk()
    return snapshot

def _parse_scryfall_card_object(card: dict) -> dict:
    name = card.get("name", "")
    type_line = card.get("type_line", "")
    if not type_line and "card_faces" in card and len(card["card_faces"]) > 0:
        type_line = card["card_faces"][0].get("type_line", "Altro")

    images = []
    art_crops = []
    oracles = []

    if "card_faces" in card and len(card["card_faces"]) > 1 and "image_uris" in card["card_faces"][0]:
        for face in card["card_faces"]:
            if "image_uris" in face:
                images.append(face["image_uris"].get("normal"))
                art_crops.append(face["image_uris"].get("art_crop"))
    else:
        if "image_uris" in card:
            images.append(card["image_uris"].get("normal"))
            art_crops.append(card["image_uris"].get("art_crop"))

    if "card_faces" in card:
        for face in card["card_faces"]:
            if "oracle_text" in face:
                face_name = face.get("name", "")
                face_text = face.get("oracle_text", "")
                oracles.append(f"✦ {face_name} ✦\n{face_text}")
    elif card.get("oracle_text"):
        oracles.append(card.get("oracle_text"))

    faces = card.get("card_faces") or []
    if "colors" in card and card.get("colors") is not None:
        colors = list(card.get("colors") or [])
    else:
        seen_cols = []
        for face in faces:
            for col in (face.get("colors") or []):
                if col not in seen_cols:
                    seen_cols.append(col)
        colors = seen_cols

    mana_cost = card.get("mana_cost") or " // ".join(
        f.get("mana_cost", "") for f in faces if f.get("mana_cost")
    )
    first_face = faces[0] if faces else {}
    artist = (card.get("artist") or first_face.get("artist") or "").strip()

    return {
        "full_name": name,
        "images": images,
        "art_crops": art_crops,
        "oracles": oracles,
        "type": type_line,
        "color_identity": card.get("color_identity", []),
        "colors": colors,
        "cmc": card.get("cmc", 0.0),
        "mana_cost": mana_cost,
        "keywords": card.get("keywords", []),
        "power": card.get("power") if card.get("power") is not None else first_face.get("power"),
        "toughness": card.get("toughness") if card.get("toughness") is not None else first_face.get("toughness"),
        "loyalty": card.get("loyalty") if card.get("loyalty") is not None else first_face.get("loyalty"),
        "rarity": card.get("rarity"),
        "artist": artist
    }

def _is_valid_cached_card(entry: Optional[dict]) -> bool:
    if not entry or not entry.get("images"):
        return False
    if entry.get("type") in ("Card", " Card // Card"):
        return False
    return True

_SCRYFALL_UUID_RE = re.compile(
    r"/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.[a-z]+",
    re.IGNORECASE,
)

def _extract_scryfall_id_from_entry(entry: dict) -> Optional[str]:
    for url in (entry.get("art_crops") or []) + (entry.get("images") or []):
        if isinstance(url, str):
            m = _SCRYFALL_UUID_RE.search(url)
            if m:
                return m.group(1).lower()
    return None

def collect_missing_artist_targets() -> List[tuple[dict, List[str]]]:
    cache = load_scryfall_cache()
    grouped: Dict[tuple[str, str], tuple[dict, List[str]]] = {}
    for k, entry in cache.items():
        if not _is_valid_cached_card(entry) or (entry.get("artist") or "").strip():
            continue
        uuid_val = _extract_scryfall_id_from_entry(entry)
        if uuid_val:
            g_key = ("id", uuid_val)
            ident = {"id": uuid_val}
        else:
            name_val = (entry.get("full_name") or k).strip()
            if not name_val:
                continue
            g_key = ("name", name_val.lower())
            ident = {"name": name_val}
        if g_key not in grouped:
            grouped[g_key] = (ident, [k])
        else:
            _, keys_list = grouped[g_key]
            keys_list.append(k)

    return list(grouped.values())

SCRYFALL_BULK_BACKFILL_THRESHOLD = 75

def _extract_artist_from_scryfall_card(card: dict) -> str:
    faces_list = card.get("card_faces") or []
    first_face_matched = faces_list[0] if faces_list else {}
    return (card.get("artist") or first_face_matched.get("artist") or "").strip()

def backfill_scryfall_artists_batch(batch: List[tuple[dict, List[str]]]) -> tuple[Dict[str, dict], bool]:
    if not batch:
        return {}, False
    session = get_scryfall_session()
    identifiers = [ident for ident, _ in batch]
    try:
        _wait_scryfall_rate_limit()
        res = session.post(
            "https://api.scryfall.com/cards/collection",
            json={"identifiers": identifiers},
            timeout=15,
        )
        if res.status_code in (429, 403):
            logging.warning(f"[SCRYFALL BACKFILL] Ricevuto HTTP {res.status_code}; interrompo il backfill artisti.")
            return {}, True
        if res.status_code != 200:
            return {}, False
        body = res.json()
    except Exception as e:
        logging.warning(f"[SCRYFALL BACKFILL] Errore batch Scryfall: {e}")
        return {}, False

    by_id: Dict[str, dict] = {}
    by_name: Dict[str, dict] = {}
    for card in body.get("data", []):
        cid = (card.get("id") or "").strip().lower()
        if cid:
            by_id[cid] = card
        cname = (card.get("name") or "").strip().lower()
        if cname:
            by_name[cname] = card
            if " // " in cname:
                by_name.setdefault(cname.split(" // ")[0].strip(), card)

    cache_snapshot = load_scryfall_cache()
    updates: Dict[str, dict] = {}
    for ident, keys_list in batch:
        matched = None
        if "id" in ident:
            matched = by_id.get(ident["id"].lower())
        if matched is None and "name" in ident:
            matched = by_name.get(ident["name"].lower())
        if not matched:
            continue
        artist = _extract_artist_from_scryfall_card(matched)
        if not artist:
            continue
        for ck in keys_list:
            existing = cache_snapshot.get(ck)
            if isinstance(existing, dict):
                updates[ck] = {**existing, "artist": artist}
    return updates, False

def backfill_scryfall_artists_from_bulk(targets: List[tuple[dict, List[str]]]) -> Dict[str, dict]:
    if not targets:
        return {}

    needed_ids: Dict[str, List[str]] = {}
    needed_names: Dict[str, List[str]] = {}
    for ident, keys_list in targets:
        if "id" in ident and ident["id"]:
            needed_ids[ident["id"].lower()] = keys_list
        elif "name" in ident and ident["name"]:
            needed_names[ident["name"].lower()] = keys_list

    if not needed_ids and not needed_names:
        return {}

    session = get_scryfall_session()
    try:
        _wait_scryfall_rate_limit()
        meta_res = session.get("https://api.scryfall.com/bulk-data/default-cards", timeout=15)
        if meta_res.status_code != 200:
            logging.warning(f"[SCRYFALL BULK] HTTP {meta_res.status_code} su /bulk-data/default-cards")
            return {}
        meta = meta_res.json()
        download_url = meta.get("jsonl_download_uri") or meta.get("download_uri")
        if not download_url:
            logging.warning("[SCRYFALL BULK] URL di download non trovato nei metadati bulk-data.")
            return {}

        matched_artists: Dict[str, str] = {}

        def _process_card_obj(card: dict):
            artist = _extract_artist_from_scryfall_card(card)
            if not artist:
                return
            cid = (card.get("id") or "").strip().lower()
            if cid and cid in needed_ids:
                for ck in needed_ids.pop(cid):
                    matched_artists[ck] = artist
            if needed_names:
                cname = (card.get("name") or "").strip().lower()
                if cname in needed_names:
                    for ck in needed_names.pop(cname):
                        matched_artists.setdefault(ck, artist)
                elif " // " in cname:
                    front = cname.split(" // ")[0].strip()
                    if front in needed_names:
                        for ck in needed_names.pop(front):
                            matched_artists.setdefault(ck, artist)

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = os.path.join(tmpdir, "default_cards.dat")
            with session.get(download_url, stream=True, timeout=120) as dl_res:
                if dl_res.status_code != 200:
                    logging.warning(f"[SCRYFALL BULK] HTTP {dl_res.status_code} durante il download bulk.")
                    return {}
                with open(tmp_path, "wb") as f_out:
                    for chunk in dl_res.iter_content(chunk_size=65536):
                        if chunk:
                            f_out.write(chunk)

            with open(tmp_path, "rb") as f_head:
                magic = f_head.read(2)
            is_gzip = download_url.lower().endswith(".gz") or magic == b"\x1f\x8b"
            opener = gzip.open if is_gzip else open

            with opener(tmp_path, "rt", encoding="utf-8") as f_in:
                first_char = ""
                while True:
                    ch = f_in.read(1)
                    if not ch:
                        break
                    if not ch.isspace():
                        first_char = ch
                        break
                f_in.seek(0)

                if first_char == "[":
                    for card in json.load(f_in):
                        if isinstance(card, dict):
                            _process_card_obj(card)
                            if not needed_ids and not needed_names:
                                break
                else:
                    for line in f_in:
                        s_line = line.strip()
                        if not s_line:
                            continue
                        card = json.loads(s_line)
                        if isinstance(card, dict):
                            _process_card_obj(card)
                            if not needed_ids and not needed_names:
                                break
    except Exception as e:
        logging.warning(f"[SCRYFALL BULK] Errore durante il backfill da Bulk Data: {e}")
        return {}

    if not matched_artists:
        return {}

    cache_snapshot = load_scryfall_cache()
    updates: Dict[str, dict] = {}
    for ck, artist in matched_artists.items():
        existing = cache_snapshot.get(ck)
        if isinstance(existing, dict):
            updates[ck] = {**existing, "artist": artist}
    return updates

def backfill_scryfall_artists() -> int:
    targets = collect_missing_artist_targets()
    if not targets:
        return 0
    total_updated = 0
    if len(targets) > SCRYFALL_BULK_BACKFILL_THRESHOLD:
        bulk_updates = backfill_scryfall_artists_from_bulk(targets)
        if bulk_updates:
            _commit_scryfall_cache_updates(bulk_updates)
            total_updated += len(bulk_updates)
            targets = collect_missing_artist_targets()
        else:
            return total_updated

    if targets and len(targets) <= SCRYFALL_BULK_BACKFILL_THRESHOLD:
        updates, _ = backfill_scryfall_artists_batch(targets)
        if updates:
            _commit_scryfall_cache_updates(updates)
            total_updated += len(updates)

    if total_updated > 0:
        clear_all_caches()
    return total_updated

async def background_scryfall_artist_backfill():
    try:
        await asyncio.sleep(15)
        targets = await run_in_threadpool(collect_missing_artist_targets)
        if not targets:
            return
        print(f"[SCRYFALL BACKFILL] Avvio recupero artisti in background per {len(targets)} carte in cache...")
        total_updated = 0
        if len(targets) > SCRYFALL_BULK_BACKFILL_THRESHOLD:
            bulk_updates = await run_in_threadpool(backfill_scryfall_artists_from_bulk, targets)
            if bulk_updates:
                await run_in_threadpool(_commit_scryfall_cache_updates, bulk_updates)
                total_updated += len(bulk_updates)
                targets = await run_in_threadpool(collect_missing_artist_targets)
            else:
                targets = []

        if targets and len(targets) <= SCRYFALL_BULK_BACKFILL_THRESHOLD:
            updates, _ = await run_in_threadpool(backfill_scryfall_artists_batch, targets)
            if updates:
                await run_in_threadpool(_commit_scryfall_cache_updates, updates)
                total_updated += len(updates)

        if total_updated > 0:
            await run_in_threadpool(clear_all_caches)
        print(f"[SCRYFALL BACKFILL] Completato: aggiornati {total_updated} record nella cache.")
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logging.warning(f"[SCRYFALL BACKFILL] Errore durante il recupero artisti: {e}")

def fetch_scryfall_data(card_names):
    results = load_scryfall_cache()
    mancanti = []
    actual_names = set()
    
    for n in card_names:
        if not n:
            continue
        if " / " in n and " // " not in n:
            for part in n.split(" / "):
                actual_names.add(part.strip())
        else:
            actual_names.add(n.strip())

    cache_updated_locally = False
    bad_keys_to_delete: List[str] = []
    new_entries: Dict[str, dict] = {}
    with _scryfall_lock:
        # Rimuove eventuali voci corrotte "Card" (Jumpstart theme cards)
        for bad_key in [k for k, v in results.items() if v.get("type") in ("Card", " Card // Card")]:
            del results[bad_key]
            bad_keys_to_delete.append(bad_key)
            cache_updated_locally = True

        for n in actual_names:
            key = n.lower()
            if key in _SCRYFALL_MISSING_CACHE:
                continue
            if _is_valid_cached_card(results.get(key)):
                continue
            # Supporto carte ribilanciate Arena (es. "A-Unholy Heat" -> "Unholy Heat")
            if key.startswith("a-") and _is_valid_cached_card(results.get(key[2:])):
                results[key] = results[key[2:]]
                new_entries[key] = results[key[2:]]
                cache_updated_locally = True
                continue
            mancanti.append(n)

    any_updated = cache_updated_locally

    if mancanti:
        session = get_scryfall_session()
        weird_sets = {"sta", "otp", "wot", "mul", "brr", "sld", "mps", "mp2", "exp", "mb1", "plist", "h1r", "slx"}

        for i in range(0, len(mancanti), 75):
            chunk = mancanti[i:i+75]
            identifiers = [{"name": n} for n in chunk]
            
            try:
                _wait_scryfall_rate_limit()
                res = session.post(
                    "https://api.scryfall.com/cards/collection", 
                    json={"identifiers": identifiers}, 
                    timeout=15
                )

                if res.status_code in (429, 403):
                    logging.warning(f"[SCRYFALL] Ricevuto HTTP {res.status_code} su /cards/collection; interrompo questo batch nel rispetto del rate limit.")
                    break
                if res.status_code == 200:
                    body = res.json()
                    
                    for card in body.get("data", []):
                        name = card.get("name", "")
                        faces_names = [f.strip() for f in name.split(" // ")] if " // " in name else []

                        # Rileva carte reversibili (es. Secret Lair con stesso nome su ambo i lati)
                        is_reversible_override = (
                            card.get("layout") == "reversible_card"
                            or (len(faces_names) > 1 and faces_names[0].lower() == faces_names[1].lower())
                        )
                        is_showcase_or_weird = (
                            card.get("set") in weird_sets
                            or card.get("frame") == "showcase"
                            or card.get("type_line") in ("Card", "Card // Card")
                        )
                        
                        if is_reversible_override or is_showcase_or_weird:
                            _wait_scryfall_rate_limit()
                            query_name = faces_names[0] if is_reversible_override and faces_names else name
                            if is_reversible_override:
                                q_str = f'!"{query_name}" -is:dfc -is:split -is:adventure -is:extra -is:promo -frame:showcase -layout:art_series -layout:token -type:card'
                            else:
                                q_str = f'!"{query_name}" -is:extra -is:promo -frame:showcase -layout:art_series -layout:token -type:card'
                            
                            try:
                                search_res = session.get('https://api.scryfall.com/cards/search', params={"q": q_str}, timeout=5)
                                if search_res.status_code == 200:
                                    normal_data = search_res.json().get("data", [])
                                    if normal_data:
                                        card = normal_data[0] 
                                        name = card.get("name", "")
                                        faces_names = [f.strip() for f in name.split(" // ")] if " // " in name else []
                            except Exception as e:
                                logging.warning(f"Errore ricerca alternativa Scryfall per {query_name}: {e}")

                        card_data = _parse_scryfall_card_object(card)
                        if not _is_valid_cached_card(card_data):
                            with _scryfall_lock:
                                _SCRYFALL_MISSING_CACHE.add(name.lower())
                            continue

                        with _scryfall_lock:
                            results[name.lower()] = card_data
                            new_entries[name.lower()] = card_data
                            if " // " in name and len(faces_names) > 1:
                                f0 = faces_names[0].strip().lower()
                                f1 = faces_names[1].strip().lower()
                                if not _is_valid_cached_card(results.get(f0)) or " // " in results[f0].get("full_name", " // "):
                                    results[f0] = card_data
                                    new_entries[f0] = card_data
                                if not _is_valid_cached_card(results.get(f1)) or " // " in results[f1].get("full_name", " // "):
                                    results[f1] = card_data
                                    new_entries[f1] = card_data

                            # Allinea eventuali alias richiesti nel chunk (es. "Waste Land" -> "Wasteland")
                            norm_target = name.lower().replace(" ", "").replace("-", "")
                            for req_n in chunk:
                                req_k = req_n.lower()
                                if req_k not in results and req_k.replace(" ", "").replace("-", "") == norm_target:
                                    results[req_k] = card_data
                                    new_entries[req_k] = card_data

                        any_updated = True

                    for nf in body.get("not_found", []):
                        nf_name = nf.get("name")
                        if not nf_name:
                            continue
                        nf_key = nf_name.lower()

                        # Fallback per carte Arena "A-..."
                        if nf_key.startswith("a-") and _is_valid_cached_card(results.get(nf_key[2:])):
                            with _scryfall_lock:
                                results[nf_key] = results[nf_key[2:]]
                                new_entries[nf_key] = results[nf_key[2:]]
                            any_updated = True
                            continue

                        # Fallback con /cards/named per carte Split/DFC non risolte da /cards/collection (es. "Armed // Dangerous")
                        resolved_via_named = False
                        if " // " in nf_name:
                            try:
                                _wait_scryfall_rate_limit()
                                named_res = session.get(
                                    "https://api.scryfall.com/cards/named",
                                    params={"exact": nf_name},
                                    timeout=10
                                )
                                if named_res.status_code == 200:
                                    named_card = named_res.json()
                                    card_data = _parse_scryfall_card_object(named_card)
                                    if _is_valid_cached_card(card_data):
                                        c_name = named_card.get("name", nf_name)
                                        faces = [f.strip() for f in c_name.split(" // ")] if " // " in c_name else []
                                        with _scryfall_lock:
                                            results[nf_key] = card_data
                                            results[c_name.lower()] = card_data
                                            new_entries[nf_key] = card_data
                                            new_entries[c_name.lower()] = card_data
                                            for f_part in faces:
                                                fk = f_part.lower()
                                                if not _is_valid_cached_card(results.get(fk)) or " // " in results[fk].get("full_name", " // "):
                                                    results[fk] = card_data
                                                    new_entries[fk] = card_data
                                        any_updated = True
                                        resolved_via_named = True
                            except Exception as e:
                                logging.warning(f"Errore fallback Scryfall named per {nf_name}: {e}")

                        if not resolved_via_named:
                            with _scryfall_lock:
                                _SCRYFALL_MISSING_CACHE.add(nf_key)
                                
            except Exception as e:
                logging.warning(f"Errore API Scryfall collection: {e}")

    if any_updated:
        results = _commit_scryfall_cache_updates(new_entries, bad_keys_to_delete)
                    
    return results

def lookup_card_info(scryfall_cache: dict, card_name: str) -> dict:
    if not card_name:
        return {}
    key = card_name.strip().lower()
    info = scryfall_cache.get(key)
    if _is_valid_cached_card(info):
        return info
    if " // " in key:
        front = key.split(" // ")[0].strip()
        info = scryfall_cache.get(front)
        if _is_valid_cached_card(info):
            return info
    if key.startswith("a-"):
        info = scryfall_cache.get(key[2:])
        if _is_valid_cached_card(info):
            return info
    return info or {}

def extract_commander_metadata(name: str, scryfall_cache: dict):
    """
    Estrae e valida i metadati di un comandante (Singolo, Bifronte DFC o Partner).
    Restituisce (is_valid, images, art_crops, oracles, color_identity_list).
    """
    if not name or not name.strip():
        return False, [], [], [], []

    if " / " in name and " // " not in name:
        parts = [p.strip() for p in name.split(" / ") if p.strip()]
    else:
        parts = [name.strip()]

    is_valid = True
    images = []
    art_crops = []
    oracles = []
    seen_images = set()
    seen_art_crops = set()
    cmd_color_identity = set()

    for part in parts:
        clean_key = part.lower()
        if clean_key == "sconosciuto":
            is_valid = False
            break

        info = lookup_card_info(scryfall_cache, part)
        if not info and " // " in part:
            for face_part in part.split(" // "):
                info = lookup_card_info(scryfall_cache, face_part.strip())
                if info:
                    break

        if info:
            t_line = info.get("type", "").lower()
            if t_line and "legendary" not in t_line and "planeswalker" not in t_line:
                is_valid = False
                break

            for img in info.get("images", []):
                if img and img not in seen_images:
                    seen_images.add(img)
                    images.append(img)
            for ac in info.get("art_crops", []):
                if ac and ac not in seen_art_crops:
                    seen_art_crops.add(ac)
                    art_crops.append(ac)
            for oracle in info.get("oracles", []):
                if oracle and oracle not in oracles:
                    oracles.append(oracle)
            for color in info.get("color_identity", []):
                cmd_color_identity.add(color)

    return (is_valid and bool(parts)), images, art_crops, oracles, list(cmd_color_identity)

def extract_commander_search_fields(name: str, scryfall_cache: dict) -> dict:
    """Restituisce metadati aggiuntivi per ricerca Scryfall sui comandanti (type, cmc, keywords)."""
    if not name or not name.strip():
        return {"type": "", "cmc": 0.0, "keywords": [], "mana_cost": "", "artist": ""}
    if " / " in name and " // " not in name:
        parts = [p.strip() for p in name.split(" / ") if p.strip()]
    else:
        parts = [name.strip()]

    types = []
    cmcs = []
    keywords = []
    mana_costs = []
    artists = []
    for part in parts:
        info = lookup_card_info(scryfall_cache, part)
        if not info and " // " in part:
            for face_part in part.split(" // "):
                info = lookup_card_info(scryfall_cache, face_part.strip())
                if info:
                    break
        if info:
            if info.get("type"):
                types.append(info["type"])
            if info.get("cmc") is not None:
                cmcs.append(float(info["cmc"]))
            if info.get("mana_cost"):
                mana_costs.append(info["mana_cost"])
            if info.get("artist"):
                artists.append(info["artist"])
            for kw in info.get("keywords", []) or []:
                if kw not in keywords:
                    keywords.append(kw)

    return {
        "type": " // ".join(types),
        "cmc": max(cmcs) if cmcs else 0.0,
        "keywords": keywords,
        "mana_cost": " // ".join(mana_costs),
        "artist": artists[0] if artists else "",
    }

def categorizza_tipo(type_line):
    t = type_line.lower()
    if "creature" in t: return "Creature"
    if "instant" in t: return "Istantanei"
    if "sorcery" in t: return "Stregonerie"
    if "artifact" in t: return "Artefatti"
    if "enchantment" in t: return "Incantesimi"
    if "planeswalker" in t: return "Planeswalker"
    if "land" in t: return "Terre"
    return "Altro"

@lru_cache(maxsize=1)
def get_global_commanders_validity():
    """
    Calcola l'insieme globale di tutti i comandanti validi e la tupla (compatta)
    dei comandanti non validi presenti nel DB (es. 'Sconosciuto' o carte non leggendarie).
    """
    conn = get_db()
    c = conn.cursor()
    rows = c.execute("SELECT DISTINCT comandante FROM decks").fetchall()
    conn.close()

    all_cmds = [r["comandante"] for r in rows if r["comandante"]]
    scryfall_cache = fetch_scryfall_data(all_cmds)

    valid_all = set()
    invalid_all = []
    for cmd in all_cmds:
        is_valid, _, _, _, _ = extract_commander_metadata(cmd, scryfall_cache)
        if is_valid:
            valid_all.add(cmd)
        else:
            invalid_all.append(cmd)

    return frozenset(valid_all), tuple(sorted(invalid_all))

@lru_cache(maxsize=1)
def get_global_cards_stats():
    """
    Calcola e mette in cache il totale globale dei mazzi validi e la frequenza
    globale di ogni carta su tutto il database, evitando di ricalcolarli ad ogni richiesta.
    """
    _, invalid_cmds = get_global_commanders_validity()
    conn = get_db()
    c = conn.cursor()

    if invalid_cmds:
        placeholders = "(" + ",".join(["?"] * len(invalid_cmds)) + ")"
        global_total = c.execute(
            f"SELECT COUNT(DISTINCT id_moxfield) as tot FROM decks WHERE comandante NOT IN {placeholders}",
            invalid_cmds
        ).fetchone()["tot"]
        gc_rows = c.execute(
            f"""
            SELECT c.card_name, COUNT(DISTINCT c.deck_id) as g_freq
            FROM deck_cards c
            JOIN decks d ON c.deck_id = d.id_moxfield
            WHERE d.comandante NOT IN {placeholders}
            GROUP BY c.card_name
            """,
            invalid_cmds
        ).fetchall()
    else:
        global_total = c.execute("SELECT COUNT(DISTINCT id_moxfield) as tot FROM decks").fetchone()["tot"]
        gc_rows = c.execute("SELECT card_name, COUNT(DISTINCT deck_id) as g_freq FROM deck_cards GROUP BY card_name").fetchall()

    conn.close()
    global_cards = {r["card_name"]: r["g_freq"] for r in gc_rows}
    return global_total, global_cards


@lru_cache(maxsize=32)
def get_valid_commanders_set(filtro_tempo: str):
    _, invalid_cmds = get_global_commanders_validity()
    conn = get_db()
    c = conn.cursor()
    curr_cond, prev_cond = get_time_conditions_both(filtro_tempo)

    curr_conds = []
    curr_params = []
    if curr_cond:
        curr_conds.append(curr_cond)
    if invalid_cmds:
        placeholders = "(" + ",".join(["?"] * len(invalid_cmds)) + ")"
        curr_conds.append(f"comandante NOT IN {placeholders}")
        curr_params.extend(invalid_cmds)

    base_where = f"WHERE {' AND '.join(curr_conds)}" if curr_conds else ""
    q_curr = f"SELECT comandante, COUNT(*) as cnt FROM decks {base_where} GROUP BY comandante ORDER BY cnt DESC, comandante ASC"
    curr_rows = c.execute(q_curr, curr_params).fetchall()
    
    if prev_cond:
        prev_conds = [prev_cond]
        prev_params = []
        if invalid_cmds:
            placeholders = "(" + ",".join(["?"] * len(invalid_cmds)) + ")"
            prev_conds.append(f"comandante NOT IN {placeholders}")
            prev_params.extend(invalid_cmds)
        q_prev = f"SELECT comandante, COUNT(*) as cnt FROM decks WHERE {' AND '.join(prev_conds)} GROUP BY comandante ORDER BY cnt DESC, comandante ASC"
        prev_rows = c.execute(q_prev, prev_params).fetchall()
        prev_ranks = {r['comandante']: idx + 1 for idx, r in enumerate(prev_rows)}
        
        df_curr = []
        for idx, r in enumerate(curr_rows):
            cmd = r['comandante']
            if cmd in prev_ranks:
                trend = prev_ranks[cmd] - (idx + 1)
            else:
                trend = None
            df_curr.append({'comandante': cmd, 'cnt': r['cnt'], 'trend': trend})
    else:
        df_curr = [{'comandante': r['comandante'], 'cnt': r['cnt'], 'trend': ""} for r in curr_rows]

    conn.close()
    
    if not df_curr:
        return frozenset(), []
        
    cmd_names = [row['comandante'] for row in df_curr]
    scryfall_cache = fetch_scryfall_data(cmd_names)
    
    valid_set = set()
    result_list = []
    
    for row in df_curr:
        name = row['comandante']
        is_valid, images, art_crops, oracles, cmd_color_identity = extract_commander_metadata(name, scryfall_cache)
        
        t_val = row['trend']
        if t_val is None:
            trend_out = None if prev_cond else ""
        else:
            trend_out = int(t_val) if t_val != "" else ""
                    
        if is_valid:
            valid_set.add(name)
            search_meta = extract_commander_search_fields(name, scryfall_cache)
            result_list.append({
                "comandante": name,
                "cnt": int(row['cnt']),
                "trend": trend_out,
                "images": images,
                "art_crops": art_crops,
                "oracles": oracles,
                "color_identity": cmd_color_identity,
                "type": search_meta["type"],
                "cmc": search_meta["cmc"],
                "keywords": search_meta["keywords"],
                "mana_cost": search_meta["mana_cost"],
                "artist": search_meta["artist"],
            })
            
    return frozenset(valid_set), result_list


def _load_disk_expansions_cache() -> Optional[dict]:
    try:
        if os.path.exists(SETS_CACHE_FILE):
            with open(SETS_CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict) and isinstance(data.get("expansions"), list):
                    return data
    except Exception as e:
        logging.warning(f"Errore lettura cache set Scryfall: {e}")
    return None

def _save_disk_expansions_cache(data: dict):
    tmp_file = f"{SETS_CACHE_FILE}.{os.getpid()}.{threading.get_ident()}.tmp"
    try:
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp_file, SETS_CACHE_FILE)
    except Exception as e:
        logging.warning(f"Errore salvataggio cache set Scryfall: {e}")
        try:
            os.remove(tmp_file)
        except OSError:
            pass

def get_recent_expansions_metadata() -> List[dict]:
    """
    Recupera e mette in cache giornaliera i metadati e la lista di carte nuove (-is:reprint)
    delle ultime 3 espansioni maggiori (inclusi i relativi set Commander/Eternal abbinati)
    che abbiano almeno una carta giocata nel database Centurion.
    """
    global _EXPANSIONS_CACHE_MEMORY, _EXPANSIONS_CACHE_DATE
    today_str = datetime.now().strftime("%Y-%m-%d")
    curr_mtime = get_db_mtime()
    prerelease_cutoff = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")

    # Fast path: controllo memoria o cache disco recente protetto da lock leggero
    with _expansions_lock:
        if _EXPANSIONS_CACHE_MEMORY is not None and _EXPANSIONS_CACHE_DATE == today_str:
            return _EXPANSIONS_CACHE_MEMORY

        disk_data = _load_disk_expansions_cache()
        if (
            disk_data
            and disk_data.get("date") == today_str
            and disk_data.get("cutoff") == prerelease_cutoff
            and disk_data.get("db_mtime") == curr_mtime
            and len(disk_data.get("expansions", [])) > 0
        ):
            _EXPANSIONS_CACHE_MEMORY = disk_data["expansions"]
            _EXPANSIONS_CACHE_DATE = today_str
            return _EXPANSIONS_CACHE_MEMORY

    # Slow path: serializzazione fetch verso Scryfall per prevenire cache stampede e 429
    with _expansions_fetch_lock:
        with _expansions_lock:
            if _EXPANSIONS_CACHE_MEMORY is not None and _EXPANSIONS_CACHE_DATE == today_str:
                return _EXPANSIONS_CACHE_MEMORY

            disk_data = _load_disk_expansions_cache()
            if (
                disk_data
                and disk_data.get("date") == today_str
                and disk_data.get("cutoff") == prerelease_cutoff
                and disk_data.get("db_mtime") == curr_mtime
                and len(disk_data.get("expansions", [])) > 0
            ):
                _EXPANSIONS_CACHE_MEMORY = disk_data["expansions"]
                _EXPANSIONS_CACHE_DATE = today_str
                return _EXPANSIONS_CACHE_MEMORY

        _, global_cards = get_global_cards_stats()
        if not global_cards:
            return disk_data.get("expansions", []) if disk_data else []

        global_cards_lower = set()
        for k in global_cards.keys():
            kl = k.strip().lower()
            global_cards_lower.add(kl)
            if " // " in kl:
                global_cards_lower.add(kl.split(" // ")[0].strip())

        session = get_scryfall_session()
        try:
            _wait_scryfall_rate_limit()
            res = session.get("https://api.scryfall.com/sets", timeout=15)
            if res.status_code in (429, 403) or res.status_code != 200:
                logging.warning(f"[SCRYFALL SETS] HTTP {res.status_code} su /sets: uso eventuale cache su disco.")
                return disk_data.get("expansions", []) if disk_data else []
            all_sets = res.json().get("data", [])
        except Exception as e:
            logging.warning(f"Errore API Scryfall /sets: {e}")
            return disk_data.get("expansions", []) if disk_data else []

        valid_types = {"expansion", "core", "draft_innovation", "commander"}
        companion_types = {"commander", "eternal", "draft_innovation", "expansion"}

        released_sets = [
            s for s in all_sets
            if not s.get("digital")
            and s.get("released_at")
            and s["released_at"] <= prerelease_cutoff
        ]

        children_by_parent: Dict[str, List[str]] = {}
        for s in released_sets:
            p_code = s.get("parent_set_code")
            if p_code and s.get("set_type") in companion_types and s.get("card_count", 0) > 0:
                children_by_parent.setdefault(p_code, []).append(s["code"])

        root_candidates = [
            s for s in released_sets
            if not s.get("parent_set_code")
            and s.get("set_type") in valid_types
            and s.get("card_count", 0) >= 20
        ]
        root_candidates.sort(key=lambda s: s.get("released_at", ""), reverse=True)

        selected_expansions = []
        for root in root_candidates[:10]:
            if len(selected_expansions) >= 3:
                break

            root_code = root["code"]
            codes = [root_code] + children_by_parent.get(root_code, [])
            sets_or = " OR ".join(f"e:{c}" for c in codes)
            q_str = f"({sets_or}) -is:reprint -is:funny -t:basic"

            exp_card_keys = set()
            next_url = "https://api.scryfall.com/cards/search"
            params: Optional[dict] = {"q": q_str}
            pages_fetched = 0

            while next_url and pages_fetched < 6:
                _wait_scryfall_rate_limit()
                pages_fetched += 1
                try:
                    s_res = session.get(next_url, params=params, timeout=15)
                    params = None
                    if s_res.status_code != 200:
                        next_url = None
                        break
                    s_body = s_res.json()
                    for c_obj in s_body.get("data", []):
                        c_name = c_obj.get("name", "").strip()
                        if not c_name:
                            continue
                        c_lower = c_name.lower()
                        exp_card_keys.add(c_lower)
                        if " // " in c_lower:
                            exp_card_keys.add(c_lower.split(" // ")[0].strip())
                    next_url = s_body.get("next_page") if s_body.get("has_more") else None
                except Exception as e:
                    logging.warning(f"Errore ricerca carte espansione {root_code}: {e}")
                    break

            if exp_card_keys & global_cards_lower:
                selected_expansions.append({
                    "code": root_code.upper(),
                    "codes": [c.upper() for c in codes],
                    "name": root.get("name", root_code.upper()),
                    "released_at": root.get("released_at", ""),
                    "icon_svg_uri": root.get("icon_svg_uri", ""),
                    "card_keys": sorted(list(exp_card_keys))
                })

        if selected_expansions:
            with _expansions_lock:
                _EXPANSIONS_CACHE_MEMORY = selected_expansions
                _EXPANSIONS_CACHE_DATE = today_str
            _save_disk_expansions_cache({"date": today_str, "cutoff": prerelease_cutoff, "db_mtime": curr_mtime, "expansions": selected_expansions})
            return selected_expansions

        return disk_data.get("expansions", []) if disk_data else []


@lru_cache(maxsize=32)
def get_dashboard_data_cached(filtro_tempo: str):
    _, invalid_cmds = get_global_commanders_validity()
    conn = get_db()
    c = conn.cursor()
    curr_cond, prev_cond = get_time_conditions_both(filtro_tempo)

    inv_clause = ""
    inv_clause_d = ""
    inv_params = list(invalid_cmds)
    if invalid_cmds:
        placeholders = "(" + ",".join(["?"] * len(invalid_cmds)) + ")"
        inv_clause = f"comandante NOT IN {placeholders}"
        inv_clause_d = f"d.comandante NOT IN {placeholders}"

    def build_where(time_c: Optional[str], use_alias: bool = False):
        conds = []
        if time_c:
            conds.append(time_c.replace("data_aggiornamento", "d.data_aggiornamento") if use_alias else time_c)
        if invalid_cmds:
            conds.append(inv_clause_d if use_alias else inv_clause)
        return ("WHERE " + " AND ".join(conds)) if conds else ""

    base_where = build_where(curr_cond, False)
    tot_decks = c.execute(f"SELECT COUNT(DISTINCT id_moxfield) as tot FROM decks {base_where}", inv_params).fetchone()['tot']

    q_curr_cmd = f"SELECT comandante, COUNT(*) as cnt FROM decks {base_where} GROUP BY comandante ORDER BY cnt DESC, comandante ASC"
    curr_cmd_rows = c.execute(q_curr_cmd, inv_params).fetchall()
    
    if prev_cond:
        prev_where = build_where(prev_cond, False)
        q_prev_cmd = f"SELECT comandante, COUNT(*) as cnt FROM decks {prev_where} GROUP BY comandante ORDER BY cnt DESC, comandante ASC"
        prev_cmd_rows = c.execute(q_prev_cmd, inv_params).fetchall()
        prev_cmd_ranks = {r['comandante']: idx + 1 for idx, r in enumerate(prev_cmd_rows)}
        
        df_top_cmd = []
        for idx, r in enumerate(curr_cmd_rows[:25]):
            cmd = r['comandante']
            trend = prev_cmd_ranks[cmd] - (idx + 1) if cmd in prev_cmd_ranks else None
            df_top_cmd.append({'comandante': cmd, 'cnt': r['cnt'], 'trend': trend})
    else:
        df_top_cmd = [{'comandante': r['comandante'], 'cnt': r['cnt'], 'trend': ""} for r in curr_cmd_rows[:25]]

    p1_where = build_where("data_aggiornamento > strftime('%Y-%m-%dT%H:%M:%S', 'now', '-7 days')", False)
    p2_where = build_where("data_aggiornamento <= strftime('%Y-%m-%dT%H:%M:%S', 'now', '-7 days') AND data_aggiornamento > strftime('%Y-%m-%dT%H:%M:%S', 'now', '-14 days')", False)
    p1_rows = c.execute(f"SELECT comandante, COUNT(*) as cnt1 FROM decks {p1_where} GROUP BY comandante", inv_params).fetchall()
    p2_rows = c.execute(f"SELECT comandante, COUNT(*) as cnt2 FROM decks {p2_where} GROUP BY comandante", inv_params).fetchall()
    p2_dict = {r['comandante']: r['cnt2'] for r in p2_rows}
    
    df_hot_cmd = []
    for r in p1_rows:
        cmd = r['comandante']
        cnt1 = r['cnt1']
        cnt2 = p2_dict.get(cmd, 0)
        delta = cnt1 - cnt2
        if cnt1 >= 2 and delta > 0:
            df_hot_cmd.append({'comandante': cmd, 'cnt1': cnt1, 'delta': delta})
            
    df_hot_cmd.sort(key=lambda x: (-x['delta'], -x['cnt1'], x['comandante']))
    df_hot_cmd = df_hot_cmd[:50]

    where_curr = build_where(curr_cond, True)
    where_prev = build_where(prev_cond, True)
    
    q_curr_cards = f"""
        SELECT c.card_name, COUNT(DISTINCT c.deck_id) as freq 
        FROM deck_cards c JOIN decks d ON c.deck_id = d.id_moxfield 
        {where_curr}
        GROUP BY c.card_name ORDER BY freq DESC, c.card_name ASC
    """
    curr_card_rows = c.execute(q_curr_cards, inv_params).fetchall()
    
    if prev_cond:
        q_prev_cards = f"""
            SELECT c.card_name, COUNT(DISTINCT c.deck_id) as freq 
            FROM deck_cards c JOIN decks d ON c.deck_id = d.id_moxfield 
            {where_prev}
            GROUP BY c.card_name ORDER BY freq DESC, c.card_name ASC
        """
        prev_card_rows = c.execute(q_prev_cards, inv_params).fetchall()
        prev_card_ranks = {r['card_name']: idx + 1 for idx, r in enumerate(prev_card_rows)}
        
        df_top_cards = []
        for idx, r in enumerate(curr_card_rows[:250]):
            c_name = r['card_name']
            trend = prev_card_ranks[c_name] - (idx + 1) if c_name in prev_card_ranks else None
            df_top_cards.append({'card_name': c_name, 'freq': r['freq'], 'trend': trend})
    else:
        df_top_cards = [{'card_name': r['card_name'], 'freq': r['freq'], 'trend': ""} for r in curr_card_rows[:250]]

    hc1_where = build_where("data_aggiornamento > strftime('%Y-%m-%dT%H:%M:%S', 'now', '-7 days')", True)
    hc2_where = build_where("data_aggiornamento <= strftime('%Y-%m-%dT%H:%M:%S', 'now', '-7 days') AND data_aggiornamento > strftime('%Y-%m-%dT%H:%M:%S', 'now', '-14 days')", True)
    q_hc1 = f"""
        SELECT c.card_name, COUNT(DISTINCT c.deck_id) as cnt1 
        FROM deck_cards c JOIN decks d ON c.deck_id = d.id_moxfield 
        {hc1_where} GROUP BY c.card_name
    """
    q_hc2 = f"""
        SELECT c.card_name, COUNT(DISTINCT c.deck_id) as cnt2 
        FROM deck_cards c JOIN decks d ON c.deck_id = d.id_moxfield 
        {hc2_where} GROUP BY c.card_name
    """
    hc1_rows = c.execute(q_hc1, inv_params).fetchall()
    hc2_rows = c.execute(q_hc2, inv_params).fetchall()
    hc2_dict = {r['card_name']: r['cnt2'] for r in hc2_rows}
    
    df_hot_c = []
    for r in hc1_rows:
        c_name = r['card_name']
        cnt1 = r['cnt1']
        cnt2 = hc2_dict.get(c_name, 0)
        delta = cnt1 - cnt2
        if cnt1 >= 3 and delta > 0:
            df_hot_c.append({'card_name': c_name, 'cnt1': cnt1, 'delta': delta})
            
    df_hot_c.sort(key=lambda x: (-x['delta'], -x['cnt1'], x['card_name']))
    df_hot_c = df_hot_c[:250]
        
    conn.close()

    all_names = set()
    for r in df_top_cmd: all_names.add(r['comandante'])
    for r in df_hot_cmd: all_names.add(r['comandante'])
    for r in df_top_cards: all_names.add(r['card_name'])
    for r in df_hot_c: all_names.add(r['card_name'])

    expansions_meta = get_recent_expansions_metadata()
    expansion_raw_list = []
    key_to_exp_code: Dict[str, str] = {}
    for exp in expansions_meta:
        exp_code = exp["code"]
        valid_keys = set(exp.get("card_keys", []))
        for vk in valid_keys:
            if vk not in key_to_exp_code:
                key_to_exp_code[vk] = exp_code

        def _in_exp(c_name: str, vk=valid_keys) -> bool:
            kl = (c_name or "").strip().lower()
            if kl in vk:
                return True
            if " // " in kl and kl.split(" // ")[0].strip() in vk:
                return True
            return False

        curr_exp_rows = [r for r in curr_card_rows if _in_exp(r['card_name'])]
        if prev_cond:
            prev_exp_rows = [r for r in prev_card_rows if _in_exp(r['card_name'])]
            prev_exp_ranks = {r['card_name']: idx + 1 for idx, r in enumerate(prev_exp_rows)}
            exp_df = []
            for idx, r in enumerate(curr_exp_rows[:250]):
                c_name = r['card_name']
                trend = prev_exp_ranks[c_name] - (idx + 1) if c_name in prev_exp_ranks else None
                exp_df.append({'card_name': c_name, 'freq': r['freq'], 'trend': trend, 'set_code': exp_code})
        else:
            exp_df = [{'card_name': r['card_name'], 'freq': r['freq'], 'trend': "", 'set_code': exp_code} for r in curr_exp_rows[:250]]

        for r in exp_df:
            all_names.add(r['card_name'])

        expansion_raw_list.append((exp, exp_df, len(curr_exp_rows)))

    if len(expansions_meta) > 1:
        def _resolve_exp_code(c_name: str) -> Optional[str]:
            kl = (c_name or "").strip().lower()
            if kl in key_to_exp_code:
                return key_to_exp_code[kl]
            if " // " in kl:
                front = kl.split(" // ")[0].strip()
                if front in key_to_exp_code:
                    return key_to_exp_code[front]
            return None

        curr_all_exp_rows = [r for r in curr_card_rows if _resolve_exp_code(r['card_name']) is not None]
        if prev_cond:
            prev_all_exp_rows = [r for r in prev_card_rows if _resolve_exp_code(r['card_name']) is not None]
            prev_all_exp_ranks = {r['card_name']: idx + 1 for idx, r in enumerate(prev_all_exp_rows)}
            all_exp_df = []
            for idx, r in enumerate(curr_all_exp_rows[:250]):
                c_name = r['card_name']
                trend = prev_all_exp_ranks[c_name] - (idx + 1) if c_name in prev_all_exp_ranks else None
                all_exp_df.append({
                    'card_name': c_name,
                    'freq': r['freq'],
                    'trend': trend,
                    'set_code': _resolve_exp_code(c_name)
                })
        else:
            all_exp_df = [{
                'card_name': r['card_name'],
                'freq': r['freq'],
                'trend': "",
                'set_code': _resolve_exp_code(r['card_name'])
            } for r in curr_all_exp_rows[:250]]

        for r in all_exp_df:
            all_names.add(r['card_name'])

        combined_exp_meta = {
            "code": "ALL",
            "codes": [e["code"] for e in expansions_meta],
            "name": "Tutte le ultime espansioni",
            "released_at": "",
            "icon_svg_uri": ""
        }
        expansion_raw_list.append((combined_exp_meta, all_exp_df, len(curr_all_exp_rows)))

    # Calcolo sezioni per ruolo funzionale (Removal, Botti, Counterspell, Card Draw, ecc.)
    scryfall_cache_roles = load_scryfall_cache()
    card_roles_cache: Dict[str, List[str]] = {}

    def _get_card_roles(c_name: str) -> List[str]:
        cached_r = card_roles_cache.get(c_name)
        if cached_r is not None:
            return cached_r
        info = lookup_card_info(scryfall_cache_roles, c_name)
        r_list = scryfall_search.classify_card_roles(info)
        card_roles_cache[c_name] = r_list
        return r_list

    role_curr_rows: Dict[str, List[Any]] = {r["key"]: [] for r in scryfall_search.ROLE_DEFINITIONS}
    for r in curr_card_rows:
        for role_key in _get_card_roles(r["card_name"]):
            role_curr_rows[role_key].append(r)

    role_prev_ranks: Dict[str, Dict[str, int]] = {r["key"]: {} for r in scryfall_search.ROLE_DEFINITIONS}
    if prev_cond:
        role_prev_counters: Dict[str, int] = {r["key"]: 0 for r in scryfall_search.ROLE_DEFINITIONS}
        for r in prev_card_rows:
            c_name = r["card_name"]
            for role_key in _get_card_roles(c_name):
                role_prev_counters[role_key] += 1
                role_prev_ranks[role_key][c_name] = role_prev_counters[role_key]

    role_raw_list = []
    for role_meta in scryfall_search.ROLE_DEFINITIONS:
        r_key = role_meta["key"]
        curr_r = role_curr_rows[r_key]
        prev_ranks_map = role_prev_ranks[r_key]
        role_df = []
        for idx, r in enumerate(curr_r[:25]):
            c_name = r["card_name"]
            if prev_cond:
                trend = prev_ranks_map[c_name] - (idx + 1) if c_name in prev_ranks_map else None
            else:
                trend = ""
            role_df.append({"card_name": c_name, "freq": r["freq"], "trend": trend})
            all_names.add(c_name)
        role_raw_list.append((role_meta, role_df, len(curr_r)))
    
    scryfall_cache = fetch_scryfall_data(list(all_names))

    def format_item(row, name_col, is_cmd=False):
        name = row[name_col]
        if is_cmd:
            _, images, _, oracles, final_color_identity = extract_commander_metadata(name, scryfall_cache)
            cmd_meta = extract_commander_search_fields(name, scryfall_cache)
            cat = "Comandante"
            type_line = cmd_meta["type"]
            cmc_val = cmd_meta["cmc"]
            mana_cost_val = cmd_meta["mana_cost"]
            keywords_val = cmd_meta["keywords"]
            colors_val = final_color_identity
            artist_val = cmd_meta["artist"]
        else:
            main_info = lookup_card_info(scryfall_cache, name)
            images = list(dict.fromkeys(main_info.get("images", [])))
            type_line = main_info.get("type", "")
            cat = categorizza_tipo(type_line)
            final_color_identity = main_info.get("color_identity", [])
            oracles = main_info.get("oracles", [])
            cmc_val = main_info.get("cmc", 0.0)
            mana_cost_val = main_info.get("mana_cost", "")
            keywords_val = main_info.get("keywords", [])
            colors_val = main_info.get("colors")
            artist_val = main_info.get("artist", "")

        t_val = row.get('trend')
        if t_val is None:
            trend_out = None if prev_cond else ""
        else:
            trend_out = int(t_val) if t_val != "" else ""

        item_out = {
            "name": name,
            "images": images,
            "val1": int(row.get('cnt', row.get('freq', row.get('cnt1', 0)))),
            "trend": trend_out,
            "delta": int(row.get('delta', 0)) if 'delta' in row else None,
            "category": cat,
            "type": type_line,
            "cmc": cmc_val,
            "oracles": oracles,
            "color_identity": final_color_identity,
            "set_code": row.get("set_code"),
            "artist": artist_val,
        }
        if mana_cost_val:
            item_out["mana_cost"] = mana_cost_val
        if keywords_val:
            item_out["keywords"] = keywords_val
        if colors_val is not None:
            item_out["colors"] = colors_val
        if not is_cmd:
            item_out["roles"] = _get_card_roles(name)
        return item_out

    try:
        mtime = get_db_mtime()
        global_last_updated = datetime.fromtimestamp(mtime).isoformat() if mtime else None
    except Exception:
        global_last_updated = None

    return {
        "total_decks": tot_decks,
        "last_updated": global_last_updated,
        "top_commanders": [format_item(row, 'comandante', True) for row in df_top_cmd],
        "hot_commanders": [format_item(row, 'comandante', True) for row in df_hot_cmd],
        "top_cards": [format_item(row, 'card_name', False) for row in df_top_cards],
        "hot_cards": [format_item(row, 'card_name', False) for row in df_hot_c],
        "role_categories": [
            {
                "key": role_meta["key"],
                "label": role_meta["label"],
                "title": role_meta["title"],
                "description": role_meta["description"],
                "accent": role_meta["accent"],
                "total_cards": total_role_cards,
                "cards": [format_item(row, 'card_name', False) for row in role_df]
            }
            for role_meta, role_df, total_role_cards in role_raw_list
        ],
        "recent_expansions": [
            {
                "code": exp["code"],
                "codes": exp.get("codes", [exp["code"]]),
                "name": exp["name"],
                "released_at": exp["released_at"],
                "icon_svg_uri": exp["icon_svg_uri"],
                "total_played_cards": total_played,
                "cards": [format_item(row, 'card_name', False) for row in exp_df]
            }
            for exp, exp_df, total_played in expansion_raw_list
        ]
    }

@lru_cache(maxsize=128)
def get_decks_stats_cached(filtro_tempo: str, comandante: Optional[str]):
    try:
        mtime = get_db_mtime()
        global_last_updated = datetime.fromtimestamp(mtime).isoformat()
    except Exception:
        global_last_updated = None

    valid_all, invalid_cmds = get_global_commanders_validity()
    if not valid_all:
        return {"total_decks": 0, "last_updated": global_last_updated, "mana_curve": {}, "cards": []}

    if comandante and comandante not in valid_all:
        return {"total_decks": 0, "last_updated": global_last_updated, "mana_curve": {}, "cards": []}

    global_total, global_cards = get_global_cards_stats()
    if global_total == 0:
        return {"total_decks": 0, "last_updated": global_last_updated, "mana_curve": {}, "cards": []}

    conn = get_db()
    c = conn.cursor()

    conds = []
    t_cond = get_time_condition(filtro_tempo)
    if t_cond:
        conds.append(t_cond)
    
    params = []
    if comandante:
        conds.append("comandante = ?")
        params.append(comandante)
    elif invalid_cmds:
        placeholders_inv = '(' + ','.join(['?'] * len(invalid_cmds)) + ')'
        conds.append(f"comandante NOT IN {placeholders_inv}")
        params.extend(invalid_cmds)
        
    where_clause = (" WHERE " + " AND ".join(conds)) if conds else ""
    
    q_total = f"SELECT COUNT(DISTINCT id_moxfield) as tot FROM decks {where_clause}"
    total_decks = c.execute(q_total, params).fetchone()['tot']
    
    if total_decks == 0:
        conn.close()
        return {"total_decks": 0, "last_updated": global_last_updated, "mana_curve": {}, "cards": []}
        
    where_clause_join = where_clause.replace("comandante", "d.comandante").replace("data_aggiornamento", "d.data_aggiornamento")
    
    q_cards = f"""
        SELECT c.card_name, COUNT(DISTINCT c.deck_id) as freq
        FROM deck_cards c
        JOIN decks d ON c.deck_id = d.id_moxfield
        {where_clause_join}
        GROUP BY c.card_name
    """
        
    stats_rows = c.execute(q_cards, params).fetchall()
    conn.close()
    
    stats_list = []
    for row in stats_rows:
        c_name = row['card_name']
        freq = row['freq']
        
        perc = round((freq / total_decks) * 100, 1)
        g_freq = global_cards.get(c_name, 0)
        g_perc = (g_freq / global_total) * 100
        sinergia = round(perc - g_perc, 1)
        
        stats_list.append({
            'card_name': c_name,
            'freq': freq,
            'Percentuale': perc,
            'Sinergia': sinergia
        })
        
    stats_list.sort(key=lambda x: (-x['Percentuale'], -x['freq'], x['card_name']))
    
    scryfall_cache = fetch_scryfall_data([x['card_name'] for x in stats_list])
    
    formatted_cards = []
    mana_curve = {0: 0.0, 1: 0.0, 2: 0.0, 3: 0.0, 4: 0.0, 5: 0.0, 6: 0.0}
    type_dist = {"Creature": 0.0, "Istantanei": 0.0, "Stregonerie": 0.0, "Artefatti": 0.0, "Incantesimi": 0.0, "Planeswalker": 0.0, "Terre": 0.0}

    somma_univoche = 0.0

    for row in stats_list:
        c_name = row['card_name']
        info = lookup_card_info(scryfall_cache, c_name)
        if not info:
            info = {"images": [], "art_crops": [], "type": "Altro", "oracles": [], "color_identity": [], "cmc": 0}
            
        type_line = info.get("type", "")
        front_type = type_line.split(" // ")[0] if " // " in type_line else type_line
        cat = categorizza_tipo(type_line)
        
        avg_copies_comp = float(row['freq']) / total_decks
        somma_univoche += avg_copies_comp
        
        if cat in type_dist:
            type_dist[cat] += avg_copies_comp
        elif cat == "Altro" and "Land" in front_type:
            type_dist["Terre"] += avg_copies_comp
        
        if cat not in ["Terre", "Altro"] and "Land" not in front_type:
            cmc = int(info.get("cmc", 0))
            if cmc >= 6: cmc = 6
            mana_curve[cmc] += avg_copies_comp
            
        card_roles = scryfall_search.classify_card_roles(info)
        card_entry = {
            "card_name": c_name,
            "freq": int(row['freq']),
            "Percentuale": float(row['Percentuale']),
            "Sinergia": float(row['Sinergia']),
            "images": info.get("images", []),
            "art_crops": info.get("art_crops", []),
            "artist": info.get("artist", ""),
            "oracles": info.get("oracles", []),
            "color_identity": info.get("color_identity", []),
            "category": cat,
            "type": type_line,
            "cmc": info.get("cmc", 0.0),
            "roles": card_roles
        }
        if info.get("mana_cost"):
            card_entry["mana_cost"] = info["mana_cost"]
        if info.get("colors") is not None:
            card_entry["colors"] = info["colors"]
        if info.get("keywords"):
            card_entry["keywords"] = info["keywords"]
        if info.get("power") is not None:
            card_entry["power"] = info["power"]
        if info.get("toughness") is not None:
            card_entry["toughness"] = info["toughness"]
        if info.get("loyalty") is not None:
            card_entry["loyalty"] = info["loyalty"]
        if info.get("rarity"):
            card_entry["rarity"] = info["rarity"]
        formatted_cards.append(card_entry)
        
    is_partner_cmd = bool(comandante and " / " in comandante and " // " not in comandante)
    expected_mainboard = 98.0 if is_partner_cmd else 99.0
    terre_mancanti = expected_mainboard - somma_univoche
    if terre_mancanti > 0:
        type_dist["Terre"] += terre_mancanti
            
    return {
        "total_decks": total_decks,
        "last_updated": global_last_updated,
        "mana_curve": {k: round(v, 1) for k, v in mana_curve.items()},
        "type_distribution": {k: round(v, 1) for k, v in type_dist.items() if v > 0},
        "cards": formatted_cards
    }

@lru_cache(maxsize=512)
def get_card_details_cached(card_name: str, filtro_tempo: str):
    valid_all, invalid_cmds = get_global_commanders_validity()
    _, global_cards = get_global_cards_stats()
    total_decks_global = global_cards.get(card_name, 0)

    scryfall_cache = load_scryfall_cache()
    if total_decks_global > 0 and not lookup_card_info(scryfall_cache, card_name):
        scryfall_cache = fetch_scryfall_data([card_name])
    card_info_cache = lookup_card_info(scryfall_cache, card_name)

    if not valid_all or total_decks_global == 0:
        return {
            "card_name": card_name,
            "images": card_info_cache.get("images", []),
            "art_crops": card_info_cache.get("art_crops", []),
            "artist": card_info_cache.get("artist", ""),
            "oracles": card_info_cache.get("oracles", []),
            "total_decks_with_card": 0,
            "total_decks_global": 0,
            "commanders": [],
            "synergies": []
        }

    conn = get_db()
    c = conn.cursor()

    inv_cond_d = ""
    inv_cond_plain = ""
    inv_params = list(invalid_cmds)
    if invalid_cmds:
        placeholders_inv = '(' + ','.join(['?'] * len(invalid_cmds)) + ')'
        inv_cond_d = f" AND d.comandante NOT IN {placeholders_inv}"
        inv_cond_plain = f" WHERE comandante NOT IN {placeholders_inv}"

    t_cond = get_time_condition(filtro_tempo)
    time_filtered_cond = ""
    if t_cond:
        time_filtered_cond = " AND " + t_cond.replace("data_aggiornamento", "d.data_aggiornamento")

    if not t_cond:
        total_decks_time = total_decks_global
    else:
        q_tot_time = f"SELECT COUNT(DISTINCT d.id_moxfield) as tot FROM decks d JOIN deck_cards c ON d.id_moxfield = c.deck_id WHERE c.card_name = ?{inv_cond_d}{time_filtered_cond}"
        total_decks_time = c.execute(q_tot_time, [card_name] + inv_params).fetchone()['tot']

    if total_decks_time == 0:
        conn.close()
        return {
            "card_name": card_name,
            "images": card_info_cache.get("images", []),
            "art_crops": card_info_cache.get("art_crops", []),
            "artist": card_info_cache.get("artist", ""),
            "oracles": card_info_cache.get("oracles", []),
            "total_decks_with_card": 0,
            "total_decks_global": total_decks_global,
            "commanders": [],
            "synergies": []
        }
        
    q_cmd = f"SELECT d.comandante, COUNT(DISTINCT d.id_moxfield) as mazzi_carta FROM decks d JOIN deck_cards c ON d.id_moxfield = c.deck_id WHERE c.card_name = ?{inv_cond_d}{time_filtered_cond} GROUP BY d.comandante"
    cmd_counts = c.execute(q_cmd, [card_name] + inv_params).fetchall()
    
    if inv_cond_plain:
        q_tot_cmd = f"SELECT comandante, COUNT(DISTINCT id_moxfield) as totale_comandante FROM decks d{inv_cond_plain}{time_filtered_cond} GROUP BY comandante"
    else:
        where_time_only = f" WHERE {t_cond.replace('data_aggiornamento', 'd.data_aggiornamento')}" if t_cond else ""
        q_tot_cmd = f"SELECT comandante, COUNT(DISTINCT id_moxfield) as totale_comandante FROM decks d{where_time_only} GROUP BY comandante"
    total_counts = c.execute(q_tot_cmd, inv_params).fetchall()
    tot_map = {r['comandante']: r['totale_comandante'] for r in total_counts}
    
    res_cmd = []
    for r in cmd_counts:
        cmd = r['comandante']
        mazzi = r['mazzi_carta']
        totale = tot_map.get(cmd, 1)
        perc = round((mazzi / totale) * 100, 1)
        res_cmd.append({
            'comandante': cmd,
            'mazzi_carta': mazzi,
            'totale_comandante': totale,
            'Percentuale': perc
        })
    res_cmd.sort(key=lambda x: (-x['Percentuale'], -x['mazzi_carta'], x['comandante']))
    
    all_cmd_names = [x['comandante'] for x in res_cmd]
    scryfall_cache = fetch_scryfall_data(all_cmd_names + [card_name])

    cmd_list = []
    for row in res_cmd:
        name = row['comandante']
        _, images, art_crops, _, _ = extract_commander_metadata(name, scryfall_cache)

        cmd_list.append({
            "comandante": name,
            "mazzi_carta": int(row['mazzi_carta']),
            "totale_comandante": int(row['totale_comandante']),
            "Percentuale": float(row['Percentuale']),
            "images": images,
            "art_crops": art_crops,
            "artist": extract_commander_search_fields(name, scryfall_cache)["artist"]
        })
        
    syn_query = f"""
        SELECT c.card_name, COUNT(DISTINCT c.deck_id) as freq 
        FROM deck_cards c
        JOIN decks d ON c.deck_id = d.id_moxfield
        WHERE d.id_moxfield IN (
            SELECT deck_id FROM deck_cards WHERE card_name = ?
        )
        {inv_cond_d}
        {time_filtered_cond}
        AND c.card_name != ?
        GROUP BY c.card_name
    """
    syn_params = [card_name] + inv_params + [card_name]
    syn_rows = c.execute(syn_query, syn_params).fetchall()
    conn.close()
    
    syn_df = []
    for r in syn_rows:
        freq = r['freq']
        perc = round((freq / total_decks_time) * 100, 1)
        syn_df.append({
            'card_name': r['card_name'],
            'freq': freq,
            'Percentuale': perc
        })
    syn_df.sort(key=lambda x: (-x['Percentuale'], -x['freq'], x['card_name']))
    syn_df = syn_df[:50]
    
    syn_cache = fetch_scryfall_data([x['card_name'] for x in syn_df])
    syn_formatted = []
    for row in syn_df:
        cn = row['card_name']
        inf = lookup_card_info(syn_cache, cn)
        if not inf:
            inf = {"images": [], "art_crops": [], "type": "Altro", "oracles": [], "color_identity": []}
            
        syn_formatted.append({
            "card_name": cn,
            "freq": int(row['freq']),
            "Percentuale": float(row['Percentuale']),
            "images": inf.get("images", []),
            "art_crops": inf.get("art_crops", []),
            "artist": inf.get("artist", ""),
            "oracles": inf.get("oracles", []),
            "color_identity": inf.get("color_identity", []),
            "category": categorizza_tipo(inf.get("type", ""))
        })
        
    return {
        "card_name": card_name,
        "images": card_info_cache.get("images", []),
        "art_crops": card_info_cache.get("art_crops", []),
        "artist": card_info_cache.get("artist", ""),
        "oracles": card_info_cache.get("oracles", []),
        "total_decks_with_card": total_decks_time,
        "total_decks_global": total_decks_global,
        "commanders": cmd_list[:15],
        "synergies": syn_formatted
    }

@lru_cache(maxsize=256)
def get_commander_decks_cached(comandante: str, filtro_tempo: str, limit: int = 0, offset: int = 0):
    valid_all, _ = get_global_commanders_validity()
    if comandante not in valid_all:
        return []

    conn = get_db()
    c = conn.cursor()
    conds = ["comandante = ?"]
    t_cond = get_time_condition(filtro_tempo)
    params = [comandante]
    
    if t_cond:
        conds.append(t_cond)
        
    where_clause = " WHERE " + " AND ".join(conds)
    
    if limit > 0:
        query = f"SELECT id_moxfield, data_aggiornamento FROM decks {where_clause} ORDER BY data_aggiornamento DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
    else:
        query = f"SELECT id_moxfield, data_aggiornamento FROM decks {where_clause} ORDER BY data_aggiornamento DESC"
        
    df = c.execute(query, params).fetchall()
    conn.close()
    
    decks = []
    for row in df:
        decks.append({
            "id_moxfield": row['id_moxfield'],
            "url": f"https://moxfield.com/decks/{row['id_moxfield']}",
            "data_aggiornamento": row['data_aggiornamento']
        })
    return decks

VALID_PERIODS = {"Ultimi 7 giorni", "Ultimi 30 giorni", "Ultimi 3 mesi", "Ultimi 6 mesi", "Ultimo anno", "Tutti i tempi"}

def normalize_period(p: str) -> str:
    return p if p in VALID_PERIODS else "Tutti i tempi"

@app.get("/api/commanders")
async def get_commanders(filtro_tempo: str = "Tutti i tempi"):
    ensure_daily_caches_fresh()
    filtro_tempo = normalize_period(filtro_tempo)
    _, result_list = await run_in_threadpool(get_valid_commanders_set, filtro_tempo)
    return result_list

@app.get("/api/decks/stats")
async def get_decks_stats(filtro_tempo: str = "Tutti i tempi", comandante: Optional[str] = None, limit: int = 0, offset: int = 0):
    ensure_daily_caches_fresh()
    filtro_tempo = normalize_period(filtro_tempo)
    limit = max(0, min(limit, 500))
    offset = max(0, offset)
    
    data = await run_in_threadpool(get_decks_stats_cached, filtro_tempo, comandante)
    
    # Se è richiesta paginazione (limit > 0 o offset > 0), facciamo slice senza toccare il dict originale in cache
    if limit > 0 or offset > 0:
        cards = data.get("cards", [])
        sliced_cards = cards[offset : offset + limit] if limit > 0 else cards[offset:]
        return {
            **data,
            "cards": sliced_cards
        }
        
    # Restituisce una copia superficiale per prevenire mutazioni accidentali del dizionario in cache
    return dict(data)

def _get_card_details_safe(card_name: str, filtro_tempo: str):
    card_name = (card_name or "").strip()[:150]
    if not card_name:
        return {
            "card_name": "",
            "images": [],
            "art_crops": [],
            "artist": "",
            "oracles": [],
            "total_decks_with_card": 0,
            "total_decks_global": 0,
            "commanders": [],
            "synergies": []
        }
    _, global_cards = get_global_cards_stats()
    if card_name not in global_cards:
        # Non inquinare @lru_cache e non effettuare chiamate esterne a Scryfall per carte inesistenti nel DB
        scryfall_cache = load_scryfall_cache()
        card_info_cache = lookup_card_info(scryfall_cache, card_name)
        return {
            "card_name": card_name,
            "images": card_info_cache.get("images", []),
            "art_crops": card_info_cache.get("art_crops", []),
            "artist": card_info_cache.get("artist", ""),
            "oracles": card_info_cache.get("oracles", []),
            "total_decks_with_card": 0,
            "total_decks_global": 0,
            "commanders": [],
            "synergies": []
        }
    return get_card_details_cached(card_name, filtro_tempo)

@app.get("/api/card/details")
async def get_card_details(card_name: str, filtro_tempo: str = "Tutti i tempi"):
    ensure_daily_caches_fresh()
    filtro_tempo = normalize_period(filtro_tempo)
    return await run_in_threadpool(_get_card_details_safe, card_name, filtro_tempo)

@app.get("/api/commander/decks")
async def get_commander_decks(comandante: str, filtro_tempo: str = "Tutti i tempi", limit: int = 0, offset: int = 0):
    ensure_daily_caches_fresh()
    filtro_tempo = normalize_period(filtro_tempo)
    limit = max(0, min(limit, 500))
    offset = max(0, offset)
    return await run_in_threadpool(get_commander_decks_cached, comandante, filtro_tempo, limit, offset)

@app.get("/api/home/dashboard")
async def get_dashboard(filtro_tempo: str = "Tutti i tempi"):
    ensure_daily_caches_fresh()
    filtro_tempo = normalize_period(filtro_tempo)
    return await run_in_threadpool(get_dashboard_data_cached, filtro_tempo)

_ITALIAN_MONTHS_SHORT = (
    "", "Gen", "Feb", "Mar", "Apr", "Mag", "Giu",
    "Lug", "Ago", "Set", "Ott", "Nov", "Dic"
)

def _format_history_label(period_key: str, granularity: str) -> str:
    try:
        if granularity == "week":
            dt = datetime.strptime(period_key, "%Y-%m-%d")
            return f"{dt.day:02d} {_ITALIAN_MONTHS_SHORT[dt.month]}"
        year_s, month_s = period_key.split("-", 1)
        m_idx = int(month_s)
        if 1 <= m_idx <= 12:
            return f"{_ITALIAN_MONTHS_SHORT[m_idx]} {year_s[2:]}"
    except Exception:
        pass
    return period_key

@lru_cache(maxsize=16)
def get_metagame_history_cached(granularity: str = "month", months: int = 12, top_n: int = 6):
    granularity = "week" if granularity == "week" else "month"
    months = max(3, min(int(months), 24))
    top_n = max(3, min(int(top_n), 8))

    _, invalid_cmds = get_global_commanders_validity()
    conn = get_db()
    try:
        c = conn.cursor()
        bucket_expr = (
            "date(data_aggiornamento, 'weekday 1', '-7 days')"
            if granularity == "week"
            else "substr(data_aggiornamento, 1, 7)"
        )
        where_parts = [
            "data_aggiornamento IS NOT NULL",
            "data_aggiornamento != ''",
            "data_aggiornamento >= strftime('%Y-%m-%dT%H:%M:%S', 'now', ?)",
        ]
        params: list = [f"-{months} months"]
        if invalid_cmds:
            placeholders = "(" + ",".join(["?"] * len(invalid_cmds)) + ")"
            where_parts.append(f"comandante NOT IN {placeholders}")
            params.extend(invalid_cmds)

        query = f"""
            SELECT {bucket_expr} AS period_key,
                   comandante,
                   COUNT(DISTINCT id_moxfield) AS deck_count
            FROM decks
            WHERE {' AND '.join(where_parts)}
            GROUP BY period_key, comandante
            ORDER BY period_key ASC
        """
        rows = c.execute(query, params).fetchall()
    finally:
        conn.close()

    period_totals: dict[str, int] = {}
    period_cmds: dict[str, dict[str, int]] = {}
    global_cmd_totals: dict[str, int] = {}

    for r in rows:
        pkey = r["period_key"]
        cmd = r["comandante"]
        cnt = int(r["deck_count"] or 0)
        if not pkey or not cmd or cnt <= 0:
            continue
        period_totals[pkey] = period_totals.get(pkey, 0) + cnt
        if pkey not in period_cmds:
            period_cmds[pkey] = {}
        period_cmds[pkey][cmd] = period_cmds[pkey].get(cmd, 0) + cnt
        global_cmd_totals[cmd] = global_cmd_totals.get(cmd, 0) + cnt

    sorted_periods = sorted(period_totals.keys())
    if not sorted_periods:
        return {
            "granularity": granularity,
            "months": months,
            "top_n": top_n,
            "top_commanders": [],
            "buckets": [],
            "movers": {"rising": [], "falling": []},
            "summary": {
                "total_decks": 0,
                "periods_count": 0,
                "unique_commanders": 0,
                "peak_period": None,
                "most_diverse_period": None,
            },
        }

    top_cmd_names = [
        cmd for cmd, _ in sorted(global_cmd_totals.items(), key=lambda x: (-x[1], x[0]))[:top_n]
    ]
    scryfall_cache = fetch_scryfall_data(list(global_cmd_totals.keys()))

    cmd_color_map = {}
    for cmd in global_cmd_totals.keys():
        _, _, _, _, color_id = extract_commander_metadata(cmd, scryfall_cache)
        cmd_color_map[cmd] = color_id

    top_commanders_meta = []
    total_decks_window = sum(period_totals.values())
    for cmd in top_cmd_names:
        _, imgs, _, _, color_id = extract_commander_metadata(cmd, scryfall_cache)
        cnt = global_cmd_totals[cmd]
        top_commanders_meta.append({
            "name": cmd,
            "total_decks": cnt,
            "avg_share": round((cnt / total_decks_window) * 100, 2) if total_decks_window > 0 else 0.0,
            "color_identity": color_id,
            "image": imgs[0] if imgs else None,
        })

    buckets = []
    peak_period = None
    most_diverse_period = None

    for pkey in sorted_periods:
        tot = period_totals[pkey]
        cmds_map = period_cmds[pkey]
        unique_cnt = len(cmds_map)
        top_sum = 0
        cmd_entries = {}
        for cmd in top_cmd_names:
            c_cnt = cmds_map.get(cmd, 0)
            top_sum += c_cnt
            cmd_entries[cmd] = {
                "count": c_cnt,
                "share": round((c_cnt / tot) * 100, 2) if tot > 0 else 0.0,
            }
        others_cnt = max(0, tot - top_sum)

        color_counts = {"W": 0, "U": 0, "B": 0, "R": 0, "G": 0, "C": 0}
        for cmd_name_p, c_cnt_p in cmds_map.items():
            cid = cmd_color_map.get(cmd_name_p, [])
            if not cid:
                color_counts["C"] += c_cnt_p
            else:
                for col in cid:
                    if col in color_counts:
                        color_counts[col] += c_cnt_p

        color_shares = {
            col: round((cnt_val / tot) * 100, 1) if tot > 0 else 0.0
            for col, cnt_val in color_counts.items()
        }

        bucket = {
            "period": pkey,
            "label": _format_history_label(pkey, granularity),
            "total_decks": tot,
            "unique_commanders": unique_cnt,
            "commanders": cmd_entries,
            "top_share": round((top_sum / tot) * 100, 2) if tot > 0 else 0.0,
            "others_count": others_cnt,
            "others_share": round((others_cnt / tot) * 100, 2) if tot > 0 else 0.0,
            "color_shares": color_shares,
        }
        buckets.append(bucket)
        if peak_period is None or tot > peak_period["total_decks"]:
            peak_period = {"period": pkey, "label": bucket["label"], "total_decks": tot}
        if most_diverse_period is None or unique_cnt > most_diverse_period["unique_commanders"]:
            most_diverse_period = {"period": pkey, "label": bucket["label"], "unique_commanders": unique_cnt}

    rising = []
    falling = []
    if len(sorted_periods) >= 2:
        mid = len(sorted_periods) // 2
        first_half = sorted_periods[:mid]
        second_half = sorted_periods[mid:]
        first_total = sum(period_totals[p] for p in first_half)
        second_total = sum(period_totals[p] for p in second_half)

        if first_total > 0 and second_total > 0:
            deltas = []
            for cmd, tot_cmd in global_cmd_totals.items():
                if tot_cmd < 3:
                    continue
                first_cnt = sum(period_cmds[p].get(cmd, 0) for p in first_half)
                second_cnt = sum(period_cmds[p].get(cmd, 0) for p in second_half)
                first_share = (first_cnt / first_total) * 100
                second_share = (second_cnt / second_total) * 100
                delta = round(second_share - first_share, 2)
                if abs(delta) >= 0.2:
                    _, imgs, _, _, color_id = extract_commander_metadata(cmd, scryfall_cache)
                    deltas.append({
                        "name": cmd,
                        "first_share": round(first_share, 2),
                        "second_share": round(second_share, 2),
                        "delta_share": delta,
                        "total_decks": tot_cmd,
                        "color_identity": color_id,
                        "image": imgs[0] if imgs else None,
                    })
            rising = sorted([d for d in deltas if d["delta_share"] > 0], key=lambda x: -x["delta_share"])[:5]
            falling = sorted([d for d in deltas if d["delta_share"] < 0], key=lambda x: x["delta_share"])[:5]

    global_total_decks, _ = get_global_cards_stats()
    try:
        last_upd_iso = datetime.fromtimestamp(get_db_mtime()).isoformat()
    except Exception:
        last_upd_iso = None

    return {
        "granularity": granularity,
        "months": months,
        "top_n": top_n,
        "top_commanders": top_commanders_meta,
        "buckets": buckets,
        "movers": {"rising": rising, "falling": falling},
        "summary": {
            "total_decks": total_decks_window,
            "global_total_decks": global_total_decks,
            "last_updated": last_upd_iso,
            "periods_count": len(buckets),
            "unique_commanders": len(global_cmd_totals),
            "peak_period": peak_period,
            "most_diverse_period": most_diverse_period,
        },
    }

@app.get("/api/metagame/history")
async def get_metagame_history(granularity: str = "month", months: int = 12, top_n: int = 6):
    ensure_daily_caches_fresh()
    return await run_in_threadpool(get_metagame_history_cached, granularity, months, top_n)

@app.get("/api/health")
async def get_health_status():
    try:
        mtime = get_db_mtime()
        db_last_updated = datetime.fromtimestamp(mtime).isoformat()
    except Exception:
        db_last_updated = None

    history = {"moxfield": [], "scryfall": []}
    
    h_conn = get_health_db()
    h_c = h_conn.cursor()
    
    try:
        h_c.execute("SELECT service, status, timestamp FROM health_logs ORDER BY timestamp ASC")
        for row in h_c.fetchall():
            service = row['service']
            if service in history:
                history[service].append({
                    "status": row['status'],
                    "timestamp": row['timestamp']
                })
    except Exception:
        pass
    finally:
        h_conn.close()
        
    return {
        "db_last_updated": db_last_updated,
        "history": history
    }

def _clean_ip(ip_str: Optional[str]) -> Optional[str]:
    if not ip_str:
        return None
    candidate = ip_str.strip()
    try:
        parsed = ipaddress.ip_address(candidate)
        return str(parsed)
    except ValueError:
        return None

def _get_trusted_proxy_networks():
    nets = [
        ipaddress.ip_network("127.0.0.0/8"),
        ipaddress.ip_network("::1/128"),
    ]
    extra = os.getenv("TRUSTED_PROXY_IPS", "")
    for item in extra.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            nets.append(ipaddress.ip_network(item, strict=False))
        except ValueError:
            logging.warning(f"[SECURITY] Voce TRUSTED_PROXY_IPS non valida ignorata: {item}")
    return tuple(nets)

_TRUSTED_PROXY_NETWORKS = _get_trusted_proxy_networks()

def _is_trusted_proxy(ip_str: Optional[str]) -> bool:
    if not ip_str:
        return False
    try:
        addr = ipaddress.ip_address(ip_str.strip())
        return any(addr in net for net in _TRUSTED_PROXY_NETWORKS)
    except ValueError:
        return False

def get_client_ip(request: Request) -> str:
    peer_ip = _clean_ip(request.client.host) if (request.client and request.client.host) else None
    if not _is_trusted_proxy(peer_ip):
        return peer_ip or "127.0.0.1"

    # 1. CF-Connecting-IP: sovrascritto in modo sicuro dall'edge di Cloudflare
    cf_ip = _clean_ip(request.headers.get("cf-connecting-ip"))
    if cf_ip:
        return cf_ip

    # 2. X-Real-IP: impostato da reverse proxy locali (es. Nginx)
    x_real_ip = _clean_ip(request.headers.get("x-real-ip"))
    if x_real_ip:
        return x_real_ip

    # 3. X-Forwarded-For: analizza a ritroso la catena saltando eventuali proxy fidati
    x_forwarded_for = request.headers.get("x-forwarded-for")
    if x_forwarded_for:
        parts = [p.strip() for p in x_forwarded_for.split(",") if p.strip()]
        for part in reversed(parts):
            valid_ip = _clean_ip(part)
            if valid_ip and not _is_trusted_proxy(valid_ip):
                return valid_ip
        for part in reversed(parts):
            valid_ip = _clean_ip(part)
            if valid_ip:
                return valid_ip

    # 4. Connessione TCP diretta dal proxy
    return peer_ip or "127.0.0.1"

def _hash_admin_rl_ip(request: Request) -> str:
    ip = get_client_ip(request)
    day_salt = datetime.now().strftime("%Y-%m-%d")
    return hashlib.sha256(f"admin_rl:{day_salt}:{ip}".encode("utf-8")).hexdigest()

_ADMIN_FAILURES: dict[str, list[float]] = {}
_ADMIN_LOCK = threading.Lock()

def _check_admin_rate_limit(request: Request):
    ip_key = _hash_admin_rl_ip(request)
    now_ts = std_time.time()
    cutoff = now_ts - 3600.0
    with _ADMIN_LOCK:
        for k in list(_ADMIN_FAILURES.keys()):
            _ADMIN_FAILURES[k] = [t for t in _ADMIN_FAILURES[k] if t > cutoff]
            if not _ADMIN_FAILURES[k]:
                del _ADMIN_FAILURES[k]
        recent_failures = _ADMIN_FAILURES.get(ip_key, [])
        if len(recent_failures) >= 10:
            raise HTTPException(status_code=429, detail="Troppi tentativi admin falliti da questo indirizzo IP. Riprova più tardi.")

def _record_admin_failure(request: Request):
    ip_key = _hash_admin_rl_ip(request)
    now_ts = std_time.time()
    cutoff = now_ts - 3600.0
    with _ADMIN_LOCK:
        for k in list(_ADMIN_FAILURES.keys()):
            _ADMIN_FAILURES[k] = [t for t in _ADMIN_FAILURES[k] if t > cutoff]
            if not _ADMIN_FAILURES[k]:
                del _ADMIN_FAILURES[k]
        failures = _ADMIN_FAILURES.get(ip_key, [])
        failures.append(now_ts)
        _ADMIN_FAILURES[ip_key] = failures

@app.delete("/api/admin/remove-deck/{deck_id}", include_in_schema=False)
async def remove_deck(
    request: Request,
    deck_id: str, 
    x_admin_secret: Optional[str] = Header(None, alias="X-Admin-Secret")
):
    _check_admin_rate_limit(request)
    admin_password = os.getenv("ADMIN_SECRET")
    
    if not admin_password or not x_admin_secret:
        _record_admin_failure(request)
        raise HTTPException(status_code=403, detail="Accesso negato: credenziali non valide")
        
    if not secrets.compare_digest(x_admin_secret.encode('utf-8'), admin_password.encode('utf-8')):
        _record_admin_failure(request)
        raise HTTPException(status_code=403, detail="Accesso negato: credenziali non valide")
        
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("DELETE FROM deck_cards WHERE deck_id = ?", (deck_id,))
        c.execute("DELETE FROM decks WHERE id_moxfield = ?", (deck_id,))
        now = datetime.now().isoformat()
        c.execute("INSERT OR IGNORE INTO blacklist (id_moxfield, data_rimozione) VALUES (?, ?)", (deck_id, now))
        conn.commit()
        
        clear_all_caches()
        
        return {"status": "success", "message": f"Mazzo {deck_id} rimosso e aggiunto alla blacklist."}
    except Exception as e:
        logging.error(f"[ADMIN ERROR] Errore rimozione mazzo {deck_id}: {e}")
        raise HTTPException(status_code=500, detail="Errore interno durante l'operazione")
    finally:
        conn.close()
