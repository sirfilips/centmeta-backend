import os
import re
import json
import sqlite3
import requests
import asyncio
import time as std_time
import threading
import logging
import secrets
import hashlib
import random
from datetime import datetime, time, timedelta
from fastapi import FastAPI, HTTPException, Header, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.concurrency import run_in_threadpool
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager
from functools import lru_cache
from pydantic import BaseModel, Field
import master_scraper

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
            "User-Agent": "CentMeta/1.0 (contact@centmeta.it; https://centmeta.it)",
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

def get_community_db():
    conn = sqlite3.connect(COMMUNITY_DB_PATH, timeout=30.0)
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
            parts = [part.strip() for part in commander_name.split(' / ')]
            parts.sort()
            new_name = " / ".join(parts)
            
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
        c.execute("CREATE INDEX IF NOT EXISTS idx_decks_comandante ON decks(comandante)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_decks_data ON decks(data_aggiornamento)")

        existing_idx = {
            row[0]: (row[1] or "")
            for row in c.execute(
                "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='deck_cards'"
            ).fetchall()
        }
        needs_analyze = False
        if "(deck_id, card_name)" not in existing_idx.get("idx_deck_cards_deck_id", ""):
            c.execute("DROP INDEX IF EXISTS idx_deck_cards_deck_id")
            c.execute("CREATE INDEX idx_deck_cards_deck_id ON deck_cards(deck_id, card_name)")
            needs_analyze = True
        if "(card_name, deck_id)" not in existing_idx.get("idx_deck_cards_card_name", ""):
            c.execute("DROP INDEX IF EXISTS idx_deck_cards_card_name")
            c.execute("CREATE INDEX idx_deck_cards_card_name ON deck_cards(card_name, deck_id)")
            needs_analyze = True

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
_SCRYFALL_COOLDOWN_UNTIL = 0.0
_SCRYFALL_MISSING_CACHE = set()

def clear_all_caches():
    global _SCRYFALL_CACHE_MEMORY
    with _scryfall_lock:
        _SCRYFALL_CACHE_MEMORY = None
        _SCRYFALL_MISSING_CACHE.clear()
    get_global_commanders_validity.cache_clear()
    get_global_cards_stats.cache_clear()
    get_valid_commanders_set.cache_clear()
    get_decks_stats_cached.cache_clear()
    get_card_details_cached.cache_clear()
    get_commander_decks_cached.cache_clear()
    get_dashboard_data_cached.cache_clear()
    print("[CACHE] Tutte le cache in memoria sono state svuotate.")

def setup_health_db(init_main_blacklist: bool = True):
    if init_main_blacklist:
        conn = get_db()
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS blacklist (
                id_moxfield TEXT PRIMARY KEY,
                data_rimozione TEXT
            )
        ''')
        conn.commit()
        conn.close()

    h_conn = get_health_db()
    h_c = h_conn.cursor()
    h_c.execute('''
        CREATE TABLE IF NOT EXISTS health_logs (
            timestamp TEXT,
            service TEXT,
            status TEXT
        )
    ''')
    h_conn.commit()
    h_conn.close()

def setup_community_db():
    conn = get_community_db()
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS community_ratings (
                card_name TEXT PRIMARY KEY,
                total_votes INTEGER NOT NULL DEFAULT 0,
                score_sum INTEGER NOT NULL DEFAULT 0,
                avg_score REAL NOT NULL DEFAULT 0.0,
                stars_1 INTEGER NOT NULL DEFAULT 0,
                stars_2 INTEGER NOT NULL DEFAULT 0,
                stars_3 INTEGER NOT NULL DEFAULT 0,
                stars_4 INTEGER NOT NULL DEFAULT 0,
                stars_5 INTEGER NOT NULL DEFAULT 0,
                last_voted_at TEXT
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS community_votes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                card_name TEXT NOT NULL,
                voter_hash TEXT NOT NULL,
                score INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(voter_hash, card_name)
            )
        ''')
        c.execute("CREATE INDEX IF NOT EXISTS idx_community_votes_voter ON community_votes(voter_hash)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_community_votes_card ON community_votes(card_name)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_community_ratings_avg ON community_ratings(avg_score DESC, total_votes DESC)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_community_ratings_votes ON community_ratings(total_votes DESC)")
        conn.commit()
    finally:
        conn.close()

def _perform_health_pings():
    setup_health_db(init_main_blacklist=False)
    now = datetime.now().isoformat()
    conn = get_health_db()
    c = conn.cursor()
    
    moxfield_ua = os.getenv("MOXFIELD_UA")
    if moxfield_ua:
        moxfield_ua = moxfield_ua.strip()
    
    # 1. Ping Scryfall
    try:
        res = get_scryfall_session().get(
            "https://api.scryfall.com/cards/named?exact=Sol Ring",
            timeout=10
        )
        if res.status_code == 200:
            status = "operational"
        elif res.status_code == 429:
            status = "rate_limited"
        else:
            status = "error"
    except Exception as req_err:
        logging.warning(f"[HEALTH WARNING] Rilevata anomalia connessione scryfall: {req_err}")
        status = "down"
        
    c.execute("INSERT INTO health_logs (timestamp, service, status) VALUES (?, ?, ?)", (now, "scryfall", status))
    c.execute("""
        DELETE FROM health_logs 
        WHERE service=? AND rowid NOT IN (
            SELECT rowid FROM health_logs 
            WHERE service=? ORDER BY timestamp DESC LIMIT 48
        )
    """, ("scryfall", "scryfall"))
    
    # 2. Ping Moxfield
    if not moxfield_ua:
        logging.warning("[HEALTH WARNING] Variabile MOXFIELD_UA non impostata. Salto il ping a Moxfield.")
    else:
        try:
            res = requests.get(
                "https://api.moxfield.com/v2/decks/search?pageNumber=1&pageSize=1&fmt=centurion",
                headers={"User-Agent": moxfield_ua},
                timeout=10
            )
            if res.status_code == 200:
                status = "operational"
            elif res.status_code == 429:
                status = "rate_limited"
            else:
                status = "error"
        except Exception as req_err:
            err_msg = str(req_err)
            if moxfield_ua and moxfield_ua in err_msg:
                err_msg = err_msg.replace(moxfield_ua, "[REDACTED_UA]")
            logging.warning(f"[HEALTH WARNING] Rilevata anomalia connessione moxfield: {err_msg}")
            status = "down"
            
        c.execute("INSERT INTO health_logs (timestamp, service, status) VALUES (?, ?, ?)", (now, "moxfield", status))
        c.execute("""
            DELETE FROM health_logs 
            WHERE service=? AND rowid NOT IN (
                SELECT rowid FROM health_logs 
                WHERE service=? ORDER BY timestamp DESC LIMIT 48
            )
        """, ("moxfield", "moxfield"))
    
    conn.commit()
    conn.close()
    print(f"[HEALTH] Ping orario completato alle {now}")

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
            
            await run_in_threadpool(master_scraper.run_scraper)
            clear_all_caches()
            
            # Alle 23:00 scalda tutti i periodi
            await warmup_caches()
            
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[CRON ERROR] Errore aggiornamento: {e}")
            await asyncio.sleep(3600)

CARTELLA_SCRIPT = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(CARTELLA_SCRIPT, "centurion.db")
HEALTH_DB_PATH = os.path.join(CARTELLA_SCRIPT, "health.db")
COMMUNITY_DB_PATH = os.path.join(CARTELLA_SCRIPT, "community.db")
CACHE_FILE = os.path.join(CARTELLA_SCRIPT, "scryfall_cache.json")
COMMUNITY_SALT = os.getenv("COMMUNITY_SALT", "centmeta_community_salt_secret_2026")

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
    setup_community_db()
    cleanup_lands_from_community_db()
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

    yield  # <-- Il server apre la porta ORA ed è subito accessibile

    if warmup_task:
        warmup_task.cancel()
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
            return _SCRYFALL_CACHE_MEMORY
        try:
            if os.path.exists(CACHE_FILE):
                with open(CACHE_FILE, "r", encoding="utf-8") as f:
                    _SCRYFALL_CACHE_MEMORY = json.load(f)
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
        return _SCRYFALL_CACHE_MEMORY

def save_scryfall_cache(cache):
    global _SCRYFALL_CACHE_MEMORY
    with _scryfall_lock:
        _SCRYFALL_CACHE_MEMORY = cache
        snapshot = cache.copy()
        
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

    return {
        "full_name": name,
        "images": images,
        "art_crops": art_crops,
        "oracles": oracles,
        "type": type_line,
        "color_identity": card.get("color_identity", []),
        "cmc": card.get("cmc", 0.0)
    }

def _is_valid_cached_card(entry: Optional[dict]) -> bool:
    if not entry or not entry.get("images"):
        return False
    if entry.get("type") in ("Card", " Card // Card"):
        return False
    return True

def fetch_scryfall_data(card_names):
    global _SCRYFALL_COOLDOWN_UNTIL
    results = load_scryfall_cache()
    if std_time.time() < _SCRYFALL_COOLDOWN_UNTIL:
        logging.warning("[SCRYFALL] Circuit breaker attivo: salto chiamate API esterne.")
        return results
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
    with _scryfall_lock:
        # Rimuove eventuali voci corrotte "Card" (Jumpstart theme cards)
        for bad_key in [k for k, v in results.items() if v.get("type") in ("Card", " Card // Card")]:
            del results[bad_key]
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
                cache_updated_locally = True
                continue
            mancanti.append(n)

    if cache_updated_locally and not mancanti:
        save_scryfall_cache(results)
            
    if mancanti:
        session = get_scryfall_session()
        weird_sets = {"sta", "otp", "wot", "mul", "brr", "sld", "mps", "mp2", "exp", "mb1", "plist", "h1r", "slx"}

        for i in range(0, len(mancanti), 75):
            if i > 0:
                std_time.sleep(0.12)
                
            chunk = mancanti[i:i+75]
            identifiers = [{"name": n} for n in chunk]
            updated_in_chunk = cache_updated_locally
            cache_updated_locally = False
            
            try:
                res = session.post(
                    "https://api.scryfall.com/cards/collection", 
                    json={"identifiers": identifiers}, 
                    timeout=15
                )
                if res.status_code in (429, 403):
                    logging.warning(f"[SCRYFALL CIRCUIT BREAKER] Ricevuto HTTP {res.status_code}. Pausa forzata di 5 minuti.")
                    _SCRYFALL_COOLDOWN_UNTIL = std_time.time() + 300
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
                            std_time.sleep(0.12) 
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
                            if " // " in name and len(faces_names) > 1:
                                f0 = faces_names[0].strip().lower()
                                f1 = faces_names[1].strip().lower()
                                if not _is_valid_cached_card(results.get(f0)) or " // " in results[f0].get("full_name", " // "):
                                    results[f0] = card_data
                                if not _is_valid_cached_card(results.get(f1)) or " // " in results[f1].get("full_name", " // "):
                                    results[f1] = card_data

                            # Allinea eventuali alias richiesti nel chunk (es. "Waste Land" -> "Wasteland")
                            norm_target = name.lower().replace(" ", "").replace("-", "")
                            for req_n in chunk:
                                req_k = req_n.lower()
                                if req_k not in results and req_k.replace(" ", "").replace("-", "") == norm_target:
                                    results[req_k] = card_data

                        updated_in_chunk = True

                    for nf in body.get("not_found", []):
                        nf_name = nf.get("name")
                        if not nf_name:
                            continue
                        nf_key = nf_name.lower()

                        # Fallback per carte Arena "A-..."
                        if nf_key.startswith("a-") and _is_valid_cached_card(results.get(nf_key[2:])):
                            with _scryfall_lock:
                                results[nf_key] = results[nf_key[2:]]
                            updated_in_chunk = True
                            continue

                        # Fallback con /cards/named per carte Split/DFC non risolte da /cards/collection (es. "Armed // Dangerous")
                        resolved_via_named = False
                        if " // " in nf_name:
                            try:
                                std_time.sleep(0.12)
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
                                            for f_part in faces:
                                                fk = f_part.lower()
                                                if not _is_valid_cached_card(results.get(fk)) or " // " in results[fk].get("full_name", " // "):
                                                    results[fk] = card_data
                                        updated_in_chunk = True
                                        resolved_via_named = True
                            except Exception as e:
                                logging.warning(f"Errore fallback Scryfall named per {nf_name}: {e}")

                        if not resolved_via_named:
                            with _scryfall_lock:
                                _SCRYFALL_MISSING_CACHE.add(nf_key)
                                
            except Exception as e:
                logging.warning(f"Errore API Scryfall collection: {e}")
            
            if updated_in_chunk:
                save_scryfall_cache(results)
                    
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
    q_curr = f"SELECT comandante, COUNT(*) as cnt FROM decks {base_where} GROUP BY comandante ORDER BY cnt DESC"
    curr_rows = c.execute(q_curr, curr_params).fetchall()
    
    if prev_cond:
        prev_conds = [prev_cond]
        prev_params = []
        if invalid_cmds:
            placeholders = "(" + ",".join(["?"] * len(invalid_cmds)) + ")"
            prev_conds.append(f"comandante NOT IN {placeholders}")
            prev_params.extend(invalid_cmds)
        q_prev = f"SELECT comandante, COUNT(*) as cnt FROM decks WHERE {' AND '.join(prev_conds)} GROUP BY comandante ORDER BY cnt DESC"
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
            result_list.append({
                "comandante": name,
                "cnt": int(row['cnt']),
                "trend": trend_out,
                "images": images,
                "art_crops": art_crops,
                "oracles": oracles,
                "color_identity": cmd_color_identity
            })
            
    return frozenset(valid_set), result_list

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

    q_curr_cmd = f"SELECT comandante, COUNT(*) as cnt FROM decks {base_where} GROUP BY comandante ORDER BY cnt DESC"
    curr_cmd_rows = c.execute(q_curr_cmd, inv_params).fetchall()
    
    if prev_cond:
        prev_where = build_where(prev_cond, False)
        q_prev_cmd = f"SELECT comandante, COUNT(*) as cnt FROM decks {prev_where} GROUP BY comandante ORDER BY cnt DESC"
        prev_cmd_rows = c.execute(q_prev_cmd, inv_params).fetchall()
        prev_cmd_ranks = {r['comandante']: idx + 1 for idx, r in enumerate(prev_cmd_rows)}
        
        df_top_cmd = []
        for idx, r in enumerate(curr_cmd_rows[:25]):
            cmd = r['comandante']
            trend = prev_cmd_ranks.get(cmd, idx + 1) - (idx + 1) if cmd in prev_cmd_ranks else None
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
            
    df_hot_cmd.sort(key=lambda x: x['delta'], reverse=True)
    df_hot_cmd = df_hot_cmd[:50]

    where_curr = build_where(curr_cond, True)
    where_prev = build_where(prev_cond, True)
    
    q_curr_cards = f"""
        SELECT c.card_name, COUNT(DISTINCT c.deck_id) as freq 
        FROM deck_cards c JOIN decks d ON c.deck_id = d.id_moxfield 
        {where_curr}
        GROUP BY c.card_name ORDER BY freq DESC
    """
    curr_card_rows = c.execute(q_curr_cards, inv_params).fetchall()
    
    if prev_cond:
        q_prev_cards = f"""
            SELECT c.card_name, COUNT(DISTINCT c.deck_id) as freq 
            FROM deck_cards c JOIN decks d ON c.deck_id = d.id_moxfield 
            {where_prev}
            GROUP BY c.card_name ORDER BY freq DESC
        """
        prev_card_rows = c.execute(q_prev_cards, inv_params).fetchall()
        prev_card_ranks = {r['card_name']: idx + 1 for idx, r in enumerate(prev_card_rows)}
        
        df_top_cards = []
        for idx, r in enumerate(curr_card_rows[:250]):
            c_name = r['card_name']
            trend = prev_card_ranks.get(c_name, idx + 1) - (idx + 1) if c_name in prev_card_ranks else None
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
            
    df_hot_c.sort(key=lambda x: x['delta'], reverse=True)
    df_hot_c = df_hot_c[:250]
        
    conn.close()

    all_names = set()
    for r in df_top_cmd: all_names.add(r['comandante'])
    for r in df_hot_cmd: all_names.add(r['comandante'])
    for r in df_top_cards: all_names.add(r['card_name'])
    for r in df_hot_c: all_names.add(r['card_name'])
    
    scryfall_cache = fetch_scryfall_data(list(all_names))

    def format_item(row, name_col, is_cmd=False):
        name = row[name_col]
        if is_cmd:
            _, images, _, _, final_color_identity = extract_commander_metadata(name, scryfall_cache)
            cat = "Comandante"
        else:
            main_info = lookup_card_info(scryfall_cache, name)
            images = list(dict.fromkeys(main_info.get("images", [])))
            cat = categorizza_tipo(main_info.get("type", ""))
            final_color_identity = main_info.get("color_identity", [])

        t_val = row.get('trend')
        if t_val is None:
            trend_out = None if prev_cond else ""
        else:
            trend_out = int(t_val) if t_val != "" else ""

        return {
            "name": name,
            "images": images,
            "val1": int(row.get('cnt', row.get('freq', row.get('cnt1', 0)))),
            "trend": trend_out,
            "delta": int(row.get('delta', 0)) if 'delta' in row else None,
            "category": cat,
            "color_identity": final_color_identity
        }

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
        "hot_cards": [format_item(row, 'card_name', False) for row in df_hot_c]
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
        
    c.execute("PRAGMA table_info(deck_cards)")
    columns = [info[1].lower() for info in c.fetchall()]
    
    qty_col = None
    for col in ['qty', 'quantita', 'quantity', 'copies', 'copie']:
        if col in columns:
            qty_col = col
            break
            
    where_clause_join = where_clause.replace("comandante", "d.comandante").replace("data_aggiornamento", "d.data_aggiornamento")
    
    if qty_col:
        q_cards = f"""
            SELECT c.card_name, COUNT(DISTINCT c.deck_id) as freq, SUM(c.{qty_col}) as total_copies 
            FROM deck_cards c
            JOIN decks d ON c.deck_id = d.id_moxfield
            {where_clause_join}
            GROUP BY c.card_name
        """
    else:
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
        total_copies = row['total_copies'] if qty_col else None
        
        perc = round((freq / total_decks) * 100, 1)
        g_freq = global_cards.get(c_name, 0)
        g_perc = (g_freq / global_total) * 100
        sinergia = round(perc - g_perc, 1)
        
        stats_list.append({
            'card_name': c_name,
            'freq': freq,
            'total_copies': total_copies,
            'Percentuale': perc,
            'Sinergia': sinergia
        })
        
    stats_list.sort(key=lambda x: (x['Percentuale'], x['freq']), reverse=True)
    
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
            
        cat = categorizza_tipo(info.get("type", ""))
        
        if row['total_copies'] is not None:
            avg_copies_comp = float(row['total_copies']) / total_decks
        else:
            avg_copies_comp = float(row['freq']) / total_decks
            somma_univoche += avg_copies_comp
        
        if cat in type_dist:
            type_dist[cat] += avg_copies_comp
        elif cat == "Altro" and "Land" in info.get("type", ""):
            type_dist["Terre"] += avg_copies_comp
        
        if cat not in ["Terre", "Altro"] and "Land" not in info.get("type", ""):
            cmc = int(info.get("cmc", 0))
            if cmc >= 6: cmc = 6
            mana_curve[cmc] += avg_copies_comp
            
        formatted_cards.append({
            "card_name": c_name,
            "freq": int(row['freq']),
            "Percentuale": float(row['Percentuale']),
            "Sinergia": float(row['Sinergia']),
            "images": info.get("images", []),
            "art_crops": info.get("art_crops", []),
            "oracles": info.get("oracles", []),
            "color_identity": info.get("color_identity", []),
            "category": cat
        })
        
    if not qty_col:
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
    scryfall_cache = load_scryfall_cache()
    if not lookup_card_info(scryfall_cache, card_name):
        scryfall_cache = fetch_scryfall_data([card_name])
    card_info_cache = lookup_card_info(scryfall_cache, card_name)

    if not valid_all:
        return {
            "card_name": card_name,
            "images": card_info_cache.get("images", []),
            "art_crops": card_info_cache.get("art_crops", []),
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

    q_tot_glob = f"SELECT COUNT(DISTINCT d.id_moxfield) as tot FROM decks d JOIN deck_cards c ON d.id_moxfield = c.deck_id WHERE c.card_name = ?{inv_cond_d}"
    total_decks_global = c.execute(q_tot_glob, [card_name] + inv_params).fetchone()['tot']
    
    if total_decks_global == 0:
        conn.close()
        return {
            "card_name": card_name,
            "images": card_info_cache.get("images", []),
            "art_crops": card_info_cache.get("art_crops", []),
            "oracles": card_info_cache.get("oracles", []),
            "total_decks_with_card": 0,
            "total_decks_global": 0,
            "commanders": [],
            "synergies": []
        }
    
    q_tot_time = f"SELECT COUNT(DISTINCT d.id_moxfield) as tot FROM decks d JOIN deck_cards c ON d.id_moxfield = c.deck_id WHERE c.card_name = ?{inv_cond_d}{time_filtered_cond}"
    total_decks_time = c.execute(q_tot_time, [card_name] + inv_params).fetchone()['tot']

    if total_decks_time == 0:
        conn.close()
        return {
            "card_name": card_name,
            "images": card_info_cache.get("images", []),
            "art_crops": card_info_cache.get("art_crops", []),
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
    res_cmd.sort(key=lambda x: (x['Percentuale'], x['mazzi_carta']), reverse=True)
    
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
            "art_crops": art_crops
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
    syn_df.sort(key=lambda x: (x['Percentuale'], x['freq']), reverse=True)
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
            "oracles": inf.get("oracles", []),
            "color_identity": inf.get("color_identity", []),
            "category": categorizza_tipo(inf.get("type", ""))
        })
        
    return {
        "card_name": card_name,
        "images": card_info_cache.get("images", []),
        "art_crops": card_info_cache.get("art_crops", []),
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
    filtro_tempo = normalize_period(filtro_tempo)
    _, result_list = await run_in_threadpool(get_valid_commanders_set, filtro_tempo)
    return result_list

@app.get("/api/decks/stats")
async def get_decks_stats(filtro_tempo: str = "Tutti i tempi", comandante: Optional[str] = None, limit: int = 0, offset: int = 0):
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
        
    return data

@app.get("/api/card/details")
async def get_card_details(card_name: str, filtro_tempo: str = "Tutti i tempi"):
    filtro_tempo = normalize_period(filtro_tempo)
    return await run_in_threadpool(get_card_details_cached, card_name, filtro_tempo)

@app.get("/api/commander/decks")
async def get_commander_decks(comandante: str, filtro_tempo: str = "Tutti i tempi", limit: int = 0, offset: int = 0):
    filtro_tempo = normalize_period(filtro_tempo)
    limit = max(0, min(limit, 500))
    offset = max(0, offset)
    return await run_in_threadpool(get_commander_decks_cached, comandante, filtro_tempo, limit, offset)

@app.get("/api/home/dashboard")
async def get_dashboard(filtro_tempo: str = "Tutti i tempi"):
    filtro_tempo = normalize_period(filtro_tempo)
    return await run_in_threadpool(get_dashboard_data_cached, filtro_tempo)

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

@app.delete("/api/admin/remove-deck/{deck_id}", include_in_schema=False)
async def remove_deck(
    deck_id: str, 
    x_admin_secret: Optional[str] = Header(None, alias="X-Admin-Secret")
):
    admin_password = os.getenv("ADMIN_SECRET")
    
    if not admin_password or not x_admin_secret:
        raise HTTPException(status_code=403, detail="Accesso negato: credenziali non valide")
        
    if not secrets.compare_digest(x_admin_secret.encode('utf-8'), admin_password.encode('utf-8')):
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

# ==============================================================================
# COMMUNITY RATING SYSTEM ("Tutte le carte di magic sono scarse tranne le carte forti")
# ==============================================================================

BASIC_LANDS = {
    "plains", "island", "swamp", "mountain", "forest", "wastes",
    "snow-covered plains", "snow-covered island", "snow-covered swamp",
    "snow-covered mountain", "snow-covered forest"
}

def is_land_card(card_name: str, scryfall_cache: Optional[dict] = None) -> bool:
    if not card_name:
        return False
    if card_name.lower() in BASIC_LANDS:
        return True
    if scryfall_cache is None:
        scryfall_cache = load_scryfall_cache()
    info = lookup_card_info(scryfall_cache, card_name)
    if not info:
        return False
    type_line = info.get("type", "")
    front_type = type_line.split(" // ")[0] if " // " in type_line else type_line
    return bool(re.search(r'\bLand\b', front_type, re.IGNORECASE))

def cleanup_lands_from_community_db():
    conn = get_community_db()
    c = conn.cursor()
    try:
        scryfall_cache = load_scryfall_cache()
        if not scryfall_cache:
            return
        rows = c.execute("SELECT card_name FROM community_ratings").fetchall()
        lands = [r['card_name'] for r in rows if is_land_card(r['card_name'], scryfall_cache)]
        if lands:
            placeholders = ",".join("?" for _ in lands)
            c.execute(f"DELETE FROM community_ratings WHERE card_name IN ({placeholders})", lands)
            c.execute(f"DELETE FROM community_votes WHERE card_name IN ({placeholders})", lands)
            conn.commit()
            print(f"[COMMUNITY DB] Eliminate {len(lands)} terre dal database di valutazione.")
    finally:
        conn.close()

def get_client_ip(request: Request) -> str:
    cf_ip = request.headers.get("cf-connecting-ip")
    if cf_ip:
        return cf_ip.strip()
    x_forwarded_for = request.headers.get("x-forwarded-for")
    if x_forwarded_for:
        return x_forwarded_for.split(",")[0].strip()
    x_real_ip = request.headers.get("x-real-ip")
    if x_real_ip:
        return x_real_ip.strip()
    if request.client:
        return request.client.host
    return "127.0.0.1"

def get_voter_hash(request: Request) -> str:
    ip = get_client_ip(request)
    ua = request.headers.get("user-agent", "")
    payload = f"{COMMUNITY_SALT}:{ip}:{ua}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

_COMMUNITY_POOL_DATE = ""
_COMMUNITY_CORE_100 = []
_COMMUNITY_DAILY_50 = []
_COMMUNITY_FALLBACK_ALL = []
_DECKS_COUNT_30D_CACHE = {}
_DECKS_COUNT_30D_DATE = ""

def get_card_30d_decks_count(card_name: str) -> int:
    global _DECKS_COUNT_30D_CACHE, _DECKS_COUNT_30D_DATE
    today_str = datetime.now().strftime("%Y-%m-%d")
    if _DECKS_COUNT_30D_DATE != today_str:
        _DECKS_COUNT_30D_CACHE.clear()
        _DECKS_COUNT_30D_DATE = today_str

    if card_name in _DECKS_COUNT_30D_CACHE:
        return _DECKS_COUNT_30D_CACHE[card_name]

    conn = get_db()
    c = conn.cursor()
    try:
        date_limit_30d = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        cnt = c.execute("""
            SELECT COUNT(DISTINCT d.id_moxfield) as cnt 
            FROM deck_cards dc
            JOIN decks d ON dc.deck_id = d.id_moxfield
            WHERE dc.card_name = ? AND d.data_aggiornamento >= ?
        """, (card_name, date_limit_30d)).fetchone()['cnt']
        _DECKS_COUNT_30D_CACHE[card_name] = cnt
        return cnt
    finally:
        conn.close()

def get_community_card_pools():
    global _COMMUNITY_POOL_DATE, _COMMUNITY_CORE_100, _COMMUNITY_DAILY_50, _COMMUNITY_FALLBACK_ALL
    today_str = datetime.now().strftime("%Y-%m-%d")
    if _COMMUNITY_POOL_DATE == today_str and _COMMUNITY_CORE_100 and _COMMUNITY_DAILY_50:
        return _COMMUNITY_CORE_100, _COMMUNITY_DAILY_50, _COMMUNITY_FALLBACK_ALL

    conn = get_db()
    c = conn.cursor()
    try:
        # Peschiamo le carte dai mazzi aggiornati negli ultimi 60 giorni, ordinate per diffusione
        date_limit = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")
        rows = c.execute("""
            SELECT dc.card_name, COUNT(DISTINCT d.id_moxfield) as cnt
            FROM deck_cards dc
            JOIN decks d ON dc.deck_id = d.id_moxfield
            WHERE d.data_aggiornamento >= ?
            GROUP BY dc.card_name
            ORDER BY cnt DESC
        """, (date_limit,)).fetchall()

        scryfall_cache = load_scryfall_cache()
        filtered_cards = [r['card_name'] for r in rows if not is_land_card(r['card_name'], scryfall_cache)]

        # Top 100 fisse (le colonne portanti del meta)
        core_100 = filtered_cards[:100]

        # 50 carte a rotazione giornaliera pescate deterministicamente tra le posizioni 101 e 600
        rotation_candidates = filtered_cards[100:600]
        if not rotation_candidates:
            rotation_candidates = filtered_cards[100:]

        rng = random.Random(f"centmeta-daily-{today_str}")
        if len(rotation_candidates) >= 50:
            daily_50 = rng.sample(rotation_candidates, 50)
        else:
            daily_50 = rotation_candidates

        # Fallback completo (tutte le carte non base del catalogo) per utenti che completano il pool
        rows_all = c.execute("SELECT DISTINCT card_name FROM deck_cards").fetchall()
        fallback_all = [r['card_name'] for r in rows_all if not is_land_card(r['card_name'], scryfall_cache)]

        _COMMUNITY_POOL_DATE = today_str
        _COMMUNITY_CORE_100 = core_100
        _COMMUNITY_DAILY_50 = daily_50
        _COMMUNITY_FALLBACK_ALL = fallback_all
        return _COMMUNITY_CORE_100, _COMMUNITY_DAILY_50, _COMMUNITY_FALLBACK_ALL
    finally:
        conn.close()

def get_random_community_card(voter_hash: str):
    core_100, daily_50, fallback_all = get_community_card_pools()
    today_active_pool = core_100 + daily_50

    if not today_active_pool:
        today_active_pool = fallback_all
    if not today_active_pool:
        raise HTTPException(status_code=404, detail="Nessuna carta disponibile nel database")

    # Ottieni le carte già votate da questo utente e quelle in fase di consolidamento (1-4 voti totali)
    comm_conn = get_community_db()
    c_comm = comm_conn.cursor()
    try:
        voted_rows = c_comm.execute("SELECT card_name FROM community_votes WHERE voter_hash = ?", (voter_hash,)).fetchall()
        voted_set = {r['card_name'] for r in voted_rows}

        partial_rows = c_comm.execute("""
            SELECT card_name FROM community_ratings 
            WHERE total_votes >= 1 AND total_votes < 5
        """).fetchall()
        partial_votes_set = {r['card_name'] for r in partial_rows}
    finally:
        comm_conn.close()

    # Filtra le carte non ancora votate da questo utente tra le 150 attive oggi
    unvoted_pool = [card for card in today_active_pool if card not in voted_set]

    # Se l'utente ha già votato tutte le 150 del giorno, pesca dal catalogo generale
    if not unvoted_pool:
        unvoted_pool = [card for card in fallback_all if card not in voted_set]
        if not unvoted_pool:
            # Ha votato tutte le carte esistenti: ricomincia dal pool di oggi
            unvoted_pool = today_active_pool

    # CACCIA AL QUORUM: 65% di probabilità di proporre una carta che ha già 1-4 voti per portarla a 5
    quorum_candidates = [card for card in unvoted_pool if card in partial_votes_set]
    if quorum_candidates and random.random() < 0.65:
        chosen_card = random.choice(quorum_candidates)
    else:
        chosen_card = random.choice(unvoted_pool)

    # Identificazione tipologia carta nel pool odierno
    is_core_staple = chosen_card in core_100
    is_daily_spotlight = chosen_card in daily_50

    # Recupera i dettagli Scryfall
    scryfall_cache = load_scryfall_cache()
    info = lookup_card_info(scryfall_cache, chosen_card)
    if not info:
        scryfall_cache = fetch_scryfall_data([chosen_card])
        info = lookup_card_info(scryfall_cache, chosen_card) or {}

    # Calcola il numero di mazzi negli ultimi 30 giorni in cui compare la carta (in-memory cache)
    decks_count = get_card_30d_decks_count(chosen_card)

    # Statistiche di valutazione attuali per questa carta
    comm_conn = get_community_db()
    c_comm = comm_conn.cursor()
    try:
        r_row = c_comm.execute("""
            SELECT total_votes, avg_score, stars_1, stars_2, stars_3, stars_4, stars_5
            FROM community_ratings WHERE card_name = ?
        """, (chosen_card,)).fetchone()

        user_vote_row = c_comm.execute("""
            SELECT score FROM community_votes WHERE voter_hash = ? AND card_name = ?
        """, (voter_hash, chosen_card)).fetchone()
    finally:
        comm_conn.close()

    community_stats = {
        "total_votes": r_row['total_votes'] if r_row else 0,
        "avg_score": r_row['avg_score'] if r_row else 0.0,
        "stars_breakdown": {
            1: r_row['stars_1'] if r_row else 0,
            2: r_row['stars_2'] if r_row else 0,
            3: r_row['stars_3'] if r_row else 0,
            4: r_row['stars_4'] if r_row else 0,
            5: r_row['stars_5'] if r_row else 0,
        },
        "user_score": user_vote_row['score'] if user_vote_row else None
    }

    today_active_set = set(today_active_pool)
    voted_today_count = len(today_active_set.intersection(voted_set))
    pool_size = len(today_active_pool)

    return {
        "card_name": chosen_card,
        "images": info.get("images", []),
        "art_crops": info.get("art_crops", []),
        "oracles": info.get("oracles", []),
        "type": info.get("type", "Carta"),
        "cmc": info.get("cmc", 0.0),
        "color_identity": info.get("color_identity", []),
        "decks_count": decks_count,
        "is_core_staple": is_core_staple,
        "is_daily_spotlight": is_daily_spotlight,
        "is_recent_pool": is_core_staple or is_daily_spotlight,
        "community_stats": community_stats,
        "daily_progress": {
            "voted_today": voted_today_count,
            "pool_size": pool_size,
            "total_voted": len(voted_set)
        }
    }

class VotePayload(BaseModel):
    card_name: str
    score: int = Field(..., ge=1, le=5)

def record_community_vote(card_name: str, score: int, voter_hash: str):
    comm_conn = get_community_db()
    c = comm_conn.cursor()
    now = datetime.now().isoformat()
    try:
        prev = c.execute(
            "SELECT score FROM community_votes WHERE voter_hash = ? AND card_name = ?",
            (voter_hash, card_name)
        ).fetchone()

        if prev:
            old_score = prev['score']
            if old_score != score:
                score_diff = score - old_score
                c.execute("""
                    UPDATE community_votes 
                    SET score = ?, created_at = ? 
                    WHERE voter_hash = ? AND card_name = ?
                """, (score, now, voter_hash, card_name))

                star_old = f"stars_{old_score}"
                star_new = f"stars_{score}"
                c.execute(f"""
                    UPDATE community_ratings
                    SET score_sum = score_sum + ?,
                        {star_old} = {star_old} - 1,
                        {star_new} = {star_new} + 1,
                        avg_score = ROUND(CAST(score_sum + ? AS REAL) / total_votes, 2),
                        last_voted_at = ?
                    WHERE card_name = ?
                """, (score_diff, score_diff, now, card_name))
        else:
            c.execute("""
                INSERT INTO community_votes (card_name, voter_hash, score, created_at)
                VALUES (?, ?, ?, ?)
            """, (card_name, voter_hash, score, now))

            star_col = f"stars_{score}"
            c.execute(f"""
                INSERT INTO community_ratings (card_name, total_votes, score_sum, avg_score, {star_col}, last_voted_at)
                VALUES (?, 1, ?, ?, 1, ?)
                ON CONFLICT(card_name) DO UPDATE SET
                    total_votes = total_votes + 1,
                    score_sum = score_sum + excluded.score_sum,
                    {star_col} = {star_col} + 1,
                    avg_score = ROUND(CAST(score_sum + excluded.score_sum AS REAL) / (total_votes + 1), 2),
                    last_voted_at = excluded.last_voted_at
            """, (card_name, score, float(score), now))

        comm_conn.commit()

        updated = c.execute("""
            SELECT total_votes, avg_score, stars_1, stars_2, stars_3, stars_4, stars_5
            FROM community_ratings WHERE card_name = ?
        """, (card_name,)).fetchone()

        core_100, daily_50, _ = get_community_card_pools()
        today_active_set = set(core_100 + daily_50)
        voted_rows = c.execute("SELECT card_name FROM community_votes WHERE voter_hash = ?", (voter_hash,)).fetchall()
        all_voted_set = {r['card_name'] for r in voted_rows}
        voted_today_count = len(today_active_set.intersection(all_voted_set))
        pool_size = len(today_active_set)

        return {
            "status": "success",
            "card_name": card_name,
            "user_score": score,
            "total_votes": updated['total_votes'] if updated else 1,
            "avg_score": updated['avg_score'] if updated else float(score),
            "stars_breakdown": {
                1: updated['stars_1'] if updated else 0,
                2: updated['stars_2'] if updated else 0,
                3: updated['stars_3'] if updated else 0,
                4: updated['stars_4'] if updated else 0,
                5: updated['stars_5'] if updated else 0
            },
            "daily_progress": {
                "voted_today": voted_today_count,
                "pool_size": pool_size,
                "total_voted": len(all_voted_set)
            }
        }
    finally:
        comm_conn.close()

def get_community_leaderboard(sort: str = "highest", min_votes: int = 2, limit: int = 50, offset: int = 0):
    limit = max(1, min(limit, 100))
    offset = max(0, offset)
    min_votes = max(1, min_votes)

    order_clause = "ORDER BY avg_score DESC, total_votes DESC"
    if sort == "most_voted":
        order_clause = "ORDER BY total_votes DESC, avg_score DESC"
    elif sort == "lowest":
        order_clause = "ORDER BY avg_score ASC, total_votes DESC"

    comm_conn = get_community_db()
    c = comm_conn.cursor()
    try:
        total_count = c.execute("SELECT COUNT(*) as cnt FROM community_ratings WHERE total_votes >= ?", (min_votes,)).fetchone()['cnt']
        
        query = f"""
            SELECT card_name, total_votes, avg_score, stars_1, stars_2, stars_3, stars_4, stars_5, last_voted_at
            FROM community_ratings
            WHERE total_votes >= ?
            {order_clause}
            LIMIT ? OFFSET ?
        """
        rows = c.execute(query, (min_votes, limit, offset)).fetchall()
    finally:
        comm_conn.close()

    scryfall_cache = load_scryfall_cache()
    missing_names = [r['card_name'] for r in rows if not lookup_card_info(scryfall_cache, r['card_name'])]
    if missing_names:
        scryfall_cache = fetch_scryfall_data(missing_names)

    items = []
    for r in rows:
        cn = r['card_name']
        info = lookup_card_info(scryfall_cache, cn) or {}
        items.append({
            "card_name": cn,
            "total_votes": r['total_votes'],
            "avg_score": r['avg_score'],
            "stars_breakdown": {
                1: r['stars_1'],
                2: r['stars_2'],
                3: r['stars_3'],
                4: r['stars_4'],
                5: r['stars_5']
            },
            "last_voted_at": r['last_voted_at'],
            "images": info.get("images", []),
            "art_crops": info.get("art_crops", []),
            "type": info.get("type", "Carta"),
            "cmc": info.get("cmc", 0.0),
            "color_identity": info.get("color_identity", [])
        })

    return {
        "total": total_count,
        "items": items,
        "sort": sort,
        "min_votes": min_votes
    }

def get_community_overview():
    comm_conn = get_community_db()
    c = comm_conn.cursor()
    try:
        total_votes = c.execute("SELECT COUNT(*) as cnt FROM community_votes").fetchone()['cnt']
        total_cards_rated = c.execute("SELECT COUNT(*) as cnt FROM community_ratings WHERE total_votes > 0").fetchone()['cnt']

        # Top 3 per media
        top_rows = c.execute("""
            SELECT card_name, total_votes, avg_score
            FROM community_ratings
            WHERE total_votes >= (CASE WHEN (SELECT COUNT(*) FROM community_ratings WHERE total_votes >= 2) > 0 THEN 2 ELSE 1 END)
            ORDER BY avg_score DESC, total_votes DESC
            LIMIT 3
        """).fetchall()

        # Top 3 più votate
        most_voted_rows = c.execute("""
            SELECT card_name, total_votes, avg_score
            FROM community_ratings
            WHERE total_votes > 0
            ORDER BY total_votes DESC, avg_score DESC
            LIMIT 3
        """).fetchall()
    finally:
        comm_conn.close()

    scryfall_cache = load_scryfall_cache()
    all_needed = list(set([r['card_name'] for r in top_rows] + [r['card_name'] for r in most_voted_rows]))
    missing = [name for name in all_needed if not lookup_card_info(scryfall_cache, name)]
    if missing:
        scryfall_cache = fetch_scryfall_data(missing)

    def format_mini(row):
        cn = row['card_name']
        info = lookup_card_info(scryfall_cache, cn) or {}
        return {
            "card_name": cn,
            "total_votes": row['total_votes'],
            "avg_score": row['avg_score'],
            "images": info.get("images", []),
            "art_crops": info.get("art_crops", [])
        }

    return {
        "total_votes": total_votes,
        "total_cards_rated": total_cards_rated,
        "top_rated": [format_mini(r) for r in top_rows],
        "most_voted": [format_mini(r) for r in most_voted_rows]
    }

@app.get("/api/community/card/random")
async def api_community_card_random(request: Request):
    voter_hash = get_voter_hash(request)
    return await run_in_threadpool(get_random_community_card, voter_hash)

@app.post("/api/community/rate")
async def api_community_rate(payload: VotePayload, request: Request):
    voter_hash = get_voter_hash(request)
    return await run_in_threadpool(record_community_vote, payload.card_name, payload.score, voter_hash)

@app.get("/api/community/leaderboard")
async def api_community_leaderboard(
    sort: str = "highest",
    min_votes: int = 2,
    limit: int = 50,
    offset: int = 0
):
    if sort not in ("highest", "most_voted", "lowest"):
        sort = "highest"
    return await run_in_threadpool(get_community_leaderboard, sort, min_votes, limit, offset)

@app.get("/api/community/overview")
async def api_community_overview():
    return await run_in_threadpool(get_community_overview)
