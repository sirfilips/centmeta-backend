import os
import json
import sqlite3
import requests
import asyncio
import time as std_time
import threading
import logging
import secrets
from datetime import datetime, time, timedelta
from fastapi import FastAPI, HTTPException, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.concurrency import run_in_threadpool
from typing import Optional, List
from contextlib import asynccontextmanager
from functools import lru_cache
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
        session.headers.update({"User-Agent": "CentMeta/1.0 (privacy@centmeta.it)"})
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
CACHE_FILE = os.path.join(CARTELLA_SCRIPT, "scryfall_cache.json")

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
                std_time.sleep(0.1)
                
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
                            std_time.sleep(0.05) 
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
                                std_time.sleep(0.05)
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
