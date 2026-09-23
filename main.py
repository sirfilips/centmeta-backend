import os
import json
import sqlite3
import pandas as pd
import requests
import asyncio
import time as std_time
from datetime import datetime, time, timedelta
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.concurrency import run_in_threadpool
from typing import Optional
from contextlib import asynccontextmanager
from functools import lru_cache
import master_scraper

scryfall_session = requests.Session()
scryfall_session.headers.update({"User-Agent": "CentMeta/1.0"})

def sanitize_commanders_in_db():
    """Uniforma in ordine alfabetico i comandanti Partner per evitare sdoppiamenti, ignorando le carte bifronte (//)."""
    conn = get_db()
    c = conn.cursor()
    try:
        # Cerca i mazzi con Partner (che usano il singolo slash " / " con gli spazi)
        # Escludiamo esplicitamente le carte bifronte che usano " // "
        c.execute("SELECT id_moxfield, comandante FROM decks WHERE comandante LIKE '% / %' AND comandante NOT LIKE '%//%'")
        rows = c.fetchall()
        updated_count = 0
        
        for row in rows:
            deck_id = row['id_moxfield']
            commander_name = row['comandante']
            
            # Dividi i nomi dei Partner dal singolo slash, pulisci gli spazi e metti in ordine alfabetico
            parts = [part.strip() for part in commander_name.split(' / ')]
            parts.sort()
            
            # Riunisci usando lo STESSO delimitatore (spazio slash spazio)
            new_name = " / ".join(parts)
            
            # Se il nome alfabetico è diverso da quello salvato, aggiorna il database
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
        c.execute("CREATE INDEX IF NOT EXISTS idx_deck_cards_deck_id ON deck_cards(deck_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_deck_cards_card_name ON deck_cards(card_name)")
        conn.commit()
        print("[DB] Indici ottimizzati con successo. Le query saranno molto più veloci.")
    except Exception as e:
        print(f"[DB ERROR] Creazione indici fallita: {e}")
    finally:
        conn.close()

def clear_all_caches():
    global _SCRYFALL_CACHE_MEMORY
    _SCRYFALL_CACHE_MEMORY = None
    get_valid_commanders_set.cache_clear()
    get_decks_stats_cached.cache_clear()
    get_card_details_cached.cache_clear()
    get_commander_decks_cached.cache_clear()
    get_dashboard_data_cached.cache_clear()
    print("[CACHE] Tutte le cache in memoria sono state svuotate.")

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
            
            print("[CRON] Avvio pre-warming della cache in background...")
            await run_in_threadpool(get_decks_stats_cached, "Tutti i tempi", None, 0, 0)
            await run_in_threadpool(get_dashboard_data_cached, "Tutti i tempi")
            print("[CRON] Pre-warming completato. Sistema pronto e veloce!")
            
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[CRON ERROR] Errore aggiornamento: {e}")
            await asyncio.sleep(3600)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Esegue la pulizia automatica dei Partner prima di tutto il resto
    sanitize_commanders_in_db()
    setup_database_indices()
    updater_task = asyncio.create_task(background_db_updater())
    yield
    updater_task.cancel()

app = FastAPI(title="CentMeta API", version="3.4", lifespan=lifespan)

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

CARTELLA_SCRIPT = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(CARTELLA_SCRIPT, "centurion.db")
CACHE_FILE = os.path.join(CARTELLA_SCRIPT, "scryfall_cache.json")

_SCRYFALL_CACHE_MEMORY = None

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def get_time_conditions_both(filtro_tempo: str):
    if filtro_tempo == "Ultimi 7 giorni":
        return "datetime(data_aggiornamento) >= datetime('now', '-7 days')", "datetime(data_aggiornamento) >= datetime('now', '-14 days') AND datetime(data_aggiornamento) < datetime('now', '-7 days')"
    elif filtro_tempo == "Ultimi 30 giorni":
        return "datetime(data_aggiornamento) >= datetime('now', '-30 days')", "datetime(data_aggiornamento) >= datetime('now', '-60 days') AND datetime(data_aggiornamento) < datetime('now', '-30 days')"
    elif filtro_tempo == "Ultimi 3 mesi":
        return "datetime(data_aggiornamento) >= datetime('now', '-3 months')", "datetime(data_aggiornamento) >= datetime('now', '-6 months') AND datetime(data_aggiornamento) < datetime('now', '-3 months')"
    elif filtro_tempo == "Ultimi 6 mesi":
        return "datetime(data_aggiornamento) >= datetime('now', '-6 months')", "datetime(data_aggiornamento) >= datetime('now', '-12 months') AND datetime(data_aggiornamento) < datetime('now', '-6 months')"
    elif filtro_tempo == "Ultimo anno":
        return "datetime(data_aggiornamento) >= datetime('now', '-1 year')", "datetime(data_aggiornamento) >= datetime('now', '-2 years') AND datetime(data_aggiornamento) < datetime('now', '-1 year')"
    return None, None

def get_time_condition(filtro_tempo: str):
    curr, _ = get_time_conditions_both(filtro_tempo)
    return curr

def load_scryfall_cache():
    global _SCRYFALL_CACHE_MEMORY
    if _SCRYFALL_CACHE_MEMORY is not None:
        return _SCRYFALL_CACHE_MEMORY
    try:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                _SCRYFALL_CACHE_MEMORY = json.load(f)
        else:
            _SCRYFALL_CACHE_MEMORY = {}
    except Exception:
        _SCRYFALL_CACHE_MEMORY = {}
    return _SCRYFALL_CACHE_MEMORY

def save_scryfall_cache(cache):
    global _SCRYFALL_CACHE_MEMORY
    _SCRYFALL_CACHE_MEMORY = cache
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=4)
    except Exception:
        pass

def fetch_scryfall_data(card_names):
    results = load_scryfall_cache()
    mancanti = []
    actual_names = set()
    
    for n in card_names:
        if " // " in n:
            actual_names.add(n)
            actual_names.add(n.split(" // ")[0].strip())
        elif " / " in n:
            for part in n.split(" / "):
                actual_names.add(part.strip())
        else:
            actual_names.add(n)

    for n in actual_names:
        key = n.lower()
        if key not in results or not results[key].get("images"):
            mancanti.append(n)
            
    if mancanti:
        for i in range(0, len(mancanti), 75):
            chunk = mancanti[i:i+75]
            identifiers = [{"name": n} for n in chunk]
            updated_in_chunk = False
            
            try:
                res = scryfall_session.post(
                    "https://api.scryfall.com/cards/collection", 
                    json={"identifiers": identifiers}, 
                    timeout=15
                )
                if res.status_code == 200:
                    for card in res.json().get("data", []):
                        name = card.get("name", "")
                        
                        is_dfc_override = False
                        faces_names = []
                        if " // " in name:
                            faces_names = [f.strip() for f in name.split(" // ")]
                            requested_names_lower = [n.lower() for n in actual_names]
                            if name.lower() not in requested_names_lower:
                                is_dfc_override = True
                                
                        weird_sets = ["sta", "otp", "wot", "mul", "brr", "sld", "mps", "mp2", "exp", "mb1", "plist", "h1r", "slx"]
                        is_showcase_or_weird = card.get("set") in weird_sets or card.get("frame") == "showcase"
                        
                        if is_dfc_override or is_showcase_or_weird:
                            std_time.sleep(0.05) 
                            query_name = name
                            if is_dfc_override:
                                for f in faces_names:
                                    if f.lower() in [n.lower() for n in actual_names]:
                                        query_name = f
                                        break
                                q_str = f'!"{query_name}" -is:dfc -is:split -is:adventure -is:extra -is:promo -frame:showcase'
                            else:
                                q_str = f'!"{query_name}" -is:extra -is:promo -frame:showcase'
                            
                            try:
                                search_res = scryfall_session.get('https://api.scryfall.com/cards/search', params={"q": q_str}, timeout=5)
                                if search_res.status_code == 200:
                                    normal_data = search_res.json().get("data", [])
                                    if normal_data:
                                        card = normal_data[0] 
                                        name = card.get("name", "")
                                        if " // " in name: faces_names = [f.strip() for f in name.split(" // ")]
                                        else: faces_names = []
                            except:
                                pass

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

                        color_identity = card.get("color_identity", [])
                        
                        card_data = {
                            "full_name": name, "images": images, "art_crops": art_crops, "oracles": oracles, "type": type_line, "color_identity": color_identity, "cmc": card.get("cmc", 0.0)
                        }
                        
                        results[name.lower()] = card_data
                        if " // " in name and len(faces_names) > 1:
                            f0 = faces_names[0].strip().lower()
                            f1 = faces_names[1].strip().lower()
                            if f0 not in results or " // " in results[f0].get("full_name", " // "): results[f0] = card_data
                            if f1 not in results or " // " in results[f1].get("full_name", " // "): results[f1] = card_data
                            
                        updated_in_chunk = True
                        
            except Exception:
                pass
            
            if updated_in_chunk:
                save_scryfall_cache(results)
    return results

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

@lru_cache(maxsize=32)
def get_valid_commanders_set(filtro_tempo: str):
    conn = get_db()
    curr_cond, prev_cond = get_time_conditions_both(filtro_tempo)
    base_where = f"WHERE {curr_cond}" if curr_cond else ""
    
    q_curr = f"SELECT comandante, COUNT(*) as cnt FROM decks {base_where} GROUP BY comandante ORDER BY cnt DESC"
    df_curr = pd.read_sql(q_curr, conn)
    
    if not df_curr.empty:
        df_curr['rank_curr'] = df_curr['cnt'].rank(method='first', ascending=False)
        
    if prev_cond:
        q_prev = f"SELECT comandante, COUNT(*) as cnt FROM decks WHERE {prev_cond} GROUP BY comandante ORDER BY cnt DESC"
        df_prev = pd.read_sql(q_prev, conn)
        if not df_prev.empty:
            df_prev['rank_prev'] = df_prev['cnt'].rank(method='first', ascending=False)
            df_curr = pd.merge(df_curr, df_prev[['comandante', 'rank_prev']], on='comandante', how='left')
            df_curr['trend'] = df_curr['rank_prev'] - df_curr['rank_curr']
        else:
            df_curr['trend'] = None
    else:
        df_curr['trend'] = ""

    conn.close()
    
    if df_curr.empty:
        return frozenset(), []
        
    cmd_names = df_curr['comandante'].tolist()
    scryfall_cache = fetch_scryfall_data(cmd_names)
    
    valid_set = set()
    result_list = []
    
    for _, row in df_curr.iterrows():
        name = row['comandante']
        parts = []
        if " / " in name: parts = [p.strip() for p in name.split(" / ")]
        elif " // " in name: parts = [p.strip() for p in name.split(" // ")]
        else: parts = [name.strip()]
            
        is_valid = True
        images = []
        art_crops = []
        oracles = []
        seen_images = set()
        seen_art_crops = set()
        
        for part in parts:
            clean_key = part.lower()
            info = scryfall_cache.get(clean_key, {})
            
            t_line = info.get("type", "").lower()
            if "legendary" not in t_line and "planeswalker" not in t_line:
                is_valid = False
                break
                
            for img in info.get("images", []):
                if img not in seen_images: seen_images.add(img); images.append(img)
            for ac in info.get("art_crops", []):
                if ac not in seen_art_crops: seen_art_crops.add(ac); art_crops.append(ac)
            for oracle in info.get("oracles", []):
                if oracle not in oracles: oracles.append(oracle)
        
        t_val = row.get('trend')
        if pd.isna(t_val):
            trend_out = None if prev_cond else ""
        else:
            trend_out = int(t_val) if t_val != "" else ""
                    
        if is_valid and parts:
            valid_set.add(name)
            result_list.append({
                "comandante": name,
                "cnt": int(row['cnt']),
                "trend": trend_out,
                "images": images,
                "art_crops": art_crops,
                "oracles": oracles
            })
            
    return frozenset(valid_set), result_list

@lru_cache(maxsize=32)
def get_dashboard_data_cached(filtro_tempo: str):
    conn = get_db()
    curr_cond, prev_cond = get_time_conditions_both(filtro_tempo)
    base_where = f"WHERE {curr_cond}" if curr_cond else ""
    
    q_tot = f"SELECT COUNT(DISTINCT id_moxfield) as tot FROM decks {base_where}"
    tot_decks = int(pd.read_sql(q_tot, conn).iloc[0]['tot'])

    q_curr_cmd = f"SELECT comandante, COUNT(*) as cnt FROM decks {base_where} GROUP BY comandante ORDER BY cnt DESC"
    df_curr_cmd = pd.read_sql(q_curr_cmd, conn)
    if not df_curr_cmd.empty:
        df_curr_cmd['rank_curr'] = df_curr_cmd['cnt'].rank(method='first', ascending=False)
    
    if prev_cond:
        q_prev_cmd = f"SELECT comandante, COUNT(*) as cnt FROM decks WHERE {prev_cond} GROUP BY comandante ORDER BY cnt DESC"
        df_prev_cmd = pd.read_sql(q_prev_cmd, conn)
        if not df_prev_cmd.empty:
            df_prev_cmd['rank_prev'] = df_prev_cmd['cnt'].rank(method='first', ascending=False)
            df_top_cmd = pd.merge(df_curr_cmd.head(25), df_prev_cmd[['comandante', 'rank_prev']], on='comandante', how='left')
            df_top_cmd['trend'] = df_top_cmd['rank_prev'] - df_top_cmd['rank_curr']
        else:
            df_top_cmd = df_curr_cmd.head(25)
            df_top_cmd['trend'] = None
    else:
        df_top_cmd = df_curr_cmd.head(25) if not df_curr_cmd.empty else pd.DataFrame(columns=['comandante', 'cnt'])
        df_top_cmd['trend'] = ""

    q_p1 = "SELECT comandante, COUNT(*) as cnt1 FROM decks WHERE datetime(data_aggiornamento) > datetime('now', '-7 days') GROUP BY comandante"
    q_p2 = "SELECT comandante, COUNT(*) as cnt2 FROM decks WHERE datetime(data_aggiornamento) <= datetime('now', '-7 days') AND datetime(data_aggiornamento) > datetime('now', '-14 days') GROUP BY comandante"
    df_p1 = pd.read_sql(q_p1, conn)
    df_p2 = pd.read_sql(q_p2, conn)
    
    if not df_p1.empty:
        df_hot_cmd = pd.merge(df_p1, df_p2, on='comandante', how='left').fillna(0)
        df_hot_cmd['delta'] = df_hot_cmd['cnt1'] - df_hot_cmd['cnt2']
        df_hot_cmd = df_hot_cmd[(df_hot_cmd['cnt1'] >= 2) & (df_hot_cmd['delta'] > 0)]
        df_hot_cmd = df_hot_cmd.sort_values('delta', ascending=False).head(50)
    else:
        df_hot_cmd = pd.DataFrame(columns=['comandante', 'cnt1', 'cnt2', 'delta'])

    # --- INIZIO NUOVA LOGICA OTTIMIZZATA PER IL TREND DELLE CARTE ---
    if prev_cond:
        where_curr = base_where.replace('data_aggiornamento', 'd.data_aggiornamento')
        where_prev = "WHERE " + prev_cond.replace('data_aggiornamento', 'd.data_aggiornamento')
        
        # Usiamo le funzioni finestra (Window Functions) di SQLite per calcolare i rank
        # direttamente nel database ed estrarre solo la top 250 con il trend calcolato.
        q_top_cards = f"""
            WITH curr AS (
                SELECT c.card_name, COUNT(DISTINCT c.deck_id) as freq,
                       ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT c.deck_id) DESC) as rank_curr
                FROM deck_cards c JOIN decks d ON c.deck_id = d.id_moxfield 
                {where_curr}
                GROUP BY c.card_name
            ),
            prev AS (
                SELECT c.card_name, COUNT(DISTINCT c.deck_id) as freq,
                       ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT c.deck_id) DESC) as rank_prev
                FROM deck_cards c JOIN decks d ON c.deck_id = d.id_moxfield 
                {where_prev}
                GROUP BY c.card_name
            )
            SELECT curr.card_name, curr.freq, (prev.rank_prev - curr.rank_curr) as trend
            FROM curr
            LEFT JOIN prev ON curr.card_name = prev.card_name
            ORDER BY curr.freq DESC
            LIMIT 250
        """
        df_top_cards = pd.read_sql(q_top_cards, conn)
    else:
        where_curr = base_where.replace('data_aggiornamento', 'd.data_aggiornamento') if base_where else ""
        q_top_cards = f"""
            SELECT c.card_name, COUNT(DISTINCT c.deck_id) as freq, "" as trend
            FROM deck_cards c JOIN decks d ON c.deck_id = d.id_moxfield 
            {where_curr}
            GROUP BY c.card_name
            ORDER BY freq DESC
            LIMIT 250
        """
        df_top_cards = pd.read_sql(q_top_cards, conn)
    # --- FINE NUOVA LOGICA ---

    q_hc1 = """
        SELECT c.card_name, COUNT(DISTINCT c.deck_id) as cnt1 
        FROM deck_cards c JOIN decks d ON c.deck_id = d.id_moxfield 
        WHERE datetime(d.data_aggiornamento) > datetime('now', '-7 days') GROUP BY c.card_name
    """
    q_hc2 = """
        SELECT c.card_name, COUNT(DISTINCT c.deck_id) as cnt2 
        FROM deck_cards c JOIN decks d ON c.deck_id = d.id_moxfield 
        WHERE datetime(d.data_aggiornamento) <= datetime('now', '-7 days') AND datetime(d.data_aggiornamento) > datetime('now', '-14 days') GROUP BY c.card_name
    """
    df_hc1 = pd.read_sql(q_hc1, conn)
    df_hc2 = pd.read_sql(q_hc2, conn)
    
    if not df_hc1.empty:
        df_hot_c = pd.merge(df_hc1, df_hc2, on='card_name', how='left').fillna(0)
        df_hot_c['delta'] = df_hot_c['cnt1'] - df_hot_c['cnt2']
        df_hot_c = df_hot_c[(df_hot_c['cnt1'] >= 3) & (df_hot_c['delta'] > 0)]
        df_hot_c = df_hot_c.sort_values('delta', ascending=False).head(250)
    else:
        df_hot_c = pd.DataFrame()
        
    conn.close()

    all_names = set()
    def add_names(names_list, is_cmd):
        for name in names_list:
            if is_cmd:
                if " // " in name: all_names.add(name.split(" // ")[0].strip())
                elif " / " in name: 
                    for p in name.split(" / "): all_names.add(p.strip())
                else: all_names.add(name)
            else:
                all_names.add(name.split(" // ")[0].strip())

    if not df_top_cmd.empty: add_names(df_top_cmd['comandante'].tolist(), True)
    if not df_hot_cmd.empty: add_names(df_hot_cmd['comandante'].tolist(), True)
    if not df_top_cards.empty: add_names(df_top_cards['card_name'].tolist(), False)
    if not df_hot_c.empty: add_names(df_hot_c['card_name'].tolist(), False)
    
    scryfall_cache = fetch_scryfall_data(list(all_names))

    def format_item(row, name_col, is_cmd=False):
        name = row[name_col]
        if is_cmd:
            if " // " in name: parts = [name.split(" // ")[0].strip()]
            elif " / " in name: parts = [p.strip() for p in name.split(" / ")]
            else: parts = [name]
        else:
            parts = [name.split(" // ")[0].strip()]

        images = []
        for part in parts:
            info = scryfall_cache.get(part.lower(), {})
            images.extend(info.get("images", []))
            
        seen = set()
        images = [x for x in images if not (x in seen or seen.add(x))]

        t_val = row.get('trend')
        if pd.isna(t_val):
            trend_out = None if prev_cond else ""
        else:
            trend_out = int(t_val) if t_val != "" else ""
        
        main_info = scryfall_cache.get(parts[0].lower(), {})
        cat = categorizza_tipo(main_info.get("type", "")) if not is_cmd else "Comandante"

        return {
            "name": name,
            "images": images,
            "val1": int(row.get('cnt', row.get('freq', row.get('cnt1', 0)))),
            "trend": trend_out,
            "delta": int(row.get('delta', 0)) if 'delta' in row else None,
            "category": cat,
            "color_identity": main_info.get("color_identity", [])
        }

    return {
        "total_decks": tot_decks,
        "top_commanders": [format_item(row, 'comandante', True) for _, row in df_top_cmd.iterrows()] if not df_top_cmd.empty else [],
        "hot_commanders": [format_item(row, 'comandante', True) for _, row in df_hot_cmd.iterrows()] if not df_hot_cmd.empty else [],
        "top_cards": [format_item(row, 'card_name', False) for _, row in df_top_cards.iterrows()] if not df_top_cards.empty else [],
        "hot_cards": [format_item(row, 'card_name', False) for _, row in df_hot_c.iterrows()] if not df_hot_c.empty else []
    }

@lru_cache(maxsize=128)
def get_decks_stats_cached(filtro_tempo: str, comandante: Optional[str], limit: int = 0, offset: int = 0):
    try:
        mtime = os.path.getmtime(DB_PATH)
        global_last_updated = datetime.fromtimestamp(mtime).isoformat()
    except Exception:
        global_last_updated = None

    conn = get_db()
    valid_commanders, _ = get_valid_commanders_set(filtro_tempo)
    if not valid_commanders:
        conn.close()
        return {"total_decks": 0, "last_updated": global_last_updated, "mana_curve": {}, "cards": []}

    global_total = int(pd.read_sql("SELECT COUNT(DISTINCT id_moxfield) as tot FROM decks", conn).iloc[0]['tot'])
    global_cards = pd.read_sql("SELECT card_name, COUNT(DISTINCT deck_id) as g_freq FROM deck_cards GROUP BY card_name", conn)
    global_cards['g_perc'] = (global_cards['g_freq'] / global_total * 100)

    conds = []
    t_cond = get_time_condition(filtro_tempo)
    if t_cond: conds.append(t_cond)
    
    params = []
    if comandante:
        if comandante not in valid_commanders:
            conn.close()
            return {"total_decks": 0, "last_updated": global_last_updated, "mana_curve": {}, "cards": []}
        conds.append("comandante = ?")
        params.append(comandante)
    else:
        valid_cmd_list = list(valid_commanders)
        placeholders_cmd = '(' + ','.join(['?'] * len(valid_cmd_list)) + ')'
        conds.append(f"comandante IN {placeholders_cmd}")
        params.extend(valid_cmd_list)
        
    where_clause = " WHERE " + " AND ".join(conds)
    
    q_total = f"SELECT COUNT(DISTINCT id_moxfield) as tot FROM decks {where_clause}"
    total_decks = int(pd.read_sql(q_total, conn, params=params).iloc[0]['tot'])
    
    if total_decks == 0:
        conn.close()
        return {"total_decks": 0, "last_updated": global_last_updated, "mana_curve": {}, "cards": []}
        
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(deck_cards)")
    columns = [info[1].lower() for info in cursor.fetchall()]
    
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
        
    stats_df = pd.read_sql(q_cards, conn, params=params)
    conn.close()
    
    stats_df['Percentuale'] = (stats_df['freq'] / total_decks * 100).round(1)
    
    stats_df = pd.merge(stats_df, global_cards[['card_name', 'g_perc']], on='card_name', how='left').fillna(0)
    stats_df['Sinergia'] = (stats_df['Percentuale'] - stats_df['g_perc']).round(1)
    
    stats_df = stats_df.sort_values(by=['Percentuale', 'freq'], ascending=False)
    if limit > 0: stats_df = stats_df.iloc[offset : offset + limit]
    
    scryfall_cache = fetch_scryfall_data(stats_df['card_name'].tolist())
    
    formatted_cards = []
    mana_curve = {0: 0.0, 1: 0.0, 2: 0.0, 3: 0.0, 4: 0.0, 5: 0.0, 6: 0.0}
    type_dist = {"Creature": 0.0, "Istantanei": 0.0, "Stregonerie": 0.0, "Artefatti": 0.0, "Incantesimi": 0.0, "Planeswalker": 0.0, "Terre": 0.0}

    somma_univoche = 0.0

    for _, row in stats_df.iterrows():
        c_name = row['card_name']
        info = scryfall_cache.get(c_name.lower(), {})
        if not info:
            info = scryfall_cache.get(c_name.split("//")[0].strip().lower(), {"images": [], "art_crops": [], "type": "Altro", "oracles": [], "color_identity": [], "cmc": 0})
            
        cat = categorizza_tipo(info.get("type", ""))
        
        if 'total_copies' in row and pd.notna(row['total_copies']):
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
        
    if 'total_copies' not in stats_df.columns:
        terre_mancanti = 100.0 - somma_univoche
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
    conn = get_db()
    valid_commanders, _ = get_valid_commanders_set(filtro_tempo)
    if not valid_commanders:
        conn.close()
        return {"commanders": [], "synergies": []}
        
    valid_cmd_list = list(valid_commanders)
    placeholders_cmd = '(' + ','.join(['?'] * len(valid_cmd_list)) + ')'
    
    t_cond = get_time_condition(filtro_tempo)
    time_filtered_cond = ""
    if t_cond:
        time_filtered_cond = " AND " + t_cond.replace("data_aggiornamento", "d.data_aggiornamento")

    q_tot_glob = f"SELECT COUNT(DISTINCT d.id_moxfield) as tot FROM decks d JOIN deck_cards c ON d.id_moxfield = c.deck_id WHERE c.card_name = ? AND d.comandante IN {placeholders_cmd}"
    total_decks_global = int(pd.read_sql(q_tot_glob, conn, params=[card_name] + valid_cmd_list).iloc[0]['tot'])
    
    q_tot_time = f"SELECT COUNT(DISTINCT d.id_moxfield) as tot FROM decks d JOIN deck_cards c ON d.id_moxfield = c.deck_id WHERE c.card_name = ? AND d.comandante IN {placeholders_cmd} {time_filtered_cond}"
    total_decks_time = int(pd.read_sql(q_tot_time, conn, params=[card_name] + valid_cmd_list).iloc[0]['tot'])

    scryfall_cache = load_scryfall_cache()
    if card_name.lower() not in scryfall_cache:
        scryfall_cache = fetch_scryfall_data([card_name])

    card_info_cache = scryfall_cache.get(card_name.lower(), {})
    if not card_info_cache:
        card_info_cache = scryfall_cache.get(card_name.split("//")[0].strip().lower(), {})

    if total_decks_time == 0:
        conn.close()
        return {
            "card_name": card_name, "images": card_info_cache.get("images", []), "art_crops": card_info_cache.get("art_crops", []),
            "oracles": card_info_cache.get("oracles", []), "total_decks_with_card": 0, "total_decks_global": total_decks_global,
            "commanders": [], "synergies": []
        }
        
    q_cmd = f"SELECT d.comandante, COUNT(DISTINCT d.id_moxfield) as mazzi_carta FROM decks d JOIN deck_cards c ON d.id_moxfield = c.deck_id WHERE c.card_name = ? AND d.comandante IN {placeholders_cmd} {time_filtered_cond} GROUP BY d.comandante"
    cmd_counts = pd.read_sql(q_cmd, conn, params=[card_name] + valid_cmd_list)
    
    q_tot_cmd = f"SELECT comandante, COUNT(DISTINCT id_moxfield) as totale_comandante FROM decks d WHERE comandante IN {placeholders_cmd} {time_filtered_cond} GROUP BY comandante"
    total_counts = pd.read_sql(q_tot_cmd, conn, params=valid_cmd_list)
    
    res_cmd = pd.merge(cmd_counts, total_counts, on='comandante')
    res_cmd['Percentuale'] = (res_cmd['mazzi_carta'] / res_cmd['totale_comandante'] * 100).round(1)
    res_cmd = res_cmd.sort_values(by=['Percentuale', 'mazzi_carta'], ascending=False)
    
    all_cmd_names = res_cmd['comandante'].tolist()
    scryfall_cache = fetch_scryfall_data(all_cmd_names + [card_name])

    cmd_list = []
    for _, row in res_cmd.iterrows():
        name = row['comandante']
        parts = [p.strip() for p in name.split(" / ")] if " / " in name else ([p.strip() for p in name.split(" // ")] if " // " in name else [name.strip()])
        images, art_crops, seen_images, seen_art_crops = [], [], set(), set()
        for part in parts:
            info = scryfall_cache.get(part.lower(), {})
            for img in info.get("images", []):
                if img not in seen_images: seen_images.add(img); images.append(img)
            for ac in info.get("art_crops", []):
                if ac not in seen_art_crops: seen_art_crops.add(ac); art_crops.append(ac)

        cmd_list.append({
            "comandante": name, "mazzi_carta": int(row['mazzi_carta']), "totale_comandante": int(row['totale_comandante']),
            "Percentuale": float(row['Percentuale']), "images": images, "art_crops": art_crops
        })
        
    syn_query = f"""
        SELECT c.card_name, COUNT(DISTINCT c.deck_id) as freq 
        FROM deck_cards c
        JOIN decks d ON c.deck_id = d.id_moxfield
        WHERE d.id_moxfield IN (
            SELECT deck_id FROM deck_cards WHERE card_name = ?
        )
        AND d.comandante IN {placeholders_cmd}
        {time_filtered_cond}
        AND c.card_name != ?
        GROUP BY c.card_name
    """
    syn_params = [card_name] + valid_cmd_list + [card_name]
    syn_df = pd.read_sql(syn_query, conn, params=syn_params)
    conn.close()
    
    if not syn_df.empty:
        syn_df['Percentuale'] = (syn_df['freq'] / total_decks_time * 100).round(1)
        syn_df = syn_df.sort_values(by=['Percentuale', 'freq'], ascending=False).head(50)
    else:
        syn_df['Percentuale'] = 0.0
    
    syn_cache = fetch_scryfall_data(syn_df['card_name'].tolist())
    syn_formatted = []
    for _, row in syn_df.iterrows():
        c = row['card_name']
        inf = syn_cache.get(c.lower(), {})
        if not inf: inf = syn_cache.get(c.split("//")[0].strip().lower(), {"images": [], "art_crops": [], "type": "Altro", "oracles": [], "color_identity": []})
            
        syn_formatted.append({
            "card_name": c, "freq": int(row['freq']), "Percentuale": float(row['Percentuale']),
            "images": inf.get("images", []), "art_crops": inf.get("art_crops", []), "oracles": inf.get("oracles", []),
            "color_identity": inf.get("color_identity", []), "category": categorizza_tipo(inf.get("type", ""))
        })
        
    return {
        "card_name": card_name, "images": card_info_cache.get("images", []), "art_crops": card_info_cache.get("art_crops", []),
        "oracles": card_info_cache.get("oracles", []), "total_decks_with_card": total_decks_time, "total_decks_global": total_decks_global,
        "commanders": cmd_list[:15], "synergies": syn_formatted
    }

@lru_cache(maxsize=256)
def get_commander_decks_cached(comandante: str, filtro_tempo: str, limit: int = 0, offset: int = 0):
    conn = get_db()
    conds = ["comandante = ?"]
    t_cond = get_time_condition(filtro_tempo)
    params = [comandante]
    
    if t_cond:
        conds.append(t_cond)
        
    where_clause = " WHERE " + " AND ".join(conds)
    
    if limit > 0:
        query = f"SELECT id_moxfield, data_aggiornamento FROM decks {where_clause} ORDER BY data_aggiornamento DESC LIMIT {limit} OFFSET {offset}"
    else:
        query = f"SELECT id_moxfield, data_aggiornamento FROM decks {where_clause} ORDER BY data_aggiornamento DESC"
        
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    
    decks = []
    for _, row in df.iterrows():
        decks.append({
            "id_moxfield": row['id_moxfield'],
            "url": f"https://moxfield.com/decks/{row['id_moxfield']}",
            "data_aggiornamento": row['data_aggiornamento']
        })
    return decks

@app.get("/api/commanders")
async def get_commanders(filtro_tempo: str = "Tutti i tempi"):
    _, result_list = await run_in_threadpool(get_valid_commanders_set, filtro_tempo)
    return result_list

@app.get("/api/decks/stats")
async def get_decks_stats(filtro_tempo: str = "Tutti i tempi", comandante: Optional[str] = None, limit: int = 0, offset: int = 0):
    return await run_in_threadpool(get_decks_stats_cached, filtro_tempo, comandante, limit, offset)

@app.get("/api/card/details")
async def get_card_details(card_name: str, filtro_tempo: str = "Tutti i tempi"):
    return await run_in_threadpool(get_card_details_cached, card_name, filtro_tempo)

@app.get("/api/commander/decks")
async def get_commander_decks(comandante: str, filtro_tempo: str = "Tutti i tempi", limit: int = 0, offset: int = 0):
    return await run_in_threadpool(get_commander_decks_cached, comandante, filtro_tempo, limit, offset)

@app.get("/api/home/dashboard")
async def get_dashboard(filtro_tempo: str = "Tutti i tempi"):
    return await run_in_threadpool(get_dashboard_data_cached, filtro_tempo)