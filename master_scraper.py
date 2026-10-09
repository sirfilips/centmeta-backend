import os
import json
import sqlite3
import time
import logging
import requests
from datetime import datetime

# Costanti di sicurezza e configurazione
SOGLIA_MINIMA_RISULTATI = 0.9
MIN_MAZZI_PER_CONTROLLO = 20
MAX_PAGINE_RICERCA = 500

# Inizializza i percorsi base
cartella_script = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(cartella_script, "centurion.db")
json_path = os.path.join(cartella_script, "database_centurion.json")
log_path = os.path.join(cartella_script, "scraper.log")

# Configurazione del Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_path, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Inizializza la sessione HTTP globale e lo stato del rate limiter
session = requests.Session()
last_request_time = 0.0

def rate_limited_get(url, params=None, timeout=15):
    """
    Esegue una richiesta GET garantendo che passino almeno 1.2 secondi
    dall'inizio della richiesta precedente.
    """
    global last_request_time
    current_time = time.monotonic()
    elapsed = current_time - last_request_time
    if elapsed < 1.2:
        time.sleep(1.2 - elapsed)
    last_request_time = time.monotonic()
    return session.get(url, params=params, timeout=timeout)

def init_db(db_path):
    conn = sqlite3.connect(db_path, timeout=30)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS decks (
            id_moxfield TEXT PRIMARY KEY,
            comandante TEXT,
            data_aggiornamento TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS deck_cards (
            deck_id TEXT,
            card_name TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS blacklist (
            id_moxfield TEXT PRIMARY KEY,
            data_rimozione TEXT
        )
    ''')
    # Creazione degli indici ottimizzati (allineati a main.py)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_decks_comandante ON decks(comandante)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_decks_data ON decks(data_aggiornamento)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_deck_cards_deck_id ON deck_cards(deck_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_deck_cards_card_name ON deck_cards(card_name)")
    
    # Pulizia indici duplicati o desueti
    cursor.execute("DROP INDEX IF EXISTS idx_card_name")
    cursor.execute("DROP INDEX IF EXISTS idx_comandante")
    
    conn.commit()
    conn.close()

def scarica_singolo_mazzo(deck_base):
    deck_id = deck_base.get("publicId")
    detail_url = f"https://api.moxfield.com/v2/decks/all/{deck_id}"

    # Massimo 1 tentativo iniziale + 3 retry
    for attempt in range(4):
        try:
            response = rate_limited_get(detail_url, timeout=15)
            
            if response.status_code == 200:
                deck_dettagliato = response.json()
                
                comandanti_nomi = []
                boards = deck_dettagliato.get("boards", {})
                cmd_board = boards.get("commanders", {}).get("cards", {})
                if not cmd_board:
                    cmd_board = deck_dettagliato.get("commanders", {})

                if isinstance(cmd_board, dict):
                    for card_info in cmd_board.values():
                        c_name = card_info.get("card", {}).get("name")
                        if c_name:
                            comandanti_nomi.append(c_name)
                elif isinstance(cmd_board, list):
                    for card_info in cmd_board:
                        c_name = card_info.get("card", {}).get("name")
                        if c_name:
                            comandanti_nomi.append(c_name)

                # Ordina alfabeticamente la lista dei comandanti (per i Partner) prima di unirla
                if comandanti_nomi:
                    comandanti_nomi.sort()
                    
                comandante_nome = " / ".join(comandanti_nomi) if comandanti_nomi else "Sconosciuto"
                data_aggiornamento = deck_dettagliato.get("lastUpdatedAtUtc")
                carte_unigne_mazzo = set()
                
                mainboard = boards.get("mainboard", {}).get("cards", {})
                if not mainboard:
                    mainboard = deck_dettagliato.get("mainboard", {})

                for carta_dati in mainboard.values():
                    nome_carta = carta_dati.get("card", {}).get("name")
                    if nome_carta:
                        carte_unigne_mazzo.add(nome_carta)

                return {
                    "id_moxfield": deck_id,
                    "comandante": comandante_nome,
                    "data_aggiornamento": data_aggiornamento,
                    "carte": list(carte_unigne_mazzo)
                }
                
            elif response.status_code == 429:
                if attempt == 0: wait_time = 10
                elif attempt == 1: wait_time = 30
                elif attempt == 2: wait_time = 60
                else:
                    logging.error(f"Mazzo {deck_id} saltato: esauriti i tentativi per 429.")
                    break
                logging.warning(f"Rilevato 429 per mazzo {deck_id}. Attesa di {wait_time}s")
                time.sleep(wait_time)
            else:
                logging.warning(f"Errore {response.status_code} per mazzo {deck_id}")
                if attempt < 3:
                    time.sleep(5)
                else:
                    logging.error(f"Mazzo {deck_id} saltato: esauriti i tentativi per errore di stato.")
                    break
        except Exception as e:
            logging.warning(f"Eccezione {type(e).__name__} per mazzo {deck_id}")
            if attempt < 3:
                time.sleep(5)
            else:
                logging.error(f"Mazzo {deck_id} saltato: esauriti i tentativi per eccezione di rete.")
                break
                
    return None

def _chunks(lst, n):
    """Generatore per suddividere una lista in blocchi da n elementi."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def run_scraper():
    # Verifica e impostazione dello User-Agent obbligatorio
    ua = os.environ.get("MOXFIELD_UA")
    if not ua:
        logging.error("Variabile d'ambiente MOXFIELD_UA non impostata")
        return
        
    session.headers.update({"User-Agent": ua})
    
    is_dry_run = os.environ.get("SCRAPER_DRY_RUN") == "1"
    
    init_db(db_path)

    # 1. IMPORTAZIONE JSON STORICO (con Transazione Sicura, Blacklist e Rinomina)
    if os.path.exists(json_path) and not is_dry_run:
        logging.info(f"Trovato il file '{json_path}'. Conversione diretta in SQLite in corso...")
        import_success = False
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                database_pulito = json.load(f)

            conn = sqlite3.connect(db_path, timeout=30)
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT id_moxfield FROM blacklist")
                blacklisted_ids = {row[0] for row in cursor.fetchall()}
                
                cursor.execute("DELETE FROM decks")
                cursor.execute("DELETE FROM deck_cards")
                
                skipped_blacklist = 0
                for mazzo in database_pulito:
                    deck_id = mazzo.get("id_moxfield")
                    if deck_id in blacklisted_ids:
                        skipped_blacklist += 1
                        continue
                        
                    cmd_name = mazzo.get("comandante", "Sconosciuto")
                    if cmd_name and " / " in cmd_name and " // " not in cmd_name:
                        parts = [p.strip() for p in cmd_name.split(" / ")]
                        parts.sort()
                        cmd_name = " / ".join(parts)
                    
                    cursor.execute(
                        "INSERT OR REPLACE INTO decks (id_moxfield, comandante, data_aggiornamento) VALUES (?, ?, ?)",
                        (deck_id, cmd_name, mazzo.get("data_aggiornamento"))
                    )
                    for carta in set(mazzo.get("carte", [])):
                        cursor.execute("INSERT INTO deck_cards (deck_id, card_name) VALUES (?, ?)", (deck_id, carta))
                        
                conn.commit()
                import_success = True
                logging.info(f"Conversione completata! {len(database_pulito) - skipped_blacklist} mazzi importati ({skipped_blacklist} ignorati per blacklist).")
            except Exception as e:
                conn.rollback()
                raise e
            finally:
                conn.close()
                
            if import_success:
                ts = datetime.now().strftime("%Y%m%d-%H%M%S")
                new_json_path = f"{json_path}.imported-{ts}"
                try:
                    os.replace(json_path, new_json_path)
                except Exception as e:
                    logging.error(f"Impossibile rinominare {json_path} post-import: {e}")
                return
        except Exception as e:
            logging.error(f"Errore nell'import JSON: {type(e).__name__} - {e}. Procedo con il download web...")
    elif os.path.exists(json_path) and is_dry_run:
        logging.info("Dry run: import del JSON saltato (il file verrà importato al prossimo run normale).")

    # 2. LETTURA STATO ATTUALE (Snapshot Veloce DB)
    db_decks = {}
    blacklisted_ids = set()
    try:
        conn = sqlite3.connect(db_path, timeout=30)
        cursor = conn.cursor()
        cursor.execute("SELECT id_moxfield, data_aggiornamento FROM decks")
        for row in cursor.fetchall():
            db_decks[row[0]] = str(row[1]).strip() if row[1] else None
        cursor.execute("SELECT id_moxfield FROM blacklist")
        blacklisted_ids = {row[0] for row in cursor.fetchall()}
        conn.close()
    except Exception as e:
        logging.warning(f"Impossibile leggere lo stato del DB locale: {e}")

    # 3. RICERCA PAGINATA (con deduplica publicId e uscita anticipata)
    logging.info("Cerco i mazzi per CentMeta su Moxfield...")
    search_url = "https://api.moxfield.com/v2/decks/search"
    
    page = 1
    page_size = 50
    search_decks = []
    seen_public_ids = set()
    search_failed = False

    while page <= MAX_PAGINE_RICERCA:
        params = {"pageNumber": page, "pageSize": page_size, "fmt": "centurion"}
        success = False
        
        for attempt in range(4):
            try:
                response = rate_limited_get(search_url, params=params, timeout=10)
                if response.status_code == 200:
                    success = True
                    break
                elif response.status_code == 429:
                    if attempt == 0: wait_time = 10
                    elif attempt == 1: wait_time = 30
                    elif attempt == 2: wait_time = 60
                    else: break
                    logging.warning(f"Rilevato 429 durante la ricerca alla pagina {page}. Attesa di {wait_time}s")
                    time.sleep(wait_time)
                else:
                    logging.warning(f"Errore {response.status_code} alla pagina {page}")
                    if attempt < 3:
                        time.sleep(5)
            except Exception as e:
                logging.warning(f"Eccezione {type(e).__name__} alla pagina {page}")
                if attempt < 3:
                    time.sleep(5)
                    
        if not success:
            logging.error(f"Ricerca fallita definitivamente alla pagina {page}.")
            search_failed = True
            break
            
        data = response.json().get("data", [])
        if not data:
            break
            
        new_ids_in_page = 0
        for d in data:
            pid = d.get("publicId")
            if pid and pid not in seen_public_ids:
                seen_public_ids.add(pid)
                search_decks.append(d)
                new_ids_in_page += 1
                
        if new_ids_in_page == 0:
            logging.info(f"Pagina {page} non ha aggiunto nuovi publicId unici. Fine paginazione.")
            break
            
        logging.info(f"Raccolti link pagina {page} (Totale mazzi in coda: {len(search_decks)})...")
        page += 1
        time.sleep(3.0)

    if page > MAX_PAGINE_RICERCA:
        logging.warning(f"Raggiunto limite massimo pagine ({MAX_PAGINE_RICERCA}). Procedo con i mazzi raccolti.")

    if search_failed:
        logging.error("Fase di ricerca fallita. Interruzione senza modificare il database.")
        return

    # 4. CONTROLLO DI SICUREZZA SOGLIA (Safety Check)
    force_run = os.environ.get("SCRAPER_FORCE") == "1"
    num_db_decks = len(db_decks)
    num_search_decks = len(search_decks)
    
    if num_db_decks >= MIN_MAZZI_PER_CONTROLLO and not force_run:
        if num_search_decks < (num_db_decks * SOGLIA_MINIMA_RISULTATI):
            logging.error(f"CRITICO: Trovati solo {num_search_decks} mazzi, contro {num_db_decks} nel DB "
                          f"(soglia minima per non svuotare: {int(num_db_decks * SOGLIA_MINIMA_RISULTATI)}). "
                          "Possibile errore API Moxfield. Interruzione per sicurezza.")
            return

    # 5. CONFRONTO E CREAZIONE CODE (Delta/Incremental Logic)
    to_download = []
    unchanged_ids = set()
    active_search_ids = set()
    
    stat_found = len(search_decks)
    stat_blacklisted = 0
    stat_unchanged = 0
    stat_new = 0
    stat_updated = 0

    for deck in search_decks:
        deck_id = deck.get("publicId")
        
        if deck_id in blacklisted_ids:
            stat_blacklisted += 1
            continue
            
        active_search_ids.add(deck_id)
        remote_date = deck.get("lastUpdatedAtUtc")
        remote_date_str = str(remote_date).strip() if remote_date else ""
        
        if deck_id in db_decks:
            if remote_date_str and remote_date_str == db_decks[deck_id]:
                stat_unchanged += 1
                unchanged_ids.add(deck_id)
            else:
                stat_updated += 1
                to_download.append(deck)
        else:
            stat_new += 1
            to_download.append(deck)

    # Rimossi = tutti i mazzi nel DB che NON sono in active_search_ids (quindi mancanti dalla search O in blacklist)
    db_removed_ids = [d_id for d_id in db_decks.keys() if d_id not in active_search_ids]

    # 6. MODALITÀ DRY RUN
    if is_dry_run:
        logging.info("--- DRY RUN RIASSUNTO ---")
        logging.info(f"Trovati (pre-blacklist): {stat_found}")
        logging.info(f"In Blacklist (saltati): {stat_blacklisted}")
        logging.info(f"Invariati (nessun calcolo extra): {stat_unchanged}")
        logging.info(f"Nuovi (da scaricare): {stat_new}")
        logging.info(f"Da Aggiornare (da scaricare): {stat_updated}")
        logging.info(f"Da Rimuovere (mancanti o blacklist): {len(db_removed_ids)}")
        logging.info("Dry run concluso. Nessuna chiamata di dettaglio e nessun dato salvato.")
        return

    # 7. DOWNLOAD INCREMENTALE
    downloaded_data = {}
    stat_failed_kept = 0
    stat_failed_skipped = 0
    
    if to_download:
        logging.info(f"Avvio download dettagli per {len(to_download)} mazzi nuovi o aggiornati...")
    
    for i, deck in enumerate(to_download):
        deck_id = deck.get("publicId")
        mazzo = scarica_singolo_mazzo(deck)
        
        if mazzo:
            downloaded_data[deck_id] = mazzo
        else:
            if deck_id in db_decks:
                # Se è fallito ma lo avevamo già, teniamo i dati vecchi
                stat_failed_kept += 1
                unchanged_ids.add(deck_id)
                logging.warning(f"Download fallito per {deck_id}, mantengo i dati vecchi nel DB.")
            else:
                # Se è fallito ed è nuovo, lo scartiamo del tutto
                stat_failed_skipped += 1
                active_search_ids.discard(deck_id)
                logging.warning(f"Download fallito per mazzo nuovo {deck_id}, scartato per questo giro.")
                
        if (i + 1) % 100 == 0:
            logging.info(f"Progresso download: {i + 1}/{len(to_download)}...")

    # 8. SCRITTURA NEL DB (Singola Transazione con Riconciliazione)
    logging.info("Avvio scrittura incrementale nel DB (Transazione sicura)...")
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        cursor = conn.cursor()
        
        # (a) Rileggi blacklist in caso di modifiche runtime dell'admin
        cursor.execute("SELECT id_moxfield FROM blacklist")
        runtime_blacklisted = {row[0] for row in cursor.fetchall()}
        
        # (b) Ricalcola i veri ID attivi validi tenendo conto della blacklist runtime
        final_active_ids = {d_id for d_id in active_search_ids if d_id not in runtime_blacklisted}
        
        # Rileggiamo gli ID dal DB vero prima della pulizia (potrebbe essere variato per mazzi droppati dall'admin)
        cursor.execute("SELECT id_moxfield FROM decks")
        current_db_decks = {row[0] for row in cursor.fetchall()}
        
        # (c) Rimozione dei mazzi obsoleti (Mancanti da final_active_ids)
        final_removed_ids = [d_id for d_id in current_db_decks if d_id not in final_active_ids]
        for chunk in _chunks(final_removed_ids, 500):
            placeholders = ",".join("?" * len(chunk))
            cursor.execute(f"DELETE FROM decks WHERE id_moxfield IN ({placeholders})", chunk)
            cursor.execute(f"DELETE FROM deck_cards WHERE deck_id IN ({placeholders})", chunk)
            
        # (d) Aggiornamento/Inserimento dei soli mazzi processati oggi
        for deck_id, mazzo in downloaded_data.items():
            if deck_id in runtime_blacklisted:
                continue # Evita di reinserire mazzi appena blacklistati a runtime
                
            cursor.execute("DELETE FROM deck_cards WHERE deck_id = ?", (deck_id,))
            cursor.execute(
                "INSERT OR REPLACE INTO decks (id_moxfield, comandante, data_aggiornamento) VALUES (?, ?, ?)",
                (deck_id, mazzo["comandante"], mazzo["data_aggiornamento"])
            )
            
            cards_to_insert = [(deck_id, carta) for carta in mazzo["carte"]]
            if cards_to_insert:
                cursor.executemany("INSERT INTO deck_cards (deck_id, card_name) VALUES (?, ?)", cards_to_insert)
                
        conn.commit()
        
        # (e) Tocco l'mtime del file solo ad aggiornamento effettivamente completato
        try:
            os.utime(db_path, None)
        except OSError as e:
            logging.warning(f"Impossibile aggiornare mtime del DB: {e}")
            
        logging.info("--- RIEPILOGO AGGIORNAMENTO ---")
        logging.info(f"Trovati (pre-blacklist): {stat_found}")
        logging.info(f"In Blacklist (iniziale): {stat_blacklisted}")
        logging.info(f"Invariati (saltato download): {stat_unchanged}")
        logging.info(f"Scaricati (Nuovi: {stat_new}, Aggiornati: {stat_updated})")
        logging.info(f"Falliti: {stat_failed_kept} (tenuti vecchi) / {stat_failed_skipped} (scartati)")
        logging.info(f"Rimossi dal DB (non più online o in blacklist): {len(final_removed_ids)}")
        logging.info("Finito! I dati incrementali sono pronti per CentMeta.")
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Errore critico durante la scrittura nel DB: {type(e).__name__} - {e}. Rollback effettuato.")
    finally:
        conn.close()

if __name__ == "__main__":
    run_scraper()