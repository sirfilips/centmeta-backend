import os
import sys
import json
import sqlite3
import time
import logging
from logging.handlers import RotatingFileHandler
import threading
import requests
from datetime import datetime
from typing import Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Costanti di sicurezza e configurazione
SOGLIA_MINIMA_RISULTATI = 0.9
MIN_MAZZI_PER_CONTROLLO = 20
MAX_PAGINE_RICERCA = 500
MOXFIELD_SEARCH_CAP = 10000

# Inizializza i percorsi base
cartella_script = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(cartella_script, "centurion.db")
json_path = os.path.join(cartella_script, "database_centurion.json")
log_path = os.path.join(cartella_script, "scraper.log")
lock_path = os.path.join(cartella_script, "scraper.lock")

# Configurazione del Logging (su domanda in run_scraper/__main__, non a import-time)
_LOGGING_CONFIGURED = False


def configure_scraper_logging() -> None:
    """Configura RotatingFileHandler e console una sola volta per lo scraper CLI."""
    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    if not any(isinstance(h, RotatingFileHandler) for h in root_logger.handlers):
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=5 * 1024 * 1024,
            backupCount=3,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    if not any(isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler) for h in root_logger.handlers):
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        root_logger.addHandler(stream_handler)
    _LOGGING_CONFIGURED = True

class ScraperProcessLock:
    """
    Lock a livello di processo del sistema operativo per garantire che una sola
    istanza dello scraper sia attiva contemporaneamente su tutta la macchina.
    Compatibile sia con Linux/Unix (fcntl.flock) sia con Windows (msvcrt.locking).
    """
    def __init__(self, lock_file_path: str):
        self.lock_file_path = lock_file_path
        self.file_handle = None

    def __enter__(self):
        try:
            self.file_handle = open(self.lock_file_path, "a+")
            if sys.platform != "win32":
                import fcntl
                fcntl.flock(self.file_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            else:
                import msvcrt
                self.file_handle.seek(0)
                msvcrt.locking(self.file_handle.fileno(), msvcrt.LK_NBLCK, 1)
            self.file_handle.seek(0)
            self.file_handle.truncate()
            self.file_handle.write(f"{os.getpid()}\n")
            self.file_handle.flush()
            return True
        except (BlockingIOError, OSError, IOError):
            if self.file_handle:
                try:
                    self.file_handle.close()
                except Exception:
                    pass
                self.file_handle = None
            return False

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.file_handle:
            try:
                if sys.platform != "win32":
                    import fcntl
                    fcntl.flock(self.file_handle, fcntl.LOCK_UN)
                else:
                    import msvcrt
                    self.file_handle.seek(0)
                    msvcrt.locking(self.file_handle.fileno(), msvcrt.LK_UNLCK, 1)
            except Exception:
                pass
            try:
                self.file_handle.close()
            except Exception:
                pass
            self.file_handle = None

# Inizializza lo stato del rate limiter e le sessioni HTTP thread-local
_rate_limit_lock = threading.Lock()
_thread_local = threading.local()
_default_user_agent = ""
last_request_time = 0.0

def _get_http_session() -> requests.Session:
    http_session = getattr(_thread_local, "http_session", None)
    if http_session is None:
        http_session = requests.Session()
        _thread_local.http_session = http_session
    if _default_user_agent and http_session.headers.get("User-Agent") != _default_user_agent:
        http_session.headers.update({"User-Agent": _default_user_agent})
    return http_session

def rate_limited_get(url, params=None, headers=None, timeout=15):
    """
    Esegue una richiesta GET garantendo in modo thread-safe che passino almeno
    1.2 secondi dall'inizio della richiesta precedente.
    """
    global last_request_time
    with _rate_limit_lock:
        current_time = time.monotonic()
        elapsed = current_time - last_request_time
        if elapsed < 1.2:
            time.sleep(1.2 - elapsed)
        last_request_time = time.monotonic()
    return _get_http_session().get(url, params=params, headers=headers, timeout=timeout)

def normalize_partner_commander(cmd_name):
    """Normalizza e ordina alfabeticamente i nomi dei comandanti Partner (' / ')."""
    if not cmd_name:
        return "Sconosciuto"
    if " / " in cmd_name and " // " not in cmd_name:
        parts = [p.strip() for p in cmd_name.split(" / ") if p.strip()]
        parts.sort()
        return " / ".join(parts)
    return cmd_name.strip()

def ensure_covering_indices(cursor):
    """Crea e verifica gli indici covering ottimizzati su decks e deck_cards. Restituisce True se sono stati ricreati."""
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_decks_comandante ON decks(comandante)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_decks_data ON decks(data_aggiornamento)")

    existing_idx = {
        row[0]: (row[1] or "")
        for row in cursor.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='deck_cards'"
        ).fetchall()
    }
    needs_analyze = False
    if "(deck_id, card_name)" not in existing_idx.get("idx_deck_cards_deck_id", ""):
        cursor.execute("DROP INDEX IF EXISTS idx_deck_cards_deck_id")
        cursor.execute("CREATE INDEX idx_deck_cards_deck_id ON deck_cards(deck_id, card_name)")
        needs_analyze = True
    if "(card_name, deck_id)" not in existing_idx.get("idx_deck_cards_card_name", ""):
        cursor.execute("DROP INDEX IF EXISTS idx_deck_cards_card_name")
        cursor.execute("CREATE INDEX idx_deck_cards_card_name ON deck_cards(card_name, deck_id)")
        needs_analyze = True

    # Pulizia indici duplicati o desueti
    cursor.execute("DROP INDEX IF EXISTS idx_card_name")
    cursor.execute("DROP INDEX IF EXISTS idx_comandante")
    return needs_analyze

def init_db(db_path):
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout = 30000;")
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
        # Creazione degli indici covering ottimizzati (condivisi con main.py)
        ensure_covering_indices(cursor)

        conn.commit()
    finally:
        conn.close()

def _interruptible_sleep(seconds: float, stop_event: Optional[threading.Event]) -> bool:
    """Attende `seconds` secondi; restituisce True se interrotto da stop_event."""
    if stop_event is None:
        time.sleep(seconds)
        return False
    return stop_event.wait(seconds)

def scarica_singolo_mazzo(deck_base, stop_event: Optional[threading.Event] = None):
    deck_id = deck_base.get("publicId")
    detail_url = f"https://api.moxfield.com/v2/decks/all/{deck_id}"

    # Massimo 1 tentativo iniziale + 3 retry
    for attempt in range(4):
        if stop_event is not None and stop_event.is_set():
            return None
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
                comandante_nome = normalize_partner_commander(" / ".join(comandanti_nomi)) if comandanti_nomi else "Sconosciuto"
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
                if _interruptible_sleep(wait_time, stop_event):
                    return None
            else:
                logging.warning(f"Errore {response.status_code} per mazzo {deck_id}")
                if attempt < 3:
                    if _interruptible_sleep(5, stop_event):
                        return None
                else:
                    logging.error(f"Mazzo {deck_id} saltato: esauriti i tentativi per errore di stato.")
                    break
        except Exception as e:
            logging.warning(f"Eccezione {type(e).__name__} per mazzo {deck_id}")
            if attempt < 3:
                if _interruptible_sleep(5, stop_event):
                    return None
            else:
                logging.error(f"Mazzo {deck_id} saltato: esauriti i tentativi per eccezione di rete.")
                break
                
    return None

def _chunks(lst, n):
    """Generatore per suddividere una lista in blocchi da n elementi."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def run_scraper(stop_event: Optional[threading.Event] = None) -> bool:
    configure_scraper_logging()
    with ScraperProcessLock(lock_path) as acquired:
        if not acquired:
            logging.warning("[LOCK] Un'altra istanza dello scraper è già in esecuzione. Operazione ignorata.")
            return False
        return _run_scraper_locked(stop_event=stop_event)

def _run_scraper_locked(stop_event: Optional[threading.Event] = None) -> bool:
    global _default_user_agent
    # Verifica e impostazione dello User-Agent obbligatorio
    ua = os.environ.get("MOXFIELD_UA")
    if not ua:
        logging.error("Variabile d'ambiente MOXFIELD_UA non impostata")
        return False
        
    ua = ua.strip()
    _default_user_agent = ua
    _get_http_session().headers.update({"User-Agent": ua})
    
    is_dry_run = os.environ.get("SCRAPER_DRY_RUN") == "1"

    if stop_event is not None and stop_event.is_set():
        logging.info("[SHUTDOWN] Scraper interrotto in modo pulito prima dell'avvio.")
        return False

    init_db(db_path)

    # 1. IMPORTAZIONE JSON STORICO (abilitata solo se esplicitamente autorizzata con ALLOW_JSON_IMPORT=1)
    allow_json_migration = os.environ.get("ALLOW_JSON_IMPORT") == "1"
    if os.path.exists(json_path) and not is_dry_run:
        if not allow_json_migration:
            logging.warning(
                f"File '{json_path}' rilevato ma ignorato per sicurezza (evita azzeramento accidentale del DB). "
                "Per autorizzare l'importazione, avviare con ALLOW_JSON_IMPORT=1."
            )
        else:
            logging.info(f"Trovato il file '{json_path}' e autorizzato ALLOW_JSON_IMPORT=1. Conversione diretta in SQLite in corso...")
            import_success = False
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    database_pulito = json.load(f)

                conn = sqlite3.connect(db_path, timeout=30)
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA busy_timeout = 30000;")
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
                            
                        cmd_name = normalize_partner_commander(mazzo.get("comandante", "Sconosciuto"))
                        
                        cursor.execute(
                            "INSERT OR REPLACE INTO decks (id_moxfield, comandante, data_aggiornamento) VALUES (?, ?, ?)",
                            (deck_id, cmd_name, mazzo.get("data_aggiornamento"))
                        )
                        for carta in set(mazzo.get("carte", [])):
                            cursor.execute("INSERT INTO deck_cards (deck_id, card_name) VALUES (?, ?)", (deck_id, carta))
                            
                    cursor.execute("ANALYZE")
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
                    return True
            except Exception as e:
                logging.error(f"Errore nell'import JSON: {type(e).__name__} - {e}. Procedo con il download web...")
    elif os.path.exists(json_path) and is_dry_run:
        logging.info("Dry run: import del JSON saltato (il file verrà importato al prossimo run normale).")

    # 2. LETTURA STATO ATTUALE (Snapshot Veloce DB)
    db_decks = {}
    blacklisted_ids = set()
    db_exists = os.path.exists(db_path)
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=30)
        conn.execute("PRAGMA busy_timeout = 30000;")
        cursor = conn.cursor()
        cursor.execute("SELECT id_moxfield, data_aggiornamento FROM decks")
        for row in cursor.fetchall():
            db_decks[row[0]] = str(row[1]).strip() if row[1] else None
        cursor.execute("SELECT id_moxfield FROM blacklist")
        blacklisted_ids = {row[0] for row in cursor.fetchall()}
    except Exception as e:
        if db_exists:
            logging.error(f"Impossibile leggere lo stato del DB locale '{db_path}': {e}. Interruzione per sicurezza.")
            return False
        logging.warning(f"Impossibile leggere lo stato del DB locale: {e}")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    # 3. RICERCA PAGINATA (con deduplica publicId e uscita anticipata)
    logging.info("Cerco i mazzi per CentMeta su Moxfield...")
    search_url = "https://api.moxfield.com/v2/decks/search"
    
    page = 1
    page_size = 50
    search_decks = []
    seen_public_ids = set()
    search_failed = False
    reported_total_results: Optional[int] = None
    reported_total_pages: Optional[int] = None

    while page <= MAX_PAGINE_RICERCA:
        if stop_event is not None and stop_event.is_set():
            logging.info("[SHUTDOWN] Scraper interrotto in modo pulito durante la ricerca pagine (nessuna modifica al DB).")
            return False

        params = {"pageNumber": page, "pageSize": page_size, "fmt": "centurion"}
        success = False
        
        data = []
        for attempt in range(4):
            if stop_event is not None and stop_event.is_set():
                logging.info("[SHUTDOWN] Scraper interrotto in modo pulito durante il retry di ricerca.")
                return False
            try:
                response = rate_limited_get(search_url, params=params, timeout=10)
                if response.status_code == 200:
                    body = response.json()
                    if not isinstance(body, dict):
                        raise ValueError("Risposta JSON non valida dalla ricerca Moxfield")
                    if isinstance(body.get("totalResults"), int):
                        reported_total_results = body["totalResults"]
                    if isinstance(body.get("totalPages"), int):
                        reported_total_pages = body["totalPages"]
                    data = body.get("data", [])
                    success = True
                    break
                elif response.status_code == 429:
                    if attempt == 0: wait_time = 10
                    elif attempt == 1: wait_time = 30
                    elif attempt == 2: wait_time = 60
                    else: break
                    logging.warning(f"Rilevato 429 durante la ricerca alla pagina {page}. Attesa di {wait_time}s")
                    if _interruptible_sleep(wait_time, stop_event):
                        logging.info("[SHUTDOWN] Scraper interrotto in modo pulito durante attesa 429.")
                        return False
                else:
                    logging.warning(f"Errore {response.status_code} alla pagina {page}")
                    if attempt < 3:
                        if _interruptible_sleep(5, stop_event):
                            logging.info("[SHUTDOWN] Scraper interrotto in modo pulito durante retry pagina.")
                            return False
            except Exception as e:
                logging.warning(f"Eccezione {type(e).__name__} alla pagina {page}")
                if attempt < 3:
                    if _interruptible_sleep(5, stop_event):
                        logging.info("[SHUTDOWN] Scraper interrotto in modo pulito durante attesa errore pagina.")
                        return False
                    
        if not success:
            logging.error(f"Ricerca fallita definitivamente alla pagina {page}.")
            search_failed = True
            break
            
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
        if reported_total_pages is not None and page >= reported_total_pages:
            break
        page += 1

    if search_failed:
        logging.error("Fase di ricerca fallita. Interruzione senza modificare il database.")
        return False

    hit_page_limit = page > MAX_PAGINE_RICERCA
    hit_moxfield_cap = (
        (reported_total_results is not None and reported_total_results >= MOXFIELD_SEARCH_CAP)
        or len(search_decks) >= MOXFIELD_SEARCH_CAP
    )
    early_empty_stop = (
        reported_total_pages is not None
        and page < reported_total_pages
    )
    search_truncated = hit_page_limit or hit_moxfield_cap or early_empty_stop

    if search_truncated:
        logging.warning(
            "[SAFETY] Ricerca Moxfield troncata (page=%s, totalResults=%s, totalPages=%s, raccolti=%s): "
            "rimozione dei mazzi non trovati disabilitata per questo giro (verranno rimossi solo i mazzi in blacklist).",
            page,
            reported_total_results,
            reported_total_pages,
            len(search_decks),
        )

    # 4. CONTROLLO DI SICUREZZA SOGLIA (Safety Check)
    force_run = os.environ.get("SCRAPER_FORCE") == "1"
    num_db_decks = len(db_decks)
    num_search_decks = len(search_decks)
    
    if num_db_decks >= MIN_MAZZI_PER_CONTROLLO and not force_run:
        if num_search_decks < (num_db_decks * SOGLIA_MINIMA_RISULTATI):
            logging.error(f"CRITICO: Trovati solo {num_search_decks} mazzi, contro {num_db_decks} nel DB "
                          f"(soglia minima per non svuotare: {int(num_db_decks * SOGLIA_MINIMA_RISULTATI)}). "
                          "Possibile errore API Moxfield. Interruzione per sicurezza.")
            return False

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

    # Rimossi = mazzi non presenti in active_search_ids (se ricerca completa) o solo in blacklist (se ricerca troncata)
    if search_truncated:
        db_removed_ids = [d_id for d_id in db_decks.keys() if d_id in blacklisted_ids]
    else:
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
        return True

    # 7. DOWNLOAD INCREMENTALE
    downloaded_data = {}
    stat_failed_kept = 0
    stat_failed_skipped = 0
    
    if to_download:
        logging.info(f"Avvio download dettagli per {len(to_download)} mazzi nuovi o aggiornati...")
    
    for i, deck in enumerate(to_download):
        if stop_event is not None and stop_event.is_set():
            logging.info("[SHUTDOWN] Scraper interrotto in modo pulito durante il download mazzi (nessuna modifica al DB).")
            return False
        deck_id = deck.get("publicId")
        mazzo = scarica_singolo_mazzo(deck, stop_event=stop_event)
        if stop_event is not None and stop_event.is_set() and mazzo is None:
            logging.info("[SHUTDOWN] Scraper interrotto in modo pulito durante il download del mazzo (nessuna modifica al DB).")
            return False
        
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

    if stop_event is not None and stop_event.is_set():
        logging.info("[SHUTDOWN] Scraper interrotto in modo pulito prima della scrittura DB.")
        return False

    # 8. BACKUP PREVENTIVO E SCRITTURA NEL DB (Singola Transazione con Riconciliazione)
    if os.path.exists(db_path):
        try:
            backup_path = f"{db_path}.bak"
            logging.info("Creazione snapshot di backup preventivo del database...")
            b_src = sqlite3.connect(db_path, timeout=30)
            b_dst = sqlite3.connect(backup_path, timeout=30)
            with b_dst:
                b_src.backup(b_dst)
            b_dst.close()
            b_src.close()
            logging.info(f"Snapshot preventivo completato con successo: {os.path.basename(backup_path)}")
        except Exception as b_err:
            logging.warning(f"Impossibile completare lo snapshot di backup preventivo: {b_err}")

    logging.info("Avvio scrittura incrementale nel DB (Transazione sicura)...")
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout = 30000;")
    try:
        cursor = conn.cursor()
        cursor.execute("BEGIN IMMEDIATE")
        
        # (a) Rileggi blacklist in caso di modifiche runtime dell'admin
        cursor.execute("SELECT id_moxfield FROM blacklist")
        runtime_blacklisted = {row[0] for row in cursor.fetchall()}
        
        # (b) Ricalcola i veri ID attivi validi tenendo conto della blacklist runtime
        final_active_ids = {d_id for d_id in active_search_ids if d_id not in runtime_blacklisted}
        
        # Rileggiamo gli ID dal DB vero prima della pulizia (potrebbe essere variato per mazzi droppati dall'admin)
        cursor.execute("SELECT id_moxfield FROM decks")
        current_db_decks = {row[0] for row in cursor.fetchall()}
        
        # (c) Rimozione dei mazzi obsoleti (protetta contro troncamento ricerca e drift paginazione)
        blacklisted_to_remove = [d_id for d_id in current_db_decks if d_id in runtime_blacklisted]
        missing_non_blacklisted = [
            d_id for d_id in current_db_decks
            if d_id not in final_active_ids and d_id not in runtime_blacklisted
        ]
        if search_truncated:
            final_removed_ids = blacklisted_to_remove
        elif (
            not force_run
            and len(current_db_decks) >= MIN_MAZZI_PER_CONTROLLO
            and len(missing_non_blacklisted) > max(1, int(len(current_db_decks) * (1.0 - SOGLIA_MINIMA_RISULTATI)))
        ):
            max_safe_deletions = max(1, int(len(current_db_decks) * (1.0 - SOGLIA_MINIMA_RISULTATI)))
            logging.warning(
                "[SAFETY] Rilevate %s rimozioni oltre la soglia sicura (%s su %s mazzi): "
                "possibile drift di paginazione; rimozione non-blacklist saltata.",
                len(missing_non_blacklisted),
                max_safe_deletions,
                len(current_db_decks),
            )
            final_removed_ids = blacklisted_to_remove
        else:
            final_removed_ids = blacklisted_to_remove + missing_non_blacklisted
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
                
        cursor.execute("ANALYZE")
        cursor.execute("COMMIT")
        
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
        return True
    except Exception as e:
        if conn.in_transaction:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
        logging.error(f"Errore critico durante la scrittura nel DB: {type(e).__name__} - {e}. Rollback effettuato.")
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    configure_scraper_logging()
    run_scraper()
