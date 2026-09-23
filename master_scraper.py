import os
import json
import sqlite3
import time
import logging
import requests

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
    conn = sqlite3.connect(db_path)
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
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_card_name ON deck_cards(card_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_comandante ON decks(comandante)')
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

def run_scraper():
    # Verifica e impostazione dello User-Agent obbligatorio
    ua = os.environ.get("MOXFIELD_UA")
    if not ua:
        logging.error("Variabile d'ambiente MOXFIELD_UA non impostata")
        return
        
    session.headers.update({"User-Agent": ua})
    
    init_db(db_path)

    if os.path.exists(json_path):
        logging.info(f"Trovato il file '{json_path}'. Conversione diretta in SQLite in corso...")
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                database_pulito = json.load(f)

            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM decks")
            cursor.execute("DELETE FROM deck_cards")
            
            for mazzo in database_pulito:
                deck_id = mazzo.get("id_moxfield")
                cmd_name = mazzo.get("comandante", "Sconosciuto")
                
                # Riordina alfabeticamente i Partner anche dal JSON storico
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
            conn.close()
            logging.info(f"Conversione completata! {len(database_pulito)} mazzi importati nel DB locale.")
            return
        except Exception as e:
            logging.error(f"Errore nella lettura del JSON: {type(e).__name__}. Procedo con il download web...")

    logging.info("Cerco i mazzi per CentMeta su Moxfield...")
    search_url = "https://api.moxfield.com/v2/decks/search"
    
    page = 1
    page_size = 50
    decks = []
    search_failed = False

    while True:
        params = {"pageNumber": page, "pageSize": page_size, "fmt": "centurion"}
        success = False
        
        # Massimo 1 tentativo iniziale + 3 retry
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
            
        decks.extend(data)
        logging.info(f"Raccolti link pagina {page} (Totale mazzi in coda: {len(decks)})...")
        page += 1
        time.sleep(3.0)  # Pausa specifica per non stressare l'API di ricerca

    # Interrompe l'aggiornamento senza toccare il database locale in caso di errore di paginazione
    if search_failed:
        logging.error("Fase di ricerca fallita. Interruzione senza modificare il database.")
        return

    logging.info(f"Trovati {len(decks)} mazzi. Avvio il download sequenziale...")
    
    database_pulito = []
    for i, deck in enumerate(decks):
        mazzo = scarica_singolo_mazzo(deck)
        if mazzo:
            database_pulito.append(mazzo)
        if (i + 1) % 100 == 0:
            logging.info(f"Progresso: {i + 1}/{len(decks)} mazzi elaborati...")

    logging.info("Salvataggio nel database SQLite in corso...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("DELETE FROM decks")
    cursor.execute("DELETE FROM deck_cards")
    
    for mazzo in database_pulito:
        cursor.execute(
            "INSERT OR REPLACE INTO decks (id_moxfield, comandante, data_aggiornamento) VALUES (?, ?, ?)",
            (mazzo["id_moxfield"], mazzo["comandante"], mazzo["data_aggiornamento"])
        )
        for carta in mazzo["carte"]:
            cursor.execute(
                "INSERT INTO deck_cards (deck_id, card_name) VALUES (?, ?)",
                (mazzo["id_moxfield"], carta)
            )
            
    conn.commit()
    conn.close()
    logging.info(f"Finito! {len(database_pulito)} mazzi salvati correttamente per CentMeta.")

if __name__ == "__main__":
    run_scraper()