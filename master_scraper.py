import os
import json
import sqlite3
import time
import random
import concurrent.futures
import cloudscraper
import logging

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

# Inizializza lo scraper per superare le protezioni anti-bot
scraper = cloudscraper.create_scraper(browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True})

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

def scarica_singolo_mazzo(deck_base, retries=3):
    time.sleep(random.uniform(0.6, 1.5))
    
    deck_id = deck_base.get("publicId")
    detail_url = f"https://api.moxfield.com/v2/decks/all/{deck_id}"

    for attempt in range(retries):
        try:
            detail_response = scraper.get(detail_url, timeout=15)
            
            if detail_response.status_code == 200:
                deck_dettagliato = detail_response.json()
                
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
                
            elif detail_response.status_code == 429:
                wait_time = (attempt + 1) * 3
                logging.warning(f"Rilevato 429 per mazzo {deck_id}. Attesa di {wait_time}s (Tentativo {attempt+1}/{retries})")
                time.sleep(wait_time)
            else:
                logging.error(f"Errore {detail_response.status_code} per mazzo {deck_id}")
                break
        except Exception as e:
            logging.error(f"Eccezione durante il download del mazzo {deck_id}: {e}")
            time.sleep(2)
            
    return None

def run_scraper():
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
            logging.error(f"Errore nella lettura del JSON: {e}. Procedo con il download web...")

    logging.info("Cerco i mazzi per CentMeta su Moxfield...")
    search_url = "https://api.moxfield.com/v2/decks/search"
    
    page = 1
    page_size = 50
    decks = []

    while True:
        params = {"pageNumber": page, "pageSize": page_size, "fmt": "centurion"}
        try:
            response = scraper.get(search_url, params=params, timeout=10)
            if response.status_code == 429:
                logging.warning("Rilevato rallentamento (429) durante la ricerca. Attendo 10 secondi...")
                time.sleep(10)
                continue
            if response.status_code != 200:
                logging.warning(f"Errore {response.status_code} alla pagina {page}. Riprovo tra 5 sec...")
                time.sleep(5)
                continue
                
            data = response.json().get("data", [])
            if not data:
                break
                
            decks.extend(data)
            logging.info(f"Raccolti link pagina {page} (Totale mazzi in coda: {len(decks)})...")
            page += 1
            time.sleep(1.0)
        except Exception as e:
            logging.error(f"Errore di rete nella paginazione: {e}")
            time.sleep(5)

    logging.info(f"Trovati {len(decks)} mazzi. Avvio il download parallelo bilanciato (4 worker)...")
    
    database_pulito = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        results = executor.map(scarica_singolo_mazzo, decks)
        for i, mazzo in enumerate(results):
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