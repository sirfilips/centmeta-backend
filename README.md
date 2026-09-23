# CentMeta - Backend

CentMeta è un'applicazione web indipendente e open-source dedicata all'analisi statistica e al monitoraggio del metagame per il formato Centurion Commander di Magic: The Gathering. 

Questo repository contiene l'infrastruttura di backend, responsabile dell'acquisizione dei dati, dell'elaborazione matematica delle statistiche e della fornitura delle API RESTful al frontend.

## Architettura e Funzionalità Principali

*   **API RESTful ad Alte Prestazioni:** Sviluppate con FastAPI e ottimizzate tramite policy di caching multilivello (in-memory LRU cache e storage JSON locale) per garantire tempi di risposta minimi.
*   **Acquisizione Dati Automatizzata:** Motore di scraping asincrono (`master_scraper.py`) che interroga le liste pubbliche di Moxfield aggirando i limiti di rate-limiting e le protezioni anti-bot tramite `cloudscraper` e ThreadPoolExecutor.
*   **Elaborazione Statistica Avanzata:** Utilizzo della libreria `pandas` per il calcolo in tempo reale delle distribuzioni (curva del mana, tipologie di carte), dei tassi di sinergia e dei trend di utilizzo storici rispetto ai meta precedenti.
*   **Integrazione Metadati Dinamica:** Risoluzione e normalizzazione delle entità di gioco (inclusi comandanti Partner e carte bifronte) interfacciandosi con le API pubbliche di Scryfall per testi Oracle e immagini.
*   **Manutenzione Autonoma:** Sistema integrato per l'aggiornamento automatico notturno del database SQLite e il pre-warming delle cache, assicurando che i dati serviti siano sempre aggiornati senza penalizzare il primo utente della giornata.

## Stack Tecnologico

*   **Linguaggio:** Python 3
*   **Framework API:** FastAPI
*   **Data Science e Analisi:** Pandas
*   **Database:** SQLite
*   **Integrazioni di Rete:** Requests, Cloudscraper

## Requisiti di Sistema

Per eseguire l'ambiente di sviluppo in locale, è necessario:
*   Python 3.8 o superiore.
*   Un ambiente virtuale isolato (`venv`).

## Installazione e Avvio

1. Clonare il repository:
```bash
git clone [https://github.com/sirfilips/centmeta-backend.git](https://github.com/TUO_USERNAME/centmeta-backend.git)
```

2. Accedere alla directory del progetto:
```bash
cd centmeta-backend
```

3. Creare e attivare l'ambiente virtuale:
```bash
# Su sistemi Linux/macOS:
python3 -m venv venv
source venv/bin/activate

# Su sistemi Windows:
python -m venv venv
venv\Scripts\activate
```

4. Installare le dipendenze:
*(Assicurarsi di avere un file requirements.txt aggiornato contenente fastapi, uvicorn, pandas, cloudscraper, requests).*
```bash
pip install -r requirements.txt
```

5. Inizializzare il database e avviare il server:
```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Le API saranno disponibili all'indirizzo `http://localhost:8000`. La documentazione interattiva generata automaticamente da FastAPI (Swagger UI) sarà accessibile su `http://localhost:8000/docs`.

## Struttura del Progetto

*   `/main.py`: Core dell'applicazione, definizione degli endpoint API, logica di calcolo statistico e gestione delle cache.
*   `/master_scraper.py`: Script indipendente per il fetch massivo e il parsing dei deck data, con logica di salvataggio diretto in SQLite.
*   `centurion.db`: (Non versionato) Database SQLite locale generato dinamicamente.
*   `scryfall_cache.json`: (Non versionato) Cache locale per ottimizzare il traffico di rete verso le API di Scryfall.

## Conformità alla Privacy

Il backend è progettato secondo logiche "Privacy-First". Le chiamate API sono processate in forma totalmente anonima. Non viene richiesto, elaborato o salvato alcun dato personale, indirizzo e-mail o identificativo di profilazione. I log di sistema (indirizzi IP e User-Agent) vengono processati localmente ed esclusivamente per la prevenzione di attacchi DDoS (Distributed Denial of Service) e per il monitoraggio della stabilità dell'infrastruttura, conformemente al principio di Legittimo Interesse (Art. 6, par. 1, lett. f del GDPR).

## Note Legali e Proprietà Intellettuale

L'architettura del software backend e gli algoritmi di aggregazione dati sono proprietà intellettuale di Filippo Zanardi.

Le liste analizzate sono estratte pubblicamente da Moxfield. I metadati relativi alle carte sono forniti da Scryfall. CentMeta non è affiliato ufficialmente con nessuna di queste due piattaforme.

**Wizards of the Coast Fan Content Policy:**
CentMeta è un contenuto amatoriale non ufficiale consentito dalle Linee guida sui contenuti amatoriali. Non è approvato né autorizzato da Wizards. Parte dei materiali utilizzati è proprietà di Wizards of the Coast. © Wizards of the Coast LLC.
