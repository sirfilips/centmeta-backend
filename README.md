# CentMeta - Backend

CentMeta è un'applicazione web indipendente e open-source dedicata all'analisi statistica e al monitoraggio del metagame per il formato Centurion Commander di Magic: The Gathering. 

Questo repository contiene l'infrastruttura di backend, responsabile dell'acquisizione dei dati, dell'elaborazione matematica delle statistiche e della fornitura delle API RESTful al frontend.

## Architettura e Funzionalità Principali

*   **API RESTful ad Alte Prestazioni:** Sviluppate con FastAPI e ottimizzate tramite policy di caching multilivello (in-memory LRU cache e storage JSON locale) per garantire tempi di risposta minimi.
*   **Acquisizione Dati Autorizzata:** Lo scraper (`master_scraper.py`) raccoglie le liste rese pubbliche dai rispettivi autori su Moxfield tramite un accesso concordato per iscritto con Moxfield. Si identifica con uno User-Agent dedicato e rispetta il limite di frequenza indicato da Moxfield (una richiesta ogni 1,2 secondi, con backoff progressivo sulle risposte 429). L'aggiornamento è incrementale: vengono scaricati solo i mazzi nuovi o modificati.
*   **Elaborazione Statistica:** Calcolo tramite query SQL e Python delle distribuzioni (curva del mana, tipologie di carte), dei tassi di sinergia e dei trend di utilizzo rispetto al periodo precedente.
*   **Integrazione Metadati Dinamica:** Risoluzione e normalizzazione delle entità di gioco (inclusi comandanti Partner e carte bifronte) interfacciandosi con le API pubbliche di Scryfall per testi Oracle e immagini.
*   **Manutenzione Autonoma:** Sistema integrato per l'aggiornamento automatico notturno del database SQLite e il pre-warming delle cache, assicurando che i dati serviti siano sempre aggiornati senza penalizzare il primo utente della giornata.

## Stack Tecnologico

*   **Linguaggio:** Python 3
*   **Framework API:** FastAPI (Uvicorn)
*   **Database:** SQLite (modalità WAL)
*   **Integrazioni di Rete:** Requests

## Requisiti di Sistema

Per eseguire l'ambiente di sviluppo in locale, è necessario:
*   Python 3.8 o superiore.
*   Un ambiente virtuale isolato (`venv`).

## Installazione e Avvio

1. Clonare il repository:
```bash
git clone https://github.com/sirfilips/centmeta-backend.git
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
```bash
pip install fastapi uvicorn requests
```

5. Configurare le variabili d'ambiente:

| Variabile | Obbligatoria | Descrizione |
|---|---|---|
| `MOXFIELD_UA` | Sì (per lo scraper) | User-Agent dedicato concordato con Moxfield. Senza questa variabile lo scraper e il ping di stato verso Moxfield non vengono eseguiti. |
| `ADMIN_SECRET` | Sì (per l'endpoint admin) | Segreto richiesto nell'header `X-Admin-Secret` per rimuovere un mazzo e metterlo in blacklist. |
| `SCRAPER_DRY_RUN` | No | Se vale `1`, lo scraper calcola le differenze senza scaricare né salvare nulla. |
| `SCRAPER_FORCE` | No | Se vale `1`, salta il controllo di sicurezza sulla soglia minima dei risultati. |

6. Inizializzare il database e avviare il server:
```bash
uvicorn main:app --host 127.0.0.1 --port 8000 --reload
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

Le liste analizzate derivano da liste rese pubbliche dai rispettivi autori su Moxfield. La raccolta avviene tramite un accesso concordato per iscritto con Moxfield, nel rispetto del limite di frequenza da loro indicato. Di ciascun mazzo vengono conservati solo l'identificativo pubblico, il comandante, la data di aggiornamento e l'elenco delle carte. Chi non desidera che un proprio mazzo sia incluso nelle statistiche può chiederne la rimozione scrivendo a privacy@centmeta.it. I metadati relativi alle carte sono forniti da Scryfall. CentMeta non è affiliato ufficialmente con nessuna di queste due piattaforme.

**Wizards of the Coast Fan Content Policy:**
CentMeta è un contenuto amatoriale non ufficiale consentito dalle Linee guida sui contenuti amatoriali. Non è approvato né autorizzato da Wizards. Parte dei materiali utilizzati è proprietà di Wizards of the Coast. © Wizards of the Coast LLC.
