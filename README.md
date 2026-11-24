# CentMeta - Backend

<p align="left">
  <a href="https://centmeta.it"><img src="https://img.shields.io/badge/Website-centmeta.it-3b82f6?style=for-the-badge&logo=googlechrome&logoColor=white" alt="Website" /></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-10b981?style=for-the-badge" alt="License: MIT" /></a>
  <a href="https://www.python.org/"><img src="https://img.shields.io/badge/Python_3.9+-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python 3.9+" /></a>
  <a href="https://fastapi.tiangolo.com/"><img src="https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white" alt="FastAPI" /></a>
  <a href="https://www.sqlite.org/"><img src="https://img.shields.io/badge/SQLite-WAL_Mode-003B57?style=for-the-badge&logo=sqlite&logoColor=white" alt="SQLite WAL" /></a>
  <a href="https://www.moxfield.com/"><img src="https://img.shields.io/badge/Decklists-Moxfield-582CD6?style=for-the-badge" alt="Moxfield" /></a>
  <a href="https://scryfall.com/"><img src="https://img.shields.io/badge/Cards-Scryfall_API-2B2D42?style=for-the-badge" alt="Scryfall API" /></a>
  <a href="https://github.com/sirfilips/centmeta-frontend"><img src="https://img.shields.io/badge/Frontend_Repo-Next.js-black?style=for-the-badge&logo=next.js&logoColor=white" alt="Frontend Repo" /></a>
  <a href="https://ko-fi.com/centmeta"><img src="https://img.shields.io/badge/Support_on-Ko--fi-FF5E5B?style=for-the-badge&logo=kofi&logoColor=white" alt="Support on Ko-fi" /></a>
</p>

CentMeta è un'applicazione web indipendente e open-source dedicata all'analisi statistica e al monitoraggio del metagame per il formato Centurion Commander di Magic: The Gathering.

Questo repository contiene l'infrastruttura di backend, responsabile dell'acquisizione dei dati, dell'elaborazione matematica delle statistiche e della fornitura delle API RESTful al frontend.

> 🔗 **Repository Frontend Correlato**: L'interfaccia utente web, i grafici interattivi e la dashboard analitica in Next.js 14 sono disponibili su [sirfilips/centmeta-frontend](https://github.com/sirfilips/centmeta-frontend).

## Architettura e Funzionalità Principali

*   **API RESTful ad Alte Prestazioni:** Sviluppate con FastAPI, compressione automatica `GZip`, gestione pulita del ciclo di vita (`lifespan` con inizializzazione DB e graceful shutdown) e caching multilivello (in-memory LRU cache e storage JSON locale).
*   **Acquisizione Dati Autorizzata:** Lo scraper (`master_scraper.py`) raccoglie le liste rese pubbliche dai rispettivi autori su Moxfield tramite un accesso concordato per iscritto con Moxfield. Si identifica con uno User-Agent dedicato e rispetta il limite di frequenza indicato da Moxfield (una richiesta ogni 1,2 secondi, con backoff progressivo sulle risposte 429). L'aggiornamento è incrementale, con protezione da cancellazioni su ricerche troncate o drift di paginazione.
*   **Elaborazione Statistica:** Calcolo tramite query SQL indicizzate (inclusi indici compositi su comandante e data) e Python delle distribuzioni (curva del mana, tipologie e ruoli di gioco), dei tassi di sinergia, dello storico settimanale e dei trend rispetto al periodo precedente.
*   **Integrazione Metadati Dinamica:** Risoluzione e normalizzazione delle entità di gioco (inclusi comandanti Partner e carte bifronte) interfacciandosi con le API pubbliche e i bulk data in streaming di Scryfall per testi Oracle e immagini.
*   **Manutenzione & Monitoraggio Autonomi:** Scheduler giornaliero alle ore 23:00 (`Europe/Rome`, resistente al cambio ora legale) con pre-warming delle cache, ping periodico su `health.db` (senza bloccare l'event loop HTTP), timestamp ISO UTC (`Z`) ed endpoint `/api/health` con verifica reale SQLite.

## Stack Tecnologico

*   **Linguaggio:** Python 3.9+
*   **Framework API:** FastAPI, Uvicorn & `GZipMiddleware`
*   **Database:** SQLite (modalità WAL e indici compositi)
*   **Integrazioni & Configurazione:** Requests, `python-dotenv` & `zoneinfo`

## Requisiti di Sistema

Per eseguire l'ambiente di sviluppo in locale, è necessario:
*   **Python 3.9 o superiore** (richiesto da `zoneinfo.ZoneInfo`; consigliato **Python 3.12**, utilizzato in CI. Su Windows, qualora mancasse il database dei fusi orari di sistema, installare anche `tzdata`).
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

# Su sistemi Windows (CMD / PowerShell):
python -m venv venv
venv\Scripts\activate        # CMD
.\venv\Scripts\Activate.ps1  # PowerShell
```

4. Aggiornare `pip` e installare le dipendenze dal `requirements.txt`:
```bash
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

5. Configurare le variabili d'ambiente (file `.env` opzionale in locale):

| Variabile | Obbligatoria | Descrizione |
|---|---|---|
| `MOXFIELD_UA` | Sì (per lo scraper) | User-Agent dedicato concordato con Moxfield. Senza questa variabile lo scraper e il ping di stato verso Moxfield non vengono eseguiti. |
| `ADMIN_SECRET` | Sì (per l'endpoint admin) | Segreto richiesto nell'header `X-Admin-Secret` per rimuovere un mazzo e metterlo in blacklist. |
| `TRUST_CF_HEADERS` | No | Default `1`: utilizza l'header `CF-Connecting-IP` per il rate-limit dietro Cloudflare Tunnel (`cloudflared`). Impostare a `0` se esposto direttamente senza Cloudflare. |
| `TRUSTED_PROXY_IPS` | No | Elenco di IP proxy fidati aggiuntivi (oltre a localhost) separati da virgola per l'estrazione di `X-Forwarded-For`. |
| `SCRAPER_DRY_RUN` | No | Se vale `1`, lo scraper calcola le differenze senza scaricare né salvare nulla. |
| `SCRAPER_FORCE` | No | Se vale `1`, salta il controllo di sicurezza sulla soglia minima dei risultati. |
| `ALLOW_JSON_IMPORT` | No | Se vale `1`, autorizza la conversione una-tantum da `database_centurion.json` verso SQLite. |
| `CORS_ALLOW_ORIGINS` | No | Origini extra separate da virgola consentite dal middleware CORS (es. in staging o sviluppo locale). |
| `ALLOW_LOCALHOST_CORS` | No | Se vale `1`, abilita `http://localhost:3000` nelle origini CORS consentite. |

> Tutti i parametri opzionali hanno default sicuri per la produzione dietro `cloudflared` (in particolare `TRUST_CF_HEADERS=1`): non è necessario impostarli se non per esigenze specifiche.

6. Inizializzare il database e avviare il server:
```bash
uvicorn main:app --host 127.0.0.1 --port 8000 --reload
```

Le API saranno disponibili all'indirizzo `http://localhost:8000` e la documentazione interattiva Swagger UI su `http://localhost:8000/docs`. Anche senza `MOXFIELD_UA` configurato, `uvicorn` si avvia normalmente creando uno schema SQLite locale vuoto (`centurion.db`), utile per testare gli endpoint e sviluppare in locale.

## Test e Verifica

```bash
# Esecuzione suite di unit test
python -m unittest discover -s tests -v

# Controllo errori di sintassi/logica con ruff (come in CI)
ruff check --select E9,F .
```

## Struttura del Progetto

*   `/main.py`: Core dell'applicazione FastAPI, `lifespan()`, endpoint API, calcolo statistico, rate-limit e gestione delle cache.
*   `/master_scraper.py`: Script indipendente per il fetch incrementale e il parsing dei mazzi, con gestione schema/indici e salvataggio in SQLite.
*   `/scryfall_search.py`: Parser e motore di query compatibile con la sintassi Scryfall (operatori booleani, ruoli funzionali, `id:`, `commander:`, regex sicure).
*   `/start.sh`: Script di avvio produzione per servizio systemd (Uvicorn backend + frontend Next.js).
*   `/tests/`: Suite di unit test backend (`unittest`).
*   `centurion.db` & `health.db`: (Non versionati) Database SQLite locali generati dinamicamente per i mazzi e per lo storico ping/stato servizi.
*   `scryfall_cache.json` & `scryfall_sets_cache.json`: (Non versionati) Cache locali per carte ed espansioni da Scryfall.

## Conformità alla Privacy

Il backend è progettato secondo logiche "Privacy-First". Le chiamate API sono processate in forma totalmente anonima. Non viene richiesto, elaborato o salvato alcun dato anagrafico o identificativo di profilazione. I log tecnici di sistema (indirizzi IP e User-Agent) e i contatori in memoria per il rate-limit vengono processati localmente ed esclusivamente per la prevenzione di attacchi informatici e per il monitoraggio della stabilità dell'infrastruttura, conformemente al principio di Legittimo Interesse (Art. 6, par. 1, lett. f del GDPR).

## Contribuire

Lo sviluppo avviene sul branch `dev`, poi unito su `main` per il deploy:

1. Apri una issue per segnalare bug o proporre miglioramenti.
2. Lavora su un branch derivato da `dev`.
3. Prima di aprire una Pull Request, assicurati che `python -m unittest discover -s tests -v` e `ruff check --select E9,F .` passino senza errori.

## Licenza, Note Legali e Proprietà Intellettuale

Il codice sorgente è rilasciato sotto licenza open-source [MIT](./LICENSE).

L'architettura del software backend e gli algoritmi di aggregazione dati sono proprietà intellettuale di Filippo Zanardi.

Le informazioni letterali e grafiche riguardanti Magic: The Gathering sono di proprietà di Wizards of the Coast, LLC (Hasbro, Inc.). CentMeta è un contenuto amatoriale non ufficiale consentito dalla **Wizards of the Coast Fan Content Policy** e non è approvato né autorizzato da Wizards.
