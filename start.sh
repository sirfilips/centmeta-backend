#!/usr/bin/env bash
# Avvio unico di CentMeta: backend FastAPI + frontend Next.js.
# Pensato per essere eseguito come singolo servizio (es. systemd con Restart=always):
# se uno dei due processi termina, lo script ferma anche l'altro ed esce con errore,
# così il service manager può riavviare tutto.
set -euo pipefail

# --- Configurazione (sovrascrivibile da variabili d'ambiente) -----------------
BASE_DIR="${BASE_DIR:-/home/CenturionREC}"          # cartella con main.py e venv/
FRONTEND_DIR="${FRONTEND_DIR:-$BASE_DIR/frontend}"  # cartella del progetto Next.js
VENV_DIR="${VENV_DIR:-$BASE_DIR/venv}"
ENV_FILE="${ENV_FILE:-$BASE_DIR/.env}"              # file con MOXFIELD_UA, ADMIN_SECRET, ...
BIND_HOST="${BIND_HOST:-127.0.0.1}"                 # solo locale: il traffico esterno passa dal proxy
BACKEND_PORT="${BACKEND_PORT:-8000}"
FRONTEND_PORT="${FRONTEND_PORT:-3000}"
STOP_TIMEOUT="${STOP_TIMEOUT:-30}"                  # secondi concessi per lo shutdown pulito

log() { echo "[start.sh] $*"; }
die() { echo "[start.sh] ERRORE: $*" >&2; exit 1; }

# --- Variabili d'ambiente -------------------------------------------------------
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
  log "Variabili caricate da $ENV_FILE"
else
  log "Nessun file $ENV_FILE trovato: uso solo le variabili già presenti nell'ambiente."
fi

[[ -n "${MOXFIELD_UA:-}" ]]  || log "ATTENZIONE: MOXFIELD_UA non impostata, lo scraper notturno non verrà eseguito."
[[ -n "${ADMIN_SECRET:-}" ]] || log "ATTENZIONE: ADMIN_SECRET non impostata, l'endpoint admin risponderà sempre 403."

# --- Controlli preliminari --------------------------------------------------------
[[ -f "$BASE_DIR/main.py" ]]                 || die "main.py non trovato in $BASE_DIR"
[[ -x "$VENV_DIR/bin/uvicorn" ]]             || die "uvicorn non trovato in $VENV_DIR (venv mancante o dipendenze non installate)"
[[ -x "$FRONTEND_DIR/node_modules/.bin/next" ]] || die "Next.js non trovato in $FRONTEND_DIR (eseguire 'npm ci')"
[[ -f "$FRONTEND_DIR/.next/BUILD_ID" ]]      || die "Build del frontend assente in $FRONTEND_DIR (eseguire 'npm run build')"

# --- Shutdown pulito --------------------------------------------------------------
PID_BACKEND=""
PID_FRONTEND=""

stop_all() {
  trap - SIGINT SIGTERM
  log "Arresto in corso..."
  for pid in $PID_BACKEND $PID_FRONTEND; do
    kill -TERM "$pid" 2>/dev/null || true
  done
  # Attende la chiusura spontanea, poi forza
  local waited=0
  while (( waited < STOP_TIMEOUT )); do
    local alive=0
    for pid in $PID_BACKEND $PID_FRONTEND; do
      kill -0 "$pid" 2>/dev/null && alive=1
    done
    (( alive == 0 )) && break
    sleep 1
    waited=$((waited + 1))
  done
  for pid in $PID_BACKEND $PID_FRONTEND; do
    if kill -0 "$pid" 2>/dev/null; then
      log "Processo $pid non terminato entro ${STOP_TIMEOUT}s, invio SIGKILL."
      kill -KILL "$pid" 2>/dev/null || true
    fi
  done
}

on_signal() {
  stop_all
  exit 0
}
trap on_signal SIGINT SIGTERM

# --- Avvio ------------------------------------------------------------------------
cd "$BASE_DIR"
log "Avvio backend su $BIND_HOST:$BACKEND_PORT"
"$VENV_DIR/bin/uvicorn" main:app --app-dir "$BASE_DIR" --host "$BIND_HOST" --port "$BACKEND_PORT" &
PID_BACKEND=$!

cd "$FRONTEND_DIR"
log "Avvio frontend su $BIND_HOST:$FRONTEND_PORT"
"$FRONTEND_DIR/node_modules/.bin/next" start -p "$FRONTEND_PORT" -H "$BIND_HOST" &
PID_FRONTEND=$!

# --- Supervisione -----------------------------------------------------------------
# Se uno dei due processi termina (crash o altro), ferma anche l'altro ed esce con errore.
set +e
wait -n "$PID_BACKEND" "$PID_FRONTEND"
EXIT_CODE=$?
set -e

log "Uno dei processi è terminato (codice $EXIT_CODE). Arresto anche l'altro."
stop_all
exit $(( EXIT_CODE == 0 ? 1 : EXIT_CODE ))
