#!/bin/bash

# Percorso base del progetto
BASE_DIR="/home/CenturionREC"

# 1. Avvia il Backend FastAPI in background
cd "$BASE_DIR"
source venv/bin/activate
uvicorn main:app --host 0.0.0.0 --port 8000 &
PID_BACKEND=$!

# 2. Avvia il Frontend Next.js in background
cd "$BASE_DIR/frontend"
npm run start -- -p 3000 &
PID_FRONTEND=$!

# Gestione della chiusura pulita (interrompe entrambi se il servizio si ferma)
trap "kill $PID_BACKEND $PID_FRONTEND; exit" SIGINT SIGTERM

# Mantiene lo script attivo per gestire i processi figli
wait
