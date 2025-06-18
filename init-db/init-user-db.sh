#!/bin/bash
set -e

# Skrip ini akan membaca variabel FORECASTING_DB_USER, FORECASTING_DB_PASSWORD, dll.
# yang sudah ada di dalam environment kontainer.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Membuat user baru menggunakan variabel environment yang sudah diteruskan
    CREATE USER "${FORECASTING_USER}" WITH PASSWORD '${FORECASTING_PASSWORD}';

    -- Membuat database baru dengan owner user yang baru dibuat
    CREATE DATABASE "${FORECASTING_DB}" WITH OWNER "${FORECASTING_USER}";
EOSQL