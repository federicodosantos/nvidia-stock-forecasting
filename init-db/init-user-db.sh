#!/bin/bash
set -e 

# Jalankan perintah SQL menggunakan psql.
# Perintah ini akan dieksekusi sebagai superuser 'postgres' (atau $POSTGRES_USER)
# yang sudah ada secara default saat container pertama kali dijalankan.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Membuat user baru menggunakan variabel environment yang sudah diteruskan
    CREATE USER ${FORECASTING_DB_USER} WITH PASSWORD '${FORECASTING_DB_PASSWORD}';

    -- Membuat database baru dengan owner user yang baru dibuat
    CREATE DATABASE "${FORECASTING_DB_NAME}" WITH OWNER ${FORECASTING_DB_USER};

EOSQL
