"""
DAG: citybikes_load
Загружает данные CityBikes из JSON в PostgreSQL.
Таблицы: e_raikhin_networks, e_raikhin_stations
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from psycopg2.extras import Json, execute_values

log = logging.getLogger(__name__)

POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")
DATA_DIR = Path(os.getenv("CITYBIKES_DATA_DIR", "/opt/airflow/data"))

NETWORKS_FILE = DATA_DIR / "networks.json"
STATIONS_FILE = DATA_DIR / "stations_bicing.json"

NETWORKS_TABLE = "e_raikhin_networks"
STATIONS_TABLE = "e_raikhin_stations"

CREATE_TABLES_SQL = f"""
CREATE TABLE IF NOT EXISTS {NETWORKS_TABLE} (
    id          TEXT PRIMARY KEY,
    name        TEXT,
    location    JSONB,
    href        TEXT,
    company     JSONB,
    gbfs_href   TEXT,
    system      TEXT,
    source      TEXT,
    ebikes      BOOLEAN,
    scooters    BOOLEAN,
    license     JSONB,
    license_url TEXT,
    instances   JSONB,
    raw_data    JSONB,
    loaded_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS {STATIONS_TABLE} (
    id          TEXT,
    network_id  TEXT NOT NULL REFERENCES {NETWORKS_TABLE}(id) ON DELETE CASCADE,
    name        TEXT,
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    "timestamp" TEXT,
    free_bikes  INTEGER,
    empty_slots INTEGER,
    extra       JSONB,
    raw_data    JSONB,
    loaded_at   TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (network_id, id)
);

CREATE INDEX IF NOT EXISTS idx_{STATIONS_TABLE}_network
    ON {STATIONS_TABLE}(network_id);
"""


def _read_json(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Файл не найден: {path}")

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _to_int(value):
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_bool(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "y")
    return bool(value)


def _as_jsonb(value):
    """Оборачивает dict/list в Json, None пропускает как NULL."""
    return Json(value) if isinstance(value, (dict, list)) else None


def _station_id(station: dict) -> str:
    sid = station.get("id")
    if sid is not None:
        return str(sid)

    extra = station.get("extra")
    if isinstance(extra, dict):
        sid = extra.get("id") or extra.get("uid")
        if sid is not None:
            return str(sid)

    raw = f"{station.get('name')}|{station.get('latitude')}|{station.get('longitude')}"
    return "hash_" + hashlib.md5(raw.encode("utf-8")).hexdigest()


def create_tables():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLES_SQL)
        conn.commit()
        log.info("Таблицы %s и %s созданы/обновлены", NETWORKS_TABLE, STATIONS_TABLE)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def load_networks():
    data = _read_json(NETWORKS_FILE)

    networks = data.get("networks", []) if isinstance(data, dict) else data
    if not isinstance(networks, list):
        networks = []

    rows = []
    for net in networks:
        if not isinstance(net, dict):
            continue

        net_id = net.get("id")
        if net_id is None:
            log.warning("Пропуск сети без id: %s", net.get("name"))
            continue

        company = net.get("company")
        if isinstance(company, str):
            company = [company]

        # license в выгрузке — объект {"name": ..., "url": ...}; приводим к JSONB,
        # а license_url выделяем в отдельную колонку для удобства запросов.
        license_obj = net.get("license")
        license_url = None
        if isinstance(license_obj, dict):
            license_url = license_obj.get("url")
        elif isinstance(license_obj, str):
            license_url = license_obj
            license_obj = None

        rows.append((
            str(net_id),
            net.get("name"),
            _as_jsonb(net.get("location")),
            net.get("href"),
            _as_jsonb(company),
            net.get("gbfs_href"),
            net.get("system"),
            net.get("source"),
            _to_bool(net.get("ebikes")),
            _to_bool(net.get("scooters")),
            _as_jsonb(license_obj),
            license_url,
            _as_jsonb(net.get("instances")),
            Json(net),  # сохраняем исходный объект целиком
        ))

    if not rows:
        log.warning("В %s нет валидных сетей", NETWORKS_FILE)
        return

    insert_sql = f"""
        INSERT INTO {NETWORKS_TABLE}
            (id, name, location, href, company, gbfs_href, system, source, ebikes,
             scooters, license, license_url, instances, raw_data)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            location = EXCLUDED.location,
            href = EXCLUDED.href,
            company = EXCLUDED.company,
            gbfs_href = EXCLUDED.gbfs_href,
            system = EXCLUDED.system,
            source = EXCLUDED.source,
            ebikes = EXCLUDED.ebikes,
            scooters = EXCLUDED.scooters,
            license = EXCLUDED.license,
            license_url = EXCLUDED.license_url,
            instances = EXCLUDED.instances,
            raw_data = EXCLUDED.raw_data,
            loaded_at = NOW();
    """

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, rows, page_size=1000)
        conn.commit()
        log.info("Загружено/обновлено сетей: %d", len(rows))
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _network_id_from_filename(path: Path) -> str | None:
    """stations_bicing.json -> bicing.

    В выгрузке по сети нет блока "network", поэтому сеть берётся из имени файла.
    """
    stem = path.stem
    if stem.startswith("stations_"):
        return stem[len("stations_"):] or None
    return None


def _pick(station: dict, *keys):
    """Возвращает первое непустое значение среди ключей (в разных выгрузках
    свободные велосипеды/слоты называются по-разному)."""
    for key in keys:
        value = station.get(key)
        if value is not None:
            return value
    return None


def load_stations():
    data = _read_json(STATIONS_FILE)

    if isinstance(data, dict):
        network_obj = data.get("network")
        if not isinstance(network_obj, dict):
            network_obj = data
    else:
        network_obj = {}

    network_id = network_obj.get("id") or _network_id_from_filename(STATIONS_FILE)
    stations = network_obj.get("stations", [])

    if network_id is None:
        log.error("Не удалось определить network_id в %s", STATIONS_FILE)
        raise ValueError("network_id не найден")

    network_id = str(network_id)
    log.info("Обрабатываем станции сети %s из %s", network_id, STATIONS_FILE)

    if not isinstance(stations, list) or not stations:
        log.warning("Для сети %s нет станций в %s", network_id, STATIONS_FILE)
        return

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT 1 FROM {NETWORKS_TABLE} WHERE id = %s",
                (network_id,),
            )
            if cur.fetchone() is None:
                log.warning(
                    "Сеть %s не найдена в %s, станции пропущены",
                    network_id,
                    NETWORKS_TABLE,
                )
                return

        rows = []
        for station in stations:
            if not isinstance(station, dict):
                continue

            sid = _station_id(station)
            if not sid:
                continue

            extra = station.get("extra")

            rows.append((
                sid,
                network_id,
                station.get("name"),
                _to_float(station.get("latitude")),
                _to_float(station.get("longitude")),
                station.get("timestamp"),
                # в выгрузке bicing свободные велосипеды — "bikes", слоты — "free"
                _to_int(_pick(station, "free_bikes", "bikes", "num_bikes_available")),
                _to_int(_pick(station, "empty_slots", "free", "num_docks_available")),
                _as_jsonb(extra),
                Json(station),
            ))

        if not rows:
            log.warning("Для сети %s нет валидных станций", network_id)
            return

        insert_sql = f"""
            INSERT INTO {STATIONS_TABLE}
                (id, network_id, name, latitude, longitude, "timestamp",
                 free_bikes, empty_slots, extra, raw_data)
            VALUES %s
            ON CONFLICT (network_id, id) DO UPDATE SET
                name = EXCLUDED.name,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                "timestamp" = EXCLUDED."timestamp",
                free_bikes = EXCLUDED.free_bikes,
                empty_slots = EXCLUDED.empty_slots,
                extra = EXCLUDED.extra,
                raw_data = EXCLUDED.raw_data,
                loaded_at = NOW();
        """

        with conn.cursor() as cur:
            execute_values(cur, insert_sql, rows, page_size=1000)
        conn.commit()
        log.info("Загружено/обновлено станций для сети %s: %d", network_id, len(rows))
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


default_args = {
    "owner": "e_raikhin",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="e_raikhin_citybikes_load",
    default_args=default_args,
    description="Load CityBikes networks and stations JSON to PostgreSQL",
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["citybikes", "e_raikhin"],
) as dag:
    create_tables_task = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables,
    )

    load_networks_task = PythonOperator(
        task_id="load_networks",
        python_callable=load_networks,
    )

    load_stations_task = PythonOperator(
        task_id="load_stations",
        python_callable=load_stations,
    )

    create_tables_task >> load_networks_task >> load_stations_task
