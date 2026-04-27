"""
Weather Data Consumer - Kafka → PostgreSQL
=============================================
Consumes weather and air quality messages from Kafka
and writes them to PostgreSQL in the raw_weather schema.

Tables created:
  - raw_weather.weather_observations
  - raw_weather.air_quality_observations
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "weather-consumer-group")
WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather.observations.raw")
AIR_QUALITY_TOPIC = os.getenv("AIR_QUALITY_TOPIC", "weather.air_quality.raw")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "analytics")
POSTGRES_USER = os.getenv("POSTGRES_USER", "analytics")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "analytics")
POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA", "raw_weather")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
BATCH_TIMEOUT_MS = int(os.getenv("BATCH_TIMEOUT_MS", "5000"))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("weather-consumer")


# ---------------------------------------------------------------------------
# PostgreSQL Setup
# ---------------------------------------------------------------------------

DDL_WEATHER = f"""
CREATE SCHEMA IF NOT EXISTS {POSTGRES_SCHEMA};

CREATE TABLE IF NOT EXISTS {POSTGRES_SCHEMA}.weather_observations (
    id                      BIGSERIAL PRIMARY KEY,
    location_id             VARCHAR(50) NOT NULL,
    location_name           VARCHAR(100),
    latitude                DOUBLE PRECISION,
    longitude               DOUBLE PRECISION,
    elevation_m             DOUBLE PRECISION,
    context                 VARCHAR(50),
    observation_time        TIMESTAMP NOT NULL,
    data_resolution         VARCHAR(20),
    ingested_at             TIMESTAMP WITH TIME ZONE,
    loaded_at               TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    source                  VARCHAR(50),
    model                   VARCHAR(50),
    -- Core weather
    temperature_2m          DOUBLE PRECISION,
    relative_humidity_2m    DOUBLE PRECISION,
    dew_point_2m            DOUBLE PRECISION,
    apparent_temperature    DOUBLE PRECISION,
    -- Precipitation
    precipitation           DOUBLE PRECISION,
    rain                    DOUBLE PRECISION,
    snowfall                DOUBLE PRECISION,
    snow_depth              DOUBLE PRECISION,
    weather_code            INTEGER,
    -- Pressure
    pressure_msl            DOUBLE PRECISION,
    surface_pressure        DOUBLE PRECISION,
    -- Cloud & visibility
    cloud_cover             DOUBLE PRECISION,
    cloud_cover_low         DOUBLE PRECISION,
    cloud_cover_mid         DOUBLE PRECISION,
    cloud_cover_high        DOUBLE PRECISION,
    visibility              DOUBLE PRECISION,
    -- Wind
    wind_speed_10m          DOUBLE PRECISION,
    wind_direction_10m      DOUBLE PRECISION,
    wind_gusts_10m          DOUBLE PRECISION,
    -- UV & radiation
    uv_index                DOUBLE PRECISION,
    uv_index_clear_sky      DOUBLE PRECISION,
    direct_radiation        DOUBLE PRECISION,
    diffuse_radiation       DOUBLE PRECISION,
    -- Soil
    soil_temperature_0cm    DOUBLE PRECISION,
    soil_moisture_0_to_1cm  DOUBLE PRECISION
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_weather_obs_location_time
    ON {POSTGRES_SCHEMA}.weather_observations (location_id, observation_time);

CREATE INDEX IF NOT EXISTS idx_weather_obs_time
    ON {POSTGRES_SCHEMA}.weather_observations (observation_time);

CREATE INDEX IF NOT EXISTS idx_weather_obs_resolution
    ON {POSTGRES_SCHEMA}.weather_observations (data_resolution);

-- Prevent duplicate observations
CREATE UNIQUE INDEX IF NOT EXISTS idx_weather_obs_unique
    ON {POSTGRES_SCHEMA}.weather_observations (location_id, observation_time, data_resolution);
"""

DDL_AIR_QUALITY = f"""
CREATE TABLE IF NOT EXISTS {POSTGRES_SCHEMA}.air_quality_observations (
    id                          BIGSERIAL PRIMARY KEY,
    location_id                 VARCHAR(50) NOT NULL,
    location_name               VARCHAR(100),
    latitude                    DOUBLE PRECISION,
    longitude                   DOUBLE PRECISION,
    context                     VARCHAR(50),
    observation_time            TIMESTAMP NOT NULL,
    data_resolution             VARCHAR(20),
    ingested_at                 TIMESTAMP WITH TIME ZONE,
    loaded_at                   TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    source                      VARCHAR(50),
    -- Particulates
    pm10                        DOUBLE PRECISION,
    pm2_5                       DOUBLE PRECISION,
    dust                        DOUBLE PRECISION,
    -- Gases
    carbon_monoxide             DOUBLE PRECISION,
    nitrogen_dioxide            DOUBLE PRECISION,
    sulphur_dioxide             DOUBLE PRECISION,
    ozone                       DOUBLE PRECISION,
    -- UV
    uv_index                    DOUBLE PRECISION,
    uv_index_clear_sky          DOUBLE PRECISION,
    -- AQI indices
    us_aqi                      DOUBLE PRECISION,
    us_aqi_pm2_5                DOUBLE PRECISION,
    us_aqi_pm10                 DOUBLE PRECISION,
    us_aqi_nitrogen_dioxide     DOUBLE PRECISION,
    us_aqi_ozone                DOUBLE PRECISION,
    us_aqi_sulphur_dioxide      DOUBLE PRECISION,
    us_aqi_carbon_monoxide      DOUBLE PRECISION,
    european_aqi                DOUBLE PRECISION,
    -- Pollen
    alder_pollen                DOUBLE PRECISION,
    birch_pollen                DOUBLE PRECISION,
    grass_pollen                DOUBLE PRECISION,
    mugwort_pollen              DOUBLE PRECISION,
    olive_pollen                DOUBLE PRECISION,
    ragweed_pollen              DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_aq_obs_location_time
    ON {POSTGRES_SCHEMA}.air_quality_observations (location_id, observation_time);

CREATE INDEX IF NOT EXISTS idx_aq_obs_time
    ON {POSTGRES_SCHEMA}.air_quality_observations (observation_time);

CREATE UNIQUE INDEX IF NOT EXISTS idx_aq_obs_unique
    ON {POSTGRES_SCHEMA}.air_quality_observations (location_id, observation_time, data_resolution);
"""


def get_pg_connection():
    """Create a PostgreSQL connection (single attempt, no retry)."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def connect_postgres(max_retries: int = 12, retry_delay: int = 10) -> psycopg2.extensions.connection:
    """Connect to PostgreSQL with retry logic — mirrors create_consumer() for Kafka."""
    for attempt in range(1, max_retries + 1):
        try:
            conn = get_pg_connection()
            logger.info("Connected to PostgreSQL at %s:%s", POSTGRES_HOST, POSTGRES_PORT)
            return conn
        except psycopg2.OperationalError as exc:
            logger.warning(
                "PostgreSQL not available (attempt %d/%d), retrying in %ds...: %s",
                attempt, max_retries, retry_delay, exc,
            )
            time.sleep(retry_delay)
    raise RuntimeError(f"Failed to connect to PostgreSQL after {max_retries} attempts")


def init_database():
    """Create schema and tables if they don't exist."""
    conn = connect_postgres()
    try:
        with conn.cursor() as cur:
            cur.execute(DDL_WEATHER)
            cur.execute(DDL_AIR_QUALITY)
        conn.commit()
        logger.info("Database schema and tables initialized")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Upsert Logic
# ---------------------------------------------------------------------------

WEATHER_COLUMNS = [
    "location_id", "location_name", "latitude", "longitude", "elevation_m",
    "context", "observation_time", "data_resolution", "ingested_at",
    "source", "model",
    "temperature_2m", "relative_humidity_2m", "dew_point_2m",
    "apparent_temperature", "precipitation", "rain", "snowfall", "snow_depth",
    "weather_code", "pressure_msl", "surface_pressure",
    "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high",
    "visibility", "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m",
    "uv_index", "uv_index_clear_sky", "direct_radiation", "diffuse_radiation",
    "soil_temperature_0cm", "soil_moisture_0_to_1cm",
]

AQ_COLUMNS = [
    "location_id", "location_name", "latitude", "longitude", "context",
    "observation_time", "data_resolution", "ingested_at", "source",
    "pm10", "pm2_5", "dust",
    "carbon_monoxide", "nitrogen_dioxide", "sulphur_dioxide", "ozone",
    "uv_index", "uv_index_clear_sky",
    "us_aqi", "us_aqi_pm2_5", "us_aqi_pm10",
    "us_aqi_nitrogen_dioxide", "us_aqi_ozone",
    "us_aqi_sulphur_dioxide", "us_aqi_carbon_monoxide",
    "european_aqi",
    "alder_pollen", "birch_pollen", "grass_pollen",
    "mugwort_pollen", "olive_pollen", "ragweed_pollen",
]


def upsert_weather_batch(conn, records: list[dict]) -> int:
    """Upsert a batch of weather observations using ON CONFLICT."""
    if not records:
        return 0

    cols = ", ".join(WEATHER_COLUMNS)
    placeholders = ", ".join([f"%({c})s" for c in WEATHER_COLUMNS])
    update_cols = ", ".join(
        [f"{c} = EXCLUDED.{c}" for c in WEATHER_COLUMNS
         if c not in ("location_id", "observation_time", "data_resolution")]
    )

    sql = f"""
        INSERT INTO {POSTGRES_SCHEMA}.weather_observations ({cols})
        VALUES ({placeholders})
        ON CONFLICT (location_id, observation_time, data_resolution)
        DO UPDATE SET {update_cols}, loaded_at = NOW()
    """

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, records, page_size=100)
    conn.commit()
    return len(records)


def upsert_aq_batch(conn, records: list[dict]) -> int:
    """Upsert a batch of air quality observations using ON CONFLICT."""
    if not records:
        return 0

    cols = ", ".join(AQ_COLUMNS)
    placeholders = ", ".join([f"%({c})s" for c in AQ_COLUMNS])
    update_cols = ", ".join(
        [f"{c} = EXCLUDED.{c}" for c in AQ_COLUMNS
         if c not in ("location_id", "observation_time", "data_resolution")]
    )

    sql = f"""
        INSERT INTO {POSTGRES_SCHEMA}.air_quality_observations ({cols})
        VALUES ({placeholders})
        ON CONFLICT (location_id, observation_time, data_resolution)
        DO UPDATE SET {update_cols}, loaded_at = NOW()
    """

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, records, page_size=100)
    conn.commit()
    return len(records)


# ---------------------------------------------------------------------------
# Kafka Consumer Setup
# ---------------------------------------------------------------------------


def create_consumer(max_retries: int = 10, retry_delay: int = 10) -> KafkaConsumer:
    """Create Kafka consumer with retry logic."""
    for attempt in range(1, max_retries + 1):
        try:
            consumer = KafkaConsumer(
                WEATHER_TOPIC,
                AIR_QUALITY_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                max_poll_records=500,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
            )
            logger.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
            return consumer
        except NoBrokersAvailable:
            logger.warning(
                "Kafka not available (attempt %d/%d), retrying in %ds...",
                attempt, max_retries, retry_delay,
            )
            time.sleep(retry_delay)

    raise RuntimeError(f"Failed to connect to Kafka after {max_retries} attempts")


# ---------------------------------------------------------------------------
# Main Consumer Loop
# ---------------------------------------------------------------------------


def main():
    """Main consumer loop with batch processing."""
    logger.info("Weather Consumer starting up...")
    logger.info("Kafka: %s | Group: %s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID)
    logger.info("Topics: %s, %s", WEATHER_TOPIC, AIR_QUALITY_TOPIC)
    logger.info("PostgreSQL: %s:%s/%s.%s", POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_SCHEMA)

    # Initialize database
    init_database()

    # Create consumer
    consumer = create_consumer()
    conn = connect_postgres()

    weather_batch: list[dict] = []
    aq_batch: list[dict] = []
    last_flush = time.time()

    try:
        while True:
            messages = consumer.poll(timeout_ms=BATCH_TIMEOUT_MS)

            for tp, records in messages.items():
                for record in records:
                    msg = record.value

                    # Normalize the record for PostgreSQL
                    # Fill missing columns with None
                    if record.topic == WEATHER_TOPIC:
                        row = {col: msg.get(col) for col in WEATHER_COLUMNS}
                        weather_batch.append(row)
                    elif record.topic == AIR_QUALITY_TOPIC:
                        row = {col: msg.get(col) for col in AQ_COLUMNS}
                        aq_batch.append(row)

            # Flush batches when size or time threshold met
            now = time.time()
            should_flush = (
                len(weather_batch) >= BATCH_SIZE
                or len(aq_batch) >= BATCH_SIZE
                or (now - last_flush) >= (BATCH_TIMEOUT_MS / 1000)
            )

            if should_flush and (weather_batch or aq_batch):
                try:
                    w_count = upsert_weather_batch(conn, weather_batch)
                    aq_count = upsert_aq_batch(conn, aq_batch)

                    if w_count or aq_count:
                        logger.info(
                            "Flushed → Weather: %d rows | Air Quality: %d rows",
                            w_count, aq_count,
                        )

                    consumer.commit()
                    weather_batch.clear()
                    aq_batch.clear()
                    last_flush = now

                except psycopg2.Error as e:
                    logger.error("PostgreSQL error: %s", e)
                    conn.close()
                    conn = get_pg_connection()
                    # Don't clear batches — retry on next cycle

    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    finally:
        # Final flush
        if weather_batch or aq_batch:
            try:
                upsert_weather_batch(conn, weather_batch)
                upsert_aq_batch(conn, aq_batch)
                consumer.commit()
            except Exception:
                pass

        consumer.close()
        conn.close()
        logger.info("Consumer closed.")


if __name__ == "__main__":
    main()
