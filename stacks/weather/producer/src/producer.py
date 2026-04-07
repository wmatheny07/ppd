"""
Weather Data Producer - Open-Meteo → Kafka
============================================
Polls Open-Meteo Forecast and Air Quality APIs every 15 minutes
for 5 monitored locations and publishes to Kafka topics.

Locations:
  - Summerville, SC (home base)
  - La Plata, MD
  - Ooltewah, TN
  - Fairfax, VA
  - Clifton Forge, VA
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather.observations.raw")
AIR_QUALITY_TOPIC = os.getenv("AIR_QUALITY_TOPIC", "weather.air_quality.raw")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "900"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("weather-producer")

# ---------------------------------------------------------------------------
# Monitored Locations
# ---------------------------------------------------------------------------

LOCATIONS = [
    {
        "location_id": "summerville_sc",
        "name": "Summerville, SC",
        "latitude": 33.0185,
        "longitude": -80.1756,
        "timezone": "America/New_York",
        "context": "home_base",
    },
    {
        "location_id": "la_plata_md",
        "name": "La Plata, MD",
        "latitude": 38.5293,
        "longitude": -76.9750,
        "timezone": "America/New_York",
        "context": "family",
    },
    {
        "location_id": "ooltewah_tn",
        "name": "Ooltewah, TN",
        "latitude": 35.0773,
        "longitude": -85.0586,
        "timezone": "America/New_York",
        "context": "interest_area",
    },
    {
        "location_id": "fairfax_va",
        "name": "Fairfax, VA",
        "latitude": 38.8462,
        "longitude": -77.3064,
        "timezone": "America/New_York",
        "context": "family",
    },
    {
        "location_id": "clifton_forge_va",
        "name": "Clifton Forge, VA",
        "latitude": 37.8165,
        "longitude": -79.8242,
        "timezone": "America/New_York",
        "context": "eldercare",
    },
]

# ---------------------------------------------------------------------------
# Open-Meteo API Configuration
# ---------------------------------------------------------------------------

FORECAST_BASE_URL = "https://api.open-meteo.com/v1/forecast"
AIR_QUALITY_BASE_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

# Health-relevant weather variables (15-minute resolution via HRRR for US)
WEATHER_VARIABLES_15MIN = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "apparent_temperature",
    "precipitation",
    "rain",
    "weather_code",
    "pressure_msl",
    "surface_pressure",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
]

WEATHER_VARIABLES_HOURLY = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "apparent_temperature",
    "precipitation",
    "rain",
    "snowfall",
    "snow_depth",
    "weather_code",
    "pressure_msl",
    "surface_pressure",
    "cloud_cover",
    "cloud_cover_low",
    "cloud_cover_mid",
    "cloud_cover_high",
    "visibility",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "uv_index",
    "uv_index_clear_sky",
    "soil_temperature_0cm",
    "soil_moisture_0_to_1cm",
    "direct_radiation",
    "diffuse_radiation",
]

AIR_QUALITY_VARIABLES = [
    "pm10",
    "pm2_5",
    "carbon_monoxide",
    "nitrogen_dioxide",
    "sulphur_dioxide",
    "ozone",
    "dust",
    "uv_index",
    "uv_index_clear_sky",
    "us_aqi",
    "us_aqi_pm2_5",
    "us_aqi_pm10",
    "us_aqi_nitrogen_dioxide",
    "us_aqi_ozone",
    "us_aqi_sulphur_dioxide",
    "us_aqi_carbon_monoxide",
    "european_aqi",
    "alder_pollen",
    "birch_pollen",
    "grass_pollen",
    "mugwort_pollen",
    "olive_pollen",
    "ragweed_pollen",
]


# ---------------------------------------------------------------------------
# Kafka Producer Setup
# ---------------------------------------------------------------------------


def create_producer(max_retries: int = 10, retry_delay: int = 10) -> KafkaProducer:
    """Create a Kafka producer with retry logic for startup ordering."""
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                max_in_flight_requests_per_connection=1,
                compression_type="gzip",
                linger_ms=100,
                batch_size=32768,
            )
            logger.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            logger.warning(
                "Kafka not available (attempt %d/%d), retrying in %ds...",
                attempt,
                max_retries,
                retry_delay,
            )
            time.sleep(retry_delay)

    raise RuntimeError(f"Failed to connect to Kafka after {max_retries} attempts")


# ---------------------------------------------------------------------------
# Open-Meteo API Calls
# ---------------------------------------------------------------------------


def fetch_weather(location: dict) -> dict[str, Any] | None:
    """Fetch current weather data from Open-Meteo for a location."""
    params = {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "minutely_15": ",".join(WEATHER_VARIABLES_15MIN),
        "hourly": ",".join(WEATHER_VARIABLES_HOURLY),
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch",
        "timezone": location["timezone"],
        "forecast_days": 1,
        "past_days": 0,
    }

    try:
        resp = requests.get(FORECAST_BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logger.error("Weather API error for %s: %s", location["name"], e)
        return None


def fetch_air_quality(location: dict) -> dict[str, Any] | None:
    """Fetch air quality data from Open-Meteo for a location."""
    params = {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "hourly": ",".join(AIR_QUALITY_VARIABLES),
        "timezone": location["timezone"],
        "forecast_days": 1,
        "past_days": 0,
    }

    try:
        resp = requests.get(AIR_QUALITY_BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logger.error("Air Quality API error for %s: %s", location["name"], e)
        return None


# ---------------------------------------------------------------------------
# Message Builders
# ---------------------------------------------------------------------------


def build_weather_messages(location: dict, data: dict) -> list[dict]:
    """
    Flatten 15-minute weather data into individual observation messages.
    Each message represents one 15-minute observation window.
    """
    messages = []
    ingested_at = datetime.now(timezone.utc).isoformat()

    # Use minutely_15 data (native HRRR resolution for US locations)
    minutely_data = data.get("minutely_15", {})
    timestamps = minutely_data.get("time", [])

    for i, ts in enumerate(timestamps):
        observation = {
            "location_id": location["location_id"],
            "location_name": location["name"],
            "latitude": data.get("latitude", location["latitude"]),
            "longitude": data.get("longitude", location["longitude"]),
            "elevation_m": data.get("elevation"),
            "context": location["context"],
            "observation_time": ts,
            "data_resolution": "15min",
            "ingested_at": ingested_at,
            "source": "open-meteo",
            "model": "best_match",
        }

        # Add all weather variables for this timestamp
        for var in WEATHER_VARIABLES_15MIN:
            values = minutely_data.get(var, [])
            observation[var] = values[i] if i < len(values) else None

        messages.append(observation)

    # Also flatten hourly data for variables not in 15-min
    hourly_data = data.get("hourly", {})
    hourly_timestamps = hourly_data.get("time", [])

    for i, ts in enumerate(hourly_timestamps):
        observation = {
            "location_id": location["location_id"],
            "location_name": location["name"],
            "latitude": data.get("latitude", location["latitude"]),
            "longitude": data.get("longitude", location["longitude"]),
            "elevation_m": data.get("elevation"),
            "context": location["context"],
            "observation_time": ts,
            "data_resolution": "hourly",
            "ingested_at": ingested_at,
            "source": "open-meteo",
            "model": "best_match",
        }

        for var in WEATHER_VARIABLES_HOURLY:
            values = hourly_data.get(var, [])
            observation[var] = values[i] if i < len(values) else None

        messages.append(observation)

    return messages


def build_air_quality_messages(location: dict, data: dict) -> list[dict]:
    """Flatten hourly air quality data into individual messages."""
    messages = []
    ingested_at = datetime.now(timezone.utc).isoformat()

    hourly_data = data.get("hourly", {})
    timestamps = hourly_data.get("time", [])

    for i, ts in enumerate(timestamps):
        observation = {
            "location_id": location["location_id"],
            "location_name": location["name"],
            "latitude": data.get("latitude", location["latitude"]),
            "longitude": data.get("longitude", location["longitude"]),
            "context": location["context"],
            "observation_time": ts,
            "data_resolution": "hourly",
            "ingested_at": ingested_at,
            "source": "open-meteo",
        }

        for var in AIR_QUALITY_VARIABLES:
            values = hourly_data.get(var, [])
            observation[var] = values[i] if i < len(values) else None

        messages.append(observation)

    return messages


# ---------------------------------------------------------------------------
# Publishing
# ---------------------------------------------------------------------------


def publish_messages(
    producer: KafkaProducer, topic: str, messages: list[dict], location_id: str
) -> int:
    """Publish messages to Kafka topic, keyed by location_id for partitioning."""
    count = 0
    for msg in messages:
        producer.send(topic, key=location_id, value=msg)
        count += 1
    producer.flush()
    return count


# ---------------------------------------------------------------------------
# Main Loop
# ---------------------------------------------------------------------------


def poll_cycle(producer: KafkaProducer) -> None:
    """Execute one polling cycle across all locations."""
    cycle_start = datetime.now(timezone.utc)
    logger.info("=" * 60)
    logger.info("Starting poll cycle at %s", cycle_start.isoformat())
    logger.info("=" * 60)

    total_weather = 0
    total_air_quality = 0

    for location in LOCATIONS:
        logger.info("Fetching data for %s...", location["name"])

        # Weather observations
        weather_data = fetch_weather(location)
        if weather_data:
            messages = build_weather_messages(location, weather_data)
            count = publish_messages(
                producer, WEATHER_TOPIC, messages, location["location_id"]
            )
            total_weather += count
            logger.info(
                "  → Published %d weather observations for %s",
                count,
                location["name"],
            )
        else:
            logger.warning("  → No weather data for %s", location["name"])

        # Air quality
        aq_data = fetch_air_quality(location)
        if aq_data:
            messages = build_air_quality_messages(location, aq_data)
            count = publish_messages(
                producer, AIR_QUALITY_TOPIC, messages, location["location_id"]
            )
            total_air_quality += count
            logger.info(
                "  → Published %d air quality observations for %s",
                count,
                location["name"],
            )
        else:
            logger.warning("  → No air quality data for %s", location["name"])

        # Be polite to Open-Meteo (free tier)
        time.sleep(1)

    elapsed = (datetime.now(timezone.utc) - cycle_start).total_seconds()
    logger.info("-" * 60)
    logger.info(
        "Cycle complete in %.1fs | Weather: %d msgs | Air Quality: %d msgs",
        elapsed,
        total_weather,
        total_air_quality,
    )
    logger.info("-" * 60)


def main():
    """Main entry point with graceful shutdown."""
    logger.info("Weather Producer starting up...")
    logger.info("Locations: %s", ", ".join(loc["name"] for loc in LOCATIONS))
    logger.info("Poll interval: %ds (%d min)", POLL_INTERVAL, POLL_INTERVAL // 60)
    logger.info("Kafka: %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("Topics: %s, %s", WEATHER_TOPIC, AIR_QUALITY_TOPIC)

    producer = create_producer()

    try:
        while True:
            poll_cycle(producer)
            logger.info("Sleeping %d seconds until next cycle...", POLL_INTERVAL)
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    finally:
        producer.close()
        logger.info("Producer closed.")


if __name__ == "__main__":
    main()
