import os
from pathlib import Path


DATA_LAKE_ROOT = Path(os.getenv("B3_DATA_LAKE_ROOT", "/mnt/d/b3_datalake"))
RAW_COTAHIST_DIR = os.getenv(
    "B3_RAW_COTAHIST_DIR",
    str(DATA_LAKE_ROOT / "raw" / "cotahist"),
)
BRONZE_COTAHIST_DIR = os.getenv(
    "B3_BRONZE_COTAHIST_DIR",
    str(DATA_LAKE_ROOT / "bronze" / "cotahist"),
)
SILVER_COTAHIST_DIR = os.getenv(
    "B3_SILVER_COTAHIST_DIR",
    str(DATA_LAKE_ROOT / "silver" / "cotacoes_historicas"),
)


def _positive_int_from_env(name: str, default: int) -> int:
    value = os.getenv(name, str(default))

    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} deve ser um número inteiro positivo.") from exc

    if parsed <= 0:
        raise ValueError(f"{name} deve ser um número inteiro positivo.")
    return parsed


DUCKDB_MEMORY_LIMIT = os.getenv("DUCKDB_MEMORY_LIMIT", "8GB")
DUCKDB_THREADS = _positive_int_from_env(
    "DUCKDB_THREADS",
    os.cpu_count() or 4,
)
BRONZE_MAX_WORKERS = _positive_int_from_env("BRONZE_MAX_WORKERS", 4)

DEFAULT_START_YEAR = _positive_int_from_env("B3_START_YEAR", 2024)
DEFAULT_END_YEAR = _positive_int_from_env("B3_END_YEAR", 2025)
if DEFAULT_START_YEAR > DEFAULT_END_YEAR:
    raise ValueError("B3_START_YEAR deve ser menor ou igual a B3_END_YEAR.")

MANUAL_RUN_ID_PREFIX = "manual"
RUN_ID_TIMESTAMP_FORMAT = "%Y%m%dT%H%M%SZ"

RAW_YEAR_DIRECTORY_TEMPLATE = "ano={year}"
RAW_FILENAME_TEMPLATE = "COTAHIST_A{year}.TXT"
RAW_FILENAME_YEAR_PATTERN = r"COTAHIST_A(\d{4})"
BRONZE_YEAR_PARTITION_TEMPLATE = "year={year}"
INGESTED_DATE_PARTITION_NAME = "_ingested_date"
INGESTED_DATE_FORMAT = "%Y-%m-%d_%H-%M-%S"

RAW_TEXT_DELIMITER = r"\n"
RAW_TEXT_QUOTE = ""
DUCKDB_MEMORY_LIMIT_PATTERN = r"\s*(\d+(?:\.\d+)?|\.\d+)\s*(B|KB|MB|GB|TB)\s*"

PARQUET_COMPRESSION = "ZSTD"
PARQUET_ROW_GROUP_SIZE = 122880
DUCKDB_PRESERVE_INSERTION_ORDER = False

B3_RECORD_TYPE_START = 1
B3_RECORD_TYPE_LENGTH = 2
B3_RECORD_TYPE_CODE = "01"
B3_TRADE_DATE_START = 3
B3_TRADE_DATE_LENGTH = 8
B3_TRADE_DATE_FORMAT = "%Y%m%d"
B3_TICKER_START = 13
B3_TICKER_LENGTH = 12
B3_MARKET_TYPE_START = 25
B3_MARKET_TYPE_LENGTH = 3
B3_OPEN_PRICE_START = 57
B3_HIGH_PRICE_START = 70
B3_LOW_PRICE_START = 83
B3_CLOSE_PRICE_START = 109
B3_PRICE_LENGTH = 13
B3_TOTAL_VOLUME_START = 171
B3_TOTAL_VOLUME_LENGTH = 18
B3_DECIMAL_SCALE = 100.0