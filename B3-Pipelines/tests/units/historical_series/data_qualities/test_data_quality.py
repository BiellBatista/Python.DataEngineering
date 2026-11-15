"""Testes de qualidade dos arquivos RAW e Bronze."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import duckdb

from common.exceptions import BronzeError, DataQualityError
from config import (
    B3_CLOSE_PRICE_START,
    B3_HIGH_PRICE_START,
    B3_LOW_PRICE_START,
    B3_MARKET_TYPE_START,
    B3_OPEN_PRICE_START,
    B3_TICKER_START,
    B3_TRADE_DATE_START,
)
from historical_series.data_qualities import data_quality
from historical_series.ingestions.raw import get_raw_dir


class RawQualityTests(unittest.TestCase):
    def test_accepts_valid_records_and_crlf_terminators(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            raw_file = _make_raw_file(temporary_directory, 2024)
            raw_file.write_bytes(
                _record("00") + b"\r\n" + _record("01") + b"\n" + _record("99")
            )

            data_quality.validate_raw_quality(2024, 2024, temporary_directory)

    def test_rejects_empty_file_blank_line_bad_width_and_invalid_type(self) -> None:
        cases = (
            (b"", "arquivo vazio"),
            (_record("01") + b"\n\n", "registro vazio"),
            (b"01\n", "largura inválida"),
            (_record("XX"), "tipo de registro inválido"),
            (b"\xff\xff" + b" " * 243, "tipo de registro inválido"),
        )
        for contents, message in cases:
            with self.subTest(message=message), tempfile.TemporaryDirectory() as directory:
                _make_raw_file(directory, 2024).write_bytes(contents)
                with self.assertRaisesRegex(DataQualityError, message):
                    data_quality.validate_raw_quality(2024, 2024, directory)

    def test_reports_file_with_no_iterated_records(self) -> None:
        path = MagicMock()
        path.stat.return_value.st_size = 1
        path.open.return_value.__enter__.return_value = iter(())
        with (
            patch.object(data_quality, "list_raw_dirs", return_value=["raw.txt"]),
            patch.object(data_quality, "Path", return_value=path),
        ):
            with self.assertRaisesRegex(DataQualityError, "nenhum registro"):
                data_quality.validate_raw_quality(2024, 2024, "/unused")

    def test_propagates_missing_raw_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            with self.assertRaises(FileNotFoundError):
                data_quality.validate_raw_quality(
                    2024, 2024, temporary_directory
                )


class BronzeQualityTests(unittest.TestCase):
    def test_accepts_valid_and_unpriced_fractional_records(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            _write_bronze(
                root,
                [
                    _record("01"),
                    _record("01", market="020", prices=(0, 0, 0, 0)),
                    _record("99"),
                ],
            )

            data_quality.validate_bronze_quality(
                2024,
                2024,
                input_dir=str(root),
                memory_limit=None,
                threads=1,
            )

    def test_reports_invalid_keys_prices_and_price_ranges(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            _write_bronze(
                root,
                [
                    _record("01", trade_date="00000000", ticker=""),
                    _record("01", prices=(0, 0, 0, 0)),
                    _record("01", prices=(1000, 1000, 2000, 1500)),
                ],
            )

            with self.assertRaises(DataQualityError) as error:
                data_quality.validate_bronze_quality(
                    2024,
                    2024,
                    input_dir=str(root),
                    memory_limit=None,
                    threads=1,
                )

        message = str(error.exception)
        self.assertIn("1 registro(s) com chaves", message)
        self.assertIn("1 registro(s) com preços", message)
        self.assertIn("1 registro(s) com preço máximo", message)

    def test_reports_no_trading_records(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            _write_bronze(root, [_record("99")])
            with self.assertRaisesRegex(DataQualityError, "nenhum registro"):
                data_quality.validate_bronze_quality(
                    2024,
                    2024,
                    input_dir=str(root),
                    memory_limit=None,
                    threads=1,
                )

    def test_rejects_invalid_range_threads_and_missing_partitions(self) -> None:
        with self.assertRaisesRegex(ValueError, "start_year"):
            data_quality.validate_bronze_quality(2025, 2024, "/unused")
        with self.assertRaisesRegex(ValueError, "threads"):
            data_quality.validate_bronze_quality(2024, 2024, "/unused", threads=0)

        with tempfile.TemporaryDirectory() as temporary_directory:
            with self.assertRaisesRegex(BronzeError, "partição anual inexistente"):
                data_quality.validate_bronze_quality(
                    2024, 2024, temporary_directory, threads=1
                )

            (Path(temporary_directory) / "year=2024").mkdir()
            with self.assertRaisesRegex(BronzeError, "nenhum Parquet"):
                data_quality.validate_bronze_quality(
                    2024, 2024, temporary_directory, threads=1
                )

    def test_closes_connection_when_query_fails_or_returns_no_result(self) -> None:
        for query_result, expected_error in (
            (RuntimeError("query failed"), RuntimeError),
            (None, RuntimeError),
        ):
            connection = Mock()
            result = Mock()
            result.fetchone.return_value = query_result
            with (
                patch.object(data_quality, "connect_duckdb", return_value=connection),
                patch.object(
                    data_quality,
                    "execute_query",
                    side_effect=query_result if isinstance(query_result, Exception) else None,
                    return_value=result,
                ),
                patch.object(data_quality, "find_most_recent_bronze_parquets", return_value=["file.parquet"]),
                patch.object(Path, "is_dir", return_value=True),
            ):
                if isinstance(query_result, Exception):
                    with self.assertRaises(expected_error):
                        data_quality.validate_bronze_quality(
                            2024, 2024, "/bronze", threads=1
                        )
                else:
                    with self.assertRaisesRegex(RuntimeError, "não retornou resultados"):
                        data_quality.validate_bronze_quality(
                            2024, 2024, "/bronze", threads=1
                        )
            connection.close.assert_called_once()


def _make_raw_file(root: str, year: int) -> Path:
    raw_file = Path(get_raw_dir(year, root))
    raw_file.parent.mkdir(parents=True, exist_ok=True)
    return raw_file


def _record(
    record_type: str,
    *,
    trade_date: str = "20240102",
    ticker: str = "TEST3",
    market: str = "010",
    prices: tuple[int, int, int, int] = (1000, 2000, 500, 1500),
) -> bytes:
    record = bytearray(b" " * 245)
    fields = (
        (0, record_type),
        (B3_TRADE_DATE_START - 1, trade_date),
        (B3_TICKER_START - 1, ticker.ljust(12)),
        (B3_MARKET_TYPE_START - 1, market),
        (B3_OPEN_PRICE_START - 1, f"{prices[0]:013d}"),
        (B3_HIGH_PRICE_START - 1, f"{prices[1]:013d}"),
        (B3_LOW_PRICE_START - 1, f"{prices[2]:013d}"),
        (B3_CLOSE_PRICE_START - 1, f"{prices[3]:013d}"),
    )
    for start, value in fields:
        encoded = value.encode("ascii")
        record[start : start + len(encoded)] = encoded
    return bytes(record)


def _write_bronze(root: Path, records: list[bytes]) -> None:
    parquet_dir = root / "year=2024" / "_ingested_date=2024-01-03_00-00-00"
    parquet_dir.mkdir(parents=True)
    parquet_file = parquet_dir / "part.parquet"
    connection = duckdb.connect(":memory:")
    try:
        connection.execute("CREATE TABLE bronze (content VARCHAR)")
        connection.executemany(
            "INSERT INTO bronze VALUES (?)",
            [(record.decode("ascii"),) for record in records],
        )
        escaped_path = str(parquet_file).replace("'", "''")
        connection.execute(f"COPY bronze TO '{escaped_path}' (FORMAT PARQUET)")
    finally:
        connection.close()


if __name__ == "__main__":
    unittest.main()