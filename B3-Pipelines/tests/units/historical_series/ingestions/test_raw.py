"""Testes da localização dos arquivos RAW."""

import tempfile
import unittest
from pathlib import Path

from common.exceptions import RawError
from historical_series.ingestions.raw import get_raw_dir, list_raw_dirs


class RawIngestionTests(unittest.TestCase):
    def test_get_raw_dir_uses_expected_year_layout(self) -> None:
        self.assertEqual(
            get_raw_dir(2024, Path("/data/raw")),
            "/data/raw/ano=2024/COTAHIST_A2024.TXT",
        )

    def test_lists_existing_files_inclusive_and_in_year_order(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            expected = []
            for year in (2023, 2024):
                raw_file = Path(get_raw_dir(year, root))
                raw_file.parent.mkdir(parents=True, exist_ok=True)
                raw_file.touch()
                expected.append(str(raw_file))

            self.assertEqual(list_raw_dirs(2023, 2024, root), expected)

    def test_rejects_reversed_year_range(self) -> None:
        with self.assertRaisesRegex(ValueError, "menor ou igual"):
            list_raw_dirs(2025, 2024, "/unused")

    def test_reports_all_missing_files(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            with self.assertRaises(RawError) as error:
                list_raw_dirs(2023, 2024, temporary_directory)

        self.assertIn("COTAHIST_A2023.TXT", str(error.exception))
        self.assertIn("COTAHIST_A2024.TXT", str(error.exception))


if __name__ == "__main__":
    unittest.main()