"""Testes dos utilitários de diretório."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from utils.helpers import check_directories_exist, ensure_directory_available


class DirectoryHelperTests(unittest.TestCase):
    def test_check_directories_accepts_single_and_multiple_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            existing = Path(temporary_directory) / "existing"
            existing.mkdir()
            missing = Path(temporary_directory) / "missing"

            self.assertEqual(check_directories_exist(str(existing)), [])
            self.assertEqual(
                check_directories_exist([str(existing), str(missing)]),
                [str(missing)],
            )

    def test_check_directories_reports_missing_windows_drive(self) -> None:
        directory = Mock(drive="D:")
        drive = Mock()
        drive.exists.return_value = False
        with patch("utils.helpers.Path", side_effect=[directory, drive]):
            self.assertEqual(check_directories_exist(r"D:\data"), [r"D:\data"])

    def test_check_directories_reports_missing_mount_point(self) -> None:
        directory = Mock(drive="", parts=("/", "mnt", "missing", "data"))
        mount = Mock()
        mount.exists.return_value = False
        with patch("utils.helpers.Path", side_effect=[directory, mount]):
            self.assertEqual(
                check_directories_exist("/mnt/missing/data"),
                ["/mnt/missing/data"],
            )

    def test_ensure_creates_directory_and_rejects_file_path(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            new_directory = Path(temporary_directory) / "new"
            ensure_directory_available(str(new_directory))
            self.assertTrue(new_directory.is_dir())

        directory = Mock(drive="", parts=("/tmp", "not-a-directory"))
        directory.is_dir.return_value = False
        with patch("utils.helpers.Path", return_value=directory):
            with self.assertRaisesRegex(NotADirectoryError, "não é um diretório"):
                ensure_directory_available("/tmp/not-a-directory")

    def test_ensure_rejects_unavailable_windows_drive(self) -> None:
        directory = Mock(drive="D:")
        drive = Mock()
        drive.exists.return_value = False
        with patch("utils.helpers.Path", side_effect=[directory, drive]):
            with self.assertRaisesRegex(FileNotFoundError, "Unidade indisponível"):
                ensure_directory_available(r"D:\data")

    def test_ensure_rejects_unavailable_mount_point(self) -> None:
        directory = Mock(drive="", parts=("/", "mnt", "missing", "data"))
        mount = Mock()
        mount.exists.return_value = False
        with patch("utils.helpers.Path", side_effect=[directory, mount]):
            with self.assertRaisesRegex(FileNotFoundError, "Ponto de montagem"):
                ensure_directory_available("/mnt/missing/data")

    def test_ensure_creates_directory_on_available_windows_drive(self) -> None:
        directory = Mock(drive="D:")
        directory.is_dir.return_value = True
        drive = Mock()
        drive.exists.return_value = True
        with patch("utils.helpers.Path", side_effect=[directory, drive]):
            ensure_directory_available(r"D:\data")

        directory.mkdir.assert_called_once_with(parents=True, exist_ok=True)

    def test_ensure_creates_directory_on_available_mount_point(self) -> None:
        directory = Mock(drive="", parts=("/", "mnt", "disk", "data"))
        directory.is_dir.return_value = True
        mount = Mock()
        mount.exists.return_value = True
        with patch("utils.helpers.Path", side_effect=[directory, mount]):
            ensure_directory_available("/mnt/disk/data")

        directory.mkdir.assert_called_once_with(parents=True, exist_ok=True)


if __name__ == "__main__":
    unittest.main()
