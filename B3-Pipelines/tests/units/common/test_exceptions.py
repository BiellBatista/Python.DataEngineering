"""Testes das exceções de domínio."""

import unittest

from common.exceptions import (
    BronzeError,
    DataQualityError,
    GoldError,
    PipelineError,
    RawError,
    SilverError,
)


class PipelineExceptionTests(unittest.TestCase):
    def test_domain_errors_preserve_expected_base_classes(self) -> None:
        self.assertIsInstance(RawError("raw"), FileNotFoundError)
        self.assertIsInstance(BronzeError("bronze"), FileNotFoundError)
        self.assertIsInstance(SilverError("silver"), NotADirectoryError)
        self.assertIsInstance(DataQualityError("quality"), ValueError)
        self.assertIsInstance(GoldError("gold"), PipelineError)


if __name__ == "__main__":
    unittest.main()
