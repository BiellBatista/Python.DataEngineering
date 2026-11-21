import json
import logging
import unittest

from backend.observability import JsonLogFormatter


class JsonLogFormatterTests(unittest.TestCase):
    def test_formats_portuguese_message_and_allowed_metadata(self) -> None:
        record = logging.LogRecord(
            "b3_app_web",
            logging.INFO,
            __file__,
            1,
            "Evento de observabilidade recebido do frontend.",
            (),
            None,
        )
        record.frontend_event = "consulta_concluida"
        record.result_rows = 12

        entry = json.loads(JsonLogFormatter().format(record))

        self.assertEqual(
            entry["message"],
            "Evento de observabilidade recebido do frontend.",
        )
        self.assertEqual(entry["frontend_event"], "consulta_concluida")
        self.assertEqual(entry["result_rows"], 12)


if __name__ == "__main__":
    unittest.main()
