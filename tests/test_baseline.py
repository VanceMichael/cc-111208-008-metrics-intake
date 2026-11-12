import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from storemetrics.parser import inspect
from storemetrics.journal import Journal
from storemetrics.transport import capture
from scripts.materialize import main as materialize
from scripts.source_server import running


class BaselineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        materialize()

    def test_capture_journal_is_content_addressed(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = Journal(directory)
            try:
                first = journal.save(b'synthetic', 'source-1')
                second = journal.save(b'synthetic', 'source-1')
                self.assertEqual(first, second)
                self.assertEqual(Path(first['path']).read_bytes(), b'synthetic')
                self.assertEqual(journal.database.execute('SELECT COUNT(*) FROM captures').fetchone()[0], 1)
            finally:
                journal.close()

    def test_remote_transport_is_rejected_before_io(self):
        with self.assertRaises(ValueError):
            capture('https://example.invalid/private')

    def test_cli_help(self):
        result = subprocess.run([sys.executable, '-m', 'storemetrics', '--help'], capture_output=True)
        self.assertEqual(result.returncode, 0)
        self.assertIn(b'inspect', result.stdout)

    def test_local_source_and_request_log(self):
        with running() as server:
            status, headers, payload = capture(server.origin + '/device/B', 'GET', headers={})
            self.assertEqual(status, 200)
            self.assertGreater(len(payload), 0)
            self.assertEqual(server.requests[0]['method'], 'GET')

    def test_unknown_source_route_is_not_success(self):
        with running() as server:
            self.assertEqual(capture(server.origin + '/not-present')[0], 404)

    def test_scalar_inspection(self):
        result = inspect(Path('fixtures/gauge.prom').read_bytes())
        self.assertTrue(result['eof'])
        self.assertEqual(result['samples'][0]['value'], '4.5')

    def test_labeled_series_not_silently_misparsed(self):
        with self.assertRaises(ValueError):
            inspect(Path('fixtures/labels.prom').read_bytes())

    def test_endpoint_success_failure_success_sequence(self):
        with running() as server:
            values = [capture(server.origin + '/device/A')[2] for index in range(3)]
            self.assertIn(b'# EOF', values[0])
            self.assertNotIn(b'# EOF', values[1])
            self.assertNotIn(b'4.5', values[2])


if __name__ == '__main__':
    unittest.main()
