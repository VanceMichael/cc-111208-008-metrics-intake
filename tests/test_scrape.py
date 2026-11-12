import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone

from scripts.source_server import running
from storemetrics.parser import (
    DuplicateLabelError, DuplicateSampleError, HistogramError, ParseError,
    SeriesBudgetExceeded, parse_exposition, series_identity,
)
from storemetrics.scraper import Endpoint, ScrapeConfig, Scraper, StepClock
from storemetrics.store import BatchConflict, Store


START = datetime(2026, 1, 1, tzinfo=timezone.utc)
FIXTURES = Path('fixtures/exposition')


def config_for(server, budget=20, names=('A', 'B')):
    routes = {'A': '/device/A', 'B': '/device/B', 'C': '/device/C',
              'slow': '/device/slow'}
    return ScrapeConfig(tuple(Endpoint(name, server.origin + routes[name])
                              for name in names), series_budget=budget,
                        timeout_seconds=1.0)


class ParserTests(unittest.TestCase):
    def test_label_order_does_not_change_series_identity(self):
        payload = (b'# TYPE t gauge\n'
                   b't{a="1",b="2"} 1\nt{b="2",a="1"} 1\n# EOF\n')
        exposition = parse_exposition(payload)
        self.assertEqual(exposition.series_count, 1)
        self.assertEqual(exposition.samples[0].occurrences, 2)
        self.assertEqual(series_identity('t', {'a': '1', 'b': '2'}),
                         series_identity('t', {'b': '2', 'a': '1'}))

    def test_escaped_quotes_and_newlines_in_labels(self):
        exposition = parse_exposition(
            b'# TYPE t gauge\nt{a="x\\"y",b="line1\\nline2",c="back\\\\slash"} 1\n# EOF\n')
        self.assertEqual(exposition.samples[0].labels,
                         {'a': 'x"y', 'b': 'line1\nline2', 'c': 'back\\slash'})

    def test_duplicate_label_is_rejected(self):
        with self.assertRaises(DuplicateLabelError):
            parse_exposition(b'# TYPE t gauge\nt{a="1",a="2"} 1\n# EOF\n')

    def test_conflicting_duplicate_sample_is_rejected_not_last_wins(self):
        with self.assertRaises(DuplicateSampleError):
            parse_exposition(b'# TYPE t gauge\nt{a="1"} 1\nt{a="1"} 2\n# EOF\n')
        with self.assertRaises(DuplicateSampleError):
            parse_exposition(b'# TYPE t gauge\nt{a="1"} 1 10\nt{a="1"} 1 11\n# EOF\n')
        with self.assertRaises(DuplicateSampleError):
            parse_exposition(b'# TYPE t gauge\nt{a="1"} 1\nt{a="1"} 1.0\n# EOF\n')

    def test_identical_duplicate_sample_is_accepted(self):
        exposition = parse_exposition(
            b'# TYPE t gauge\nt{a="1"} 1 10\nt{a="1"} 1 10\n# EOF\n')
        self.assertEqual(exposition.series_count, 1)
        self.assertEqual(exposition.samples[0].occurrences, 2)

    def test_non_finite_values_keep_their_semantics(self):
        exposition = parse_exposition(
            b'# TYPE t gauge\nt{a="NaN"} NaN\nt{b="inf"} +Inf\nt{c="ninf"} -Inf\n# EOF\n')
        kinds = {sample.labels['a'] if 'a' in sample.labels else
                 sample.labels['b'] if 'b' in sample.labels else sample.labels['c']:
                 sample.value.kind for sample in exposition.samples}
        self.assertEqual(sorted(kinds.values()), ['nan', 'neginf', 'posinf'])
        for sample in exposition.samples:
            self.assertEqual(sample.value.kind != 'finite', sample.value.number is None)

    def test_histogram_boundaries_are_sorted_numerically(self):
        exposition = parse_exposition(
            b'# TYPE h histogram\n'
            b'h_bucket{le="2.5"} 3\nh_bucket{le="1"} 2\n'
            b'h_bucket{le="+Inf"} 4\nh_sum 5.5\nh_count 4\n# EOF\n')
        buckets = [s for s in exposition.samples if s.kind == 'histogram_bucket']
        self.assertEqual([s.labels['le'] for s in buckets], ['2.5', '1', '+Inf'])
        # the parser validates ascending cumulative values even if the lines
        # appear in a different textual order
        self.assertEqual(exposition.series_count, 5)

    def test_decreasing_cumulative_bucket_is_rejected(self):
        with self.assertRaises(HistogramError):
            parse_exposition(
                b'# TYPE h histogram\nh_bucket{le="1"} 3\nh_bucket{le="2"} 2\n'
                b'h_bucket{le="+Inf"} 4\nh_sum 4\nh_count 4\n# EOF\n')

    def test_inf_bucket_must_equal_count_and_be_present(self):
        with self.assertRaises(HistogramError):
            parse_exposition(b'# TYPE h histogram\nh_bucket{le="+Inf"} 3\n'
                             b'h_count 4\n# EOF\n')
        with self.assertRaises(HistogramError):
            parse_exposition(b'# TYPE h histogram\nh_bucket{le="1"} 1\n'
                             b'h_count 1\n# EOF\n')

    def test_histogram_series_are_checked_per_label_set(self):
        payload = (b'# TYPE h histogram\n'
                   b'h_bucket{a="x",le="1"} 1\nh_bucket{a="x",le="+Inf"} 1\n'
                   b'h_bucket{a="y",le="1"} 2\nh_bucket{a="y",le="+Inf"} 3\n'
                   b'h_count{a="x"} 1\nh_count{a="y"} 3\n# EOF\n')
        self.assertEqual(parse_exposition(payload).series_count, 6)
        with self.assertRaises(HistogramError):
            # y's +Inf bucket no longer matches y's count
            parse_exposition(payload.replace(b'h_count{a="y"} 3', b'h_count{a="y"} 4'))
        with self.assertRaises(HistogramError):
            # x has buckets but no count of its own
            parse_exposition(payload.replace(b'h_count{a="x"} 1\n', b''))

    def test_missing_eof_is_rejected(self):
        with self.assertRaises(ParseError):
            parse_exposition(FIXTURES.joinpath('device_a_no_eof.prom').read_bytes())
        with self.assertRaises(ParseError):
            parse_exposition(b'# TYPE t gauge\nt 1\n# EOF\n# TYPE u gauge\n')
        with self.assertRaises(ParseError):
            parse_exposition(b'# TYPE t gauge\n# EOF\nt 1\n')

    def test_budget_counts_histogram_components_and_admits_nothing(self):
        payload = FIXTURES.joinpath('device_a_complete.prom').read_bytes()
        self.assertEqual(parse_exposition(payload, series_budget=8).series_count, 8)
        with self.assertRaises(SeriesBudgetExceeded) as caught:
            parse_exposition(payload, series_budget=7)
        self.assertEqual(caught.exception.count, 8)


class StoreAndScrapeTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.state = self.directory.name

    def tearDown(self):
        self.directory.cleanup()

    def _scraper(self, server, clock, budget=20, names=('A', 'B')):
        store = Store(self.state)
        self.addCleanup(store.close)
        return Scraper(store, config_for(server, budget, names), clock=clock), store

    def test_success_parse_failure_success_drops_series_only_on_last(self):
        with running(scenario='scrape.json') as server:
            scraper, store = self._scraper(server, StepClock(START))
            first = scraper.scrape('A')
            self.assertEqual(first['outcome'], 'success')
            self.assertEqual(first['series_written'], 8)
            self.assertEqual({s['metric_name'] for s in store.series('A', present=True)},
                             {'cabinet_temperature', 'door_events_total',
                              'service_duration_bucket', 'service_duration_sum',
                              'service_duration_count'})

            second = scraper.scrape('A')
            self.assertEqual(second['outcome'], 'failure')
            self.assertEqual(second['failure_kind'], 'parse')
            self.assertEqual(second['disappeared'], 0)
            # The failed batch leaves no rows of its own, but prior state remains.
            self.assertEqual(len(store.samples(batch_id='A#2')), 0)
            self.assertEqual(len(store.series('A', present=True)), 8)

            third = scraper.scrape('A')
            self.assertEqual(third['outcome'], 'success')
            self.assertEqual(third['series_written'], 0)
            self.assertEqual(third['disappeared'], 8)
            self.assertEqual(store.series('A', present=True), [])

            batches = store.batches('A')
            self.assertEqual([b['outcome'] for b in batches],
                             ['success', 'failure', 'success'])
            self.assertIn('missing required # EOF', batches[1]['diagnostic'])
            self.assertEqual(batches[2]['series_removed'], 8)

    def test_bad_histogram_batch_rejected_without_disappearance(self):
        with running(scenario='scrape.json') as server:
            config = ScrapeConfig((Endpoint('H', server.origin +
                                            '/device/B-bad-histogram'),))
            store = Store(self.state)
            try:
                scraper = Scraper(store, config, clock=StepClock(START))
                result = scraper.scrape('H')
            finally:
                store.close()
            self.assertEqual(result['failure_kind'], 'parse')
            self.assertEqual(result['series_written'], 0)

    def test_budget_rejection_writes_no_partial_batch_and_spares_other_endpoints(self):
        with running(scenario='scrape.json') as server:
            # Seed B (escapes fixture, 2 series) and a small A.
            scraper, store = self._scraper(server, StepClock(START), budget=10)
            scraper.scrape('B')
            before_b = store.series('B', present=True)
            self.assertEqual(len(before_b), 2)

            # A's next fixture is an empty successful scrape.  Point a separate
            # endpoint at the exploding fixture instead, under the same budget.
            explosion = ScrapeConfig(
                (Endpoint('A', server.origin + '/device/A'),
                 Endpoint('B', server.origin + '/device/B'),
                 Endpoint('X', server.origin + '/device/B-budget')),
                series_budget=10, timeout_seconds=1.0)
            scraper = Scraper(store, explosion, clock=StepClock(START))
            results = scraper.run_once(['X', 'B'], include_not_due=True)
            failed = results[0]
            self.assertEqual(failed['outcome'], 'failure')
            self.assertEqual(failed['failure_kind'], 'budget')
            self.assertEqual(failed['series_seen'], 30)
            self.assertEqual(failed['series_written'], 0)
            # Nothing from the rejected batch is visible anywhere.
            self.assertEqual(store.samples('X'), [])
            self.assertEqual(store.series('X'), [])
            batch = store.batches('X')[0]
            self.assertIn('budget is 10', batch['diagnostic'])
            self.assertIsNotNone(batch['payload_digest'])
            # B advanced independently, and A was not touched by X's failure.
            self.assertEqual(results[1]['outcome'], 'success')
            self.assertEqual(len(store.series('B', present=True)), 1)

    def test_two_endpoints_advance_independently_after_failure(self):
        with running(scenario='scrape.json') as server:
            scraper, store = self._scraper(server, StepClock(START))
            first = scraper.run_once(['A', 'B'], include_not_due=True)
            self.assertEqual([r['outcome'] for r in first], ['success', 'success'])

            # A fails (missing EOF); B must still succeed in the same run.
            second = scraper.run_once(['A', 'B'], include_not_due=True)
            self.assertEqual(second[0]['failure_kind'], 'parse')
            self.assertEqual(second[1]['outcome'], 'success')
            self.assertEqual(second[1]['disappeared'], 1)  # B's extra series dropped
            self.assertEqual(len(store.series('A', present=True)), 8)
            self.assertEqual(store.series('A')[0]['last_seen_batch'], 'A#1')
            self.assertEqual(store.batches('B')[1]['outcome'], 'success')

    def test_source_clock_rollback_and_local_time_are_both_kept(self):
        with running(scenario='scrape.json') as server:
            scraper, store = self._scraper(server, StepClock(START))
            first = scraper.scrape('B')
            # B's second fixture reports an older device clock (1760000000 ->
            # 1759990000).  The original timestamp is kept; the local
            # collection time still advances with the injected clock.
            second = scraper.scrape('B')
            self.assertEqual(second['outcome'], 'success')
            gauge = [s for s in store.samples('B', 'B#2')
                     if s['metric_name'] == 'cabinet_temperature'][0]
            self.assertEqual(gauge['timestamp_text'], '1759990000')
            self.assertEqual(gauge['timestamp_ns'], 1759990000000000000)
            self.assertGreater(gauge['local_collected_at'],
                               store.batches('B')[0]['finished_at'])
            # No fabricated linear time: the device time went backwards while
            # the local time went forwards.
            self.assertLess(gauge['timestamp_ns'], 1760000000000000000)

    def test_exemplar_is_separate_from_sample_labels(self):
        with running(scenario='scrape.json') as server:
            scraper, store = self._scraper(server, StepClock(START))
            scraper.scrape('B')
            counter = [s for s in store.samples('B', 'B#1')
                       if s['metric_name'] == 'door_events_total'][0]
            labels = json.loads(counter['labels'])
            self.assertNotIn('trace_id', labels)
            self.assertNotIn('user', labels)
            exemplar = json.loads(counter['exemplar'])
            self.assertEqual(exemplar['labels'], {'trace_id': 'abc123',
                                                  'user': 'op"7'})
            self.assertEqual(exemplar['value'], '6.5')
            self.assertEqual(exemplar['timestamp'], '1760000001')

    def test_nonfinite_values_reach_sqlite_without_null_conversion(self):
        with running(scenario='scrape.json') as server:
            config = ScrapeConfig((Endpoint('C', server.origin + '/device/C'),))
            store = Store(self.state)
            try:
                Scraper(store, config, clock=StepClock(START)).scrape('C')
                samples = store.samples('C')
            finally:
                store.close()
            kinds = {sample['labels']: (sample['value_text'], sample['value_kind'],
                                       sample['value_real']) for sample in samples}
            by_zone = {}
            for labels, values in kinds.items():
                parsed_labels = json.loads(labels)
                if 'zone' in parsed_labels:
                    by_zone[parsed_labels['zone']] = values
            self.assertEqual(by_zone['cold'], ('NaN', 'nan', None))
            self.assertEqual(by_zone['frozen'], ('-Inf', 'neginf', None))
            self.assertEqual(by_zone['ambient'], ('+Inf', 'posinf', None))
            for text, kind, real in kinds.values():
                self.assertIsNotNone(text)
                self.assertNotEqual(kind, '')
                if kind != 'finite':
                    self.assertIsNone(real)

    def test_endpoint_timeout_is_a_failure_not_a_disappearance(self):
        with running(scenario='scrape.json') as server:
            config = ScrapeConfig(
                (Endpoint('A', server.origin + '/device/A'),
                 Endpoint('slow', server.origin + '/device/slow')),
                series_budget=20, timeout_seconds=0.2)
            store = Store(self.state)
            try:
                scraper = Scraper(store, config, clock=StepClock(START))
                scraper.scrape('A')
                before = store.series('A', present=True)
                slow = scraper.scrape('slow')
            finally:
                store.close()
            self.assertEqual(slow['outcome'], 'failure')
            self.assertEqual(slow['failure_kind'], 'timeout')
            self.assertEqual(len(before), 8)

    def test_only_success_advances_and_failure_keeps_prior_state_after_restart(self):
        with running(scenario='scrape.json') as server:
            scraper, store = self._scraper(server, StepClock(START))
            scraper.scrape('A')
            scraper.scrape('A')
            store.close()

            reopened = Store(self.state)
            try:
                reloaded = Scraper(reopened, config_for(server),
                                   clock=StepClock(START))
                # A's last attempt just finished, so only the never-scraped B
                # is due; restarting did not invent a scrape for A.
                self.assertEqual([e.name for e in reloaded.due()], ['B'])
                self.assertEqual(reopened.batches('A')[-1]['outcome'], 'failure')
                # Restart does not infer a missing scrape from a process timer;
                # an advanced injected clock makes the endpoint due again.
                later = Scraper(reopened, config_for(server),
                                clock=StepClock(START + timedelta(seconds=60)))
                self.assertEqual([e.name for e in later.due()], ['A', 'B'])
                self.assertEqual(len(reopened.series('A', present=True)), 8)
                self.assertEqual(reopened.batches('A')[-1]['outcome'], 'failure')
            finally:
                reopened.close()

    def test_one_unfinished_scrape_per_endpoint_and_stale_release(self):
        with running(scenario='scrape.json') as server:
            scraper, store = self._scraper(server, StepClock(START))
            # Simulate a lost worker: a claim exists but the batch has not finished.
            store.claim('A', '2026-01-01T00:00:00+00:00', 'dead-worker')
            with self.assertRaises(BatchConflict):
                scraper.scrape('A')
            self.assertEqual(store.batches('A'), [])

            # The injected clock releases the stale claim; series state is
            # untouched because no successful batch ever happened.
            stale_scraper = Scraper(store, config_for(server),
                                    clock=StepClock(START + timedelta(seconds=120)))
            released = stale_scraper.release_stale_claims(60)
            self.assertEqual(released, ['A'])
            result = stale_scraper.scrape('A')
            self.assertEqual(result['outcome'], 'success')
            self.assertIsNone(store.database.execute(
                'SELECT 1 FROM scrape_claims WHERE endpoint = ?', ('A',)).fetchone())

    def test_killed_process_leaves_no_success_and_no_disappearance(self):
        with running(scenario='scrape.json') as server, \
                tempfile.TemporaryDirectory() as config_dir:
            config_path = Path(config_dir) / 'config.json'
            config_path.write_text(json.dumps({
                'endpoints': [{'name': 'A', 'url': server.origin + '/device/A'},
                              {'name': 'slow', 'url': server.origin + '/device/slow'}],
                'series_budget': 20, 'timeout_seconds': 5.0}))
            scraper, store = self._scraper(server, StepClock(START))
            scraper.scrape('A')
            before = len(store.series('A', present=True))
            store.close()

            environment = dict(os.environ, PYTHONPATH=str(Path.cwd()))
            child = subprocess.Popen(
                [sys.executable, '-m', 'storemetrics', 'scrape',
                 '--config', str(config_path),
                 '--state', self.state, '--endpoint', 'slow',
                 '--system-clock'],
                cwd=str(Path.cwd()), stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL, env=environment)
            # Wait until the child has taken its claim, then kill it mid-flight.
            child_store = Store(self.state)
            deadline = time.time() + 5
            while time.time() < deadline:
                claim = child_store.database.execute(
                    'SELECT 1 FROM scrape_claims WHERE endpoint = ?',
                    ('slow',)).fetchone()
                if claim:
                    break
                time.sleep(0.05)
            child_store.close()
            self.assertIsNotNone(claim, 'child never claimed its endpoint')
            child.kill()
            child.wait()

            reopened = Store(self.state)
            try:
                self.assertIsNotNone(reopened.database.execute(
                    'SELECT 1 FROM scrape_claims WHERE endpoint = ?',
                    ('slow',)).fetchone())
                self.assertEqual(reopened.batches('slow'), [])
                self.assertEqual(len(reopened.series('A', present=True)), before)
                # Releasing the stale claim with the injected clock never
                # advances series existence.
                Scraper(reopened, config_for(server),
                        clock=StepClock(START + timedelta(seconds=600))
                        ).release_stale_claims(60)
                self.assertEqual(len(reopened.series('A', present=True)), before)
            finally:
                reopened.close()

    def test_cli_scrape_and_query_round_trip(self):
        with running(scenario='scrape.json') as server, tempfile.TemporaryDirectory() as state:
            config = Path(state) / 'config.json'
            config.write_text(json.dumps({
                'endpoints': [{'name': 'A', 'url': server.origin + '/device/A'}],
                'series_budget': 20, 'interval_seconds': 60,
            }))
            environment = dict(os.environ, PYTHONPATH=str(Path.cwd()))
            run = subprocess.run(
                [sys.executable, '-m', 'storemetrics', 'scrape',
                 '--config', str(config), '--state', state,
                 '--clock-start', '2026-01-01T00:00:00+00:00', '--all'],
                capture_output=True, text=True, env=environment)
            self.assertEqual(run.returncode, 0, run.stderr)
            result = json.loads(run.stdout)
            self.assertEqual(result['results'][0]['outcome'], 'success')

            query = subprocess.run(
                [sys.executable, '-m', 'storemetrics', 'query', '--state', state,
                 '--batches', '--endpoint', 'A'],
                capture_output=True, text=True, env=environment)
            self.assertEqual(query.returncode, 0, query.stderr)
            batches = json.loads(query.stdout)['batches']
            self.assertEqual(batches[0]['batch_id'], 'A#1')
            self.assertIsNotNone(batches[0]['payload_digest'])


if __name__ == '__main__':
    unittest.main()
