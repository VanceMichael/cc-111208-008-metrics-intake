"""End-to-end demonstration of one scrape business loop.

Shows: a complete successful scrape, a parse failure that must not remove
series, a later complete empty scrape that does remove them, a series-budget
rejection that writes no half batch, two endpoints advancing independently,
non-finite values and exemplar/escaping retention.

All output is JSON lines so the run can be audited.  The whole command runs
against local synthetic sources and finishes well under 90 seconds.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
import sys
import tempfile

from .materialize import main as materialize
from .source_server import running
from storemetrics.scraper import Endpoint, ScrapeConfig, Scraper, StepClock
from storemetrics.store import Store


START = datetime(2026, 1, 1, tzinfo=timezone.utc)


def emit(label, value):
    print(json.dumps({'step': label, 'value': value}, ensure_ascii=True,
                     sort_keys=True, default=str), flush=True)


def main():
    materialize()
    with running(scenario='scrape.json') as server, \
            tempfile.TemporaryDirectory() as state:
        config = ScrapeConfig(
            (Endpoint('A', server.origin + '/device/A'),
             Endpoint('B', server.origin + '/device/B'),
             Endpoint('C', server.origin + '/device/C'),
             Endpoint('budget', server.origin + '/device/B-budget')),
            series_budget=20, interval_seconds=0, timeout_seconds=1.0)
        store = Store(state)
        clock = StepClock(START)
        scraper = Scraper(store, config, clock=clock)
        try:
            emit('scrape_a_success_b_success', scraper.run_once(['A', 'B'],
                                                               include_not_due=True))
            emit('series_after_first', {
                'A': _series(store, 'A'), 'B': _series(store, 'B')})

            emit('scrape_a_parse_failure_b_success', scraper.run_once(
                ['A', 'B'], include_not_due=True))
            emit('series_after_failure', {
                'A': _series(store, 'A'), 'B': _series(store, 'B')})
            emit('a_failed_batch_diagnostic', store.batches('A')[1])

            emit('scrape_a_empty_success', scraper.run_once(['A'],
                                                              include_not_due=True))
            emit('series_after_empty', {'A': _series(store, 'A')})
            emit('a_batches', store.batches('A'))

            emit('budget_rejection', scraper.run_once(['budget'],
                                                        include_not_due=True))
            emit('budget_diagnostic', store.batches('budget')[0])
            emit('budget_rows_visible', {
                'samples': len(store.samples('budget')),
                'series': len(store.series('budget')),
            })

            emit('scrape_c_nonfinite', scraper.scrape('C'))
            emit('c_values', _values(store, 'C'))

            emit('raw_escaping_example', {
                'payload_line': _payload_line('note='),
                'parsed_series': _labels(store, 'B', batch='B#1'),
            })
        finally:
            store.close()


def _series(store, endpoint):
    return [{'metric': s['metric_name'], 'labels': json.loads(s['labels']),
             'kind': s['kind'], 'present': s['present']}
            for s in store.series(endpoint)]


def _values(store, endpoint):
    return [{'metric': s['metric_name'], 'labels': json.loads(s['labels']),
             'value': s['value_text'], 'kind': s['value_kind'],
             'value_real': s['value_real'], 'timestamp': s['timestamp_text'],
             'local_collected_at': s['local_collected_at'], 'batch': s['batch_id']}
            for s in store.samples(endpoint)]


def _payload_line(marker):
    payload = Path('fixtures/exposition/device_b_escapes.prom').read_bytes()
    for line in payload.splitlines():
        if marker in line.decode():
            return line.decode()
    return None


def _labels(store, endpoint, batch=None):
    return [{'metric': s['metric_name'], 'labels': json.loads(s['labels']),
             'exemplar': json.loads(s['exemplar']) if s['exemplar'] else None}
            for s in store.samples(endpoint, batch_id=batch)]


if __name__ == '__main__':
    sys.exit(main())
