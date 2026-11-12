"""SQLite receive batches for the scrape pipeline.

A scrape is one independently auditable batch.  Every batch -- including a
rejected one -- is recorded with its raw payload digest, outcome and
diagnostics.  Only a fully valid scraped batch writes samples and advances the
existence state of that endpoint's series.  Time is never inferred from the
process clock here; the caller injects timestamps and a clock.
"""

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile

from .parser import canonical_labels


SCHEMA = """
CREATE TABLE IF NOT EXISTS scrape_batches (
    batch_id TEXT PRIMARY KEY,
    endpoint TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    outcome TEXT NOT NULL,              -- success / failure
    failure_kind TEXT,                  -- timeout / parse / budget / transport / internal
    http_status INTEGER,
    series_seen INTEGER NOT NULL,
    series_written INTEGER NOT NULL,
    series_removed INTEGER NOT NULL DEFAULT 0,
    payload_digest TEXT,
    payload_size INTEGER,
    diagnostic TEXT,
    UNIQUE (endpoint, attempt)
);
CREATE INDEX IF NOT EXISTS scrape_batches_endpoint ON scrape_batches (endpoint, attempt);

CREATE TABLE IF NOT EXISTS series (
    endpoint TEXT NOT NULL,
    series_key TEXT NOT NULL,           -- endpoint + metric name + canonical labels
    metric_name TEXT NOT NULL,
    labels TEXT NOT NULL,               -- canonical JSON, label order is not identity
    kind TEXT NOT NULL,
    family_name TEXT NOT NULL,
    unit TEXT,
    help TEXT,
    first_seen_batch TEXT NOT NULL,
    last_seen_batch TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    present INTEGER NOT NULL,           -- 1 while the endpoint still exposes it
    PRIMARY KEY (endpoint, series_key)
);

CREATE TABLE IF NOT EXISTS samples (
    batch_id TEXT NOT NULL REFERENCES scrape_batches (batch_id),
    endpoint TEXT NOT NULL,
    series_key TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    labels TEXT NOT NULL,
    kind TEXT NOT NULL,
    value_text TEXT NOT NULL,
    value_kind TEXT NOT NULL,           -- finite / nan / posinf / neginf
    value_real REAL,                    -- finite only; never NULL for a non-finite marker
    timestamp_text TEXT,
    timestamp_ns INTEGER,
    local_collected_at TEXT NOT NULL,
    exemplar TEXT,
    occurrences INTEGER NOT NULL,
    line_number INTEGER NOT NULL,
    PRIMARY KEY (batch_id, series_key)
);
CREATE INDEX IF NOT EXISTS samples_series ON samples (endpoint, series_key);
CREATE INDEX IF NOT EXISTS samples_batch ON samples (batch_id);

CREATE TABLE IF NOT EXISTS scrape_claims (
    endpoint TEXT PRIMARY KEY,          -- at most one unfinished scrape per endpoint
    owner TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    started_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS series_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    endpoint TEXT NOT NULL,
    series_key TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    event TEXT NOT NULL,                -- appeared / updated / disappeared
    at TEXT NOT NULL
);
"""


@dataclass
class BatchResult:
    batch_id: str
    endpoint: str
    attempt: int
    outcome: str
    failure_kind: str = None
    series_seen: int = 0
    series_written: int = 0
    disappeared: int = 0
    diagnostic: str = None

    def as_dict(self):
        result = {
            'batch_id': self.batch_id,
            'endpoint': self.endpoint,
            'attempt': self.attempt,
            'outcome': self.outcome,
            'series_seen': self.series_seen,
            'series_written': self.series_written,
            'disappeared': self.disappeared,
        }
        if self.failure_kind:
            result['failure_kind'] = self.failure_kind
        if self.diagnostic:
            result['diagnostic'] = self.diagnostic
        return result


def _finite_real(value):
    """Return a REAL for a finite value, or None when it cannot be represented.

    Non-finite values are still stored with their exact text and kind; callers
    never turn a non-finite marker into NULL.
    """
    if value.kind != 'finite':
        return None
    try:
        real = float(value.number)
    except (OverflowError, ValueError):
        return None
    if real != real or real in (float('inf'), float('-inf')):
        # Decimal literal beyond binary float range; the exact text remains.
        return None
    return real


class Store:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.blobs = self.root / 'blobs'
        self.blobs.mkdir(exist_ok=True)
        self.database = sqlite3.connect(str(self.root / 'store.sqlite3'))
        self.database.execute('PRAGMA foreign_keys = ON')
        with self.database:
            self.database.executescript(SCHEMA)

    def close(self):
        self.database.close()

    # --------------------------------------------------------------- claims
    def claim(self, endpoint, started_at, owner):
        """Take the endpoint's single unfinished-scrape slot.

        Runs in its own IMMEDIATE transaction so two processes cannot both
        claim one endpoint.  The attempt number comes from durable history.
        """
        try:
            self.database.execute('BEGIN IMMEDIATE')
            row = self.database.execute(
                'SELECT owner, started_at FROM scrape_claims WHERE endpoint = ?',
                (endpoint,)).fetchone()
            if row is not None:
                raise BatchConflict('claimed by %s since %s' % row)
            attempt = self.database.execute(
                'SELECT COALESCE(MAX(attempt), 0) + 1 FROM scrape_batches '
                'WHERE endpoint = ?', (endpoint,)).fetchone()[0]
            self.database.execute(
                'INSERT INTO scrape_claims (endpoint, owner, attempt, started_at) '
                'VALUES (?, ?, ?, ?)', (endpoint, owner, attempt, started_at))
            self.database.commit()
            return attempt
        except BaseException:
            if self.database.in_transaction:
                self.database.rollback()
            raise

    def release_claim(self, endpoint, owner):
        with self.database:
            self.database.execute(
                'DELETE FROM scrape_claims WHERE endpoint = ? AND owner = ?',
                (endpoint, owner))

    # ---------------------------------------------------------------- batches
    def save_payload(self, payload):
        digest = hashlib.sha256(payload).hexdigest()
        destination = self.blobs / digest
        if not destination.exists():
            descriptor, temporary = tempfile.mkstemp(dir=self.blobs)
            try:
                with os.fdopen(descriptor, 'wb') as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, destination)
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)
        return digest, len(payload)

    def record_failure(self, endpoint, attempt, failure_kind, started_at, finished_at,
                       diagnostic=None, http_status=None, payload=None,
                       series_seen=0):
        batch_id = '%s#%d' % (endpoint, attempt)
        payload_digest = payload_size = None
        if payload is not None:
            payload_digest, payload_size = self.save_payload(payload)
        with self.database:
            self.database.execute(
                'INSERT INTO scrape_batches (batch_id, endpoint, attempt, started_at, '
                'finished_at, outcome, failure_kind, http_status, series_seen, '
                'series_written, series_removed, payload_digest, payload_size, diagnostic) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (batch_id, endpoint, attempt, started_at, finished_at, 'failure',
                 failure_kind, http_status, series_seen, 0, 0, payload_digest,
                 payload_size, diagnostic))
        return BatchResult(batch_id, endpoint, attempt, 'failure',
                           failure_kind=failure_kind, series_seen=series_seen,
                           series_written=0, diagnostic=diagnostic)

    def commit_success(self, endpoint, attempt, exposition, started_at, finished_at,
                       collected_at, payload_digest, payload_size, http_status=None):
        """Atomically write one complete successful scrape.

        A budget violation is checked by the caller before this method is
        called; if anything raises here the transaction rolls back and no
        half batch is visible.
        """
        batch_id = '%s#%d' % (endpoint, attempt)
        seen_keys = set()
        disappeared = 0
        with self.database:
            self.database.execute(
                'INSERT INTO scrape_batches (batch_id, endpoint, attempt, started_at, '
                'finished_at, outcome, failure_kind, http_status, series_seen, '
                'series_written, series_removed, payload_digest, payload_size, diagnostic) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (batch_id, endpoint, attempt, started_at, finished_at, 'success',
                 None, http_status, exposition.series_count, exposition.series_count,
                 0, payload_digest, payload_size, None))
            for sample in exposition.samples:
                key = endpoint + '\x1f' + sample.series_key
                seen_keys.add(key)
                labels = canonical_labels(sample.labels)
                self.database.execute(
                    'INSERT INTO samples (batch_id, endpoint, series_key, metric_name, '
                    'labels, kind, value_text, value_kind, value_real, timestamp_text, '
                    'timestamp_ns, local_collected_at, exemplar, occurrences, '
                    'line_number) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    (batch_id, endpoint, key, sample.metric_name, labels, sample.kind,
                     sample.value.text, sample.value.kind, _finite_real(sample.value),
                     sample.timestamp_text, sample.timestamp_ns, collected_at,
                     json.dumps(sample.exemplar.as_json(), sort_keys=True,
                                ensure_ascii=False) if sample.exemplar else None,
                     sample.occurrences, sample.line_number))

                existing = self.database.execute(
                    'SELECT 1 FROM series WHERE endpoint = ? AND series_key = ?',
                    (endpoint, key)).fetchone()
                if existing is None:
                    self.database.execute(
                        'INSERT INTO series (endpoint, series_key, metric_name, labels, '
                        'kind, family_name, unit, help, first_seen_batch, '
                        'last_seen_batch, first_seen_at, last_seen_at, present) '
                        'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)',
                        (endpoint, key, sample.metric_name, labels, sample.kind,
                         sample.family_name, sample.unit, sample.help, batch_id,
                         batch_id, collected_at, collected_at))
                    event = 'appeared'
                else:
                    event = 'updated'
                    self.database.execute(
                        'UPDATE series SET last_seen_batch = ?, last_seen_at = ?, '
                        'present = 1, kind = ?, family_name = ?, unit = ?, help = ? '
                        'WHERE endpoint = ? AND series_key = ?',
                        (batch_id, collected_at, sample.kind, sample.family_name,
                         sample.unit, sample.help, endpoint, key))
                self.database.execute(
                    'INSERT INTO series_events (endpoint, series_key, batch_id, event, at) '
                    'VALUES (?, ?, ?, ?, ?)', (endpoint, key, batch_id, event, collected_at))

            # Only series of this batch's own exposition are eligible for
            # disappearance; a histogram component and its family are separate
            # rows, so the budget is checked by the parser over all rows first.
            stale = self.database.execute(
                'SELECT series_key FROM series WHERE endpoint = ? AND present = 1',
                (endpoint,)).fetchall()
            for (key,) in stale:
                if key in seen_keys:
                    continue
                self.database.execute(
                    'UPDATE series SET present = 0 WHERE endpoint = ? AND series_key = ?',
                    (endpoint, key))
                self.database.execute(
                    'INSERT INTO series_events (endpoint, series_key, batch_id, event, at) '
                    'VALUES (?, ?, ?, ?, ?)', (endpoint, key, batch_id, 'disappeared',
                                               collected_at))
                disappeared += 1
            if disappeared:
                self.database.execute(
                    'UPDATE scrape_batches SET series_removed = ? WHERE batch_id = ?',
                    (disappeared, batch_id))
        return BatchResult(batch_id, endpoint, attempt, 'success', None,
                           exposition.series_count, exposition.series_count,
                           disappeared)

    # ------------------------------------------------------------------ query
    def batches(self, endpoint=None):
        query = ('SELECT batch_id, endpoint, attempt, started_at, finished_at, outcome, '
                 'failure_kind, http_status, series_seen, series_written, series_removed, '
                 'payload_digest, payload_size, diagnostic FROM scrape_batches')
        parameters = ()
        if endpoint is not None:
            query += ' WHERE endpoint = ?'
            parameters = (endpoint,)
        query += ' ORDER BY rowid'
        columns = ('batch_id', 'endpoint', 'attempt', 'started_at', 'finished_at',
                   'outcome', 'failure_kind', 'http_status', 'series_seen',
                   'series_written', 'series_removed', 'payload_digest', 'payload_size',
                   'diagnostic')
        return [dict(zip(columns, row)) for row in self.database.execute(query, parameters)]

    def samples(self, endpoint=None, batch_id=None):
        query = ('SELECT batch_id, endpoint, series_key, metric_name, labels, kind, '
                 'value_text, value_kind, value_real, timestamp_text, timestamp_ns, '
                 'local_collected_at, exemplar, occurrences FROM samples')
        clauses = []
        parameters = []
        if endpoint is not None:
            clauses.append('endpoint = ?')
            parameters.append(endpoint)
        if batch_id is not None:
            clauses.append('batch_id = ?')
            parameters.append(batch_id)
        if clauses:
            query += ' WHERE ' + ' AND '.join(clauses)
        query += ' ORDER BY endpoint, series_key, batch_id'
        columns = ('batch_id', 'endpoint', 'series_key', 'metric_name', 'labels',
                   'kind', 'value_text', 'value_kind', 'value_real', 'timestamp_text',
                   'timestamp_ns', 'local_collected_at', 'exemplar', 'occurrences')
        return [dict(zip(columns, row)) for row in self.database.execute(query, parameters)]

    def series(self, endpoint=None, present=None):
        query = ('SELECT endpoint, series_key, metric_name, labels, kind, family_name, '
                 'unit, help, first_seen_batch, last_seen_batch, first_seen_at, '
                 'last_seen_at, present FROM series')
        clauses = []
        parameters = []
        if endpoint is not None:
            clauses.append('endpoint = ?')
            parameters.append(endpoint)
        if present is not None:
            clauses.append('present = ?')
            parameters.append(1 if present else 0)
        if clauses:
            query += ' WHERE ' + ' AND '.join(clauses)
        query += ' ORDER BY endpoint, series_key'
        columns = ('endpoint', 'series_key', 'metric_name', 'labels', 'kind',
                   'family_name', 'unit', 'help', 'first_seen_batch', 'last_seen_batch',
                   'first_seen_at', 'last_seen_at', 'present')
        return [dict(zip(columns, row)) for row in self.database.execute(query, parameters)]


class BatchConflict(Exception):
    """The endpoint already has an unfinished (claimed) scrape."""
