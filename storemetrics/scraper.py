"""Scrape scheduling and batch orchestration.

One scrape attempt for one endpoint is one receive batch.  The store is the
only durable scheduling state: the next attempt number and the last completed
attempt are read from SQLite, so a restarted process never invents a missing
scrape from in-memory timers.  A per-endpoint claim row (created in its own
IMMEDIATE transaction) guarantees at most one unfinished scrape per endpoint.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import socket
from urllib.error import URLError

from .parser import ParseError, SeriesBudgetExceeded, parse_exposition
from .store import BatchConflict
from .transport import capture


class Clock:
    """Injected clock interface."""

    def now(self):
        raise NotImplementedError


class SystemClock(Clock):
    def now(self):
        return datetime.now(timezone.utc)


class StepClock(Clock):
    """Deterministic clock: a fixed start with a fixed step per read."""

    def __init__(self, start, step_seconds=1.0):
        self.start = start
        self.step_seconds = step_seconds
        self.reads = 0

    def now(self):
        self.reads += 1
        return self.start + timedelta(seconds=self.step_seconds * self.reads)


@dataclass(frozen=True)
class Endpoint:
    name: str
    url: str


@dataclass(frozen=True)
class ScrapeConfig:
    endpoints: tuple
    series_budget: int = 200
    timeout_seconds: float = 3.0
    interval_seconds: float = 0.0

    def endpoint(self, name):
        for endpoint in self.endpoints:
            if endpoint.name == name:
                return endpoint
        raise ValueError('unknown endpoint %r' % name)

    def as_json(self):
        return {
            'endpoints': [{'name': e.name, 'url': e.url} for e in self.endpoints],
            'series_budget': self.series_budget,
            'timeout_seconds': self.timeout_seconds,
            'interval_seconds': self.interval_seconds,
        }


def load_config(path):
    document = json.loads(Path(path).read_text(encoding='utf-8'))
    if not isinstance(document, dict):
        raise ValueError('config must be a JSON object')
    endpoints = document.get('endpoints')
    if not isinstance(endpoints, list) or not endpoints:
        raise ValueError('config endpoints must be a non-empty list')
    seen = set()
    parsed = []
    for item in endpoints:
        if not isinstance(item, dict):
            raise ValueError('each endpoint must be an object')
        name, url = item.get('name'), item.get('url')
        if not isinstance(name, str) or not name:
            raise ValueError('endpoint name must be a non-empty string')
        if name in seen:
            raise ValueError('duplicate endpoint name %r' % name)
        if not isinstance(url, str) or not url:
            raise ValueError('endpoint url must be a non-empty string')
        seen.add(name)
        parsed.append(Endpoint(name, url))
    series_budget = document.get('series_budget', 200)
    if not isinstance(series_budget, int) or series_budget <= 0:
        raise ValueError('series_budget must be a positive integer')
    timeout = document.get('timeout_seconds', 3.0)
    if not isinstance(timeout, (int, float)) or timeout <= 0:
        raise ValueError('timeout_seconds must be positive')
    interval = document.get('interval_seconds', 0.0)
    if not isinstance(interval, (int, float)) or interval < 0:
        raise ValueError('interval_seconds must be non-negative')
    return ScrapeConfig(tuple(parsed), series_budget, float(timeout), float(interval))


def _iso(moment):
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc).isoformat(timespec='microseconds')


def _parse_iso(text):
    moment = datetime.fromisoformat(text)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)


def _failure_kind(error):
    if isinstance(error, SeriesBudgetExceeded):
        return 'budget'
    if isinstance(error, ParseError):
        return 'parse'
    if isinstance(error, (TimeoutError, socket.timeout)):
        return 'timeout'
    if isinstance(error, URLError) and isinstance(error.reason, (TimeoutError, socket.timeout)):
        return 'timeout'
    if isinstance(error, (TimeoutError, socket.timeout)):
        return 'timeout'
    return 'transport'


class Scraper:
    def __init__(self, store, config, clock=None, owner=None, fetcher=None):
        if not isinstance(config, ScrapeConfig):
            raise TypeError('config must be a ScrapeConfig')
        self.store = store
        self.config = config
        self.clock = clock or SystemClock()
        self.owner = owner or ('pid-%d' % os.getpid())
        self.fetcher = fetcher or capture

    # ------------------------------------------------------------- scheduling
    def release_stale_claims(self, ttl_seconds):
        """Release claims older than the TTL using the injected clock.

        Releasing a stale claim never touches series existence; a crash or a
        restart cannot fabricate a complete scrape or a disappearance.
        """
        if ttl_seconds is None or ttl_seconds <= 0:
            return []
        cutoff = self.clock.now() - timedelta(seconds=ttl_seconds)
        released = []
        with self.store.database:
            for endpoint, started_at in self.store.database.execute(
                    'SELECT endpoint, started_at FROM scrape_claims').fetchall():
                if _parse_iso(started_at) < cutoff:
                    self.store.database.execute(
                        'DELETE FROM scrape_claims WHERE endpoint = ? AND started_at = ?',
                        (endpoint, started_at))
                    released.append(endpoint)
        return released

    def due(self):
        """Endpoints whose durable last attempt is old enough.

        This uses persisted finish times and the injected clock, so restarting
        the process cannot synthesise or skip scrapes based on a timer.
        """
        now = self.clock.now()
        due = []
        for endpoint in self.config.endpoints:
            last = self.store.database.execute(
                'SELECT MAX(finished_at) FROM scrape_batches WHERE endpoint = ?',
                (endpoint.name,)).fetchone()[0]
            if last is None:
                due.append(endpoint)
            elif now - _parse_iso(last) >= timedelta(seconds=self.config.interval_seconds):
                due.append(endpoint)
        return due

    def run_once(self, endpoint_names=None, include_not_due=False):
        """Scrape each requested endpoint; one failure never blocks another."""
        if endpoint_names is None:
            targets = [e.name for e in (self.config.endpoints if include_not_due
                                        else self.due())]
        else:
            targets = list(endpoint_names)
            for name in targets:
                self.config.endpoint(name)
        results = []
        for name in targets:
            try:
                results.append(self.scrape(name))
            except BatchConflict as error:
                results.append({'endpoint': name, 'outcome': 'skipped',
                                'failure_kind': 'conflict', 'diagnostic': str(error)})
        return results

    # ---------------------------------------------------------------- scrape
    def _claim(self, endpoint_name):
        now = _iso(self.clock.now())
        try:
            attempt = self.store.claim(endpoint_name, now, self.owner)
        except BatchConflict as error:
            raise BatchConflict('endpoint %s already has an unfinished scrape: %s'
                                % (endpoint_name, error))
        return attempt, now

    def scrape(self, endpoint_name):
        endpoint = self.config.endpoint(endpoint_name)
        attempt, started_at = self._claim(endpoint_name)
        payload = None
        status = None
        try:
            try:
                status, headers, payload = self.fetcher(
                    endpoint.url, 'GET', timeout=self.config.timeout_seconds,
                    maximum=4194304)
                finished_at = _iso(self.clock.now())
                if status < 200 or status >= 300:
                    return self.store.record_failure(
                        endpoint_name, attempt, 'transport', started_at, finished_at,
                        diagnostic='HTTP status %d' % status, http_status=status,
                        payload=payload).as_dict()
                try:
                    exposition = parse_exposition(payload,
                                                  series_budget=self.config.series_budget)
                except SeriesBudgetExceeded as error:
                    return self.store.record_failure(
                        endpoint_name, attempt, 'budget', started_at, finished_at,
                        diagnostic=str(error), http_status=status, payload=payload,
                        series_seen=error.count).as_dict()
                except ParseError as error:
                    return self.store.record_failure(
                        endpoint_name, attempt, 'parse', started_at, finished_at,
                        diagnostic=str(error), http_status=status,
                        payload=payload).as_dict()
                digest, size = self.store.save_payload(payload)
                result = self.store.commit_success(
                    endpoint_name, attempt, exposition, started_at, finished_at,
                    collected_at=finished_at, payload_digest=digest,
                    payload_size=size, http_status=status)
                return result.as_dict()
            except (TimeoutError, socket.timeout, URLError, OSError,
                    ValueError) as error:
                finished_at = _iso(self.clock.now())
                kind = _failure_kind(error)
                return self.store.record_failure(
                    endpoint_name, attempt, kind, started_at, finished_at,
                    diagnostic=str(error), http_status=status, payload=payload).as_dict()
            except Exception as error:
                finished_at = _iso(self.clock.now())
                return self.store.record_failure(
                    endpoint_name, attempt, 'internal', started_at, finished_at,
                    diagnostic='%s: %s' % (type(error).__name__, error),
                    http_status=status, payload=payload).as_dict()
        finally:
            self.store.release_claim(endpoint_name, self.owner)
