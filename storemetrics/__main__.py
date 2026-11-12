import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

from .journal import Journal
from .parser import inspect
from .scraper import Scraper, StepClock, SystemClock, load_config
from .store import Store
from .transport import capture


def _emit(value):
    print(json.dumps(value, ensure_ascii=True, sort_keys=True, default=str))


def _scrape(args):
    config = load_config(args.config)
    if args.endpoint and args.all:
        raise ValueError('choose --endpoint or --all, not both')
    store = Store(args.state)
    try:
        clock = (StepClock(datetime.fromisoformat(args.clock_start))
                 if args.clock_start else SystemClock() if args.system_clock
                 else StepClock(datetime(2026, 1, 1, tzinfo=timezone.utc)))
        scraper = Scraper(store, config, clock=clock, owner=args.owner)
        if args.release_stale:
            _emit({'released_claims': scraper.release_stale_claims(args.release_stale)})
        if args.endpoint:
            # An explicit endpoint is an operator-requested scrape, so schedule
            # windows do not suppress it.
            results = scraper.run_once([args.endpoint], include_not_due=True)
        else:
            # ``--all`` honours durable due times: a restart must not repeat
            # scrapes just because a process-local timer was lost.
            names = None if args.force else [e.name for e in scraper.due()]
            results = scraper.run_once(names, include_not_due=True)
        _emit({'config': config.as_json(), 'results': results})
    finally:
        store.close()


def _query(args):
    store = Store(args.state)
    try:
        if args.batches:
            _emit({'batches': store.batches(args.endpoint)})
        elif args.series:
            present = None if args.present is None else args.present == 'yes'
            _emit({'series': store.series(args.endpoint, present=present)})
        else:
            _emit({'samples': store.samples(args.endpoint, args.batch)})
    finally:
        store.close()


def main():
    parser = argparse.ArgumentParser(description='门店设备测量 exposition 文本与 HTTP 抓取原文工具。')
    sub = parser.add_subparsers(dest='command', required=True)

    view = sub.add_parser('inspect')
    view.add_argument('source')

    raw = sub.add_parser('capture')
    raw.add_argument('url')
    raw.add_argument('--state', default='var')
    raw.add_argument('--method', default='GET')
    raw.add_argument('--body')
    raw.add_argument('--header', action='append', default=[])

    scrape = sub.add_parser('scrape', help='抓取配置中的本地设备端点，一次抓取即一个接收批')
    scrape.add_argument('--config', required=True)
    scrape.add_argument('--state', required=True)
    scrape.add_argument('--endpoint')
    scrape.add_argument('--all', action='store_true')
    scrape.add_argument('--force', action='store_true',
                        help='with --all, scrape even when the interval is not due')
    scrape.add_argument('--owner', default=None,
                        help='claim owner recorded in SQLite (defaults to the process id)')
    scrape.add_argument('--clock-start', default=None,
                        help='injected clock start (ISO-8601) for a deterministic manual run')
    scrape.add_argument('--release-stale', type=float, default=None,
                        metavar='SECONDS')
    scrape.add_argument('--system-clock', action='store_true',
                        help='use the process clock instead of the deterministic clock')

    query = sub.add_parser('query', help='查询接收批、样本或系列身份')
    query.add_argument('--state', required=True)
    query.add_argument('--endpoint')
    query.add_argument('--batch')
    group = query.add_mutually_exclusive_group()
    group.add_argument('--batches', action='store_true')
    group.add_argument('--series', action='store_true')
    query.add_argument('--present', choices=('yes', 'no'))

    args = parser.parse_args()
    try:
        if args.command == 'capture':
            headers = dict(value.split(':', 1) for value in args.header)
            headers = {key.strip(): value.strip() for key, value in headers.items()}
            status, response_headers, payload = capture(args.url, args.method,
                Path(args.body).read_bytes() if args.body else None, headers)
            journal = Journal(args.state)
            try:
                result = journal.save(payload, args.url)
            finally:
                journal.close()
            result.update({'http_status': status, 'headers': response_headers})
            _emit(result)
        elif args.command == 'scrape':
            _scrape(args)
        elif args.command == 'query':
            _query(args)
        else:
            _emit(inspect(Path(args.source).read_bytes()))
    except (ValueError, OSError) as error:
        print(str(error), file=sys.stderr)
        return 2
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
