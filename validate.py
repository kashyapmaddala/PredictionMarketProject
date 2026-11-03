"""Validate one normalized development dataset; print JSON, exit 1 on errors."""

import argparse
import json
from collections import Counter
from decimal import Decimal
from itertools import groupby
from pathlib import Path

import pyarrow.parquet as pq

from normalize import VERSION, decimal_text, digest, identifier, schemas
from reconstruct import Reconstructor, levels, position


class Report:
    def __init__(self):
        self.counts = Counter()
        self.findings = []
        self.rows = Counter()
        self.assets = set()
        self.lags = []
        self.replay_checked = False

    def add(self, severity, code, message, row=None):
        self.counts[severity] += 1
        finding = dict(severity=severity, code=code, message=message)
        if row is not None:
            finding['source'] = {k: row.get(k) for k in
                                 ('source_file', 'source_line', 'event_index', 'change_index')}
        self.findings.append(finding)

    def result(self):
        lags = sorted(self.lags)
        return dict(status='errors' if self.counts['error'] else
                    'warnings' if self.counts['warning'] else 'ok',
                    counts={k: self.counts[k] for k in ('error', 'warning', 'info')},
                    rows=dict(sorted(self.rows.items())), assets_observed=len(self.assets),
                    replay_checked=self.replay_checked,
                    exchange_to_receive_ms=None if not lags else dict(
                        samples=len(lags), minimum=lags[0], maximum=lags[-1],
                        p50=lags[(len(lags)-1)//2], p95=lags[int((len(lags)-1)*.95)]),
                    limitations=['Internal consistency only; no REST or on-chain reconciliation.',
                                 'No sequence numbers: gap-free collection cannot be certified.',
                                 'Timestamp differences include clock skew and snapshot age.',
                                 'In-memory reader intended for development recordings.'],
                    findings=self.findings)


def check_row(table, row):
    if row['normalizer_version'] != VERSION:
        raise ValueError('unsupported row version')
    if row['venue'] != 'polymarket':
        raise ValueError('unsupported venue')
    identifier(row['source_file'])
    for name, minimum in [('source_line', 1), ('event_index', 0), ('recv_ts_ns', 0)]:
        if type(row[name]) is not int or not minimum <= row[name] <= 2**63-1:
            raise ValueError(f'invalid {name}')
    if table in {'books_snapshots', 'book_deltas', 'trades'}:
        identifier(row['market_id'])
        identifier(row['asset_id'])
        ts = row['exchange_ts_ns']
        if ts is not None and (type(ts) is not int or not 0 <= ts <= 2**63-1):
            raise ValueError('invalid exchange timestamp')
    if table == 'books_snapshots':
        levels(row['bids'])
        levels(row['asks'])
        if any(Decimal(x['size']) <= 0 for x in row['bids'] + row['asks']):
            raise ValueError('snapshot contains nonpositive size')
    elif table == 'book_deltas':
        if type(row['change_index']) is not int or row['change_index'] < 0:
            raise ValueError('invalid change index')
        if row['side'] not in {'bid', 'ask'}:
            raise ValueError('invalid delta side')
        decimal_text(row['price'], price=True)
        decimal_text(row['new_size'])
    elif table == 'trades':
        decimal_text(row['price'], price=True)
        if row['size'] is not None and Decimal(decimal_text(row['size'])) <= 0:
            raise ValueError('nonpositive trade size')
        if row['reported_side'] not in {None, 'BUY', 'SELL'}:
            raise ValueError('invalid reported trade side')
    else:
        identifier(row['kind'])
        json.loads(row['payload'])


def validate_records(records, report=None, lag_warning_ms=5000):
    report = report or Report()
    positions = set()
    valid = []
    unsafe = False
    subscriptions, snapshots = set(), set()
    for table, row in records:
        report.rows[table] += 1
        try:
            if table not in schemas():
                raise ValueError('unknown table')
            check_row(table, row)
            loc = position(row)
            if loc in positions:
                raise ValueError('duplicate source position')
            positions.add(loc)
            valid.append((table, row))
        except (ValueError, KeyError, TypeError, ArithmeticError) as exc:
            report.add('error', 'invalid_record', str(exc), row)
            unsafe = True
            continue
        if 'asset_id' in row:
            report.assets.add((row['venue'], row['market_id'], row['asset_id']))
            if row['exchange_ts_ns'] is None:
                report.add('warning', 'missing_exchange_timestamp', 'Exchange time is unavailable.', row)
            else:
                lag = (row['recv_ts_ns'] - row['exchange_ts_ns']) / 1_000_000
                report.lags.append(lag)
                if lag < 0 or lag > lag_warning_ms:
                    report.add('warning', 'timestamp_difference', f'Exchange-to-receive difference: {lag} ms.', row)
        if table == 'books_snapshots':
            snapshots.add(row['asset_id'])
            if not row['bids'] or not row['asks']:
                report.add('info', 'empty_book_side', 'Snapshot has at least one empty side.', row)
        if table == 'collector_events':
            if row['kind'] == 'subscribed':
                try:
                    tokens = json.loads(row['payload'])['request']['assets_ids']
                    if not isinstance(tokens, list) or any(not isinstance(t, str) for t in tokens):
                        raise ValueError('invalid assets_ids')
                    subscriptions.update(tokens)
                except (KeyError, TypeError, ValueError):
                    report.add('warning', 'subscription_unreadable', 'Cannot determine subscribed assets.', row)
            if row['kind'] in {'disconnected', 'universe_changed'}:
                report.add('warning', 'collection_interruption', row['kind'], row)
            elif row['kind'] == 'stopped':
                report.add('info', 'collection_stopped', 'Historical state is retained.', row)
        elif table == 'unsupported_events':
            report.add('warning', 'unsupported_event', row['kind'], row)
    for token in sorted(subscriptions - snapshots):
        report.add('warning', 'missing_snapshot', f'No snapshot observed for subscribed asset {token}.')
    if not snapshots:
        report.add('error', 'no_snapshots', 'No valid snapshots in dataset.')
    if unsafe:
        report.add('warning', 'replay_skipped', 'Invalid records make replay unreliable.')
        return report
    engine = Reconstructor()
    previous_recv = None
    # Evaluate crossed books only after the entire exchange event, not between
    # individual level changes in one price_change message.
    ordered = sorted(valid, key=lambda item: position(item[1]))
    for _, group in groupby(ordered, key=lambda item: position(item[1])[:3]):
        touched = {}
        for table, row in group:
            if previous_recv is not None and row['recv_ts_ns'] < previous_recv:
                report.add('warning', 'receive_clock_regression', 'Receive clock moved backwards in capture order.', row)
            previous_recv = row['recv_ts_ns']
            before = len(engine.issues)
            engine.apply(table, row)
            for issue in engine.issues[before:]:
                if issue['reason'] == 'delta_without_snapshot':
                    report.add('warning', 'delta_without_snapshot', 'Update ignored until a fresh snapshot.', row)
            if table in {'books_snapshots', 'book_deltas'}:
                touched[(row['venue'], row['market_id'], row['asset_id'])] = row
        for key, row in touched.items():
            book = engine.books[key]
            if book.synchronized and book.bids and book.asks and max(book.bids) >= min(book.asks):
                report.add('error', 'crossed_or_locked_book', 'Best bid is at or above best ask after the event.', row)
    report.replay_checked = True
    return report


def validate_dataset(root, lag_warning_ms=5000):
    report = Report()
    root = Path(root).resolve()
    records = []
    try:
        manifest = json.loads((root / 'manifest.json').read_text())
        if manifest['normalizer_version'] != VERSION:
            raise ValueError('unsupported manifest version')
        outputs = manifest['outputs']
        actual = {p.relative_to(root).as_posix() for p in root.rglob('*.parquet')}
        if actual != set(outputs):
            raise ValueError('manifest file list differs from dataset Parquet files')
        expected_schemas = schemas()
        for relative, expected_hash in sorted(outputs.items()):
            path = (root / relative).resolve()
            if root not in path.parents or digest(path) != expected_hash:
                raise ValueError(f'invalid or modified file: {relative}')
            table = Path(relative).parts[0]
            parquet = pq.ParquetFile(path)
            if table not in expected_schemas or not parquet.schema_arrow.equals(expected_schemas[table]):
                raise ValueError(f'unsupported schema: {relative}')
            records.extend((table, row) for row in parquet.read().to_pylist())
        if dict(Counter(table for table, _ in records)) != manifest['counts']:
            raise ValueError('manifest row counts differ from Parquet data')
    except (OSError, ValueError, KeyError, TypeError) as exc:
        report.add('error', 'dataset_integrity', str(exc))
        return report.result()
    return validate_records(records, report, lag_warning_ms).result()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('dataset', type=Path)
    parser.add_argument('--lag-warning-ms', type=int, default=5000)
    args = parser.parse_args()
    if args.lag_warning_ms < 0:
        parser.error('--lag-warning-ms must be nonnegative')
    result = validate_dataset(args.dataset, args.lag_warning_ms)
    print(json.dumps(result, indent=2))
    return 1 if result['counts']['error'] else 0


if __name__ == '__main__':
    raise SystemExit(main())
