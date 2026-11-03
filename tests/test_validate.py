import json

import pytest

from normalize import build, normalize
from validate import validate_dataset, validate_records


def snapshot():
    return dict(event_type='book', market='m', asset_id='yes', timestamp='1000',
                bids=[dict(price='0.4', size='10')], asks=[dict(price='0.6', size='20')])


def records(event=None, line=1, recv=1_000_000_000):
    return normalize(dict(kind='message', recv_ts_ns=recv,
                          raw=json.dumps(event or snapshot())), '18.jsonl', line)


def lifecycle(kind, line=2, payload=None):
    return normalize(dict(kind=kind, recv_ts_ns=1_000_000_000, raw=payload or {}), '18.jsonl', line)


def codes(report):
    return {finding['code'] for finding in report['findings']}


def test_valid_snapshot_and_clean_stop():
    result = validate_records(records() + lifecycle('stopped')).result()
    assert result['status'] == 'ok'
    assert result['replay_checked']
    assert result['assets_observed'] == 1
    assert result['exchange_to_receive_ms']['p50'] == 0
    assert codes(result) == {'collection_stopped'}


def test_one_sided_book_is_informational():
    event = snapshot()
    event['asks'] = []
    result = validate_records(records(event)).result()
    assert result['counts'] == dict(error=0, warning=0, info=1)


def test_crossed_book_has_source():
    event = snapshot()
    event['bids'][0]['price'] = '0.7'
    result = validate_records(records(event)).result()
    assert result['status'] == 'errors'
    assert result['findings'][0]['source']['source_line'] == 1
    assert 'crossed_or_locked_book' in codes(result)


def test_atomic_price_changes_avoid_transient_cross():
    change = dict(event_type='price_change', market='m', timestamp='1000', price_changes=[
        dict(asset_id='yes', side='BUY', price='0.7', size='3'),
        dict(asset_id='yes', side='SELL', price='0.6', size='0'),
        dict(asset_id='yes', side='SELL', price='0.8', size='3')])
    result = validate_records(records() + records(change, line=2)).result()
    assert result['counts']['error'] == 0


def test_duplicate_position_prevents_replay():
    result = validate_records(records() + records()).result()
    assert 'invalid_record' in codes(result)
    assert not result['replay_checked']


@pytest.mark.parametrize('field,value', [('size', '-1'), ('size', '0'), ('price', 'NaN'), ('price', '1.2')])
def test_invalid_snapshot_level(field, value):
    data = records()
    data[0][1]['bids'][0][field] = value
    assert validate_records(data).result()['counts']['error'] > 0


def test_timestamp_warnings_and_regression():
    data = records(recv=7_000_000_000) + records(line=2, recv=900_000_000)
    result = validate_records(data).result()
    assert {'timestamp_difference', 'receive_clock_regression'} <= codes(result)
    data[0][1]['exchange_ts_ns'] = None
    assert 'missing_exchange_timestamp' in codes(validate_records(data).result())


def test_disconnect_and_uninitialized_delta():
    change = dict(event_type='price_change', market='m', timestamp='1000', price_changes=[
        dict(asset_id='yes', side='BUY', price='0.4', size='3')])
    result = validate_records(records() + lifecycle('disconnected') + records(change, line=3)).result()
    assert {'collection_interruption', 'delta_without_snapshot'} <= codes(result)


def test_unsupported_and_missing_subscribed_asset():
    data = lifecycle('subscribed', line=1, payload={'request': {'assets_ids': ['yes', 'no']}})
    data += records(line=2) + records({'event_type': 'new_type'}, line=3)
    assert {'unsupported_event', 'missing_snapshot'} <= codes(validate_records(data).result())


def dataset(tmp_path):
    raw = tmp_path / 'raw'
    raw.mkdir()
    (raw / '18.jsonl').write_text(json.dumps(dict(kind='message', recv_ts_ns=1_000_000_000,
                                                 raw=json.dumps(snapshot()))) + '\n')
    clean = tmp_path / 'clean'
    build(raw, clean)
    return clean


def test_real_parquet_dataset_and_tampering(tmp_path):
    clean = dataset(tmp_path)
    result = validate_dataset(clean)
    assert result['status'] == 'ok' and result['replay_checked']
    next(clean.rglob('*.parquet')).write_bytes(b'broken')
    result = validate_dataset(clean)
    assert result['status'] == 'errors'
    assert codes(result) == {'dataset_integrity'}


def test_manifest_count_mismatch(tmp_path):
    clean = dataset(tmp_path)
    path = clean / 'manifest.json'
    manifest = json.loads(path.read_text())
    manifest['counts']['books_snapshots'] = 100
    path.write_text(json.dumps(manifest))
    assert validate_dataset(clean)['status'] == 'errors'


def test_no_snapshot_and_missing_manifest(tmp_path):
    assert 'no_snapshots' in codes(validate_records([]).result())
    assert validate_dataset(tmp_path)['status'] == 'errors'
