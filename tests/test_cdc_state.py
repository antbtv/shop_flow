import json

from scripts.cdc_state import summarise


def line(key, op, lsn):
    return f'{{"id":{key}}}|' + json.dumps({"op": op, "source": {"lsn": lsn}})


def test_live_keys_follow_last_op():
    events = [line(1, "c", 10), line(2, "c", 11), line(1, "u", 12), line(2, "d", 13)]
    assert summarise(events) == {"live_keys": 1, "ops": {"c": 2, "u": 1, "d": 1}, "duplicates": 0}


def test_snapshot_rows_are_live():
    assert summarise([line(1, "r", 5), line(2, "r", 5)])["live_keys"] == 2


def test_redelivered_event_is_a_duplicate_not_a_new_row():
    events = [line(1, "c", 10), line(1, "u", 12), line(1, "u", 12)]
    assert summarise(events) == {"live_keys": 1, "ops": {"c": 1, "u": 2}, "duplicates": 1}


def test_tombstone_is_ignored():
    assert summarise([line(1, "c", 10), '{"id":1}|null'])["live_keys"] == 1
