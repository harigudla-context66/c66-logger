import pytest

from c66_logger import InvalidLogInputError, parse_items


def test_single_dict():
    assert parse_items([{"event": "login"}]) == [{"event": "login"}]


def test_several_items_in_one_call():
    assert parse_items([{"a": 1}, {"b": 2}]) == [{"a": 1}, {"b": 2}]


def test_list_of_dicts():
    assert parse_items([[{"a": 1}, {"a": 2}]]) == [{"a": 1}, {"a": 2}]


def test_json_object_and_array():
    assert parse_items(['{"a": 1}']) == [{"a": 1}]
    assert parse_items(['[{"a": 1}, {"a": 2}]']) == [{"a": 1}, {"a": 2}]


def test_json_lines():
    assert parse_items(['{"a": 1}\n{"a": 2}\n']) == [{"a": 1}, {"a": 2}]


def test_csv_with_header():
    text = "event, user\nlogin, alice\nlogout, bob\n"
    assert parse_items([text]) == [
        {"event": "login", "user": "alice"},
        {"event": "logout", "user": "bob"},
    ]


def test_csv_with_explicit_fieldnames():
    assert parse_items(["login,alice"], fmt="csv", csv_fieldnames=["event", "user"]) == [
        {"event": "login", "user": "alice"}
    ]


def test_mixed_inputs_in_one_call():
    items = parse_items([{"a": 1}, '{"a": 2}', "a\n3"])
    assert items == [{"a": 1}, {"a": 2}, {"a": "3"}]


def test_bytes_are_decoded():
    assert parse_items([b'{"a": 1}']) == [{"a": 1}]


@pytest.mark.parametrize(
    "bad",
    [
        "",                      # empty
        "a,b\n1,2,3",            # more values than columns
        '{"a": 1',               # broken JSON
        "[1, 2]",                # JSON array of non-objects
        [1, 2],                  # list of non-dicts
        42,                      # unsupported type
    ],
)
def test_bad_input_raises(bad):
    with pytest.raises(InvalidLogInputError):
        parse_items([bad])


def test_no_items_raises():
    with pytest.raises(InvalidLogInputError):
        parse_items([])


def test_error_says_which_item():
    with pytest.raises(InvalidLogInputError, match="item 1"):
        parse_items([{"ok": 1}, 42])


def test_fmt_mismatch_raises():
    with pytest.raises(InvalidLogInputError):
        parse_items([{"a": 1}], fmt="csv")


def test_single_line_string_is_a_message():
    assert parse_items(["sync started"]) == [{"message": "sync started"}]


def test_csv_header_without_rows_raises_when_csv_is_forced():
    with pytest.raises(InvalidLogInputError, match="header row"):
        parse_items(["a,b"], fmt="csv")
