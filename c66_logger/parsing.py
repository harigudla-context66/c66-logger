"""
Turns whatever the caller passes to AuditLogger.log() into a list of dicts.

Accepted inputs (and any number of them, comma-separated, in one call):

* a dict                                  -> 1 item
* a list/tuple of dicts                   -> 1 item per dict
* a JSON string: object, array of objects, or JSON Lines -> 1 item per object
* a CSV string with a header row          -> 1 item per data row
  (or a header-less CSV plus `csv_fieldnames=[...]`)
* a single-line plain string              -> 1 item: {"message": <the string>}
* bytes of any of the string forms (decoded as UTF-8)
"""

from __future__ import annotations

import csv
import io
import json
from collections.abc import Mapping
from typing import Any, Dict, Iterable, List, Optional, Sequence

from .exceptions import InvalidLogInputError

FORMATS = ("auto", "dict", "json", "csv")


def parse_items(
    items: Iterable[Any],
    *,
    fmt: str = "auto",
    csv_fieldnames: Optional[Sequence[str]] = None,
    csv_delimiter: str = ",",
) -> List[Dict[str, Any]]:
    if fmt not in FORMATS:
        raise InvalidLogInputError(f"fmt must be one of {FORMATS}, got {fmt!r}")

    result: List[Dict[str, Any]] = []
    for position, item in enumerate(items):
        try:
            result.extend(
                _parse_one(item, fmt=fmt, csv_fieldnames=csv_fieldnames, csv_delimiter=csv_delimiter)
            )
        except InvalidLogInputError as exc:
            raise InvalidLogInputError(f"item {position}: {exc}") from None
    if not result:
        raise InvalidLogInputError("no log items given")
    return result


def _parse_one(item: Any, *, fmt: str, csv_fieldnames, csv_delimiter) -> List[Dict[str, Any]]:
    if isinstance(item, Mapping):
        if fmt not in ("auto", "dict"):
            raise InvalidLogInputError(f"got a dict but fmt={fmt!r}")
        return [_as_str_keys(item)]

    if isinstance(item, (list, tuple)):
        if not item:
            raise InvalidLogInputError("empty list")
        out = []
        for i, element in enumerate(item):
            if not isinstance(element, Mapping):
                raise InvalidLogInputError(
                    f"list element {i} is {type(element).__name__}, expected a dict"
                )
            out.append(_as_str_keys(element))
        return out

    if isinstance(item, (bytes, bytearray)):
        try:
            item = bytes(item).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise InvalidLogInputError(f"bytes are not valid UTF-8: {exc}") from None

    if isinstance(item, str):
        text = item.strip()
        if not text:
            raise InvalidLogInputError("empty string")
        if fmt == "json" or (fmt == "auto" and text[0] in "{["):
            return _parse_json(text)
        if fmt == "dict":
            raise InvalidLogInputError("got a string but fmt='dict'")
        if fmt == "auto" and "\n" not in text and not csv_fieldnames:
            # A single line that isn't JSON is a plain message: log.info("sync started")
            return [{"message": text}]
        return _parse_csv(text, csv_fieldnames, csv_delimiter)

    raise InvalidLogInputError(
        f"unsupported type {type(item).__name__}; pass a dict, list of dicts, JSON or CSV string"
    )


def _parse_json(text: str) -> List[Dict[str, Any]]:
    try:
        data = json.loads(text)
    except json.JSONDecodeError as whole_error:
        # Not one JSON document — try JSON Lines (one object per line).
        lines = [line for line in text.splitlines() if line.strip()]
        if len(lines) > 1:
            try:
                data = [json.loads(line) for line in lines]
            except json.JSONDecodeError:
                raise InvalidLogInputError(f"invalid JSON: {whole_error}") from None
        else:
            raise InvalidLogInputError(f"invalid JSON: {whole_error}") from None

    if isinstance(data, dict):
        return [data]
    if isinstance(data, list) and data and all(isinstance(d, dict) for d in data):
        return data
    raise InvalidLogInputError("JSON must be an object or a non-empty array of objects")


def _parse_csv(text: str, fieldnames, delimiter: str) -> List[Dict[str, Any]]:
    reader = csv.DictReader(
        io.StringIO(text),
        fieldnames=list(fieldnames) if fieldnames else None,
        delimiter=delimiter,
        skipinitialspace=True,
    )
    rows: List[Dict[str, Any]] = []
    for line_no, row in enumerate(reader, start=1 if fieldnames else 2):
        if None in row:  # more values than headers
            raise InvalidLogInputError(
                f"CSV line {line_no} has more values than the {len(reader.fieldnames or [])} columns"
            )
        rows.append({str(k).strip(): v for k, v in row.items()})
    if not rows:
        raise InvalidLogInputError(
            "CSV input needs a header row plus at least one data row "
            "(or pass csv_fieldnames=[...] for header-less CSV)"
        )
    return rows


def _as_str_keys(mapping: Mapping) -> Dict[str, Any]:
    return {str(k): v for k, v in mapping.items()}
