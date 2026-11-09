"""History is supported by dated source evidence, never by first observation."""

import hashlib
import json
from datetime import UTC, datetime

from warehouse.model import DIMENSIONS


def event(kind, key, effective_at, attributes, source_uri, generation, basis):
    if kind not in DIMENSIONS or not key or key == "_UNKNOWN":
        raise ValueError("Invalid dimension identity")
    instant = datetime.fromisoformat(effective_at.replace("Z", "+00:00"))
    if instant.tzinfo is None:
        raise ValueError("Dimension evidence needs an explicit UTC offset")
    attrs = json.dumps(attributes, sort_keys=True, separators=(",", ":"))
    return {
        "kind": kind,
        "natural_key": key,
        "effective_at_utc": instant.astimezone(UTC).isoformat(),
        "attributes": attributes,
        "row_hash": hashlib.sha256(attrs.encode()).hexdigest(),
        "source_uri": source_uri,
        "source_generation": str(generation),
        "effective_basis": basis,
    }


def versions(events):
    """Reference model used by synthetic history tests and event validation."""
    unique = {}
    for item in events:
        identity = (item["kind"], item["natural_key"], item["effective_at_utc"])
        if identity in unique and unique[identity]["row_hash"] != item["row_hash"]:
            raise ValueError("Conflicting attributes at the same effective instant")
        unique[identity] = item
    output = []
    for identity in sorted(unique):
        item = unique[identity]
        previous = output[-1] if output else None
        same_key = (
            previous and (previous["kind"], previous["natural_key"]) == identity[:2]
        )
        if same_key and previous["row_hash"] == item["row_hash"]:
            continue
        if same_key:
            previous["valid_to_utc"] = item["effective_at_utc"]
            previous["is_current"] = False
        output.append(
            {
                **item,
                "valid_from_utc": item["effective_at_utc"],
                "valid_to_utc": None,
                "is_current": True,
            }
        )
    return output


def at_time(history, kind, key, instant):
    timestamp = datetime.fromisoformat(instant.replace("Z", "+00:00"))
    for row in history:
        start = datetime.fromisoformat(row["valid_from_utc"])
        end = (
            datetime.fromisoformat(row["valid_to_utc"]) if row["valid_to_utc"] else None
        )
        if (
            row["kind"] == kind
            and row["natural_key"] == key
            and start <= timestamp
            and (end is None or timestamp < end)
        ):
            return row
    return None
