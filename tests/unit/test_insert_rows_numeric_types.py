"""Offline regression for the destination types used by insert_rows.

Run with pytest after installing dbt-core and jinja2.
"""

from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from dbt.adapters.base.column import Column
from jinja2 import Environment


class MacroReturn(Exception):
    def __init__(self, value):
        self.value = value


def _return(value):
    raise MacroReturn(value)


def _call(macro, *args):
    try:
        return macro(*args)
    except MacroReturn as result:
        return result.value


@pytest.fixture
def macros():
    path = (
        Path(__file__).resolve().parents[2]
        / "macros/utils/table_operations/insert_rows.sql"
    )
    return (
        Environment(extensions=["jinja2.ext.do"])
        .from_string(path.read_text())
        .make_module(
            {
                "return": _return,
                "elementary": SimpleNamespace(normalize_data_type=lambda _: "numeric"),
            }
        )
    )


@pytest.mark.parametrize(
    "dtype,precision,scale,value,expected",
    [
        ("numeric", 20, 8, Decimal("123.12345678"), "numeric(20,8)"),
        (
            "decimal",
            30,
            10,
            Decimal("12345678901234567890.1234567890"),
            "decimal(30,10)",
        ),
        ("float", None, None, 0.125, "float"),
        ("bigint", None, None, 123, "bigint"),
    ],
)
def test_numeric_cast_preserves_destination_precision(
    macros, dtype, precision, scale, value, expected
):
    column = Column("amount", dtype, numeric_precision=precision, numeric_scale=scale)
    row = {"amount": value}
    metadata = _call(macros.get_columns_metadata, [column], row)
    rendered = _call(
        macros.render_row_to_sql,
        row,
        metadata,
        "current_timestamp",
        lambda v, *_: str(v),
        None,
    )
    assert rendered == f"(cast({value} as {expected}))"


def test_dict_columns_without_data_type_keep_the_existing_render_harness(macros):
    row = {"amount": 0.125}
    metadata = _call(
        macros.get_columns_metadata, [{"name": "amount", "dtype": "float"}], row
    )
    assert metadata[0]["dtype"] == "float"


@pytest.mark.parametrize("value", [None, True, False])
def test_null_and_boolean_values_are_not_cast(macros, value):
    row = {"amount": value}
    metadata = _call(
        macros.get_columns_metadata,
        [Column("amount", "numeric", numeric_precision=20, numeric_scale=8)],
        row,
    )
    rendered = _call(
        macros.render_row_to_sql,
        row,
        metadata,
        "current_timestamp",
        lambda *_: "sentinel",
        None,
    )
    assert rendered == "(sentinel)"
