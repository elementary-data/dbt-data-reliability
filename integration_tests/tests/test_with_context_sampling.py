import json

import pytest
from dbt_project import DbtProject

COLUMN_NAME = "some_column"
CONTEXT_COLUMN = "context_column"
OTHER_COLUMN = "other_column"

TEST_SAMPLE_ROW_COUNT = 7

TEST_VARS = {
    "enable_elementary_test_materialization": True,
    "test_sample_row_count": TEST_SAMPLE_ROW_COUNT,
}


def get_samples(dbt_project: DbtProject, test_id: str):
    return [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(dbt_project.samples_query(test_id))
    ]


def test_with_context_test_is_sampled(test_id: str, dbt_project: DbtProject):
    """with_context tests rely on the test materialization to capture their failing rows."""
    null_count = 50
    data = [
        {COLUMN_NAME: None, CONTEXT_COLUMN: f"context-{index}"}
        for index in range(null_count)
    ]
    test_result = dbt_project.test(
        test_id,
        "elementary.not_null_with_context",
        dict(column_name=COLUMN_NAME, context_columns=[CONTEXT_COLUMN]),
        data=data,
        test_vars=TEST_VARS,
    )
    assert test_result["status"] == "fail"
    assert test_result["failed_row_count"] == null_count
    # Must stay dbt_test: the alerts models partition on test_type exactly.
    assert test_result["test_type"] == "dbt_test"

    samples = get_samples(dbt_project, test_id)
    assert len(samples) == TEST_SAMPLE_ROW_COUNT
    # Membership, since sampling returns an arbitrary TEST_SAMPLE_ROW_COUNT of the failing rows.
    expected_context_values = {f"context-{index}" for index in range(null_count)}
    for sample in samples:
        assert sample[COLUMN_NAME] is None
        assert sample[CONTEXT_COLUMN] in expected_context_values


def test_with_context_selects_only_requested_columns(
    test_id: str, dbt_project: DbtProject
):
    """context_columns narrows the sample, so an unrequested column must not appear."""
    data = [
        {
            COLUMN_NAME: None,
            CONTEXT_COLUMN: f"context-{index}",
            OTHER_COLUMN: f"other-{index}",
        }
        for index in range(10)
    ]
    test_result = dbt_project.test(
        test_id,
        "elementary.not_null_with_context",
        dict(column_name=COLUMN_NAME, context_columns=[CONTEXT_COLUMN]),
        data=data,
        test_vars=TEST_VARS,
    )
    assert test_result["status"] == "fail"

    samples = get_samples(dbt_project, test_id)
    assert samples
    for sample in samples:
        assert set(sample.keys()) == {COLUMN_NAME, CONTEXT_COLUMN}


def test_with_context_skips_nonexistent_context_column(
    test_id: str, dbt_project: DbtProject
):
    """An unknown context column is warned about and dropped, not a compile error."""
    data = [
        {COLUMN_NAME: None, CONTEXT_COLUMN: f"context-{index}"} for index in range(10)
    ]
    test_result = dbt_project.test(
        test_id,
        "elementary.not_null_with_context",
        dict(
            column_name=COLUMN_NAME,
            context_columns=[CONTEXT_COLUMN, "column_that_does_not_exist"],
        ),
        data=data,
        test_vars=TEST_VARS,
    )
    assert test_result["status"] == "fail"

    samples = get_samples(dbt_project, test_id)
    assert samples
    for sample in samples:
        assert set(sample.keys()) == {COLUMN_NAME, CONTEXT_COLUMN}


def test_expression_is_true_with_context(test_id: str, dbt_project: DbtProject):
    """expression_is_true is table-level, so context_columns is its only way to sample data."""
    data = [
        {COLUMN_NAME: index, OTHER_COLUMN: index + 1, CONTEXT_COLUMN: f"ctx-{index}"}
        for index in range(10)
    ]
    test_result = dbt_project.test(
        test_id,
        "elementary.expression_is_true_with_context",
        dict(
            expression=f"{COLUMN_NAME} > {OTHER_COLUMN}",
            context_columns=[COLUMN_NAME, CONTEXT_COLUMN],
        ),
        data=data,
        test_vars=TEST_VARS,
    )
    assert test_result["status"] == "fail"
    assert test_result["failed_row_count"] == 10
    assert test_result["test_type"] == "dbt_test"

    samples = get_samples(dbt_project, test_id)
    assert samples
    for sample in samples:
        assert set(sample.keys()) == {COLUMN_NAME, CONTEXT_COLUMN}


# The default seeder writes a CSV and runs `dbt seed`, and dbt types seed
# columns with agate Text(null_values=("null", "")), whose cast() checks
# `d.strip().lower() in null_values`. Any all-whitespace or empty cell is
# therefore read as NULL, so a genuine empty string cannot be seeded. These
# three targets bypass `dbt seed` and preserve the value verbatim.
@pytest.mark.only_on_targets(["clickhouse", "vertica", "spark"])
def test_not_empty_string_with_context(test_id: str, dbt_project: DbtProject):
    empty_count = 10
    data = [
        {COLUMN_NAME: "   ", CONTEXT_COLUMN: f"ctx-{index}"}
        for index in range(empty_count)
    ] + [{COLUMN_NAME: "value", CONTEXT_COLUMN: "ctx-ok"}]
    test_result = dbt_project.test(
        test_id,
        "elementary.not_empty_string_with_context",
        dict(column_name=COLUMN_NAME, context_columns=[CONTEXT_COLUMN]),
        data=data,
        test_vars=TEST_VARS,
    )
    assert test_result["status"] == "fail"
    assert test_result["failed_row_count"] == empty_count
    assert test_result["test_type"] == "dbt_test"

    samples = get_samples(dbt_project, test_id)
    assert samples
    for sample in samples:
        assert set(sample.keys()) == {COLUMN_NAME, CONTEXT_COLUMN}


def test_expect_column_pair_values_a_to_be_greater_than_b_with_context(
    test_id: str, dbt_project: DbtProject
):
    failing_count = 10
    data = [
        {COLUMN_NAME: index, OTHER_COLUMN: index + 1, CONTEXT_COLUMN: f"ctx-{index}"}
        for index in range(failing_count)
    ] + [{COLUMN_NAME: 100, OTHER_COLUMN: 1, CONTEXT_COLUMN: "ctx-ok"}]
    test_result = dbt_project.test(
        test_id,
        "elementary.expect_column_pair_values_A_to_be_greater_than_B_with_context",
        dict(
            column_A=COLUMN_NAME,
            column_B=OTHER_COLUMN,
            context_columns=[CONTEXT_COLUMN],
        ),
        data=data,
        test_vars=TEST_VARS,
    )
    assert test_result["status"] == "fail"
    assert test_result["failed_row_count"] == failing_count
    assert test_result["test_type"] == "dbt_test"

    samples = get_samples(dbt_project, test_id)
    assert samples
    # Both tested columns are selected before the context column.
    for sample in samples:
        assert set(sample.keys()) == {COLUMN_NAME, OTHER_COLUMN, CONTEXT_COLUMN}


def test_expect_compound_columns_to_be_unique_with_context(
    test_id: str, dbt_project: DbtProject
):
    """Duplicates are on the pair, so neither column alone identifies the failure."""
    data = [
        {COLUMN_NAME: "a", OTHER_COLUMN: "x", CONTEXT_COLUMN: "ctx-1"},
        {COLUMN_NAME: "a", OTHER_COLUMN: "x", CONTEXT_COLUMN: "ctx-2"},
        {COLUMN_NAME: "a", OTHER_COLUMN: "y", CONTEXT_COLUMN: "ctx-3"},
        {COLUMN_NAME: "b", OTHER_COLUMN: "x", CONTEXT_COLUMN: "ctx-4"},
    ]
    test_result = dbt_project.test(
        test_id,
        "elementary.expect_compound_columns_to_be_unique_with_context",
        dict(
            column_list=[COLUMN_NAME, OTHER_COLUMN],
            context_columns=[CONTEXT_COLUMN],
        ),
        data=data,
        test_vars=TEST_VARS,
    )
    assert test_result["status"] == "fail"
    assert test_result["failed_row_count"] == 2
    assert test_result["test_type"] == "dbt_test"

    samples = get_samples(dbt_project, test_id)
    assert samples
    for sample in samples:
        # n_records is a helper column and must never reach the sample.
        assert set(sample.keys()) == {COLUMN_NAME, OTHER_COLUMN, CONTEXT_COLUMN}
        assert sample[COLUMN_NAME] == "a"
        assert sample[OTHER_COLUMN] == "x"


# T-SQL has no regex functions, so elementary.regexp_match raises a compiler
# error there by design. See sqlserver__regexp_match in regexp_match.sql.
@pytest.mark.skip_targets(["sqlserver", "fabric"])
def test_match_regex_with_context_searches_substrings(
    test_id: str, dbt_project: DbtProject
):
    """An unanchored pattern must match anywhere in the value, not the whole value.

    This is the regression test for adapters whose regexp_like implicitly anchors
    at both ends (Snowflake, Dremio). If one of those regressed to an anchored
    match, both rows would fail here instead of one.
    """
    data = [
        {COLUMN_NAME: "prefix-abc-suffix", CONTEXT_COLUMN: "ctx-match"},
        {COLUMN_NAME: "nothing-here", CONTEXT_COLUMN: "ctx-nomatch"},
    ]
    test_result = dbt_project.test(
        test_id,
        "elementary.expect_column_values_to_match_regex_with_context",
        dict(column_name=COLUMN_NAME, regex="abc", context_columns=[CONTEXT_COLUMN]),
        data=data,
        test_vars=TEST_VARS,
    )
    assert test_result["status"] == "fail"
    assert test_result["failed_row_count"] == 1
    assert test_result["test_type"] == "dbt_test"

    samples = get_samples(dbt_project, test_id)
    assert len(samples) == 1
    assert samples[0][COLUMN_NAME] == "nothing-here"
    assert set(samples[0].keys()) == {COLUMN_NAME, CONTEXT_COLUMN}


# T-SQL has no regex functions, so elementary.regexp_match raises a compiler
# error there by design. See sqlserver__regexp_match in regexp_match.sql.
@pytest.mark.skip_targets(["sqlserver", "fabric"])
def test_match_regex_with_context_honors_case_insensitive_flag(
    test_id: str, dbt_project: DbtProject
):
    data = [
        {COLUMN_NAME: "ABC", CONTEXT_COLUMN: "ctx-1"},
        {COLUMN_NAME: "xyz", CONTEXT_COLUMN: "ctx-2"},
    ]
    test_result = dbt_project.test(
        test_id,
        "elementary.expect_column_values_to_match_regex_with_context",
        dict(
            column_name=COLUMN_NAME,
            regex="abc",
            flags="i",
            context_columns=[CONTEXT_COLUMN],
        ),
        data=data,
        test_vars=TEST_VARS,
    )
    assert test_result["status"] == "fail"
    # Only 'xyz' fails; 'ABC' matches once the i flag is honored.
    assert test_result["failed_row_count"] == 1

    samples = get_samples(dbt_project, test_id)
    assert len(samples) == 1
    assert samples[0][COLUMN_NAME] == "xyz"


# T-SQL has no regex functions, so elementary.regexp_match raises a compiler
# error there by design. See sqlserver__regexp_match in regexp_match.sql.
@pytest.mark.skip_targets(["sqlserver", "fabric"])
def test_match_regex_list_with_context_match_on_any(
    test_id: str, dbt_project: DbtProject
):
    data = [
        {COLUMN_NAME: "abc", CONTEXT_COLUMN: "ctx-1"},
        {COLUMN_NAME: "xyz", CONTEXT_COLUMN: "ctx-2"},
        {COLUMN_NAME: "qqq", CONTEXT_COLUMN: "ctx-3"},
    ]
    test_result = dbt_project.test(
        test_id,
        "elementary.expect_column_values_to_match_regex_list_with_context",
        dict(
            column_name=COLUMN_NAME,
            regex_list=["^abc$", "^xyz$"],
            match_on="any",
            context_columns=[CONTEXT_COLUMN],
        ),
        data=data,
        test_vars=TEST_VARS,
    )
    assert test_result["status"] == "fail"
    # Only 'qqq' matches neither pattern.
    assert test_result["failed_row_count"] == 1

    samples = get_samples(dbt_project, test_id)
    assert len(samples) == 1
    assert samples[0][COLUMN_NAME] == "qqq"
    assert set(samples[0].keys()) == {COLUMN_NAME, CONTEXT_COLUMN}


# T-SQL has no regex functions, so elementary.regexp_match raises a compiler
# error there by design. See sqlserver__regexp_match in regexp_match.sql.
@pytest.mark.skip_targets(["sqlserver", "fabric"])
def test_match_regex_list_with_context_match_on_all(
    test_id: str, dbt_project: DbtProject
):
    data = [
        {COLUMN_NAME: "abc-1", CONTEXT_COLUMN: "ctx-1"},
        {COLUMN_NAME: "abc-2", CONTEXT_COLUMN: "ctx-2"},
        {COLUMN_NAME: "zzz-1", CONTEXT_COLUMN: "ctx-3"},
    ]
    test_result = dbt_project.test(
        test_id,
        "elementary.expect_column_values_to_match_regex_list_with_context",
        dict(
            column_name=COLUMN_NAME,
            regex_list=["^abc-", "-1$"],
            match_on="all",
            context_columns=[CONTEXT_COLUMN],
        ),
        data=data,
        test_vars=TEST_VARS,
    )
    assert test_result["status"] == "fail"
    # Only 'abc-1' matches both patterns.
    assert test_result["failed_row_count"] == 2
