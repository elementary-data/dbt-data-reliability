# Execution SLA Test

Verifies that a dbt model was executed successfully before a specified SLA deadline time.

## Parameters

| Parameter  | Required | Description                                          |
| ---------- | -------- | ---------------------------------------------------- |
| `sla_time` | Yes      | Deadline time (e.g., `"07:00"`, `"7am"`, `"2:30pm"`) |
| `timezone` | Yes      | IANA timezone name (e.g., `"America/Los_Angeles"`)   |

## Examples

```yaml
models:
  - name: daily_revenue
    tests:
      - elementary.execution_sla:
          sla_time: "06:00"
          timezone: "America/New_York"

  - name: customer_metrics
    tests:
      - elementary.execution_sla:
          sla_time: "7am"
          timezone: "Europe/London"
```

## Logic

1. Query `dbt_run_results` for successful runs of this model today
2. If any run completed before the SLA deadline: **PASS**
3. If SLA deadline hasn't passed yet today: **PASS** (still time)
4. If model ran but all runs were after the deadline: **FAIL** (MISSED_SLA)
5. If model ran but all executions failed: **FAIL** (ALL_FAILED)
6. If model didn't run today: **FAIL** (NOT_RUN)

---

## Time Format

The test accepts flexible time formats:

| Format    | Example              | Interpreted As   |
| --------- | -------------------- | ---------------- |
| 24-hour   | `"07:00"`, `"14:30"` | 7:00 AM, 2:30 PM |
| 12-hour   | `"7am"`, `"2:30pm"`  | 7:00 AM, 2:30 PM |
| Hour only | `"7"`, `"14"`        | 7:00 AM, 2:00 PM |

**Recommendation:** Use 24-hour format for clarity.

---

## Timezone

Must be a valid IANA timezone name.

### Common Timezones

| Region      | IANA Name             |
| ----------- | --------------------- |
| US Pacific  | `America/Los_Angeles` |
| US Eastern  | `America/New_York`    |
| UK          | `Europe/London`       |
| Netherlands | `Europe/Amsterdam`    |
| Germany     | `Europe/Berlin`       |
| India       | `Asia/Kolkata`        |
| Japan       | `Asia/Tokyo`          |
| Australia   | `Australia/Sydney`    |

Full list: <https://en.wikipedia.org/wiki/List_of_tz_database_time_zones>

---

## How Timezone Handling Works

The test uses Python's `pytz` library at **compile time** to:

1. Determine what "today" means in the specified timezone
2. Convert the SLA deadline to UTC
3. All SQL comparisons happen in UTC

This approach is **database-agnostic** - no database-specific timezone functions needed.

---

## Test Output

When the test fails, the result includes:

| Field                | Description                                      |
| -------------------- | ------------------------------------------------ |
| `target_date`        | The date being checked                           |
| `sla_time`           | The configured SLA time                          |
| `timezone`           | The configured timezone                          |
| `sla_deadline_utc`   | The SLA deadline in UTC                          |
| `sla_status`         | `MET_SLA`, `MISSED_SLA`, `NOT_RUN`, `ALL_FAILED` |
| `result_description` | Human-readable explanation                       |

**When the SLA deadline has not been reached yet:** The test passes and **no result row is returned**. This means if you run the test at 5:00 AM for a 7:00 AM SLA, the test will pass because there is still time to meet the deadline. Once the deadline passes, the test evaluates the actual run history and returns a row only if the SLA was missed.

---

## Notes

1. **DST Handling**: IANA timezone names automatically handle daylight saving time.

2. **Elementary Required**: Requires Elementary's `dbt_run_results` table to be populated.
