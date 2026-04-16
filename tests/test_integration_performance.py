"""
tests/test_integration_performance.py
Automated performance and integration tests for ProcureSpendIQ (req 9).

Run from the project root:
    python -m pytest tests/test_integration_performance.py -v

Or run the module directly for a quick CI-style report:
    python tests/test_integration_performance.py
"""

from __future__ import annotations

import logging
import time
from typing import List, Tuple

import pytest

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _timed(fn, *args, **kwargs) -> Tuple[float, object]:
    """Execute fn(*args, **kwargs) and return (elapsed_seconds, result)."""
    t0 = time.perf_counter()
    result = fn(*args, **kwargs)
    return time.perf_counter() - t0, result


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------

class TestDatabaseConnectivity:
    """Verify that the Fabric Lakehouse and Warehouse are reachable."""

    def test_lakehouse_connection(self):
        from db_service import test_connection
        assert test_connection(), "Lakehouse connection test failed."

    def test_warehouse_connection(self):
        from db_service import run_warehouse_df
        df = run_warehouse_df("SELECT 1 AS probe")
        assert not df.empty, "Warehouse connection returned empty result."

    def test_fact_invoice_view_accessible(self):
        from db_service import run_df
        df = run_df("SELECT TOP 1 INVOICE_AMOUNT_LOCAL FROM INFORMATION_MART.FACT_ALL_SOURCES_VW")
        assert not df.empty, "FACT_ALL_SOURCES_VW is not accessible."

    def test_dim_vendor_view_accessible(self):
        from db_service import run_df
        df = run_df("SELECT TOP 1 VENDOR_ID FROM INFORMATION_MART.DIM_VENDOR_VW")
        assert not df.empty, "DIM_VENDOR_VW is not accessible."


class TestCacheLayer:
    """Verify the query result cache round-trip."""

    def test_cache_write_and_read(self):
        import pandas as pd
        from db_service import cache_get, cache_set, cache_invalidate

        question = "__test_cache_probe_question__"
        sql      = "SELECT 1 AS probe"
        df       = pd.DataFrame({"probe": [1]})

        # Clean state
        cache_invalidate(question)

        # Write
        cache_set(question, sql, df)

        # Read
        hit = cache_get(question)
        assert hit is not None, "Cache miss immediately after cache_set."
        assert hit["sql"] == sql, "Cached SQL does not match."

        # Cleanup
        cache_invalidate(question)

    def test_cache_miss_for_unknown_question(self):
        from db_service import cache_get
        hit = cache_get("__definitely_not_in_cache_xyz_abc__")
        assert hit is None, "Unexpected cache hit for unknown question."


class TestAIConnectivity:
    """Verify that Azure OpenAI is reachable and produces non-empty output."""

    def test_sql_generation_returns_select(self):
        from llm_service_full import generate_sql
        sql = generate_sql("How many invoices were posted last month?")
        assert sql.strip().upper().startswith("SELECT"), (
            f"Generated SQL does not start with SELECT: {sql[:80]}"
        )

    def test_cortex_complete_returns_text(self):
        from llm_service_full import cortex_complete
        result = cortex_complete("Reply with the single word: pong")
        assert len(result.strip()) > 0, "cortex_complete returned empty string."

    def test_prescriptive_insights_returns_text(self):
        from llm_service_full import generate_prescriptive_insights
        summary  = "Top vendor ACME: $2.1M (35% of total)."
        insights = generate_prescriptive_insights(summary, "Who are the top vendors?")
        assert len(insights.strip()) > 20, "Prescriptive insights response is too short."


class TestSecurityValidation:
    """Validate SQL security rules."""

    def test_select_passes(self):
        from security import validate_sql
        assert validate_sql("SELECT 1")

    def test_drop_blocked(self):
        from security import validate_sql
        with pytest.raises(ValueError, match="Forbidden"):
            validate_sql("DROP TABLE dbo.foo")

    def test_non_select_blocked(self):
        from security import validate_sql
        with pytest.raises(ValueError, match="Only SELECT"):
            validate_sql("INSERT INTO dbo.foo VALUES (1)")

    def test_delete_blocked(self):
        from security import validate_sql
        with pytest.raises(ValueError):
            validate_sql("SELECT * FROM t; DELETE FROM t")


# ---------------------------------------------------------------------------
# Performance tests
# ---------------------------------------------------------------------------

class TestQueryPerformance:
    """Assert that key analytical queries finish within acceptable latency."""

    _MAX_LATENCY_S = 30  # 30-second ceiling

    def _assert_latency(self, fn, *args, label: str = "", ceiling: float = None):
        ceiling = ceiling or self._MAX_LATENCY_S
        elapsed, _ = _timed(fn, *args)
        assert elapsed < ceiling, (
            f"Query '{label}' took {elapsed:.2f}s, exceeding ceiling of {ceiling}s."
        )
        logger.info("Latency check '%s': %.3f s (limit: %.0f s)", label, elapsed, ceiling)

    def test_fact_invoice_count_latency(self):
        from db_service import run_df
        self._assert_latency(
            run_df,
            "SELECT COUNT(*) AS cnt FROM INFORMATION_MART.FACT_ALL_SOURCES_VW",
            label="fact_invoice_count",
        )

    def test_top_vendors_latency(self):
        from db_service import run_df
        sql = """
            SELECT TOP 10 VENDOR_ID, SUM(INVOICE_AMOUNT_LOCAL) AS spend
            FROM   INFORMATION_MART.FACT_ALL_SOURCES_VW
            WHERE  INVOICE_STATUS NOT IN ('CANCELLED', 'REJECTED')
            GROUP  BY VENDOR_ID
            ORDER  BY spend DESC
        """
        self._assert_latency(run_df, sql, label="top_vendors")

    def test_monthly_trend_latency(self):
        from db_service import run_df
        sql = """
            SELECT DATETRUNC(month, POSTING_DATE) AS month_start,
                   SUM(INVOICE_AMOUNT_LOCAL) AS spend
            FROM   INFORMATION_MART.FACT_ALL_SOURCES_VW
            WHERE  POSTING_DATE >= DATEADD(month, -6, GETDATE())
            GROUP  BY DATETRUNC(month, POSTING_DATE)
            ORDER  BY month_start
        """
        self._assert_latency(run_df, sql, label="monthly_trend")

    def test_cache_hit_faster_than_direct_query(self):
        """A cached result should be returned faster than a fresh query."""
        import pandas as pd
        from db_service import cache_get, cache_set, cache_invalidate, run_df

        question = "__perf_test_cache_vs_direct__"
        sql      = "SELECT TOP 100 * FROM INFORMATION_MART.FACT_ALL_SOURCES_VW"

        cache_invalidate(question)

        # Warm direct query
        elapsed_direct, df = _timed(run_df, sql)

        # Prime cache
        cache_set(question, sql, df)

        # Cache hit
        elapsed_cache, hit = _timed(cache_get, question)

        assert hit is not None, "Cache not populated after cache_set."
        logger.info(
            "Direct query: %.3f s | Cache hit: %.4f s", elapsed_direct, elapsed_cache
        )
        assert elapsed_cache < elapsed_direct, (
            "Cache hit was not faster than direct query."
        )

        cache_invalidate(question)


# ---------------------------------------------------------------------------
# Standalone runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    suites = [
        TestDatabaseConnectivity,
        TestCacheLayer,
        TestAIConnectivity,
        TestSecurityValidation,
        TestQueryPerformance,
    ]

    passed = failed = 0
    for suite_cls in suites:
        suite = suite_cls()
        for method_name in [m for m in dir(suite) if m.startswith("test_")]:
            label = f"{suite_cls.__name__}.{method_name}"
            try:
                getattr(suite, method_name)()
                print(f"  PASS  {label}")
                passed += 1
            except Exception as exc:
                print(f"  FAIL  {label}: {exc}")
                failed += 1

    print(f"\nResults: {passed} passed, {failed} failed out of {passed + failed} tests.")
    sys.exit(0 if failed == 0 else 1)
