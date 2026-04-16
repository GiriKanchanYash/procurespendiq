"""
data_validation.py - FINAL CORRECTED VERSION
Automated data validation tests for ProcureSpendIQ Analytics

KEY FIXES FROM LOG:
1. Table names are ALL LOWERCASE with underscores in Fabric
2. fact_all_sources_vw (correct) not information_mart.fact_all_sources_vw
3. Need to query Lakehouse (run_df), not warehouse
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, List, Optional

import pandas as pd

from config import Config
from db_service import run_df, run_warehouse_df, run_warehouse_non_query, sql_escape

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# TABLE NAMES - CORRECTED FROM LOGS
# These are the ACTUAL table names in your Lakehouse (all lowercase)
# ──────────────────────────────────────────────────────────────────────────────

# Note: Use just table name without schema - Lakehouse handles it
FACT_TABLE = "fact_all_sources_vw"          # ← CORRECT (lowercase, no schema prefix)
VENDOR_TABLE = "dim_vendor_vw"              # ← CORRECT (lowercase, no schema prefix)

# For warehouse reference
RESULT_TABLE = f"[{Config.WAREHOUSE_SCHEMA}].[DATA_VALIDATION_RESULTS]"

# ──────────────────────────────────────────────────────────────────────────────
# Result model
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class DataValidationResult:
    test_name: str
    table_name: str
    status: str            # PASS | FAIL | ERROR
    message: str
    actual_value: Optional[float] = None
    expected_value: Optional[float] = None
    run_at: datetime = field(default_factory=datetime.utcnow)


# ──────────────────────────────────────────────────────────────────────────────
# Persistence
# ──────────────────────────────────────────────────────────────────────────────

def _persist_result(result: DataValidationResult) -> bool:
    """
    Write a single validation result to the Warehouse.
    Returns True if successful, False otherwise.
    """
    try:
        sql = f"""
        INSERT INTO {RESULT_TABLE}
            (VALIDATION_ID, TEST_NAME, TABLE_NAME, STATUS, MESSAGE,
             ACTUAL_VALUE, EXPECTED_VALUE, RUN_AT)
        VALUES (
            ABS(CHECKSUM(NEWID())),
            '{sql_escape(result.test_name)}',
            '{sql_escape(result.table_name)}',
            '{sql_escape(result.status)}',
            '{sql_escape(result.message)}',
            {result.actual_value if result.actual_value is not None else 'NULL'},
            {result.expected_value if result.expected_value is not None else 'NULL'},
            CONVERT(VARCHAR(30), GETDATE(), 120)
        )
        """
        
        rows = run_warehouse_non_query(sql)
        logger.info(f"✓ Persisted: {result.test_name} - {result.status}")
        return True
        
    except Exception as exc:
        logger.error(f"✗ Failed to persist: {exc}")
        return False


# ──────────────────────────────────────────────────────────────────────────────
# Validator decorator
# ──────────────────────────────────────────────────────────────────────────────

_VALIDATORS: list[Callable[[], List[DataValidationResult]]] = []


def validator(fn: Callable) -> Callable:
    """Register a function as a data validator."""
    _VALIDATORS.append(fn)
    return fn


# ──────────────────────────────────────────────────────────────────────────────
# Individual validation tests
# ──────────────────────────────────────────────────────────────────────────────

@validator
def check_fact_invoices_not_empty() -> List[DataValidationResult]:
    """Assert that fact_all_sources_vw has at least one row."""
    try:
        # CORRECTED: No schema prefix, just table name
        sql = f"SELECT COUNT(*) AS cnt FROM {FACT_TABLE}"
        df = run_df(sql)
        count = int(df.iloc[0, 0] or 0)
        
        if count > 0:
            return [DataValidationResult(
                test_name="fact_invoices_not_empty",
                table_name=FACT_TABLE,
                status="PASS",
                message=f"Fact table has {count} rows",
                actual_value=float(count),
                expected_value=1.0
            )]
        else:
            return [DataValidationResult(
                test_name="fact_invoices_not_empty",
                table_name=FACT_TABLE,
                status="FAIL",
                message="Fact table is empty",
                actual_value=0.0,
                expected_value=1.0
            )]
    except Exception as exc:
        logger.error(f"Check failed: {exc}")
        return [DataValidationResult(
            test_name="fact_invoices_not_empty",
            table_name=FACT_TABLE,
            status="ERROR",
            message=f"Query failed: {str(exc)[:150]}"
        )]


@validator
def check_invoice_amounts_non_negative() -> List[DataValidationResult]:
    """Assert that no invoice amounts are negative."""
    try:
        sql = f"""
        SELECT COUNT(*) AS cnt
        FROM {FACT_TABLE}
        WHERE INVOICE_AMOUNT_LOCAL < 0
          AND INVOICE_STATUS NOT IN ('CANCELLED', 'REJECTED')
        """
        df = run_df(sql)
        count = int(df.iloc[0, 0] or 0)
        
        if count == 0:
            return [DataValidationResult(
                test_name="invoice_amounts_non_negative",
                table_name=FACT_TABLE,
                status="PASS",
                message="No negative invoice amounts found",
                actual_value=0.0,
                expected_value=0.0
            )]
        else:
            return [DataValidationResult(
                test_name="invoice_amounts_non_negative",
                table_name=FACT_TABLE,
                status="FAIL",
                message=f"Found {count} rows with negative amounts",
                actual_value=float(count),
                expected_value=0.0
            )]
    except Exception as exc:
        logger.error(f"Check failed: {exc}")
        return [DataValidationResult(
            test_name="invoice_amounts_non_negative",
            table_name=FACT_TABLE,
            status="ERROR",
            message=f"Query failed: {str(exc)[:150]}"
        )]


@validator
def check_vendor_referential_integrity() -> List[DataValidationResult]:
    """Assert that all VENDOR_IDs in fact table exist in vendor dimension."""
    try:
        sql = f"""
        SELECT COUNT(DISTINCT f.VENDOR_ID) AS orphan_count
        FROM {FACT_TABLE} f
        LEFT JOIN {VENDOR_TABLE} d ON f.VENDOR_ID = d.VENDOR_ID
        WHERE d.VENDOR_ID IS NULL
          AND f.VENDOR_ID IS NOT NULL
        """
        df = run_df(sql)
        count = int(df.iloc[0, 0] or 0)
        
        if count == 0:
            return [DataValidationResult(
                test_name="vendor_referential_integrity",
                table_name=FACT_TABLE,
                status="PASS",
                message="All vendor IDs have matching dimension records",
                actual_value=0.0,
                expected_value=0.0
            )]
        else:
            return [DataValidationResult(
                test_name="vendor_referential_integrity",
                table_name=FACT_TABLE,
                status="FAIL",
                message=f"Found {count} orphan vendor IDs",
                actual_value=float(count),
                expected_value=0.0
            )]
    except Exception as exc:
        logger.error(f"Check failed: {exc}")
        return [DataValidationResult(
            test_name="vendor_referential_integrity",
            table_name=FACT_TABLE,
            status="ERROR",
            message=f"Query failed: {str(exc)[:150]}"
        )]


@validator
def check_no_null_posting_dates() -> List[DataValidationResult]:
    """Assert that POSTING_DATE is not null."""
    try:
        sql = f"""
        SELECT COUNT(*) AS cnt
        FROM {FACT_TABLE}
        WHERE POSTING_DATE IS NULL
        """
        df = run_df(sql)
        count = int(df.iloc[0, 0] or 0)
        
        if count == 0:
            return [DataValidationResult(
                test_name="no_null_posting_dates",
                table_name=FACT_TABLE,
                status="PASS",
                message="No null posting dates found",
                actual_value=0.0,
                expected_value=0.0
            )]
        else:
            return [DataValidationResult(
                test_name="no_null_posting_dates",
                table_name=FACT_TABLE,
                status="FAIL",
                message=f"Found {count} rows with null posting dates",
                actual_value=float(count),
                expected_value=0.0
            )]
    except Exception as exc:
        logger.error(f"Check failed: {exc}")
        return [DataValidationResult(
            test_name="no_null_posting_dates",
            table_name=FACT_TABLE,
            status="ERROR",
            message=f"Query failed: {str(exc)[:150]}"
        )]


@validator
def check_invoice_status_valid_values() -> List[DataValidationResult]:
    """Assert that INVOICE_STATUS contains only valid values."""
    valid_statuses = ('OPEN', 'PAID', 'OVERDUE', 'DISPUTED', 'DUE', 'CANCELLED', 'REJECTED', 'BLOCKED', 'CLEARED')
    try:
        status_list = "','".join(valid_statuses)
        sql = f"""
        SELECT COUNT(*) AS cnt
        FROM {FACT_TABLE}
        WHERE UPPER(INVOICE_STATUS) NOT IN ('{status_list}')
          AND INVOICE_STATUS IS NOT NULL
        """
        df = run_df(sql)
        count = int(df.iloc[0, 0] or 0)
        
        if count == 0:
            return [DataValidationResult(
                test_name="invoice_status_valid_values",
                table_name=FACT_TABLE,
                status="PASS",
                message="All invoice statuses are valid",
                actual_value=0.0,
                expected_value=0.0
            )]
        else:
            return [DataValidationResult(
                test_name="invoice_status_valid_values",
                table_name=FACT_TABLE,
                status="FAIL",
                message=f"Found {count} rows with invalid invoice status",
                actual_value=float(count),
                expected_value=0.0
            )]
    except Exception as exc:
        logger.error(f"Check failed: {exc}")
        return [DataValidationResult(
            test_name="invoice_status_valid_values",
            table_name=FACT_TABLE,
            status="ERROR",
            message=f"Query failed: {str(exc)[:150]}"
        )]


@validator
def check_cache_table_accessible() -> List[DataValidationResult]:
    """Assert that QUERY_RESULT_CACHE table is accessible."""
    try:
        sql = f"SELECT TOP 1 CACHE_KEY FROM [{Config.WAREHOUSE_SCHEMA}].[QUERY_RESULT_CACHE]"
        df = run_warehouse_df(sql)
        
        return [DataValidationResult(
            test_name="cache_table_accessible",
            table_name=f"[{Config.WAREHOUSE_SCHEMA}].[QUERY_RESULT_CACHE]",
            status="PASS",
            message="Cache table is accessible"
        )]
    except Exception as exc:
        return [DataValidationResult(
            test_name="cache_table_accessible",
            table_name=f"[{Config.WAREHOUSE_SCHEMA}].[QUERY_RESULT_CACHE]",
            status="FAIL",
            message=f"Cache table not accessible: {str(exc)[:150]}"
        )]


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

def run_all_validations(persist: bool = True) -> List[DataValidationResult]:
    """
    Run all registered validators and optionally persist results.
    
    Args:
        persist: If True, save results to warehouse (default: True)
    
    Returns:
        List of DataValidationResult objects
    """
    results = []
    
    logger.info("Starting validation run...")
    
    # Run all validators
    for validator_fn in _VALIDATORS:
        try:
            validator_results = validator_fn()
            results.extend(validator_results)
        except Exception as exc:
            logger.error(f"Validator {validator_fn.__name__} failed: {exc}")
    
    # Persist if requested
    if persist:
        logger.info(f"Persisting {len(results)} validation results...")
        persisted = 0
        for result in results:
            if _persist_result(result):
                persisted += 1
        logger.info(f"✓ Persisted {persisted}/{len(results)} results")
    
    # Summary
    passed = sum(1 for r in results if r.status == "PASS")
    failed = sum(1 for r in results if r.status == "FAIL")
    errors = sum(1 for r in results if r.status == "ERROR")
    
    logger.info(f"Validation complete: {passed} passed, {failed} failed, {errors} errors")
    
    return results


def get_validation_summary(results: List[DataValidationResult]) -> pd.DataFrame:
    """Convert validation results to a summary DataFrame."""
    data = []
    for result in results:
        data.append({
            'Test': result.test_name,
            'Table': result.table_name,
            'Status': result.status,
            'Message': result.message,
            'Actual': result.actual_value,
            'Expected': result.expected_value,
            'Run At': result.run_at
        })
    return pd.DataFrame(data)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    print("\nRunning data validation...")
    results = run_all_validations(persist=True)
    print("\nResults:")
    print(get_validation_summary(results).to_string())
