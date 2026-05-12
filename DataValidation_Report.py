"""
=============================================================================
Data Vault Validation Runner + HTML Report Generator
=============================================================================
Reads all connection config from a .env file.
Usage:
  pip install pyodbc python-dotenv azure-identity
  python DataValidation.py
=============================================================================
"""

import os
import pyodbc
from decimal import Decimal
from datetime import datetime
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# =============================================================================
# ▶▶▶  CHANGE ONLY THIS  ◀◀◀
# =============================================================================
DB_NAME = "PROCURE2PAY"     # e.g. "DEALER" | "ORDERLENS" | "PROCURE2PAY"
# =============================================================================

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION — loaded from .env
# ─────────────────────────────────────────────────────────────────────────────
FABRIC_CONFIG = {
    "server":          os.getenv("FABRIC_SQL_SERVER"),
    "mart_server":     os.getenv("FABRIC_WAREHOUSE_SERVER"),
    "database":        os.getenv("FABRIC_DATABASE",           "ProcureSpendIQ"),
    "mart_database":   os.getenv("FABRIC_WAREHOUSE_DATABASE", "DW_ProcureSpendIQ"),
    "raw_schema":      "raw_layer",
    "mart_schema":     "dbo",
    "driver":          "ODBC Driver 18 for SQL Server",
    "tenant_id":       os.getenv("AZURE_TENANT_ID"),
    "client_id":       os.getenv("AZURE_CLIENT_ID"),
    "client_secret":   os.getenv("AZURE_CLIENT_SECRET"),
}

# ─────────────────────────────────────────────────────────────────────────────
# CONNECTION HELPER — Service Principal via azure-identity token
# ─────────────────────────────────────────────────────────────────────────────
def get_connection(server: str, database: str):
    """
    Returns a pyodbc connection using an Azure AD service-principal token.
    No username/password needed — token is injected as the access-token attribute.
    """
    from azure.identity import ClientSecretCredential
    import struct

    credential = ClientSecretCredential(
        tenant_id=FABRIC_CONFIG["tenant_id"],
        client_id=FABRIC_CONFIG["client_id"],
        client_secret=FABRIC_CONFIG["client_secret"],
    )
    # Fabric / Azure SQL requires this specific scope
    token = credential.get_token("https://database.windows.net/.default").token

    # pyodbc expects the token packed as UTF-16-LE bytes with a length prefix
    token_bytes = token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

    conn_str = (
        f"Driver={{{FABRIC_CONFIG['driver']}}};"
        f"Server={server},1433;"
        f"Database={database};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )
    conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
    return conn


# ─────────────────────────────────────────────────────────────────────────────
# DATABASE PROFILES  (unchanged from your original — omitted for brevity)
# ─────────────────────────────────────────────────────────────────────────────
DB_PROFILES = {

    "DEALER": {
        "display_name": "DealerPulse",
        "validity_views": [
            ("VW_GROSS_PROFIT_MARGIN", [
                {"description": "Gross margin > 100% (impossible)",
                 "impact": "Margin above 100% is mathematically impossible — calculation bug or bad source data.",
                 "column": "GROSS_PROFIT_MARGIN_PCT", "operator": ">", "threshold": 100, "severity": "ERROR"},
                {"description": "Gross margin < -80% (extreme negative)",
                 "impact": "Extreme negative margins suggest a sign error in revenue or cost data.",
                 "column": "GROSS_PROFIT_MARGIN_PCT", "operator": "<", "threshold": -80, "severity": "WARNING"},
                {"description": "Negative total revenue",
                 "impact": "Negative revenue breaks ranking, growth%, and Sales KPI cards.",
                 "column": "TOTAL_REVENUE", "operator": "<", "threshold": 0, "severity": "ERROR"},
            ]),
            ("VW_ORDER_LEAD_TIME", [
                {"description": "Lead time > 365 days",
                 "impact": "Lead time over a year is a data error — inflates the Lead Time KPI.",
                 "column": "AVG_ORDER_LEAD_TIME_DAYS", "operator": ">", "threshold": 365, "severity": "WARNING"},
                {"description": "Negative lead time (delivery before order)",
                 "impact": "Negative lead time = delivery before order date — timestamp error.",
                 "column": "AVG_ORDER_LEAD_TIME_DAYS", "operator": "<", "threshold": 0, "severity": "ERROR"},
            ]),
            ("VW_AVERAGE_REPAIR_TURNAROUND_TIME", [
                {"description": "Repair TAT > 8760h (>1 year, impossible)",
                 "impact": "TAT over 8760h is bad data — massively inflates the Repair TAT KPI.",
                 "column": "AVG_TURNAROUND_HOURS", "operator": ">", "threshold": 8760, "severity": "WARNING"},
                {"description": "Negative repair TAT",
                 "impact": "Negative TAT = repair completed before started — timestamp/timezone error.",
                 "column": "AVG_TURNAROUND_HOURS", "operator": "<", "threshold": 0, "severity": "ERROR"},
            ]),
            ("VW_STOCK_AVAILABILITY_DEALER", [
                {"description": "Stock availability > 100%",
                 "impact": "Stock availability above 100% is a calculation error — hides real inventory gaps.",
                 "column": "STOCK_AVAILABILITY_PCT", "operator": ">", "threshold": 100, "severity": "ERROR"},
            ]),
            ("VW_CASH_CONVERSION_CYCLE", [
                {"description": "CCC > 365 days",
                 "impact": "CCC over a year flags bad data — inflates working capital KPI.",
                 "column": "CCC", "operator": ">", "threshold": 365, "severity": "WARNING"},
            ]),
            ("VW_DEALER_REVENUE_GROWTH", [
                {"description": "Revenue growth > 1000% (likely duplicate load)",
                 "impact": "Growth above 1000% almost certainly indicates a duplicate load or bad baseline.",
                 "column": "REVENUE_GROWTH_PCT", "operator": ">", "threshold": 1000, "severity": "WARNING"},
            ]),
        ],
        "coverage_views": [
            ("VW_GROSS_PROFIT_MARGIN",            "Gross Profit",       "These dealers show blank/zero in the Gross Profit KPI card.",               "DEALER_NAME", "HUB_DEALER"),
            ("VW_ORDER_LEAD_TIME",                "Lead Time",          "Dealers excluded from Lead Time KPI average.",                              "DEALER_NAME", "HUB_DEALER"),
            ("VW_STOCK_AVAILABILITY_DEALER",      "Stock Availability", "Dealers never appear in replenishment agent recommendations.",            "DEALER_NAME", "HUB_DEALER"),
            ("VW_CASH_CONVERSION_CYCLE",          "CCC",                "Dealers show default value — distorts the CCC KPI.",                        "DEALER_NAME", "HUB_DEALER"),
            ("VW_DEALER_REVENUE_GROWTH",          "Revenue Growth",     "Dealers show stale or zero growth %.",                                      "DEALER_NAME", "HUB_DEALER"),
            ("VW_AVERAGE_REPAIR_TURNAROUND_TIME", "Repair TAT",         "Dealers excluded from Repair TAT average.",                                 "DEALER_NAME", "HUB_DEALER"),
            ("VW_TRANSACTION_LINEAGE",            "Transactions",       "Dealer present in vault but missing from VW_TRANSACTION_LINEAGE.",           "DEALER_NAME", "HUB_DEALER"),
        ],
        "recon_checks": [
            {"view": "VW_GROSS_PROFIT_MARGIN",   "description": "Dealer count in mart vs HUB_DEALER mismatch",
             "impact": "Transformation silently dropping dealers — their data exists in vault but never reaches dashboard.",
             "sql_template": "ABS((SELECT COUNT(DISTINCT DEALER_NAME) FROM {mart_view}) - (SELECT COUNT(*) FROM {hub}))",
             "hub": "HUB_DEALER", "severity": "WARNING"},
            {"view": "VW_TRANSACTION_LINEAGE",   "description": "Duplicate TRANSACTION_ID in lineage view",
             "impact": "Duplicate IDs show the same order twice — doubles revenue figures.",
             "sql_template": "COUNT(*) FROM (SELECT TRANSACTION_ID FROM {mart_view} GROUP BY TRANSACTION_ID HAVING COUNT(*) > 1)",
             "hub": None, "severity": "ERROR"},
            {"view": "VW_DEALER_REVENUE_GROWTH", "description": "NULL dealer name in revenue growth view",
             "impact": "NULL dealer names contribute to KPI averages but can't be attributed to any dealer.",
             "sql_template": "COUNT(*) FROM {mart_view} WHERE DEALER_NAME IS NULL",
             "hub": None, "severity": "ERROR"},
            {"view": "VW_GROSS_PROFIT_MARGIN",   "description": "Periods with zero revenue across ALL dealers",
             "impact": "Entire month with zero revenue = bulk ETL failure for that period.",
             "sql_template": "COUNT(*) FROM (SELECT PERIOD_YEAR,PERIOD_MONTH FROM {mart_view} GROUP BY PERIOD_YEAR,PERIOD_MONTH HAVING SUM(TOTAL_REVENUE)=0 OR SUM(TOTAL_REVENUE) IS NULL)",
             "hub": None, "severity": "WARNING"},
            {"view": "VW_TRANSACTION_LINEAGE",   "description": "Orders: DELIVERY_FLAG=Y but NULL delivery date",
             "impact": "Flagged as delivered but no delivery date — breaks lead time calculations.",
             "sql_template": "COUNT(*) FROM {mart_view} WHERE DELIVERY_FLAG='Y' AND DELIVERY_DATE IS NULL",
             "hub": None, "severity": "ERROR"},
            {"view": "VW_TRANSACTION_LINEAGE",   "description": "Orders: PAID_FLAG=Y but NULL payment date",
             "impact": "Flagged as paid but no payment date — breaks CCC calculations.",
             "sql_template": "COUNT(*) FROM {mart_view} WHERE PAID_FLAG='Y' AND PAYMENT_DATE IS NULL",
             "hub": None, "severity": "ERROR"},
            {"view": "VW_TRANSACTION_LINEAGE",   "description": "Delivery date before order date",
             "impact": "Delivery cannot happen before order was placed — timestamp or join error.",
             "sql_template": "COUNT(*) FROM {mart_view} WHERE DELIVERY_DATE < ORDER_DATE",
             "hub": None, "severity": "ERROR"},
            {"view": "VW_DEALER_JOURNEY_COUNTS", "description": "Journey totals vs lineage transaction count mismatch",
             "impact": "If VW_DEALER_JOURNEY_COUNTS differs from VW_TRANSACTION_LINEAGE, aggregation has an error.",
             "sql_template": "ABS((SELECT SUM(TOTAL_ORDERS) FROM {mart_view}) - (SELECT COUNT(DISTINCT TRANSACTION_ID) FROM {mart2}))",
             "hub": None, "mart2": "VW_TRANSACTION_LINEAGE", "severity": "WARNING"},
        ],
    },

    "ORDERLENS": {
        "display_name": "OrderLens",
        "validity_views": [
            ("VW_ORDER_FULFILLMENT_RATE", [
                {"description": "Fulfillment rate > 100%",
                 "impact": "Fulfillment above 100% is impossible — calculation or join error.",
                 "column": "FULFILLMENT_RATE_PCT", "operator": ">", "threshold": 100, "severity": "ERROR"},
                {"description": "Negative fulfillment rate",
                 "impact": "Negative fulfillment rate = bad data — breaks SLA KPI cards.",
                 "column": "FULFILLMENT_RATE_PCT", "operator": "<", "threshold": 0, "severity": "ERROR"},
            ]),
            ("VW_ORDER_CYCLE_TIME", [
                {"description": "Cycle time > 365 days",
                 "impact": "Cycle time over a year is a data error — inflates the Order Cycle KPI.",
                 "column": "AVG_CYCLE_TIME_DAYS", "operator": ">", "threshold": 365, "severity": "WARNING"},
                {"description": "Negative cycle time",
                 "impact": "Negative cycle time = order completed before placed — timestamp error.",
                 "column": "AVG_CYCLE_TIME_DAYS", "operator": "<", "threshold": 0, "severity": "ERROR"},
            ]),
            ("VW_ORDER_VALUE_SUMMARY", [
                {"description": "Negative total order value",
                 "impact": "Negative order value breaks revenue KPI aggregations.",
                 "column": "TOTAL_ORDER_VALUE", "operator": "<", "threshold": 0, "severity": "ERROR"},
                {"description": "Order value > 10,000,000 (likely bad data)",
                 "impact": "Extreme order values skew averages and revenue KPIs.",
                 "column": "TOTAL_ORDER_VALUE", "operator": ">", "threshold": 10000000, "severity": "WARNING"},
            ]),
            ("VW_BACKORDER_RATE", [
                {"description": "Backorder rate > 100%",
                 "impact": "Backorder rate above 100% is impossible — signals a data bug.",
                 "column": "BACKORDER_RATE_PCT", "operator": ">", "threshold": 100, "severity": "ERROR"},
            ]),
        ],
        "coverage_views": [
            ("VW_ORDER_FULFILLMENT_RATE", "Fulfillment Rate", "Orders missing fulfillment data skew SLA metrics.",   "ORDER_ID", "HUB_ORDER"),
            ("VW_ORDER_CYCLE_TIME",       "Cycle Time",       "Orders excluded from cycle time average.",              "ORDER_ID", "HUB_ORDER"),
            ("VW_ORDER_VALUE_SUMMARY",    "Order Value",      "Orders with no value data invisible in revenue KPIs.", "ORDER_ID", "HUB_ORDER"),
            ("VW_BACKORDER_RATE",         "Backorder Rate",   "Orders not appearing in backorder tracking.",           "ORDER_ID", "HUB_ORDER"),
        ],
        "recon_checks": [
            {"view": "VW_ORDER_VALUE_SUMMARY",    "description": "Order count in mart vs HUB_ORDER mismatch",
             "impact": "Transformation silently dropping orders — data exists in vault but not in dashboard.",
             "sql_template": "ABS((SELECT COUNT(DISTINCT ORDER_ID) FROM {mart_view}) - (SELECT COUNT(*) FROM {hub}))",
             "hub": "HUB_ORDER", "severity": "WARNING"},
            {"view": "VW_ORDER_VALUE_SUMMARY",    "description": "NULL ORDER_ID in value summary",
             "impact": "NULL order IDs contribute to totals but can't be traced to real orders.",
             "sql_template": "COUNT(*) FROM {mart_view} WHERE ORDER_ID IS NULL",
             "hub": None, "severity": "ERROR"},
            {"view": "VW_ORDER_FULFILLMENT_RATE", "description": "Periods with zero fulfilled orders",
             "impact": "Entire period with zero fulfillments = likely ETL failure.",
             "sql_template": "COUNT(*) FROM (SELECT PERIOD_YEAR,PERIOD_MONTH FROM {mart_view} GROUP BY PERIOD_YEAR,PERIOD_MONTH HAVING SUM(FULFILLED_COUNT)=0 OR SUM(FULFILLED_COUNT) IS NULL)",
             "hub": None, "severity": "WARNING"},
        ],
    },

    "PROCURE2PAY": {
        "display_name": "Procure2Pay",
        "validity_views": [
            ("VW_INVOICE_ACCURACY", [
                {"description": "Invoice accuracy > 100%",
                 "impact": "Accuracy above 100% is mathematically impossible — signals a calculation error.",
                 "column": "INVOICE_ACCURACY_PCT", "operator": ">", "threshold": 100, "severity": "ERROR"},
                {"description": "Negative invoice accuracy",
                 "impact": "Negative accuracy breaks the Invoice Accuracy KPI card.",
                 "column": "INVOICE_ACCURACY_PCT", "operator": "<", "threshold": 0, "severity": "ERROR"},
            ]),
            ("VW_PAYMENT_CYCLE_TIME", [
                {"description": "Payment cycle > 365 days",
                 "impact": "Payment cycle over a year is a data error — inflates cash flow KPI.",
                 "column": "AVG_PAYMENT_DAYS", "operator": ">", "threshold": 365, "severity": "WARNING"},
                {"description": "Negative payment cycle",
                 "impact": "Payment before invoice date — timestamp or join error.",
                 "column": "AVG_PAYMENT_DAYS", "operator": "<", "threshold": 0, "severity": "ERROR"},
            ]),
            ("VW_PURCHASE_ORDER_VALUE", [
                {"description": "Negative PO value",
                 "impact": "Negative purchase order value breaks spend analytics.",
                 "column": "TOTAL_PO_VALUE", "operator": "<", "threshold": 0, "severity": "ERROR"},
                {"description": "PO value > 50,000,000 (likely bad data)",
                 "impact": "Extreme PO values skew spend averages and budget KPIs.",
                 "column": "TOTAL_PO_VALUE", "operator": ">", "threshold": 50000000, "severity": "WARNING"},
            ]),
            ("VW_SUPPLIER_COMPLIANCE_RATE", [
                {"description": "Compliance rate > 100%",
                 "impact": "Compliance above 100% is impossible — signals a calculation bug.",
                 "column": "COMPLIANCE_RATE_PCT", "operator": ">", "threshold": 100, "severity": "ERROR"},
                {"description": "Negative compliance rate",
                 "impact": "Negative compliance rate is invalid data.",
                 "column": "COMPLIANCE_RATE_PCT", "operator": "<", "threshold": 0, "severity": "ERROR"},
            ]),
            ("VW_SPEND_ANALYSIS", [
                {"description": "Spend growth > 1000% (likely duplicate load)",
                 "impact": "Growth above 1000% almost certainly indicates a duplicate load or bad baseline.",
                 "column": "SPEND_GROWTH_PCT", "operator": ">", "threshold": 1000, "severity": "WARNING"},
            ]),
        ],
        "coverage_views": [
            ("VW_INVOICE_ACCURACY",         "Invoice Accuracy", "Suppliers missing from invoice accuracy KPI.",   "SUPPLIER_NAME", "HUB_SUPPLIER"),
            ("VW_PAYMENT_CYCLE_TIME",       "Payment Cycle",    "Suppliers excluded from payment cycle average.", "SUPPLIER_NAME", "HUB_SUPPLIER"),
            ("VW_PURCHASE_ORDER_VALUE",     "PO Value",         "Suppliers not appearing in spend analytics.",    "SUPPLIER_NAME", "HUB_SUPPLIER"),
            ("VW_SUPPLIER_COMPLIANCE_RATE", "Compliance Rate",  "Suppliers invisible in compliance KPI cards.",   "SUPPLIER_NAME", "HUB_SUPPLIER"),
            ("VW_SPEND_ANALYSIS",           "Spend Analysis",   "Suppliers show stale or zero spend data.",       "SUPPLIER_NAME", "HUB_SUPPLIER"),
        ],
        "recon_checks": [
            {"view": "VW_INVOICE_ACCURACY",    "description": "Supplier count in mart vs HUB_SUPPLIER mismatch",
             "impact": "Transformation silently dropping suppliers — data exists in vault but not in dashboard.",
             "sql_template": "ABS((SELECT COUNT(DISTINCT SUPPLIER_NAME) FROM {mart_view}) - (SELECT COUNT(*) FROM {hub}))",
             "hub": "HUB_SUPPLIER", "severity": "WARNING"},
            {"view": "VW_PURCHASE_ORDER_VALUE","description": "NULL SUPPLIER_NAME in PO value view",
             "impact": "NULL supplier names contribute to totals but can't be attributed.",
             "sql_template": "COUNT(*) FROM {mart_view} WHERE SUPPLIER_NAME IS NULL",
             "hub": None, "severity": "ERROR"},
            {"view": "VW_INVOICE_ACCURACY",    "description": "Periods with zero invoices across ALL suppliers",
             "impact": "Entire period with zero invoices = bulk ETL failure for that period.",
             "sql_template": "COUNT(*) FROM (SELECT PERIOD_YEAR,PERIOD_MONTH FROM {mart_view} GROUP BY PERIOD_YEAR,PERIOD_MONTH HAVING SUM(INVOICE_COUNT)=0 OR SUM(INVOICE_COUNT) IS NULL)",
             "hub": None, "severity": "WARNING"},
            {"view": "VW_PAYMENT_CYCLE_TIME",  "description": "Payment date before invoice date",
             "impact": "Payment cannot precede invoice — timestamp or join error.",
             "sql_template": "COUNT(*) FROM {mart_view} WHERE PAYMENT_DATE < INVOICE_DATE",
             "hub": None, "severity": "ERROR"},
            {"view": "VW_INVOICE_ACCURACY",    "description": "APPROVED_FLAG=Y but NULL approval date",
             "impact": "Approved invoices without approval date break audit trail.",
             "sql_template": "COUNT(*) FROM {mart_view} WHERE APPROVED_FLAG='Y' AND APPROVAL_DATE IS NULL",
             "hub": None, "severity": "ERROR"},
        ],
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# Data Vault column conventions & shared thresholds
# ─────────────────────────────────────────────────────────────────────────────
HUB_PREFIXES   = ("HUB_",)
LINK_PREFIXES  = ("LNK_", "LINK_")
SAT_PREFIXES   = ("SAT_",)
LOAD_DTS_COL   = "LOAD_DTS"
RECORD_SRC_COL = "RECORD_SOURCE"
HASHDIFF_NAMES = ("HASHDIFF", "HASH_DIFF", "HASH_DIFF_KEY")

STALE_HOURS       = 48
VOLUME_SPIKE_MULT = 5

REPORT_FILE = f"dv_validation_report_{DB_NAME.lower()}.html"

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def starts_with_any(n, pfx): return any(n.upper().startswith(p.upper()) for p in pfx)
def is_hub(n):  return starts_with_any(n, HUB_PREFIXES)
def is_link(n): return starts_with_any(n, LINK_PREFIXES)
def is_sat(n):  return starts_with_any(n, SAT_PREFIXES)
def get_hk(c):  return next((x for x in c if x.upper().endswith("_HK")), None)
def get_bk(c):  return next((x for x in c if x.upper().endswith("_BK")), None)
def get_hd(c):  return next((x for x in c if x.upper() in HASHDIFF_NAMES), None)
def get_ahk(c): return [x for x in c if x.upper().endswith("_HK")]

def raw(t):
    return f"{FABRIC_CONFIG['database']}.{FABRIC_CONFIG['raw_schema']}.{t}"

def mart(t):
    return f"{FABRIC_CONFIG['mart_database']}.{FABRIC_CONFIG['mart_schema']}.{t}"

def get_profile():
    return DB_PROFILES.get(DB_NAME, {
        "display_name": DB_NAME,
        "validity_views": [],
        "coverage_views": [],
        "recon_checks":   [],
    })

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — FETCH METADATA
# ─────────────────────────────────────────────────────────────────────────────
def fetch_metadata(cursor):
    db  = FABRIC_CONFIG["database"]
    sch = FABRIC_CONFIG["raw_schema"]
    cursor.execute(f"""
        SELECT TABLE_NAME FROM {db}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA='{sch}' AND TABLE_TYPE='BASE TABLE'
    """)
    table_names = [row[0] for row in cursor.fetchall()]
    print(f"  Found {len(table_names)} tables in raw_layer")
    tables = {}
    for name in table_names:
        try:
            cursor.execute(f"""
                SELECT COLUMN_NAME FROM {db}.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA='{sch}' AND TABLE_NAME='{name}'
            """)
            tables[name] = [r[0] for r in cursor.fetchall()]
        except Exception as e:
            print(f"  Could not describe {name}: {e}")
    return tables

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — BUILD ALL CHECKS  (identical logic to original)
# ─────────────────────────────────────────────────────────────────────────────
def build_checks(tables):
    profile = get_profile()
    checks  = []

    def add(category, table, description, impact, sql,
            severity="ERROR", check_type="count_zero"):
        checks.append({
            "category": category, "table": table,
            "description": description, "business_impact": impact,
            "sql": sql, "severity": severity, "check_type": check_type,
        })

    # 1. TECHNICAL
    for table, cols in tables.items():
        hk = get_hk(cols); bk = get_bk(cols)
        hd = get_hd(cols); ahk = get_ahk(cols)
        t  = raw(table)

        if is_hub(table):
            if bk:
                add("Technical", table, f"NULL business key ({bk})",
                    "Entities without a BK cannot be identified — ghost records in every downstream KPI.",
                    f"SELECT COUNT(*) AS v FROM {t} WHERE {bk} IS NULL")
                add("Technical", table, f"Duplicate business key ({bk})",
                    "Duplicate BKs cause double-counting in revenue, margin, and all aggregated metrics.",
                    f"SELECT COUNT(*) AS v FROM (SELECT {bk} FROM {t} GROUP BY {bk} HAVING COUNT(*) > 1) x")
            if hk:
                add("Technical", table, f"NULL hash key ({hk})",
                    "NULL hash keys break all joins to satellites and links — entity disappears from dashboard.",
                    f"SELECT COUNT(*) AS v FROM {t} WHERE {hk} IS NULL")
                add("Technical", table, f"Duplicate hash key ({hk})",
                    "Hash key collisions cause incorrect joins and can swap data between records.",
                    f"SELECT COUNT(*) AS v FROM (SELECT {hk} FROM {t} GROUP BY {hk} HAVING COUNT(*) > 1) x")
            if LOAD_DTS_COL in cols:
                add("Technical", table, "NULL load date",
                    "Records without a load timestamp cannot be audited or version-tracked.",
                    f"SELECT COUNT(*) AS v FROM {t} WHERE {LOAD_DTS_COL} IS NULL")
                add("Technical", table, "Future-dated records",
                    "Future-dated loads suggest a timezone or ETL clock issue.",
                    f"SELECT COUNT(*) AS v FROM {t} WHERE {LOAD_DTS_COL} > GETUTCDATE()")
            if RECORD_SRC_COL in cols:
                add("Technical", table, "NULL record source",
                    "Without a source tag, data lineage is broken.",
                    f"SELECT COUNT(*) AS v FROM {t} WHERE {RECORD_SRC_COL} IS NULL")
            add("Technical", table, "Total row count", "Baseline.",
                f"SELECT COUNT(*) AS v FROM {t}", severity="INFO", check_type="info")

        elif is_link(table):
            if LOAD_DTS_COL in cols:
                add("Technical", table, "NULL load date",
                    "Undated relationship records cannot be tracked historically.",
                    f"SELECT COUNT(*) AS v FROM {t} WHERE {LOAD_DTS_COL} IS NULL")
                add("Technical", table, "Future-dated records",
                    "Future-dated loads suggest an ETL clock issue.",
                    f"SELECT COUNT(*) AS v FROM {t} WHERE {LOAD_DTS_COL} > GETUTCDATE()")
            if RECORD_SRC_COL in cols:
                add("Technical", table, "NULL record source",
                    "Without a source tag, data lineage is broken.",
                    f"SELECT COUNT(*) AS v FROM {t} WHERE {RECORD_SRC_COL} IS NULL")
            for hk_c in ahk:
                add("Technical", table, f"NULL hub key ({hk_c})",
                    "A NULL foreign key in a link means a dangling relationship.",
                    f"SELECT COUNT(*) AS v FROM {t} WHERE {hk_c} IS NULL")
                parent = next((tb for tb in tables if is_hub(tb) and hk_c in tables[tb]), None)
                if parent:
                    add("Technical", table, f"Orphan records — {hk_c} not in {parent}",
                        "Link records with no matching hub break downstream joins.",
                        f"SELECT COUNT(*) AS v FROM {t} l "
                        f"LEFT JOIN {raw(parent)} h ON l.{hk_c}=h.{hk_c} WHERE h.{hk_c} IS NULL")
            if ahk:
                hk_list = ", ".join(ahk)
                add("Technical", table, "Duplicate HK combination",
                    "Duplicate link entries double-count relationships.",
                    f"SELECT COUNT(*) AS v FROM (SELECT {hk_list} FROM {t} "
                    f"GROUP BY {hk_list} HAVING COUNT(*) > 1) x")
            add("Technical", table, "Total row count", "Baseline.",
                f"SELECT COUNT(*) AS v FROM {t}", severity="INFO", check_type="info")

        elif is_sat(table):
            if LOAD_DTS_COL in cols:
                add("Technical", table, "NULL load date",
                    "Records without a load timestamp cannot be audited.",
                    f"SELECT COUNT(*) AS v FROM {t} WHERE {LOAD_DTS_COL} IS NULL")
                add("Technical", table, "Future-dated records",
                    "Future-dated loads suggest an ETL clock issue.",
                    f"SELECT COUNT(*) AS v FROM {t} WHERE {LOAD_DTS_COL} > GETUTCDATE()")
            if RECORD_SRC_COL in cols:
                add("Technical", table, "NULL record source",
                    "Without a source tag, lineage is broken.",
                    f"SELECT COUNT(*) AS v FROM {t} WHERE {RECORD_SRC_COL} IS NULL")
            if hk:
                add("Technical", table, f"NULL parent hash key ({hk})",
                    "NULL HK breaks all joins — satellite data becomes completely unreachable.",
                    f"SELECT COUNT(*) AS v FROM {t} WHERE {hk} IS NULL")
                if LOAD_DTS_COL in cols:
                    add("Technical", table, "Duplicate HK + LOAD_DTS",
                        "Identical snapshots waste storage and distort change-detection.",
                        f"SELECT COUNT(*) AS v FROM (SELECT {hk},{LOAD_DTS_COL} FROM {t} "
                        f"GROUP BY {hk},{LOAD_DTS_COL} HAVING COUNT(*) > 1) x")
                parent = next((tb for tb in tables
                               if (is_hub(tb) or is_link(tb)) and hk in tables[tb]), None)
                if parent:
                    add("Technical", table, f"Orphan records — {hk} not in {parent}",
                        "Satellite rows without a parent are invisible in all joins.",
                        f"SELECT COUNT(*) AS v FROM {t} s "
                        f"LEFT JOIN {raw(parent)} p ON s.{hk}=p.{hk} WHERE p.{hk} IS NULL")
            if hd:
                add("Technical", table, f"NULL hashdiff ({hd})",
                    "NULL hashdiff disables change detection — every reload treated as new version.",
                    f"SELECT COUNT(*) AS v FROM {t} WHERE {hd} IS NULL")
                if hk:
                    add("Technical", table, "Duplicate HK + hashdiff",
                        "Duplicate hashdiff for same HK indicates a pipeline retry failure.",
                        f"SELECT COUNT(*) AS v FROM (SELECT {hk},{hd} FROM {t} "
                        f"GROUP BY {hk},{hd} HAVING COUNT(*) > 1) x")
            add("Technical", table, "Total row count", "Baseline.",
                f"SELECT COUNT(*) AS v FROM {t}", severity="INFO", check_type="info")

    # 2. FRESHNESS  (DATEADD syntax for T-SQL / Fabric)
    for table, cols in tables.items():
        if LOAD_DTS_COL not in cols:
            continue
        t = raw(table)
        add("Freshness", table,
            f"No new records in last {STALE_HOURS}h (stale pipeline)",
            "ETL pipeline likely stopped. Dashboard KPIs from this table show stale data.",
            f"SELECT CASE WHEN MAX({LOAD_DTS_COL}) < DATEADD(hour,{-STALE_HOURS},GETUTCDATE()) "
            f"THEN 1 ELSE 0 END AS v FROM {t}", severity="WARNING")
        add("Freshness", table,
            "Future LOAD_DTS detected (clock skew)",
            "Future-dated load timestamp means ETL server clock is wrong — breaks time-based queries.",
            f"SELECT COUNT(*) AS v FROM {t} "
            f"WHERE {LOAD_DTS_COL} > DATEADD(hour,1,GETUTCDATE())")
        add("Freshness", table,
            "Oldest record age (days)",
            "Shows data history depth — useful for spotting truncation or historic gaps.",
            f"SELECT DATEDIFF(day,MIN({LOAD_DTS_COL}),CAST(GETUTCDATE() AS DATE)) AS v FROM {t}",
            severity="INFO", check_type="info")

    # 3. VOLUME  (CAST to DATE for T-SQL)
    for table, cols in tables.items():
        if LOAD_DTS_COL not in cols:
            continue
        t = raw(table)
        add("Volume", table,
            "Zero records loaded today (silent drop)",
            "Table received rows yesterday but zero today — upstream feed likely failed silently.",
            f"SELECT CASE WHEN "
            f"(SELECT COUNT(*) FROM {t} WHERE CAST({LOAD_DTS_COL} AS DATE)=CAST(GETUTCDATE() AS DATE))=0 "
            f"AND (SELECT COUNT(*) FROM {t} WHERE CAST({LOAD_DTS_COL} AS DATE)=CAST(DATEADD(day,-1,GETUTCDATE()) AS DATE))>0 "
            f"THEN 1 ELSE 0 END AS v", severity="WARNING")
        add("Volume", table,
            f"Load spike today vs 7-day avg (>{VOLUME_SPIKE_MULT}x)",
            f"Load {VOLUME_SPIKE_MULT}x larger than normal suggests a duplicate or runaway ETL retry.",
            f"WITH daily AS (SELECT CAST({LOAD_DTS_COL} AS DATE) AS d, COUNT(*) AS n FROM {t} "
            f"WHERE {LOAD_DTS_COL}>=DATEADD(day,-8,GETUTCDATE()) GROUP BY CAST({LOAD_DTS_COL} AS DATE)), "
            f"avg7 AS (SELECT AVG(CAST(n AS FLOAT)) AS avg_n FROM daily WHERE d<CAST(GETUTCDATE() AS DATE)), "
            f"today AS (SELECT n AS today_n FROM daily WHERE d=CAST(GETUTCDATE() AS DATE)) "
            f"SELECT CASE WHEN (SELECT today_n FROM today)>(SELECT avg_n FROM avg7)*{VOLUME_SPIKE_MULT} "
            f"THEN 1 ELSE 0 END AS v", severity="WARNING")
        add("Volume", table, "Today's load count", "Records loaded today.",
            f"SELECT COUNT(*) AS v FROM {t} WHERE CAST({LOAD_DTS_COL} AS DATE)=CAST(GETUTCDATE() AS DATE)",
            severity="INFO", check_type="info")

    # 4. VALIDITY
    for view_name, view_checks in profile.get("validity_views", []):
        for chk in view_checks:
            add("Validity", view_name,
                chk["description"], chk["impact"],
                f"SELECT COUNT(*) AS v FROM {mart(view_name)} "
                f"WHERE {chk['column']} {chk['operator']} {chk['threshold']}",
                severity=chk.get("severity", "ERROR"))

    # 5. COVERAGE
    for view_name, kpi_label, impact, entity_col, hub_table in profile.get("coverage_views", []):
        add("Coverage", view_name,
            f"Entities in {hub_table} with NO {kpi_label} data", impact,
            f"SELECT COUNT(DISTINCT h.{entity_col}) AS v "
            f"FROM {raw(hub_table)} h "
            f"LEFT JOIN {mart(view_name)} m ON h.{entity_col}=m.{entity_col} "
            f"WHERE m.{entity_col} IS NULL", severity="WARNING")

    # 6. RECONCILIATION
    for rc in profile.get("recon_checks", []):
        mart_view = mart(rc["view"])
        hub       = raw(rc["hub"]) if rc.get("hub") else None
        mart2     = mart(rc.get("mart2", "")) if rc.get("mart2") else None
        sql_body  = rc["sql_template"].format(
            mart_view=mart_view,
            hub=hub or "",
            mart2=mart2 or "",
        )
        sql = sql_body if sql_body.strip().upper().startswith("SELECT") else f"SELECT {sql_body}"
        add("Reconciliation", rc["view"],
            rc["description"], rc["impact"],
            sql, severity=rc.get("severity", "WARNING"))

    # 7. SOURCE
    for table, cols in tables.items():
        if RECORD_SRC_COL not in cols:
            continue
        t = raw(table)
        add("Source", table,
            "Single source feeding 100% of records",
            "One source feeding everything suggests a backup/fallback took over — may be partial/lower quality.",
            f"SELECT CASE WHEN MAX(pct)>=99 THEN 1 ELSE 0 END AS v FROM ("
            f"SELECT {RECORD_SRC_COL}, COUNT(*)*100.0/SUM(COUNT(*)) OVER () AS pct "
            f"FROM {t} GROUP BY {RECORD_SRC_COL}) x", severity="WARNING")
        add("Source", table,
            "Distinct record sources count",
            "Shows all source systems feeding this table — for lineage audit.",
            f"SELECT COUNT(DISTINCT {RECORD_SRC_COL}) AS v FROM {t}",
            severity="INFO", check_type="info")

    return checks

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — RUN CHECKS
# ─────────────────────────────────────────────────────────────────────────────
def run_checks(cursor, checks):
    total   = len(checks)
    results = []
    for idx, chk in enumerate(checks, 1):
        label = f"[{idx:>3}/{total}] {chk['category']:<16} {chk['table']:<35} {chk['description'][:45]:<45}"
        print(f"  {label}", end=" ... ")
        try:
            cursor.execute(chk["sql"])
            row   = cursor.fetchone()
            value = int(row[0]) if row and row[0] is not None else 0
            if chk["check_type"] == "info":
                status = "INFO"
            elif chk["check_type"] == "count_zero":
                status = "PASS" if value == 0 else "FAIL"
            else:
                status = "PASS"
            results.append({**chk, "result": value, "status": status, "error": None})
            if status == "PASS":   print("✅ PASS")
            elif status == "INFO": print(f"ℹ  {value:,}")
            else:                  print(f"❌ FAIL  ({value:,} issues)")
        except Exception as e:
            results.append({**chk, "result": None, "status": "ERROR", "error": str(e)})
            print(f"⚠  ERROR: {str(e)[:80]}")
    return results

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — GENERATE HTML REPORT
# ─────────────────────────────────────────────────────────────────────────────
def generate_html_report(results, output_path):
    profile      = get_profile()
    display_name = profile["display_name"]

    total  = len(results)
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    errors = sum(1 for r in results if r["status"] == "ERROR")
    warns  = sum(1 for r in results if r["status"] == "WARNING")
    infos  = sum(1 for r in results if r["status"] == "INFO")
    score  = int(passed / max(total - infos, 1) * 100)
    sc     = "#16a34a" if score >= 80 else "#d97706" if score >= 60 else "#dc2626"

    CAT_COLOR = {
        "Technical":"#4f46e5","Freshness":"#0891b2","Volume":"#7c3aed",
        "Validity":"#dc2626","Coverage":"#d97706","Reconciliation":"#059669","Source":"#6b7280",
    }

    by_table = {}
    for r in results:
        by_table.setdefault(r["table"], []).append(r)

    summary_cards = ""
    for tbl, tr in sorted(by_table.items()):
        tp = sum(1 for r in tr if r["status"]=="PASS")
        tf = sum(1 for r in tr if r["status"]=="FAIL")
        te = sum(1 for r in tr if r["status"]=="ERROR")
        ti = sum(1 for r in tr if r["status"]=="INFO")
        rc = next((r["result"] for r in tr if "row count" in r["description"].lower()), "N/A")
        dot = "#dc2626" if (tf or te) else "#16a34a"
        cbg = "#fff5f5" if (tf or te) else "#f0fdf4"
        cb  = "#fca5a5" if (tf or te) else "#86efac"
        summary_cards += f"""
        <div style="background:{cbg};border:1.5px solid {cb};border-radius:12px;
                    padding:14px;min-width:185px;flex:1 1 185px;">
          <div style="display:flex;align-items:center;gap:6px;margin-bottom:6px;">
            <div style="width:8px;height:8px;border-radius:50%;background:{dot};flex-shrink:0;"></div>
            <div style="font-size:12px;font-weight:700;color:#111;overflow:hidden;
                        text-overflow:ellipsis;white-space:nowrap;">{tbl}</div>
          </div>
          <div style="font-size:11px;color:#6b7280;margin-bottom:6px;">
            Rows: <b>{f"{rc:,}" if isinstance(rc,int) else rc}</b>
          </div>
          <div style="display:flex;gap:4px;flex-wrap:wrap;">
            <span style="background:#dcfce7;color:#15803d;border-radius:20px;padding:2px 7px;font-size:10px;font-weight:700;">{tp} PASS</span>
            {"<span style='background:#fee2e2;color:#dc2626;border-radius:20px;padding:2px 7px;font-size:10px;font-weight:700;'>" + str(tf) + " FAIL</span>" if tf else ""}
            {"<span style='background:#fef3c7;color:#d97706;border-radius:20px;padding:2px 7px;font-size:10px;font-weight:700;'>" + str(te) + " ERR</span>" if te else ""}
            <span style="background:#eff6ff;color:#1d4ed8;border-radius:20px;padding:2px 7px;font-size:10px;font-weight:700;">{ti} INFO</span>
          </div>
        </div>"""

    by_cat = {}
    for r in results:
        by_cat.setdefault(r["category"], []).append(r)
    cat_rows = ""
    for cat in ["Technical","Freshness","Volume","Validity","Coverage","Reconciliation","Source"]:
        if cat not in by_cat:
            continue
        cr = by_cat[cat]
        cp = sum(1 for r in cr if r["status"]=="PASS")
        cf = sum(1 for r in cr if r["status"]=="FAIL")
        ce = sum(1 for r in cr if r["status"]=="ERROR")
        ci = sum(1 for r in cr if r["status"]=="INFO")
        ct = len(cr)
        cs = int(cp / max(ct-ci, 1) * 100)
        cc = CAT_COLOR.get(cat,"#6b7280")
        cat_rows += f"""
        <tr>
          <td style="padding:10px 16px;">
            <span style="background:{cc}22;color:{cc};border-radius:6px;padding:3px 10px;font-size:12px;font-weight:700;">{cat}</span>
          </td>
          <td style="padding:10px 16px;font-size:13px;font-weight:600;">{ct}</td>
          <td style="padding:10px 16px;">
            <span style="background:#dcfce7;color:#15803d;border-radius:20px;padding:2px 9px;font-size:11px;font-weight:700;">{cp} PASS</span>
            {"<span style='background:#fee2e2;color:#dc2626;border-radius:20px;padding:2px 9px;font-size:11px;font-weight:700;margin-left:4px;'>" + str(cf) + " FAIL</span>" if cf else ""}
            {"<span style='background:#fef3c7;color:#d97706;border-radius:20px;padding:2px 9px;font-size:11px;font-weight:700;margin-left:4px;'>" + str(ce) + " ERR</span>" if ce else ""}
          </td>
          <td style="padding:10px 16px;">
            <div style="background:#e5e7eb;border-radius:4px;height:7px;width:110px;">
              <div style="width:{cs}%;background:{'#16a34a' if cs>=80 else '#d97706' if cs>=60 else '#dc2626'};border-radius:4px;height:7px;"></div>
            </div>
            <div style="font-size:11px;color:#6b7280;margin-top:3px;">{cs}%</div>
          </td>
        </tr>"""

    fail_list = ""
    for r in results:
        if r["status"] == "FAIL":
            cc = CAT_COLOR.get(r["category"],"#6b7280")
            fail_list += f"""
            <div style="background:#fff5f5;border:1px solid #fca5a5;border-left:4px solid {cc};
                        border-radius:0 8px 8px 0;padding:14px 16px;margin-bottom:8px;">
              <div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;">
                <span style="background:{cc}22;color:{cc};border-radius:6px;padding:2px 8px;font-size:10px;font-weight:700;">{r["category"]}</span>
                <span style="font-size:13px;font-weight:700;color:#dc2626;">{r["table"]}</span>
              </div>
              <div style="font-size:13px;font-weight:600;color:#374151;margin-bottom:3px;">{r["description"]}</div>
              <div style="font-size:12px;color:#dc2626;font-weight:600;margin-bottom:4px;">{r["result"]:,} issue(s) found</div>
              <div style="font-size:11px;color:#6b7280;margin-bottom:4px;font-style:italic;">{r.get("business_impact","")}</div>
              <div style="font-size:10px;color:#9ca3af;font-family:monospace;">{r["sql"]}</div>
            </div>"""
    if not fail_list:
        fail_list = '<div style="color:#16a34a;font-weight:600;padding:16px;text-align:center;">✅ No failures — all checks passed!</div>'

    rows_html = ""
    current_cat = None
    for r in results:
        if r["category"] != current_cat:
            current_cat = r["category"]
            cc = CAT_COLOR.get(current_cat, "#6b7280")
            rows_html += f'<tr><td colspan="5" style="background:{cc};color:white;font-weight:700;font-size:12px;padding:8px 16px;letter-spacing:0.5px;">■ {current_cat.upper()}</td></tr>'
        s = r["status"]
        if s=="PASS":    badge='<span style="background:#dcfce7;color:#15803d;border-radius:20px;padding:3px 11px;font-size:11px;font-weight:700;">✓ PASS</span>'
        elif s=="FAIL":  badge=f'<span style="background:#fee2e2;color:#dc2626;border-radius:20px;padding:3px 11px;font-size:11px;font-weight:700;">✗ FAIL ({r["result"]:,})</span>'
        elif s=="ERROR": badge=f'<span style="background:#fef3c7;color:#d97706;border-radius:20px;padding:3px 11px;font-size:11px;font-weight:700;">⚠ ERROR</span>'
        elif s=="WARNING":badge=f'<span style="background:#fff7ed;color:#c2410c;border-radius:20px;padding:3px 11px;font-size:11px;font-weight:700;">⚠ WARN ({r["result"]:,})</span>'
        else:            badge=f'<span style="background:#eff6ff;color:#1d4ed8;border-radius:20px;padding:3px 11px;font-size:11px;font-weight:700;">ℹ {r["result"]:,}</span>'
        bg  = "#fff" if s in ("PASS","INFO") else ("#fff5f5" if s=="FAIL" else "#fffbeb")
        err = f'<br><small style="color:#ef4444;">{r.get("error","")}</small>' if r.get("error") else ""
        imp = r.get("business_impact","")
        rows_html += f"""
        <tr style="background:{bg};border-bottom:1px solid #f1f5f9;">
          <td style="padding:9px 13px;font-size:11px;font-weight:600;color:#374151;">{r["table"]}</td>
          <td style="padding:9px 13px;font-size:11px;color:#374151;">{r["description"]}{err}</td>
          <td style="padding:9px 13px;font-size:10px;color:#9ca3af;font-style:italic;max-width:220px;">{imp[:90]}{"…" if len(imp)>90 else ""}</td>
          <td style="padding:9px 13px;font-size:10px;color:#9ca3af;font-family:monospace;max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="{r['sql'].replace(chr(34),chr(39))}">{r["sql"][:65]}…</td>
          <td style="padding:9px 13px;">{badge}</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>{display_name} — DQ Validation Report</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:system-ui,-apple-system,sans-serif;background:#f8fafc;color:#111827}}
.page{{max-width:1440px;margin:0 auto;padding:32px 24px}}
table{{width:100%;border-collapse:collapse}}
thead th{{background:#1e293b;color:#e2e8f0;padding:11px 14px;font-size:11px;font-weight:700;
          text-transform:uppercase;letter-spacing:0.5px;text-align:left}}
tbody tr:hover td{{background:#f0f9ff!important}}
</style></head><body>
<div class="page">

<div style="background:linear-gradient(135deg,#0f172a 0%,#1e293b 50%,#0f4c81 100%);
            border-radius:18px;padding:30px 36px;margin-bottom:24px;color:white;">
  <div style="display:flex;align-items:center;gap:16px;">
    <div style="width:52px;height:52px;border-radius:14px;flex-shrink:0;
                background:linear-gradient(135deg,#4f46e5,#7c3aed);
                display:flex;align-items:center;justify-content:center;font-size:26px;">🛡️</div>
    <div>
      <div style="font-size:22px;font-weight:900;">{display_name} — Data Vault Validation Report</div>
      <div style="font-size:13px;opacity:0.65;margin-top:3px;">
        {DB_NAME}.raw_layer &amp; {FABRIC_CONFIG['mart_database']} &nbsp;·&nbsp;
        {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} &nbsp;·&nbsp;
        {total} checks · 7 categories
      </div>
    </div>
  </div>
</div>

<div style="display:grid;grid-template-columns:repeat(6,1fr);gap:14px;margin-bottom:24px;">
  <div style="background:white;border-radius:12px;padding:18px;text-align:center;border-top:3px solid {sc};box-shadow:0 1px 4px rgba(0,0,0,0.07);">
    <div style="font-size:34px;font-weight:900;color:{sc};">{score}%</div>
    <div style="font-size:11px;color:#6b7280;font-weight:600;margin-top:3px;">DQ Score</div>
  </div>
  <div style="background:#f0fdf4;border:1.5px solid #86efac;border-radius:12px;padding:18px;text-align:center;">
    <div style="font-size:34px;font-weight:900;color:#16a34a;">{passed}</div>
    <div style="font-size:11px;color:#15803d;font-weight:600;margin-top:3px;">PASSED</div>
  </div>
  <div style="background:#fff5f5;border:1.5px solid #fca5a5;border-radius:12px;padding:18px;text-align:center;">
    <div style="font-size:34px;font-weight:900;color:#dc2626;">{failed}</div>
    <div style="font-size:11px;color:#dc2626;font-weight:600;margin-top:3px;">FAILED</div>
  </div>
  <div style="background:#fffbeb;border:1.5px solid #fde68a;border-radius:12px;padding:18px;text-align:center;">
    <div style="font-size:34px;font-weight:900;color:#d97706;">{errors}</div>
    <div style="font-size:11px;color:#d97706;font-weight:600;margin-top:3px;">ERRORS</div>
  </div>
  <div style="background:#fff7ed;border:1.5px solid #fed7aa;border-radius:12px;padding:18px;text-align:center;">
    <div style="font-size:34px;font-weight:900;color:#c2410c;">{warns}</div>
    <div style="font-size:11px;color:#c2410c;font-weight:600;margin-top:3px;">WARNINGS</div>
  </div>
  <div style="background:#eff6ff;border:1.5px solid #bfdbfe;border-radius:12px;padding:18px;text-align:center;">
    <div style="font-size:34px;font-weight:900;color:#1d4ed8;">{infos}</div>
    <div style="font-size:11px;color:#1d4ed8;font-weight:600;margin-top:3px;">INFO</div>
  </div>
</div>

<div style="background:white;border-radius:12px;padding:20px;box-shadow:0 1px 4px rgba(0,0,0,0.07);margin-bottom:24px;">
  <div style="font-size:15px;font-weight:700;margin-bottom:16px;">Results by Category</div>
  <table><thead><tr><th>Category</th><th>Checks</th><th>Results</th><th>Score</th></tr></thead>
  <tbody>{cat_rows}</tbody></table>
</div>

<div style="background:white;border-radius:12px;padding:20px;box-shadow:0 1px 4px rgba(0,0,0,0.07);margin-bottom:24px;">
  <div style="font-size:15px;font-weight:700;margin-bottom:14px;">Per-Table Summary</div>
  <div style="display:flex;flex-wrap:wrap;gap:10px;">{summary_cards}</div>
</div>

<div style="background:white;border-radius:12px;padding:20px;box-shadow:0 1px 4px rgba(0,0,0,0.07);margin-bottom:24px;">
  <div style="font-size:15px;font-weight:700;color:#dc2626;margin-bottom:14px;">Issues That Need Fixing — {failed} failures</div>
  {fail_list}
</div>

<div style="background:white;border-radius:12px;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,0.07);margin-bottom:24px;">
  <div style="padding:20px 20px 12px;font-size:15px;font-weight:700;">All Check Results ({total})</div>
  <table><thead><tr><th>Table / View</th><th>Check</th><th>Business Impact</th><th>Query Preview</th><th>Result</th></tr></thead>
  <tbody>{rows_html}</tbody></table>
</div>

<div style="text-align:center;color:#9ca3af;font-size:12px;padding:16px 0;">
  {display_name} · Data Vault Validation · {DB_NAME} · {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
</div>
</div></body></html>"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n  HTML report written → {output_path}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    profile      = get_profile()
    display_name = profile["display_name"]

    print("\n" + "="*70)
    print(f"  {display_name.upper()} — DATA VAULT VALIDATION RUNNER  (7 categories)")
    print("="*70 + "\n")

    # Validate that required .env vars are present
    missing = [k for k, v in {
        "FABRIC_SQL_SERVER":    FABRIC_CONFIG["server"],
        "AZURE_TENANT_ID":      FABRIC_CONFIG["tenant_id"],
        "AZURE_CLIENT_ID":      FABRIC_CONFIG["client_id"],
        "AZURE_CLIENT_SECRET":  FABRIC_CONFIG["client_secret"],
        "FABRIC_WAREHOUSE_SERVER": FABRIC_CONFIG["mart_server"],
    }.items() if not v]
    if missing:
        raise EnvironmentError(f"Missing required .env variables: {', '.join(missing)}")

    # --- RAW LAYER connection (Warehouse / SQL Analytics endpoint) ---
    print(f"Connecting to RAW layer  → {FABRIC_CONFIG['server']} / {FABRIC_CONFIG['database']}...")
    raw_conn   = get_connection(FABRIC_CONFIG["server"],      FABRIC_CONFIG["database"])
    raw_cursor = raw_conn.cursor()
    print("  Connected.\n")

    # --- MART connection (may be a different Lakehouse endpoint) ---
    print(f"Connecting to MART layer → {FABRIC_CONFIG['mart_server']} / {FABRIC_CONFIG['mart_database']}...")
    mart_conn   = get_connection(FABRIC_CONFIG["mart_server"], FABRIC_CONFIG["mart_database"])
    mart_cursor = mart_conn.cursor()
    print("  Connected.\n")

    print("Fetching raw_layer metadata...")
    tables = fetch_metadata(raw_cursor)
    hubs  = [t for t in tables if is_hub(t)]
    links = [t for t in tables if is_link(t)]
    sats  = [t for t in tables if is_sat(t)]
    print(f"  {len(hubs)} Hubs | {len(links)} Links | {len(sats)} Satellites\n")

    print("Building checks across 7 categories...")
    checks = build_checks(tables)
    by_cat = {}
    for c in checks:
        by_cat[c["category"]] = by_cat.get(c["category"], 0) + 1
    for cat in ["Technical","Freshness","Volume","Validity","Coverage","Reconciliation","Source"]:
        if cat in by_cat:
            print(f"    {cat:<20} {by_cat[cat]:>4} checks")
    print(f"    {'─'*26}")
    print(f"    {'TOTAL':<20} {len(checks):>4} checks\n")

    # Route each check to the right cursor
    print("Running all checks...")
    total   = len(checks)
    results = []
    MART_CATS = {"Validity", "Coverage", "Reconciliation"}
    for idx, chk in enumerate(checks, 1):
        label = f"[{idx:>3}/{total}] {chk['category']:<16} {chk['table']:<35} {chk['description'][:45]:<45}"
        print(f"  {label}", end=" ... ")
        cursor = mart_cursor if chk["category"] in MART_CATS else raw_cursor
        try:
            cursor.execute(chk["sql"])
            row   = cursor.fetchone()
            value = int(row[0]) if row and row[0] is not None else 0
            status = "INFO" if chk["check_type"] == "info" else ("PASS" if value == 0 else "FAIL")
            results.append({**chk, "result": value, "status": status, "error": None})
            if status == "PASS":   print("✅ PASS")
            elif status == "INFO": print(f"ℹ  {value:,}")
            else:                  print(f"❌ FAIL  ({value:,} issues)")
        except Exception as e:
            results.append({**chk, "result": None, "status": "ERROR", "error": str(e)})
            print(f"⚠  ERROR: {str(e)[:80]}")

    raw_conn.close()
    mart_conn.close()

    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    errors = sum(1 for r in results if r["status"] == "ERROR")
    infos  = sum(1 for r in results if r["status"] == "INFO")
    score  = int(passed / max(len(results)-infos, 1) * 100)

    print(f"\n{'='*70}")
    print(f"  SCORE: {score}%   |   {passed} PASSED  {failed} FAILED  {errors} ERRORS  {infos} INFO")
    print(f"{'='*70}\n")

    if failed:
        print("  Tables with failures:")
        for ft in sorted({r["table"] for r in results if r["status"]=="FAIL"}):
            n = sum(1 for r in results if r["status"]=="FAIL" and r["table"]==ft)
            print(f"    ❌ {ft}  ({n} failure{'s' if n>1 else ''})")
        print()

    print("Generating HTML report...")
    generate_html_report(results, REPORT_FILE)
    print(f"\n✅ Done!  Open  {REPORT_FILE}  in your browser.\n")


if __name__ == "__main__":
    main()